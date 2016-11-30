// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

/**
 * 表内容对象
 */
struct Table::Rep {
  /**
   * 析构函数
   *
   * 分别析构filter块读者、filter块数据和索引块
   */
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  /**
   * 选项
   */
  Options options;
  /**
   * 当前状态
   */
  Status status;
  /**
   * 指向文件的指针
   */
  RandomAccessFile* file;
  /**
   * 缓存id
   */
  uint64_t cache_id;
  /**
   * 指向filter块读者的指针
   */
  FilterBlockReader* filter;
  /**
   * filter块数据的头指针
   */
  const char* filter_data;

  /**
   * meta索引块句柄
   */
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  /**
   * 指向索引块的指针
   */
  Block* index_block;
};

/**
 * 以选项options打开大小为size的表文件file，并将打开的表指针保存到*table中
 *
 * 首先将*table置为空
 * 然后将文件最后面的footer读取并且解码
 * 根据footer中保存的索引块句柄读取并创建索引块
 * 然后创建并初始化表内容对象
 * 然后利用表内容对象创建表对象
 * 最后调用表对象的ReadMeta读取meta信息
 */
Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    delete index_block;
  }

  return s;
}

/**
 * 读取meta信息
 *
 * 首先判断filter策略是否为空，如果是则直接返回
 * 然后根据footer的meta索引块句柄读取并创建meta索引块
 * 找出meta索引块中filter块对应的元素，并且对其值调用ReadFilter读取filter块
 */
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

/**
 * 读取filter块
 *
 * 首先将filter_handle_value解码成filter块句柄
 * 然后根据此句柄读取filter块
 * 如果内存是堆分配的则将表内容对象的filter块数据头指针指向读取出来的数据
 * 最后利用读取出来的数据创建filter块读者对象
 */
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

/**
 * 析构函数
 *
 * 析构表内容对象
 */
Table::~Table() {
  delete rep_;
}

/**
 * 删除块
 *
 * 将arg转型成块指针并析构
 */
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

/**
 * 删除缓存块
 *
 * 将value转型成块指针并析构
 */
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

/**
 * 释放块
 *
 * 将arg转型成缓存指针
 * 将h转型成缓存句柄指针
 * 调用缓存的Release方法释放缓存句柄
 */
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
/**
 * 块读者方法，根据参数生成块迭代器
 *
 * 首先将arg转型为表指针
 * 并将块缓存从表内容对象的选项中取出
 * 然后将index_value解码为块句柄
 * 如果块缓存不为空则，根据表内容对象的缓存号和块句柄的偏移量作为键去快缓存中查找，如果查找到则直接使用其找到的块，
 * 否则调用ReadBlock从文件的指定位置处读取块内容并创建块对象并将其插入块缓存中
 * 如果快缓存为空则，调用ReadBlock从文件的指定位置处读取块内容并创建块对象
 * 如果当前块对象不为空，则拿到它的迭代器，如果不存在块缓存则注册清除函数为删除块函数，否则注册清除函数为释放块函数
 * 否则创建错误迭代器对象
 * 最后返回拿到的迭代器
 */
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

/**
 * 创建新的表迭代器
 *
 * 从索引块创建一个迭代器，并利用之和块读者方法共同创建并返回两层迭代器对象
 */
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

/**
 * 内部get方法
 *
 * 首先拿到索引块的迭代器，并且将其指向大于等于k的位置
 * 判断索引块迭代器是否有效，如果有效进行如下操作：
 * 获取索引块迭代器指向的值，并且将其解码为块句柄，根据块句柄和k判断其是否存在filter中，如果不存在则跳出本代码块
 * 否则利用索引块迭代器指向的值创建块迭代器，并且将其指向大于等于k的位置，如果这时块迭代器没有失效则调用saver方法并析构块迭代器
 *
 * 析构索引块迭代器
 */
Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*saver)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

/**
 * 估计key在表中的偏移量
 *
 * 首先从索引块中拿到索引块迭代器，然后将其移动到大于等于key的位置
 * 如果索引迭代器失效则返回meta索引块的偏移量
 * 将本位置的元素的值解码成块句柄，如果解码失败则返回meta索引块的偏移量
 * 否则返回块句柄的偏移量
 */
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
