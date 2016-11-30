// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

/**
 * 表构建者内容类
 */
struct TableBuilder::Rep {
  /**
   * 选项
   */
  Options options;
  /**
   * 索引块选项
   */
  Options index_block_options;
  /**
   * 指向文件的指针
   */
  WritableFile* file;
  /**
   * 偏移量
   */
  uint64_t offset;
  /**
   * 状态
   */
  Status status;
  /**
   * 数据块构建者
   */
  BlockBuilder data_block;
  /**
   * 索引块构建者
   */
  BlockBuilder index_block;
  /**
   * 最近一个键
   */
  std::string last_key;
  /**
   * 元素数
   */
  int64_t num_entries;
  /**
   * 是否完成构建
   */
  bool closed;          // Either Finish() or Abandon() has been called.
  /**
   * filter块构建者
   */
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  /**
   * 索引项准备好
   */
  bool pending_index_entry;
  /**
   * 准备添加到索引块的句柄
   */
  BlockHandle pending_handle;  // Handle to add to index block

  /**
   * 压缩过的输出
   */
  std::string compressed_output;

  /**
   * 构造函数
   *
   * 将索引块选项的块重启间隔设置为1
   */
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

/**
 * 构造函数
 *
 * 如果表构建者内容对象的filter块构建者不为空则调用其StartBlock方法在起点处开始新块
 */
TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

/**
 * 析构函数
 *
 * 将filter块构建者和表构建者内容对象析构
 */
TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

/**
 * 改变选项
 *
 * 如果改变了比较者对象则返回出错
 * 否则将选项赋予表构建者内容对象的选项和索引块选项，并将索引块选项的块重启间隔设置为1
 */
Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/**
 * 添加kv对
 *
 * 首先判断是否应该添加索引项，如果是则找到上一个键和这个键的最短分隔符，并将这个最短分隔符和准备添加到索引块的句柄添加到索引块中并将应该添加到索引项设置为假
 * 然后判断是否讯在filter块构建者，如果存在则将键添加进去
 * 然后将当前键拷贝到上一个键，将元素数加1，并将键值对添加到数据块构建者中
 * 然后获取数据块构建者的当前大小估计，如果超过了块应有大小则调用Flush方法将当前块构建者内容写入磁盘
 */
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

/**
 * 将当前数据块构建者中的内容写入磁盘
 *
 * 首先判断当前状态是否不正常或者数据块构建者为空，如果是则直接返回
 * 否则调用WriteBlock将数据库构建者的内容写入磁盘并传回准备添加到索引块的句柄
 * 然后将索引项准备好设置为真，并对表文件做flush操作
 * 最后判断filter块构建者是否存在，如果存在则调用其StartBlock在新的偏移量处开始块
 */
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
}

/**
 * 写块
 *
 * 首先调用块构建者的finish方法完成构建得到其字节表示
 * 然后根据选项得到压缩类型，如果是压缩并且压缩比较高时则将块内容设置为压缩后的字节表示类型设为压缩，否则将块内容设置为原始字节表示类型设为不压缩
 * 调用WriteRawBlock将快内容表示写入磁盘，并传回其句柄
 * 清空压缩输出
 * 重置块构建者
 */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

/**
 * 写raw块
 *
 * 将handle的偏移量设置为表构建者的偏移量，将handle的大小设置为block_contents的大小
 * 然后在表文件末尾追加block_contents
 * 并且创建一个字符数组里面存放类型和crc并将其追加到表文件末尾
 * 最后将表构建者的偏移量加上追加到文件的长度
 */
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

/**
 * 获得状态
 *
 * 返回表构建者内容对象的状态
 */
Status TableBuilder::status() const {
  return rep_->status;
}

/**
 * 完成构建
 *
 * 首先调用Flush方法将当前数据块构建者的内容写入磁盘
 * 然后将构建完标志设置为真
 * 然后判断filter块构建者是否存在，如果存在则调用其Finish方法完成构建并调用WriteRawBlock函数无压缩写入磁盘
 * 然后创建一个meta块构建者，判断filter块构建者是否存在，如果存在则将其key和句柄添加到meta块构建者中，并将其调用WriteBlock写入磁盘
 * 如果表构建者的索引项准备好则找到最近的键的最短后继
 * 将最短后继和准备好的索引项句柄添加到索引块中
 * 并且将索引项准备好设置为假，最后调用WriteBlock将索引块写入磁盘
 * 将meta索引块的句柄和索引块的句柄设置到footer中，然后将其编码并且追加到文件中
 * 最后将表构建者的偏移量加上footer编码后的大小
 */
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

/**
 * 放弃构建
 *
 * 将构建完毕设置为真
 */
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

/**
 * 返回元素数
 */
uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

/**
 * 获得文件大小
 *
 * 返回表构建者内容对象的偏移量
 */
uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
