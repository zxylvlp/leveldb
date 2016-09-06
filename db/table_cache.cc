// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

/**
 * 文件和表结构体
 */
struct TableAndFile {
  /**
   * 指向随机访问文件的指针
   */
  RandomAccessFile* file;
  /**
   * 指向表的指针
   */
  Table* table;
};

/**
 * 删除记录
 *
 * 先将值转为表和文件类型
 * 然后析构其中的表和文件
 * 最后将自己析构掉
 */
static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

/**
 * 对记录去引用
 *
 * 将参数1转为缓存
 * 将参数2转为缓存中的handle
 * 然后从缓存中释放handle
 */
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

/**
 * 构造函数
 */
TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

/**
 * 析构函数
 */
TableCache::~TableCache() {
  delete cache_;
}

/**
 * 根据文件号和文件大小找到文件
 *
 * 首先将file_number编码成key，然后根据这个key去cache_里面去查
 * 如果能查到则将handle的内容设置为相应的value并且返回
 * 如果查不到，则将数据库名和文件号拼起来形成文件名，并且打开一个这个文件名的随机访问文件，调表的Open方法，利用文件创建一个表，
 * 然后将相应的文件和表组合成文件和表结构，调cache_的插入方法插入进去，并且将handle的内容设置为刚刚插入进去的值并且返回
 *
 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

/**
 * 根据文件号创建一个迭代器
 *
 * 如果tableptr不为空，首先将tableptr的内容清空
 * 然后根据文件号找到相应的表，从表中创建一个迭代器
 * 对迭代器注册释放缓存中这张表的函数
 * 如果tableptr不为空，将tableptr的内容设置为刚刚查到的表
 * 最后返回注册的迭代器
 */
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

/**
 * 从文件号对应的表中获得键对应的值
 *
 * 首先根据文件号调用FindTable找到指定的表，
 * 在表中调用InternalGet获得相应的值
 * 用完了表之后在缓存中释放表的引用
 */
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

/**
 * 从表缓存中删除键为文件号的元素
 *
 * 首先将文件号编码成键，然后从cache_里面删除键对应的元素
 */
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
