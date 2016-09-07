// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

/**
 * 根据迭代器的内容构建一个表文件
 *
 * 首先将meta中的文件大小初始化为0
 * 然后将迭代器调整到最前面
 * 然后根据meta中的文件号创建一个表文件名
 * 如果迭代器有效则进行下面的操作
 * 首先创建一个可写文件，然后利用文件构建一个表构造者对象
 * 遍历迭代器的所有元素，将他们添加到构造者中，然后给meta设置最大和最小键范围
 * 如果刚刚没有出错则调用构造者的Finish并且给meta设置文件大小，否则调用构造者的Abandon，然后析构构造者
 * 如果前面没有出错则同步和关闭可写文件并且析构文件
 *
 * 如果迭代器状态不正常则将返回状态设置为迭代器的状态
 * 如果返回状态不正常或者文件大小为0则将文件删除
 */
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
