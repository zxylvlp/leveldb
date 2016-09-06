// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

/**
 * 文件元信息
 */
struct FileMetaData {
  /**
   * 引用数
   */
  int refs;
  /**
   * 压缩前允许seek次数
   */
  int allowed_seeks;          // Seeks allowed until compaction
  /**
   * 文件号
   */
  uint64_t number;
  /**
   * 文件大小
   */
  uint64_t file_size;         // File size in bytes
  /**
   * 最小键
   */
  InternalKey smallest;       // Smallest internal key served by table
  /**
   * 最大键
   */
  InternalKey largest;        // Largest internal key served by table

  /**
   * 构造函数
   */
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

/**
 * 版本编辑类
 */
class VersionEdit {
 public:
  /**
   * 构造函数
   */
  VersionEdit() { Clear(); }
  /**
   * 析构函数
   */
  ~VersionEdit() { }

  void Clear();

  /**
   * 设置比较者名字
   */
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }

  /**
   * 设置日志号
   */
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }

  /**
   * 设置前一个日志号
   */
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }

  /**
   * 设置下一个文件号
   */
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }

  /**
   * 设置最近的序列号
   */
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }

  /**
   * 添加压缩指针
   */
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  /**
   * 添加文件到新文件列表中
   */
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  /**
   * 将文件插入删除文件集合中
   */
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  /**
   * 删除文件集合
   */
  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  /**
   * 比较者名字
   */
  std::string comparator_;
  /**
   * 日志号
   */
  uint64_t log_number_;
  /**
   * 前一个日志号
   */
  uint64_t prev_log_number_;
  /**
   * 下一个文件号
   */
  uint64_t next_file_number_;
  /**
   * 最近序列号
   */
  SequenceNumber last_sequence_;
  /**
   * 是否有压缩者
   */
  bool has_comparator_;
  /**
   * 是否有日志号
   */
  bool has_log_number_;
  /**
   * 是否有上一个日志号
   */
  bool has_prev_log_number_;
  /**
   * 是否有下一个文件号
   */
  bool has_next_file_number_;
  /**
   * 是否有最近序列号
   */
  bool has_last_sequence_;

  /**
   * 压缩指针列表
   */
  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  /**
   * 删除文件集合
   */
  DeletedFileSet deleted_files_;
  /**
   * 新文件列表
   */
  std::vector< std::pair<int, FileMetaData> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
