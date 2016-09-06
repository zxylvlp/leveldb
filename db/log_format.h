// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

/**
 * 记录类型
 */
enum RecordType {
  // Zero is reserved for preallocated files
  /**
   * 0类型
   */
  kZeroType = 0,

  /**
   * 全类型
   */
  kFullType = 1,

  // For fragments
  /**
   * 开头类型
   */
  kFirstType = 2,
  /**
   * 中间类型
   */
  kMiddleType = 3,
  /**
   * 结尾类型
   */
  kLastType = 4
};

/**
 * 最大记录类型
 */
static const int kMaxRecordType = kLastType;

/**
 * 块大小32k
 */
static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
/**
 * 头部的大小
 */
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
