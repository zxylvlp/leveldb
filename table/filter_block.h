// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
/**
 * filter块构建者
 */
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  /**
   * filter的策略
   */
  const FilterPolicy* policy_;
  /**
   * 多个key的平坦化内容
   */
  std::string keys_;              // Flattened key contents
  /**
   * 每一个key在keys_中的开始索引
   */
  std::vector<size_t> start_;     // Starting index in keys_ of each key
  /**
   * 到目前为止输出的filter
   */
  std::string result_;            // Filter data computed so far
  /**
   * 临时key的数组
   */
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument
  /**
   * 多个filter的偏移量数组
   */
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

/**
 * filter块阅读者
 */
class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  /**
   * filter的策略
   */
  const FilterPolicy* policy_;
  /**
   * 数据的开始地址
   */
  const char* data_;    // Pointer to filter data (at block-start)
  /**
   * offset数组的开始地址
   */
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  /**
   * offset数组的大小，也就是filter的数量
   */
  size_t num_;          // Number of entries in offset array
  /**
   * 2^base_lg_就是一个filter的大小
   */
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
