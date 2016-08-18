// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

struct Options;

/**
 * 块构建者
 */
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  /**
   * 判断是否还没有元素添加进来
   */
  bool empty() const {
    return buffer_.empty();
  }

 private:
  /**
   * 选项
   */
  const Options*        options_;
  /**
   * 输出缓冲
   */
  std::string           buffer_;      // Destination buffer
  /**
   * 重启点数组
   */
  std::vector<uint32_t> restarts_;    // Restart points
  /**
   * 从上一个重启点开始到现在经过了多少个元素
   */
  int                   counter_;     // Number of entries emitted since restart
  /**
   * Finish方法是否被调用过
   */
  bool                  finished_;    // Has Finish() been called?
  /**
   * 到现在添加过的最新的key
   */
  std::string           last_key_;

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
