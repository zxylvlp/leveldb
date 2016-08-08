// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {
/**
 * 析构函数
 */
Comparator::~Comparator() { }

namespace {
/**
 * bytewise比较者实现类
 */
class BytewiseComparatorImpl : public Comparator {
 public:
  /**
   * 构造函数
   */
  BytewiseComparatorImpl() { }

  /**
   * 返回类的名字
   */
  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  /**
   * 比较Slice a和b的大小
   *
   * 通过slice的内置函数实现
   */
  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  /**
   * 找到[start, limit)最短的分隔符
   *
   * 首先找到start和limit两个串中的最小长度
   * 然后将从第0位开始找start在limit中的前缀长度
   * 如果前缀长度等于最小长度说明完全相等则直接返回
   * 然后找到start中第一个不一样的byte，
   * 如果它小于0xff并且加1之后小于limit中的这个byte则将start中这个byte加1后，将start的长度置为包含最后这个byte之后的长度
   * 否则直接返回
   */
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  /**
   * 找到key的最短后继
   *
   * 从第0位开始，找到第一个不为0xff的byte，将其加1之后将长度置为包含这个byte之后的长度
   */
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

/**
 * once的初始化
 */
static port::OnceType once = LEVELDB_ONCE_INIT;
/**
 * 用于存放比较者
 */
static const Comparator* bytewise;
/**
 * 比较者的初始化
 */
static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}
/**
 * 获得一个Bytewise比较者
 */
const Comparator* BytewiseComparator() {
  /**
   * 单例模式
   */
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
