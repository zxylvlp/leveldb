// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logging.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {

/**
 * 将num添加到str的尾部
 *
 * 首先创建一个buf，调用snprintf将num输出到buf中
 * 然后将buf添加到str的结尾
 */
void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

/**
 * 将value中的字符处理过不可见字符后追加到str后面
 *
 * 对于可见字符，则直接追加到str结尾
 * 否则将其以16进制形式追加到str结尾
 */
void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

/**
 * 将num转换成字符串
 *
 * 调用AppendNumberTo实现
 */
std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

/**
 * 将字符串中的不可见字符做处理后返回
 *
 * 调用AppendEscapedStringTo实现
 */
std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

/**
 * 消耗字符串中的十进制数字
 *
 * 如果slice不为空，则从slice中取出第一个字符，
 * 如果它是数字字符，则将它转换成数字并且将slice最前面的字符去掉，
 * 并且将它追加到前面提出的数字上面，如果溢出则返回false
 * 如果不是数字字符，则跳出循环
 * 最后将提取出来的值赋值给val
 * 返回提取出的数字的位数是否大于0
 */
bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      const int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

}  // namespace leveldb
