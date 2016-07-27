// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

#include "util/random.h"
#include "util/testharness.h"

namespace leveldb {
// 测试基类
class ArenaTest { };

// 空case
TEST(ArenaTest, Empty) {
  Arena arena;
}

// 简单case
TEST(ArenaTest, Simple) {
  // 所有分配过的size, ptr对
  std::vector<std::pair<size_t, char*> > allocated;
  Arena arena;
  // 分配总数
  const int N = 100000;
  // 总共分配了多少字节（需要的，不是分配的）
  size_t bytes = 0;
  Random rnd(301);
  // 分配过程
  for (int i = 0; i < N; i++) {
    // 这次要分配多大
    size_t s;
    if (i % (N / 10) == 0) {
      s = i; // 这个会走10次 0 10000 20000 ... 90000
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
      //s 的期望是 1/4000 * 3000 + 3999/4000 * 1/10 * 50 + 3999/4000 * 9/10 * 10 约等于15
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    if (rnd.OneIn(10)) {
      r = arena.AllocateAligned(s);
    } else {
      r = arena.Allocate(s);
    }

    for (size_t b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      // 对分配的每个字节放上第几次分配%256
      r[b] = i % 256;
    }
    bytes += s;
    allocated.push_back(std::make_pair(s, r));
    ASSERT_GE(arena.MemoryUsage(), bytes);
    if (i > N/10) {
      ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
    }
  }
  // 校验过程
  for (size_t i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;
    const char* p = allocated[i].second;
    for (size_t b = 0; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, i % 256);
    }
  }
}

}  // namespace leveldb

int main(int argc, char** argv) {
  // 运行上面的两个空和简单case
  return leveldb::test::RunAllTests();
}
