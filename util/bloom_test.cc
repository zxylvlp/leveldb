// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "util/coding.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace leveldb {

static const int kVerbose = 1;

/**
 * 将i进行小端编码之后放到buffer中构建一个slice
 */
static Slice Key(int i, char* buffer) {
  EncodeFixed32(buffer, i);
  return Slice(buffer, sizeof(uint32_t));
}

/**
 * bf测试基础类
 */
class BloomTest {
 private:
  /**
   * 用于存放bf实现
   */
  const FilterPolicy* policy_;
  /**
   * 用于存放filter_内存
   */
  std::string filter_;
  /**
   * 用于存放所有的key
   */
  std::vector<std::string> keys_;

 public:
  /**
   * 构造函数
   *
   * 创建一个bf
   */
  BloomTest() : policy_(NewBloomFilterPolicy(10)) { }

  /**
   * 析构函数
   *
   * 析构bf
   */
  ~BloomTest() {
    delete policy_;
  }

  /**
   * 清空keys和filter_
   */
  void Reset() {
    keys_.clear();
    filter_.clear();
  }

  /**
   * 将s添加到keys_中
   */
  void Add(const Slice& s) {
    keys_.push_back(s.ToString());
  }

  /**
   * 构建filter_的内容
   *
   * 将keys_的指针放到key_slices中，
   * 清空filter_的内存
   * 调用CreateFilter构建filter_的内容
   * 清空keys_
   * 判断kVerbose决定是否dumpfilter
   */
  void Build() {
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_.size(); i++) {
      key_slices.push_back(Slice(keys_[i]));
    }
    filter_.clear();
    policy_->CreateFilter(&key_slices[0], static_cast<int>(key_slices.size()),
                          &filter_);
    keys_.clear();
    if (kVerbose >= 2) DumpFilter();
  }

  /**
   * 返回filter_的大小
   */
  size_t FilterSize() const {
    return filter_.size();
  }

  /**
   * 将filter_的内容dump出来
   */
  void DumpFilter() {
    fprintf(stderr, "F(");
    for (size_t i = 0; i+1 < filter_.size(); i++) {
      const unsigned int c = static_cast<unsigned int>(filter_[i]);
      for (int j = 0; j < 8; j++) {
        fprintf(stderr, "%c", (c & (1 <<j)) ? '1' : '.');
      }
    }
    fprintf(stderr, ")\n");
  }

  /**
   * 查看s是否则filter_中
   *
   * 首先调Build构建filter_
   * 然后调KeyMayMatch判断是否在filter_中
   */
  bool Matches(const Slice& s) {
    if (!keys_.empty()) {
      Build();
    }
    return policy_->KeyMayMatch(s, filter_);
  }

  /**
   * 错误的返回true的rate
   */
  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }
};

/**
 * 测试空filter_中是否存在一些值
 */
TEST(BloomTest, EmptyFilter) {
  ASSERT_TRUE(! Matches("hello"));
  ASSERT_TRUE(! Matches("world"));
}

/**
 * 测试小filter_
 */
TEST(BloomTest, Small) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(! Matches("x"));
  ASSERT_TRUE(! Matches("foo"));
}

/**
 * 根据当前长度得到下一个长度
 */
static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

/**
 * 测试变长filter_
 */
TEST(BloomTest, VaryingLengths) {
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  /**
   * 好坏filter_的数量
   */
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    /**
     * 清空keys
     */
    Reset();
    /**
     * 添加keys
     */
    for (int i = 0; i < length; i++) {
      Add(Key(i, buffer));
    }
    /**
     * 构建filter_
     */
    Build();

    /**
     * 长度断言
     */
    ASSERT_LE(FilterSize(), static_cast<size_t>((length * 10 / 8) + 40))
        << length;

    // All added keys must match
    /**
     * 添加的Key都必须能匹配
     */
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    /**
     * 测试错误返回true的概率
     */
    double rate = FalsePositiveRate();
    /**
     * 如果kVerbose则输出调试信息
     */
    if (kVerbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
              rate*100.0, length, static_cast<int>(FilterSize()));
    }
    /**
     * 错误返回true的概率的断言和好坏分类
     */
    ASSERT_LE(rate, 0.02);   // Must not be over 2%
    if (rate > 0.0125) mediocre_filters++;  // Allowed, but not too often
    else good_filters++;
  }

  /**
   * 输出好的坏的数量
   */
  if (kVerbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n",
            good_filters, mediocre_filters);
  }
  /**
   * 好坏比例
   */
  ASSERT_LE(mediocre_filters, good_filters/5);
}

// Different bits-per-byte

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
