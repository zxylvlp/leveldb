// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
#define STORAGE_LEVELDB_UTIL_HISTOGRAM_H_

#include <string>

namespace leveldb {

/**
 * 直方图类
 */
class Histogram {
 public:
  /**
   * 构造和析构函数
   */
  Histogram() { }
  ~Histogram() { }

  void Clear();
  void Add(double value);
  void Merge(const Histogram& other);

  std::string ToString() const;

 private:
  /**
   * 所有数据中的最小值
   */
  double min_;
  /**
   * 所有数据中的最大值
   */
  double max_;
  /**
   * 所有数据的数量
   */
  double num_;
  /**
   * 所有数据的和
   */
  double sum_;
  /**
   * 所有数据的平方和
   */
  double sum_squares_;

  /**
   * bucket的数量
   */
  enum { kNumBuckets = 154 };
  /**
   * 每一个bucket可以放置的最大值的数组
   */
  static const double kBucketLimit[kNumBuckets];
  /**
   * bucket数组
   */
  double buckets_[kNumBuckets];

  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
