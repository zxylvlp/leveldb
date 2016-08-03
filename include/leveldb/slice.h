// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace leveldb {

/**
 * slice类
 */
class Slice {
 public:
  // Create an empty slice.
  /**
   * 创建一个空slice
   */
  Slice() : data_(""), size_(0) { }

  // Create a slice that refers to d[0,n-1].
  /**
   * 利用一块内存创建一个slice
   */
  Slice(const char* d, size_t n) : data_(d), size_(n) { }

  // Create a slice that refers to the contents of "s"
  /**
   * 利用一个string对象创建一个slice
   */
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

  // Create a slice that refers to s[0,strlen(s)-1]
  /**
   * 利用一个c字符串，创建一个slice
   */
  Slice(const char* s) : data_(s), size_(strlen(s)) { }

  // Return a pointer to the beginning of the referenced data
  /**
   * 返回引用数据的指针data_
   */
  const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  /**
   * 返回slice的大小size_
   */
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  /**
   * 判断slice是否为空
   *
   * 看size_是否为0
   */
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  /**
   * 返回数据中的第n个byte
   */
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  /**
   * 让slice指向空串(并不负责析构)
   *
   * 将data_指向空串的地址
   * 将size_置为0
   */
  void clear() { data_ = ""; size_ = 0; }

  // Drop the first "n" bytes from this slice.
  /**
   * 丢弃掉这个slice的前面n个byte
   *
   * 将data_指针加n
   * size_大小减n
   */
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  /**
   * 返回一个包含引用数据的拷贝的string对象
   */
  std::string ToString() const { return std::string(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  /**
   * 判断x是否是this的前缀
   *
   * 先判断size_是否大于x.size_，如果不是则返回false
   * 然后调用memcmp判断前缀部分是否相等，如果不是返回false
   * 否则返回true
   */
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  /**
   * 指向实际数据的指针
   */
  const char* data_;
  /**
   * 实际数据的大小
   */
  size_t size_;

  // Intentionally copyable
};

/**
 * 比较是否相等
 *
 * 首先比较大小，如果大小相等然后用memcmp逐个比较byte
 */
inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

/**
 * 比较是否不相等
 *
 * 调用==之后取非
 */
inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

/**
 * compare比较两个slice的大小
 *
 * 首先找到前缀长度，然后对前缀长度调用memcmp比较，如果相等再看那一个长
 */
inline int Slice::compare(const Slice& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_
