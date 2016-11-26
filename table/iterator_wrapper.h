// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
/**
 * 内部迭代器包裹器
 */
class IteratorWrapper {
 public:
  /**
   * 构造函数
   */
  IteratorWrapper(): iter_(NULL), valid_(false) { }
  /**
   * 构造函数
   */
  explicit IteratorWrapper(Iterator* iter): iter_(NULL) {
    Set(iter);
  }
  /**
   * 析构函数
   */
  ~IteratorWrapper() { delete iter_; }
  /**
   * 返回包裹的迭代器
   */
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  /**
   * 设置包裹的迭代器
   *
   * 首先析构前面包裹过的迭代器，然后iter包裹进来
   * 并且查看当前包裹的迭代器是否为空，如果是则将当前包裹器设置成无效
   * 如果不是则调用Update函数
   */
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == NULL) {
      valid_ = false;
    } else {
      Update();
    }
  }


  // Iterator interface methods
  /**
   * 返回当前包裹器是否有效
   *
   * 返回valid_
   */
  bool Valid() const        { return valid_; }
  /**
   * 返回当前包裹器的键
   *
   * 返回key_
   */
  Slice key() const         { assert(Valid()); return key_; }
  /**
   * 返回当前包裹器的值
   *
   * 返回当前包裹的迭代器的值
   */
  Slice value() const       { assert(Valid()); return iter_->value(); }
  // Methods below require iter() != NULL
  /**
   * 返回当前包裹器的状态
   *
   * 返回当前包裹的迭代器的状态
   */
  Status status() const     { assert(iter_); return iter_->status(); }
  /**
   * 将包裹器向后移动
   *
   * 将包裹的迭代器向后移动，然后调用Update
   */
  void Next()               { assert(iter_); iter_->Next();        Update(); }
  /**
   * 将包裹器向前移动
   *
   * 将包裹的迭代器向前移动，然后调用Update
   */
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  /**
   * 将包裹器移动到第k位
   *
   * 将包裹的迭代器移动到第k位，然后调用Update
   */
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  /**
   * 将包裹器移动到最前面
   *
   * 将包裹的迭代器移动到最前面，然后调用Update
   */
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  /**
   * 将包裹器移动到最后面
   *
   * 将包裹的迭代器移动到最后面，然后调用Update
   */
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }

 private:
  /**
   * 更新函数
   *
   * 根据当前包裹的迭代器的有效和键更新自己的有效和键
   */
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }

  /**
   * 包裹的迭代器
   */
  Iterator* iter_;
  /**
   * 是否有效
   */
  bool valid_;
  /**
   * 键
   */
  Slice key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
