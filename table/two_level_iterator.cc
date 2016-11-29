// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

/**
 * 块函数类型定义
 */
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

/**
 * 两层迭代器类
 */
class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  /**
   * 迭代器是否有效
   *
   * 返回数据迭代器是否有效
   */
  virtual bool Valid() const {
    return data_iter_.Valid();
  }

  /**
   * 获取当前键
   *
   * 返回数据迭代器指向的元素的键
   */
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  /**
   * 获取当前值
   *
   * 返回数据迭代器指向的元素的值
   */
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  /**
   * 获取当前状态
   *
   * 如果索引迭代器状态不正常则返回其不正常状态
   * 如果数据迭代器状态不正常则返回其不正常状态
   * 否则返回本对象状态
   */
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  /**
   * 保存错误
   *
   * 如果当前状态正常，并且传入了不正常状态，则将当前状态置为这个不正常状态
   */
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  /**
   * 块函数
   */
  BlockFunction block_function_;
  /**
   * 参数指针
   */
  void* arg_;
  /**
   * 读选项
   */
  const ReadOptions options_;
  /**
   * 当前状态
   */
  Status status_;
  /**
   * 索引迭代器
   */
  IteratorWrapper index_iter_;
  /**
   * 数据迭代器
   */
  IteratorWrapper data_iter_; // May be NULL
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  /**
   * 数据块句柄，即创建数据迭代器时索引迭代器的值
   */
  std::string data_block_handle_;
};

/**
 * 构造函数
 */
TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL) {
}

/**
 * 析构函数
 */
TwoLevelIterator::~TwoLevelIterator() {
}

/**
 * 将迭代器指向大于等于target的最小元素
 *
 * 首先将索引迭代器指向大于等于target的最小元素
 * 然后调用InitDataBlock初始化数据块
 * 如果数据迭代器不为空，则将其指向大于等于target的最小元素
 * 最后调用SkipEmptyDataBlocksForward向前跳过空数据块
 */
void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

/**
 * 设置数据迭代器
 *
 * 首先将当前数据迭代器的错误信息保存，然后将data_iter设置为当前数据迭代器
 */
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

/**
 * 初始化数据块
 *
 * 首先判断索引迭代器是否有效，如果无效则将数据迭代器置为空并返回
 * 否则先将索引迭代器指向的值取出来
 * 然后判断是否data_iter_不为空且data_block_handle_和索引迭代器指向的值一样
 * 如果一样则直接返回
 * 否则调用块函数创建一个数据迭代器，并且将当前索引迭代器指向的值放到data_block_handle_中，并且调用SetDataIterator设置数据迭代器
 */
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

/**
 * 创建两层迭代器
 */
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
