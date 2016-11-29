// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
/**
 * 合并迭代器类
 */
class MergingIterator : public Iterator {
 public:
  /**
   * 构造函数
   *
   * 初始化成员并且将children_用children迭代器指针数组填满
   */
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(NULL),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  /**
   * 析构函数
   *
   * 析构children_数组
   */
  virtual ~MergingIterator() {
    delete[] children_;
  }

  /**
   * 判断是否有效
   *
   * 如果当前指向的迭代器包裹器不为空则有效
   */
  virtual bool Valid() const {
    return (current_ != NULL);
  }

  /**
   * 将迭代器指向第一个元素
   *
   * 首先将所有孩子迭代器都指向其第一个元素
   * 然后调用FindSmallest找到其中最小的元素
   * 最后将方向置为向前
   */
  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  /**
   * 将迭代器指向最后一个元素
   *
   * 首先将所有孩子迭代器都指向其最后一个元素
   * 然后调用FindLargest找到其中最大的元素
   * 最后将方向置为向后
   */
  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  /**
   * 将迭代器指向大于等于target的第一个元素
   *
   * 首先将所有孩子迭代器都指向大于等于target的第一个元素
   * 然后调用FindSmallest找到其中最小的元素
   * 最后将方向置为向前
   */
  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  /**
   * 将迭代器指向下一个元素
   *
   * 首先判断当前方向是不是向后，如果是则进行以下操作：
   * 将所有的非当前孩子迭代器移动到大于等于当前key的位置处，并且判断其是否等于key，如果是则将其前进一个元素
   * 将当前方向改为向前
   *
   * 最后对当前孩子迭代器前进一个元素，并调用FindSmallest找到其中最小的元素
   */
  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  /**
   * 将迭代器指向上一个元素
   *
   * 首先判断当前方向是不是向前，如果是则进行以下操作：
   * 将所有的非当前孩子迭代器移动到大于等于当前key的位置处，如果这个孩子迭代器有效则将其后退一个元素，无效则将其指向最后一个元素
   * 将当前方向改为向后
   *
   * 最后对当前孩子迭代器后退一个元素，并调用FindLargest找到其中最大的元素
   */
  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  /**
   * 获得当前的键
   *
   * 返回当前孩子迭代器的键
   */
  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  /**
   * 获得当前的值
   *
   * 返回当前孩子迭代器的值
   */
  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  /**
   * 获得当前状态
   *
   * 遍历每一个孩子迭代器，找出并返回其中的错误状态
   * 如果找不到则返回正常状态
   */
  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  /**
   * 指向比较者对象的指针
   */
  const Comparator* comparator_;
  /**
   * 孩子迭代器数组的头指针
   */
  IteratorWrapper* children_;
  /**
   * 孩子迭代器数组的大小
   */
  int n_;
  /**
   * 指向当前孩子迭代器的指针
   */
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  /**
   * 移动方向类型定义
   */
  enum Direction {
    kForward,
    kReverse
  };
  /**
   * 当前移动方向
   */
  Direction direction_;
};

/**
 * 使当前迭代器为指向的元素最小的孩子迭代器
 *
 * 从头到尾循环孩子迭代器数组，找出指向的元素最小的孩子迭代器并且将其作为当前迭代器
 */
void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = NULL;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == NULL) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

/**
 * 使当前迭代器为指向的元素最大的孩子迭代器
 *
 * 从尾到头循环孩子迭代器数组，找出指向的元素最大的孩子迭代器并且将其作为当前迭代器
 */
void MergingIterator::FindLargest() {
  IteratorWrapper* largest = NULL;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == NULL) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

/**
 * 创建合并迭代器
 *
 * 首先判断列表的大小
 * 如果列表的大小是0则创建并返回一个空迭代器
 * 如果列表的大小是1则返回列表中第一个迭代器
 * 否则利用列表创建并返回一个合并迭代器
 */
Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb
