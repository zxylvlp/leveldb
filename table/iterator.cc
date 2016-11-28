// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"

namespace leveldb {

/**
 * 构造函数
 *
 * 将清除对象置空
 */
Iterator::Iterator() {
  cleanup_.function = NULL;
  cleanup_.next = NULL;
}

/**
 * 析构函数
 *
 * 调用清除对象链上面的所有清除函数
 * 同时析构那个清除对象
 */
Iterator::~Iterator() {
  if (cleanup_.function != NULL) {
    (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2);
    for (Cleanup* c = cleanup_.next; c != NULL; ) {
      (*c->function)(c->arg1, c->arg2);
      Cleanup* next = c->next;
      delete c;
      c = next;
    }
  }
}

/**
 * 注册清除对象
 *
 * 创建一个新对象并设置好清除函数和参数后添加到清除对象链的头的下一个上
 */
void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != NULL);
  Cleanup* c;
  if (cleanup_.function == NULL) {
    c = &cleanup_;
  } else {
    c = new Cleanup;
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

namespace {
/**
 * 空迭代器
 */
class EmptyIterator : public Iterator {
 public:
  /**
   * 构造函数
   */
  EmptyIterator(const Status& s) : status_(s) { }
  /**
   * 返回无效
   */
  virtual bool Valid() const { return false; }
  /**
   * 因为是空的所以下面几个造作都不做任何事
   */
  virtual void Seek(const Slice& target) { }
  virtual void SeekToFirst() { }
  virtual void SeekToLast() { }
  virtual void Next() { assert(false); }
  virtual void Prev() { assert(false); }
  /**
   * 返回一个空的键
   */
  Slice key() const { assert(false); return Slice(); }
  /**
   * 返回一个空的值
   */
  Slice value() const { assert(false); return Slice(); }
  /**
   * 返回状态信息
   */
  virtual Status status() const { return status_; }
 private:
  /**
   * 状态信息
   */
  Status status_;
};
}  // namespace

/**
 * 创建空迭代器
 */
Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

/**
 * 创建带错误信息的空迭代器
 */
Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

}  // namespace leveldb
