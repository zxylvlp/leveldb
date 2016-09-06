// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
/**
 * 快照实现类
 */
class SnapshotImpl : public Snapshot {
 public:
  /**
   * 快照的序列号，也就是mvcc的版本号
   */
  SequenceNumber number_;  // const after creation

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
  /**
   * 指向上一个快照的指针
   */
  SnapshotImpl* prev_;
  /**
   * 指向瞎一个快照的指针
   */
  SnapshotImpl* next_;

  /**
   * 指向快照列表的指针
   */
  SnapshotList* list_;                 // just for sanity checks
};

/**
 * 快照链表
 */
class SnapshotList {
 public:
  /**
   * 构造函数
   */
  SnapshotList() {
    list_.prev_ = &list_;
    list_.next_ = &list_;
  }

  /**
   * 判断链表是否为空
   */
  bool empty() const { return list_.next_ == &list_; }
  /**
   * 返回最旧的快照
   */
  SnapshotImpl* oldest() const { assert(!empty()); return list_.next_; }
  /**
   * 返回最新的快照
   */
  SnapshotImpl* newest() const { assert(!empty()); return list_.prev_; }

  /**
   * 根据序列号创建一个快照，并且插入到链表的尾端
   */
  const SnapshotImpl* New(SequenceNumber seq) {
    SnapshotImpl* s = new SnapshotImpl;
    s->number_ = seq;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    return s;
  }

  /**
   * 从链表中删除快照s
   */
  void Delete(const SnapshotImpl* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    delete s;
  }

 private:
  // Dummy head of doubly-linked list of snapshots
  /**
   * 链表的dummy头结点
   */
  SnapshotImpl list_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_
