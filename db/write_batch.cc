// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
/**
 * 头的大小
 */
static const size_t kHeader = 12;

/**
 * 构造函数
 */
WriteBatch::WriteBatch() {
  Clear();
}

/**
 * 析构函数
 */
WriteBatch::~WriteBatch() { }

/**
 * 析构函数
 */
WriteBatch::Handler::~Handler() { }

/**
 * 清空rep_，并且将大小重置为12
 */
void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

/**
 * 迭代这个批，用handler处理其中的元素的添加和删除
 *
 * 首先忽略掉头，然后遍历所有内容
 * 先找到类型，如果是值类型则获得其中的key和value，用handle添加
 * 如果是删除类型则获得其中的key，用handle删除
 * 最后比较遍历次数和实际个数如果不一致则返回数据败坏
 */
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

/**
 * 获得批的元素数量
 */
int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

/**
 * 设置批的元素数量
 */
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

/**
 * 获得批的序列号
 */
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

/**
 * 设置批的序列号
 */
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

/**
 * 添加键值对到批中
 *
 * 首先将总数量加1，然后将值类型和键值对添加进去
 */
void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

/**
 * 删除键到批中
 *
 * 首先将总数量加1，然后将删除类型和键添加进去
 */
void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

namespace {
/**
 * 内存表插入者
 */
class MemTableInserter : public WriteBatch::Handler {
 public:
  /**
   * 序列号
   */
  SequenceNumber sequence_;
  /**
   * 指向内存表的指针
   */
  MemTable* mem_;

  /**
   * 将序列号值类型键值对添加到内存表中，并且将序列号加1
   */
  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  /**
   * 将序列号删除类型键值对添加到内存表中，并且将序列号加1
   */
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

/**
 * 批量写入内存表
 *
 * 首先获得批的序列号和内存表
 * 然后调用Iterate批量插入内存表
 */
Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

/**
 * 设置批的内容
 */
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

/**
 * 追加src的内容到dst
 */
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
