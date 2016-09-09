// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

/**
 * 获取从data开始的长度前缀字符串
 */
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

/**
 * 构造函数
 */
MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

/**
 * 析构函数
 */
MemTable::~MemTable() {
  assert(refs_ == 0);
}

/**
 * 返回内存使用的估计
 */
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

/**
 * 利用内部key比较者对长度前缀字符串比较大小
 */
int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
/**
 * 编码内部键target，返回编码结果
 *
 * 首先清空scratch，然后将target的长度编码进去，然后将target的内容添加进去，最后返回scratch的头指针
 */
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

/**
 * 内存表迭代器
 */
class MemTableIterator: public Iterator {
 public:
  /**
   * 构造函数
   */
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  /**
   * 返回迭代器是否有效
   */
  virtual bool Valid() const { return iter_.Valid(); }
  /**
   * 将迭代器跳到k处
   */
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  /**
   * 将迭代器跳到开头
   */
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  /**
   * 将迭代器跳到结尾
   */
  virtual void SeekToLast() { iter_.SeekToLast(); }
  /**
   * 将迭代器向右边移动一次
   */
  virtual void Next() { iter_.Next(); }
  /**
   * 将迭代器向左边移动一次
   */
  virtual void Prev() { iter_.Prev(); }
  /**
   * 返回迭代器当前指向的键
   */
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  /**
   * 返回迭代器当前指向的值
   */
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  /**
   * 返回迭代器的状态
   */
  virtual Status status() const { return Status::OK(); }

 private:
  /**
   * 内存表中跳表的迭代器类型的对象
   */
  MemTable::Table::Iterator iter_;
  /**
   * 编码键时的临时内存空间
   */
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

/**
 * 创建一个内存表迭代器
 */
Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

/**
 * 添加数据到内存表中
 *
 * 将键值对、类型和序列号编码好之后，插入到内存表里面的跳表中
 */
void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf);
}

/**
 * 根据键在内存表中获取对应的值
 *
 * 从lookup键中拿到mem键
 * 创建一个跳表的迭代器
 * 将迭代器seek到mem键的位置
 * 如果当前迭代器无效，表示没有相应的键，返回假
 * 否则从迭代器中获得对应的键，解码获得键的长度，并且比较其中的用户键是否等于目标用户键，如果相等则获取其中的类型
 * 如果是值类型则将值解码，并且拷贝到value并且返回真，否则将状态置为找不到并且返回真
 */
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
