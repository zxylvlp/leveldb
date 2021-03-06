// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
/**
 * 层级数目，默认为7层
 */
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
/**
 * 0层压缩触发器，最大文件数为4
 */
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
/**
 * 0层写限速触发器，最大文件数是8
 */
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
/**
 * 0层停止写触发器，最大文件数是12
 */
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
/**
 * 最大内存表压缩层级，内存表压缩的时候能放入的最低层级为2
 */
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
/**
 * 在迭代的过程过读取样本之间的字节数1M
 */
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
/**
 * 值的类型增加或者删除
 */
enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
/**
 * seek需要的值类型
 */
static const ValueType kValueTypeForSeek = kTypeValue;

/**
 * 序列号类型定义
 */
typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
/**
 * 最大序列号限制
 */
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1);

/**
 * 内部键的解析结果类型
 */
struct ParsedInternalKey {
  /**
   * 用户键
   */
  Slice user_key;
  /**
   * 序列号
   */
  SequenceNumber sequence;
  /**
   * 值类型
   */
  ValueType type;

  /**
   * 构造函数
   */
  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
/**
 * 获得内部键的编码长度
 */
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
/**
 * 从内部键抽取其中的用户键部分
 */
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

/**
 * 从内部键抽取其中的值类型部分
 */
inline ValueType ExtractValueType(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<ValueType>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
/**
 * 内部键比较者
 */
class InternalKeyComparator : public Comparator {
 private:
  /**
   * 用户键比较者指针
   */
  const Comparator* user_comparator_;
 public:
  /**
   * 构造函数
   */
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { }
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;

  /**
   * 返回用户键比较者指针
   */
  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
/**
 * 内部键filter策略
 */
class InternalFilterPolicy : public FilterPolicy {
 private:
  /**
   * 用户键filter策略的指针
   */
  const FilterPolicy* const user_policy_;
 public:
  /**
   * 构造函数
   */
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) { }
  virtual const char* Name() const;
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const;
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
/**
 * 内部键
 */
class InternalKey {
 private:
  /**
   * 内部键存储空间
   */
  std::string rep_;
 public:
  /**
   * 构造函数
   */
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  /**
   * 将s解码设置到rep_中
   */
  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  /**
   * 将rep_编码
   */
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  /**
   * 返回rep_中的用户键
   */
  Slice user_key() const { return ExtractUserKey(rep_); }

  /**
   * 根据解析过的内部键p设置rep_
   */
  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  /**
   * 清空rep
   */
  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

/**
 * 内部键的比较函数
 *
 * 将内部键编码然后进行比较
 */
inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

/**
 * 解析内部键
 *
 * 将内部键中的用户键、序列号和值类型提取出来设置到结果中
 */
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
/**
 * 查找键
 */
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  /**
   * 返回内存表中的键，即用户键的长度、用户键和标记
   */
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  /**
   * 返回内部键，即用户键和标记
   */
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  /**
   * 返回用户键
   */
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  /**
   * 内存表中的键的开始位置
   */
  const char* start_;
  /**
   * 内部键的开始位置
   */
  const char* kstart_;
  /**
   * 标记后面的位置
   */
  const char* end_;
  /**
   * 临时空间
   */
  char space_[200];      // Avoid allocation for short keys

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

/**
 * 析构函数
 */
inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
