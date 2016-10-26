// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
/**
 * dump内部键迭代器
 *
 * 迭代内部键迭代器，对其中的键解析，并且调用其DebugString函数进行dump
 */
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
/**
 * 数据库迭代器类
 */
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  /**
   * 当前迭代方向的枚举类型
   */
  enum Direction {
    kForward,
    kReverse
  };

  /**
   * 构造函数
   */
  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_counter_(RandomPeriod()) {
  }
  /**
   * 析构函数
   *
   * 析构传递进来的迭代器
   */
  virtual ~DBIter() {
    delete iter_;
  }
  /**
   * 判断当前迭代器是否有效
   */
  virtual bool Valid() const { return valid_; }
  /**
   * 获取当前迭代器指向的键
   *
   * 如果方向是向前则提取当前内部键迭代器指向的键
   * 否则返回saved_key
   */
  virtual Slice key() const {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  /**
   * 获取当且迭代器指向的值
   *
   * 如果方向是向前则提取当前内部键迭代器指向的值
   * 否则返回saved_value
   */
  virtual Slice value() const {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  /**
   * 返回当前迭代器的状态
   *
   * 如果自己的状态或者内部键迭代器的状态有不正常的则返回这个不正常的状态
   * 否则返回正常状态
   */
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  /**
   * 将键k保存到dst的内存当中
   */
  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  /**
   * 将saved_value清空
   *
   * 如果当前saveed_value使用的内存过多，则将其析构，重新创建一个空对象
   * 否则对其clear
   */
  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  /**
   * 返回以1M大小为平均值的随机周期
   */
  ssize_t RandomPeriod() {
    return rnd_.Uniform(2*config::kReadBytesPeriod);
  }

  /**
   * 数据库实现对象的指针
   */
  DBImpl* db_;
  /**
   * 用户键比较者的指针
   */
  const Comparator* const user_comparator_;
  /**
   * 内部键迭代器的指针
   */
  Iterator* const iter_;
  /**
   * 当前迭代器的序列号
   */
  SequenceNumber const sequence_;

  /**
   * 当前迭代器的状态
   */
  Status status_;
  /**
   * 当迭代器方向是向左时的当前键
   */
  std::string saved_key_;     // == current key when direction_==kReverse
  /**
   * 当迭代器方向是向左时的当前值
   */
  std::string saved_value_;   // == current raw value when direction_==kReverse
  /**
   * 当前迭代器的方向
   */
  Direction direction_;
  /**
   * 当前迭代器是否有效
   */
  bool valid_;

  /**
   * 随机数生成器
   */
  Random rnd_;
  /**
   * 字节计数器
   */
  ssize_t bytes_counter_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

/**
 * 解析键保存到ikey
 *
 * 首先将当前内部键迭代器指向的键和值的长度记录下来
 * 然后让字节计数器减去这部分长度
 * 如果字节计数器小于0则重置字节计数器并且调用数据库实现者的RecordReadSample函数记录当前键
 * 然后调用ParseInternalKey解析键，保存到ikey中
 */
inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n;
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();
    db_->RecordReadSample(k);
  }
  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

/**
 * 将数据库迭代器向后移动一次
 *
 * 首先判断上次迭代的方向，如果是逆向则进行如下操作：
 * 首先判断内部迭代器是否有效，如果无效，说明现在迭代到了第一个元素的前的一个，则调用内部迭代器SeekToFirst指向第一个元素
 * 如果有效，则直接调用内部迭代器的Next指向下一个元素
 * 继续判断内部迭代器是否有效，如果无效则将当前迭代器置为无效并且清空saved_key_返回，如果有效则继续
 * 如果是正向则将当前的键提取用户键保存到saved_key_中
 * 最后调用FindNextUserEntry找到saved_key_后面的一个键值对
 */
void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
  }

  FindNextUserEntry(true, &saved_key_);
}

/**
 * 找到下一个用户键值对
 *
 * 只要内部迭代器有效就不断迭代内部迭代器，每当迭代出一个内部键之后
 * 调用ParseKey将内部键解析，并且判断它的序列号是否小于等于当前序列号，如果不是则继续迭代下一个
 * 如果是则判断内部键的类型，如果是删除类型，则将skip设置为内部键中的用户键，并且将skipping设置为真
 * 如果是值类型，则判断是否是skipping并且内部键中的用户键<=skip，如果是则跳过继续循环，如果不是则将valid_设置成真，并且将saved_key_清空返回
 * 如果内部迭代器迭代失效了还没有返回，则将valid_设置为假，并且将saved_key_清空返回
 */
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

/**
 * 将数据库迭代器向前移动一次
 *
 * 如果当前迭代方向是正向需要做如下操作：
 * 首先将当前内部迭代器指向的内部键提取出用户键并且保存到saved_key_中
 * 然后向前迭代内部迭代器，直到内部迭代器指向的用户键小于saved_key，如果过程中内部迭代器失效则将valid_置为假，并且清空saved_key_和saved_value_返回
 * 然后将方向置为反向
 * 最后调用FindPrevUserEntry找到上一个用户键值对
 */
void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

/**
 * 找到上一个用户键值对
 *
 * 首先设置值类型的初始值为删除类型
 * 然后迭代内部迭代器，首先将内部迭代器的内部键进行解析，判断其序列号是否小于等于当前序列号，如果不是则继续迭代
 * 如果是则首先判断值类型是否不是删除类型，并且当前内部键的用户键小于saved_key_，如果这样可以跳出迭代
 * 将值类型设置为内部键的类型
 * 如果内部键的类型是删除类型，则清空saved_key_和saved_value_继续迭代
 * 如果内部键的类型是值类型，则将saved_key_设置为内部键中的用户键，将saved_value_设置为迭代器中的值
 * 如果迭代器没有失效就继续迭代
 *
 * 如果现在的值类型是删除类型，说明一直没有找到对应的键值对，则将valid_置为假，清空saved_key_和saved_value_
 * 否则将valid_置为真
 */
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

/**
 * 将数据库迭代器指向大于等于target的最前面的位置
 *
 * 首先将迭代方向设置为向前
 * 然后清空saved_key_和saved_value_
 * 然后将saved_key_设置为target+当前序列号+值类型
 * 然后调用内部迭代器进行seek
 * 如果内部迭代器失效则将valid_置为假
 * 否则调用FindNextUserEntry去寻找大于等于target的最前面的位置
 */
void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

/**
 * 将数据库迭代器移动到最开始的位置
 *
 * 首先将方向设置为向前
 * 然后清空saved_value_
 * 然后将内部迭代器置为最开始的位置
 * 如果内部迭代器失效则将valid_置为假
 * 否则调用FindNextUserEntry找到下一个用户键
 */
void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

/**
 * 将数据库迭代器移动到最末尾的位置
 *
 * 首先将方向设置为反向
 * 然后清空saved_value_
 * 然后将内部迭代器指向最后面的位置
 * 调用FindPrevUserEntry找到上一个用户键
 */
void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

/**
 * 创建一个数据库迭代器
 */
Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence,
    uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
