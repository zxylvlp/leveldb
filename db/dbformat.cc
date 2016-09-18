// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

/**
 * 将序列号和类型打包
 */
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

/**
 * 将解析过的内部键添加到结果中
 *
 * 首先用户键添加到结果中，然后将序列号和类型进行打包也追加到结果中
 */
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

/**
 * 返回解析过的内部键的调试字符串
 *
 * 将用户键、序列号和类型分别添加到调试字符串中并返回
 */
std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

/**
 * 返回内部键的调试字符串
 *
 * 将内部键解析成解析过的内部键，再调用其调试字符串函数
 */
std::string InternalKey::DebugString() const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString();
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

/**
 * 返回内部键比较者的名字
 */
const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

/**
 * 比较两个内部键
 *
 * 首先比较两个用户键
 * 如果用户键不能比较出哪个更大则降序比较其序列号和其类型
 */
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

/**
 * 找到start到limit的最短分隔符
 *
 * 首先将start和limit中的用户键提取出来，然后调用用户比较者的找最短分隔符函数
 * 如果找到的分隔符长度小于start中的用户键，则将新的分隔符创建成含有最大序列号和最大值类型的内部键，然后传递给start
 */
void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

/**
 * 找到key的最短后继
 *
 * 首先从key中提取出用户键，再用用户键比较者的找到最短后继函数处理之
 * 如果找到的后继长度小于用户键，则将其添加上最大序列号和最大值类型制作成内部键，并且赋值给start
 */
void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

/**
 * 返回内部键filter策略的名字
 *
 * 调用了其中保存的用户键filter策略的名字函数
 */
const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

/**
 * 创建内部键filter
 *
 * 首先将内部键数组转换成用户键数组
 * 然后利用用户键数组调用用户filter策略的创建filter函数
 */
void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

/**
 * 判断内部键key是否在filter中
 *
 * 首先将key中的用户键提取出来
 * 然后调用用户filter策略的判断用户键是否在filter中的函数
 */
bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

/**
 * 查找键构造函数
 *
 * 首先保守判断用户键在包装成查找键之后是不是可以用space放得下，如果放得下就用space,否则就新申请空间
 * 将start设置成最开始的位置，将内部键的长度编码进去，然后将kstart设置成内部键开始的位置
 * 然后将用户键和序列号还有值类型编码进去，最后将end_设置成内部键的末尾
 */
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
