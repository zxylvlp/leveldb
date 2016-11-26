// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
/**
 * filter的大小取log
 */
static const size_t kFilterBaseLg = 11;
/**
 * filter的大小byte数
 */
static const size_t kFilterBase = 1 << kFilterBaseLg;

/**
 * 构造函数
 */
FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

/**
 * 开始块
 *
 * 首先获得块偏移量将其除以kFilterBase，可以得到filter号
 * 然后查看filter偏移量数组，看看能否差一点找到指定号码，如果不能差一点找到指定号码则调用GenerateFilter生成一个，循环此过程直到差一点能找到
 */
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

/**
 * 添加键
 *
 * 将keys_的大小追加到start_数组中
 * 并且将key的内容追加到keys_平坦化数组中
 */
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

/**
 * 完成构建
 *
 * 如果start_数组不为空就调用GenerateFilter生成filter到result_中
 * 然后将result_的大小作为偏移量数组的偏移量
 * 然后向result_中追加filter_offsets_数组的内容
 * 然后将偏移量数组的偏移量追加到result_中
 * 然后将kFilterBaseLg追加到result_中
 * 最后返回result_
 */
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

/**
 * 生成filter
 *
 * keys_中保存的是平坦化后的key数组
 * start_中保存的是平台化后的key数组中每一个key的起始位置
 * 这里首先将平坦化的key数据解析成非平坦化的key数组
 * 然后将result_当前大小保存到filter偏移量数组中
 * 然后调用CreateFilter将非平台化key数组制作成filter追加到result_中
 * 最后清空平坦化和非平坦化key数组和其起始位置数组
 */
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

/**
 * 构造函数
 */
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

/**
 * 根据块起始位置和key查找key是否可能存在
 *
 * 首先将块起始位置右移base_lg_位，得到filter号
 * 然后根据filter号从filter的偏移量数组中找到其偏移量和长度
 * 然后根据偏移量和长度确定filter
 * 然后调用并且返回KeyMayMatch判断key是否在filter中
 */
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index*4);
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
