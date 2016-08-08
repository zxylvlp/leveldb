// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

/**
 * 将u32序列化
 *
 * 如果是小端则直接将value放到buf中，
 * 否则要自己转一下
 */
void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

/**
 * 将u64序列化
 *
 * 如果是小端则直接将value放到buf中，
 * 否则要自己转一下
 */
void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

/**
 * 将u32编码并append到dst中
 *
 * 先调用EncodeFixed32将value放到一个buf中，
 * 然后将buf，append到dst中
 */
void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

/**
 * 将u64编码并append到dst中
 *
 * 先调用EncodeFixed64将value放到一个buf中，
 * 然后将buf，append到dst中
 */
void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

/**
 * uint32的变长编码
 *
 * 用每一个byte的最高位做是否是结束，如果是1表示不是结束，如果是0表示是结束
 * 首先将dst拷贝给ptr
 * 首先判断是否7位可以表示，如果可以表示则直接拷贝到ptr中，并将ptr+1
 * 然后判断是否14为可以表示，如果可以则将v与2^7做或操作之后拷贝到ptr中，并将ptr+1，然后将v右移7位之后拷贝到ptr中，并将ptr+1
 * 21,28和32位可以表示的判断和处理雷同，不再赘述
 * 最后返回ptr
 */
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

/**
 * 将uint32变长编码，并且append到dst中
 *
 * 先调用EncodeVarint32将v放到一个buf中，
 * 然后将buf按照实际编码长度，append到dst中
 */
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

/**
 * uint64的变长编码
 *
 * 处理方式类似uint32的变长编码
 */
char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

/**
 * 将uint64变长编码，并且append到dst中
 *
 * 先调用EncodeVarint64将v放到一个buf中，
 * 然后将buf按照实际编码长度，append到dst中
 */
void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

/**
 * 将一个slice以长度前缀形式放到dst中
 *
 * 先将value的长度变长编码放到dst中
 * 再将value的内容放到dst中
 */
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

/**
 * 获得v的变长编码的长度
 */
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

/**
 * 从指针获得uint32变长解码之后值的后备方法
 *
 * 从内存地址为[p, limit)的地方扫描，取出指针指向的内容，将内容与2^7与来查看是否后面还有编码
 * 如果有则将内容与2^7-1求与之后左移 (指针地址-p)*7 位后与返回值求或
 * 如果没有了则将内容左移 (指针地址-p)*7 位后与返回值求或并返回
 */
const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

/**
 * 从input所指向的slice中获取变长编码的uint32的值
 *
 * 调用GetVarint32Ptr来实现，并且将slice中已经读取过的部分截掉
 */
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

/**
 * 从指针获得uint64变长解码之后值
 *
 * 与GetVarint32PtrFallback的实现类似
 */
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

/**
 * 从input所指向的slice中获取变长编码的uint64的值
 *
 * 与GetVarint32的实现类似
 */
bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

/**
 * 从指针获得长度前缀编码的slice
 *
 * 先调用GetVarint32Ptr获得长度前缀
 * 然后在将后面的内存构建一个slice作为结果
 * 并将前面读取过的长度截取掉并返回
 */
const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;
  p = GetVarint32Ptr(p, limit, &len);
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len);
  return p + len;
}

/**
 * 从slice获得长度前缀编码的slice
 *
 * 先调用GetVarint32Ptr获得长度前缀
 * 然后在将后面的内存构建一个slice作为结果
 * 并将前面读取过的长度截取掉并返回
 */
bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
