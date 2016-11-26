// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <string>
#include <stdint.h>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
/**
 * 块句柄类
 */
class BlockHandle {
 public:
  BlockHandle();

  // The offset of the block in the file.
  /**
   * 获取块在文件中的偏移量
   */
  uint64_t offset() const { return offset_; }
  /**
   * 设置块在文件中的偏移量
   */
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  /**
   * 获取块的大小
   */
  uint64_t size() const { return size_; }
  /**
   * 设置块的大小
   */
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Maximum encoding length of a BlockHandle
  /**
   * 块句柄的最大编码长度
   */
  enum { kMaxEncodedLength = 10 + 10 };

 private:
  /**
   * 块在文件中的偏移量
   */
  uint64_t offset_;
  /**
   * 块的大小
   */
  uint64_t size_;
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
/**
 * Footer类
 */
class Footer {
 public:
  /**
   * 构造函数
   */
  Footer() { }

  // The block handle for the metaindex block of the table
  /**
   * 获得meta索引块句柄
   */
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  /**
   * 设置meta索引块句柄
   */
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  /**
   * 获得索引块句柄
   */
  const BlockHandle& index_handle() const {
    return index_handle_;
  }
  /**
   * 设置索引块句柄
   */
  void set_index_handle(const BlockHandle& h) {
    index_handle_ = h;
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  enum {
    /**
     * 编码长度
     */
    kEncodedLength = 2*BlockHandle::kMaxEncodedLength + 8
  };

 private:
  /**
   * meta索引块的句柄
   */
  BlockHandle metaindex_handle_;
  /**
   * 索引块的句柄
   */
  BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
/**
 * 表的魔数
 */
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
/**
 * block尾部大小
 */
static const size_t kBlockTrailerSize = 5;

/**
 * block内容类
 */
struct BlockContents {
  /**
   * 实际数据
   */
  Slice data;           // Actual contents of data
  /**
   * 是否可以被缓存
   */
  bool cachable;        // True iff data can be cached
  /**
   * 是否分配在堆上
   */
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
extern Status ReadBlock(RandomAccessFile* file,
                        const ReadOptions& options,
                        const BlockHandle& handle,
                        BlockContents* result);

// Implementation details follow.  Clients should ignore,

/**
 * 构造函数
 */
inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)),
      size_(~static_cast<uint64_t>(0)) {
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_
