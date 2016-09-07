// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {
/**
 * 析构函数
 */
Reader::Reporter::~Reporter() {
}

/**
 * 构造函数
 */
Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

/**
 * 析构函数
 */
Reader::~Reader() {
  delete[] backing_store_;
}

/**
 * 跳到初始偏移量所在块的开始
 *
 * 首先利用初始偏移量获得块内偏移量
 * 再利用前面二者的差得到块的开始位置
 * 如果块内偏移量到了块尾部，则将快的开始位置置为下一个块，块内偏移量置为0
 * 将buffer结尾的偏移量置为块的开始位置
 * 对于块开始位置大于0的时候，将文件跳转到指定位置，如果跳转失败则调ReportDrop报告失败并且返回假
 * 其他情况返回真
 */
bool Reader::SkipToInitialBlock() {
  size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

/**
 * 读取记录
 *
 * 如果最近一条记录的偏移量小于初始偏移量
 * 则调用SkipToInitialBlock，跳转到初始偏移量所在块的开始
 * 然后清空scratch和record
 * 然后初始化正在读的记录的偏移量为0，初始化记录是片段的为假
 * 然后进入一个恒为真的循环
 * 先调用ReadPhysicalRecord获得记录的内容和类型
 * 然后将物理记录偏移量设置为buffer结尾的偏移量减去buffer的大小减去头部的大小减去内容的大小
 * 如果正在重同步，则跳过为middle和last的类型的内容，如果遇到除了middle之外的类型都可以设置不是正在重同步了
 * 对不同的类型分别处理如下
 * 对于全类型，如果发现记录是分片的则判断数据是否为空，如果是空则将记录是分片的置为假
 * 否则报告数据出错
 * 并且将当前记录的偏移量设置为物理记录的偏移量
 * 清空scratch，将record设置为记录内的内容
 * 将最后记录的偏移量设置成当前记录的偏移量，并且返回真
 * 如果是开始类型，如果发现记录是分片的则判断数据是否为空，如果是空则将记录是分片的置为假
 * 否则报告数据出错
 * 将数据的物理偏移量设置为当前记录的偏移量，然后将读取的内容追加到sratch中，并且标记当前数据是分片的
 * 如果是中间类型，则判断数据是否是分片的，如果不是则报告数据出错
 * 如果是则将读取的内容追加到sratch中
 * 如果是最后类型，则判断数据是否是分片的，如果不是则报告数据出错
 * 如果是则将读取的内容追加到sratch中，然后将record的内容置为scratch的内容
 * 并将最近读取的记录的偏移量设置为当前记录的偏移量，并且返回真
 * 如果是eof类型，判断当前数据是否是分片的，如果是则将scratch清空，最后返回假
 * 如果是错误记录，判断当前数据是否是分片的，如果是则设置为不分片并且清空scratch
 * 对于其他情况，输出记录类型，报告数据出错，将数据是否分片设置为假，并且清空scratch
 */
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

/**
 * 返回最近读取的记录的偏移量
 */
uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

/**
 * 报告数据败坏
 */
void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

/**
 * 报告丢弃了字节
 */
void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

/**
 * 读取物理记录
 *
 * 在一个无限循环中进行以下操作
 * 如果buffer的大小小于头部大小，则需要判断是否是文件结尾，如果是文件结尾则清空buffer并且返回文件尾
 * 如果不是文件尾则清空buffer，然后继续从文件中读一个块，并且将buffer结尾的偏移量加上buffer的大小
 * 如果读取失败则清空buffer报告数据出错并且返回文件尾
 * 如果读取大小小于块大小则设置到达文件尾，并且重新循环
 * 然后从buffer_中拿到头，获取其中的长度和类型，如果头大小加上长度大于buffer的大小，先清空buffer,如果到达文件尾则返回到达文件尾，否则报告并返回数据出错
 * 判断类型是否是0类型并且长度为0，则清空buffer并且返回数据出错
 * 如果需要检查checksum
 * 则对比crc，如果对比出错则清空buffer并且设置并返回数据出错
 * 将读过的数据消费掉
 * 然后判断buffer的结尾的偏移量减去buffer的大小减去消费的长度是否小于初始偏移量，如果是这样则返回数据出错
 * 如果前面都没问题则将结果设置为消费的那部分数据，并且返回数据的类型
 */
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
