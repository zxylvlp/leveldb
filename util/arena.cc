// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

// 这里设定申请的块大小是4096
static const int kBlockSize = 4096;

// 构造函数将三个基本数据成员初始化，vector会调用默认的构造函数初始化，在这里我们不用管它
Arena::Arena() : memory_usage_(0) {
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

// 析构的时候需要将申请的资源释放掉，其他基本数据成员可以不用管
Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

// 分配内存的后备它肯定会分配一个块, 4096/4则独立分配一块内存块，否则利用当前的内存块分配内存
// 如果要分配的对象比较大的时候，一个块放不了多少东西，就会造成空间的浪费，就好像9/10 > 3/4一样。
char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

// 分配对齐内存，这里的处理还是非常的有意思，
// 我们要先计算bytes离对齐还有多远，如果指针大于8则要将对齐目标align设置为指针大小，否则就8字节对齐，
// 将当前_ptr的后log2(align)位取出来作为current_mod，也就是对齐的mod数，
// 然后取mod得补作为slop，
// 如果在当前块中能分配bytes + slop的话则分配这么大，返回指针中也要加上slop，
// 如果当前块分配不了则调AllocateFallback分配bytes内存，gcc可以自动保证对齐
char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  //uintptr_t是可以容纳指针大小的uint类型
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

// 实际分配内存的地方，将分配出来的内存指针放到blocks_中以备将来释放，
// 并将memory_usage_，加上分配的大小+一个指针的大小，这样做我猜测是加上了内存对齐的影响,
// 然后返回刚刚分配内存的指针
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

}  // namespace leveldb
