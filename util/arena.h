// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"

namespace leveldb {
// Allocate AllocateAligned的责任是分配内存，从当前块中能分就从当前块分，不能就调AllocateFallback分配一个新块
// AllocateFallback的责任是分配内存块，分配多大的内存块由它决定，它分配块的具体操作则由AllocateNewBlock完成
class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  // 返回当前使用内存的估计值
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  // 存放当前分配块中下面可以分配的位置
  char* alloc_ptr_;
  // 存放当前分配块中还剩下的bytes数
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  // 存放所有分配出来的内存块，以备将来释放
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  // 存放内存的使用情况，不过为什么会用AtomicPointer这个类型还有待探讨。
  port::AtomicPointer memory_usage_;

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

// 分配内存如果能在当前块中分配则在当前块中分配，如果不能则调用AllocateFallback再分配一个块
inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
