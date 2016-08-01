// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// AtomicPointer provides storage for a lock-free pointer.
// Platform-dependent implementation of AtomicPointer:
// - If the platform provides a cheap barrier, we use it with raw pointers
// - If <atomic> is present (on newer versions of gcc, it is), we use
//   a <atomic>-based AtomicPointer.  However we prefer the memory
//   barrier based version, because at least on a gcc 4.4 32-bit build
//   on linux, we have encountered a buggy <atomic> implementation.
//   Also, some <atomic> implementations are much slower than a memory-barrier
//   based implementation (~16ns for <atomic> based acquire-load vs. ~1ns for
//   a barrier based acquire-load).
// This code is based on atomicops-internals-* in Google's perftools:
// http://code.google.com/p/google-perftools/source/browse/#svn%2Ftrunk%2Fsrc%2Fbase

#ifndef PORT_ATOMIC_POINTER_H_
#define PORT_ATOMIC_POINTER_H_

#include <stdint.h>
#ifdef LEVELDB_ATOMIC_PRESENT
#include <atomic>
#endif
#ifdef OS_WIN
#include <windows.h>
#endif
#ifdef OS_MACOSX
#include <libkern/OSAtomic.h>
#endif

#if defined(_M_X64) || defined(__x86_64__)
#define ARCH_CPU_X86_FAMILY 1
#elif defined(_M_IX86) || defined(__i386__) || defined(__i386)
#define ARCH_CPU_X86_FAMILY 1
#elif defined(__ARMEL__)
#define ARCH_CPU_ARM_FAMILY 1
#elif defined(__aarch64__)
#define ARCH_CPU_ARM64_FAMILY 1
#elif defined(__ppc__) || defined(__powerpc__) || defined(__powerpc64__)
#define ARCH_CPU_PPC_FAMILY 1
#elif defined(__mips__)
#define ARCH_CPU_MIPS_FAMILY 1
#endif

namespace leveldb {
namespace port {

// Define MemoryBarrier() if available
// Windows on x86
/**
 * 在Win下面有OS提供的MB实现
 */
#if defined(OS_WIN) && defined(COMPILER_MSVC) && defined(ARCH_CPU_X86_FAMILY)
// windows.h already provides a MemoryBarrier(void) macro
// http://msdn.microsoft.com/en-us/library/ms684208(v=vs.85).aspx
#define LEVELDB_HAVE_MEMORY_BARRIER

// Mac OS
/**
 * 在Mac下面有OS提供的MB实现
 */
#elif defined(OS_MACOSX)
inline void MemoryBarrier() {
  OSMemoryBarrier();
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// Gcc on x86
#elif defined(ARCH_CPU_X86_FAMILY) && defined(__GNUC__)
/**
 * 在x86下的gcc中使用下面的空指令来防止编译器乱序
 * 是空指令的原因是x86的CPU只会出现SL乱序，但是release和acquire语义都不会用到SL乱序，不用防止CPU乱序
 * MB的目的就是让编译器优化的过程中和CPU执行的过程中，下面的代码，或者上面的代码不要穿越到另外一边。
 */
inline void MemoryBarrier() {
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  __asm__ __volatile__("" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// Sun Studio
#elif defined(ARCH_CPU_X86_FAMILY) && defined(__SUNPRO_CC)
/**
 * 在x86下的sun studio中使用下面的空指令来防止编译器乱序
 */
inline void MemoryBarrier() {
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  asm volatile("" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// ARM Linux
#elif defined(ARCH_CPU_ARM_FAMILY) && defined(__linux__)
typedef void (*LinuxKernelMemoryBarrierFunc)(void);
// The Linux ARM kernel provides a highly optimized device-specific memory
// barrier function at a fixed memory address that is mapped in every
// user-level process.
//
// This beats using CPU-specific instructions which are, on single-core
// devices, un-necessary and very costly (e.g. ARMv7-A "dmb" takes more
// than 180ns on a Cortex-A8 like the one on a Nexus One). Benchmarking
// shows that the extra function call cost is completely negligible on
// multi-core devices.
//
/**
 * 在arm下linux中使用下面的函数来防止编译器和CPU乱序
 */
inline void MemoryBarrier() {
  (*(LinuxKernelMemoryBarrierFunc)0xffff0fa0)();
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// ARM64
#elif defined(ARCH_CPU_ARM64_FAMILY)
/**
 * 在arm64下使用下面的指令来防止编译器和CPU乱序
 */
inline void MemoryBarrier() {
  asm volatile("dmb sy" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// PPC
#elif defined(ARCH_CPU_PPC_FAMILY) && defined(__GNUC__)
/**
 * 在ppc下使用下面的指令来防止编译器和CPU乱序
 */
inline void MemoryBarrier() {
  // TODO for some powerpc expert: is there a cheaper suitable variant?
  // Perhaps by having separate barriers for acquire and release ops.
  asm volatile("sync" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// MIPS
#elif defined(ARCH_CPU_MIPS_FAMILY) && defined(__GNUC__)
/**
 * 在mips下使用下面的指令来防止编译器和CPU乱序
 */
inline void MemoryBarrier() {
  __asm__ __volatile__("sync" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

#endif

// AtomicPointer built using platform-specific MemoryBarrier()
#if defined(LEVELDB_HAVE_MEMORY_BARRIER)
/**
 * 原子指针类型，因为是指针类型，可以保证原子读写，不会出现数据败坏
 * 还提供了Acquire和Release语义，acquire语义保证了读的下面的指令不会穿越上来，
 * release语义保证写的上面的指令不会穿越下去。
 * 单写者release_store，多读者acquire_load是常用的同步方式。
 */
class AtomicPointer {
 private:
  void* rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* p) : rep_(p) {}
  inline void* NoBarrier_Load() const { return rep_; }
  inline void NoBarrier_Store(void* v) { rep_ = v; }
  inline void* Acquire_Load() const {
    void* result = rep_;
    MemoryBarrier();
    return result;
  }
  inline void Release_Store(void* v) {
    MemoryBarrier();
    rep_ = v;
  }
};

// AtomicPointer based on <cstdatomic>
#elif defined(LEVELDB_ATOMIC_PRESENT)
/**
 * 利用c++标准库提供的atomic对象的load和store
 */
class AtomicPointer {
 private:
  std::atomic<void*> rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* v) : rep_(v) { }
  inline void* Acquire_Load() const {
    return rep_.load(std::memory_order_acquire);
  }
  inline void Release_Store(void* v) {
    rep_.store(v, std::memory_order_release);
  }
  inline void* NoBarrier_Load() const {
    return rep_.load(std::memory_order_relaxed);
  }
  inline void NoBarrier_Store(void* v) {
    rep_.store(v, std::memory_order_relaxed);
  }
};

// Atomic pointer based on sparc memory barriers
#elif defined(__sparcv9) && defined(__GNUC__)
/**
 * 在sparcv9下面在防止编译器乱序的前提下，利用不同的mb防止不同类型的CPU乱序
 * 因为acquire需要ll和ls的mb, require需要ss和ls的mb
 */
class AtomicPointer {
 private:
  void* rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* v) : rep_(v) { }
  inline void* Acquire_Load() const {
    void* val;
    __asm__ __volatile__ (
        "ldx [%[rep_]], %[val] \n\t"
         "membar #LoadLoad|#LoadStore \n\t"
        : [val] "=r" (val)
        : [rep_] "r" (&rep_)
        : "memory");
    return val;
  }
  inline void Release_Store(void* v) {
    __asm__ __volatile__ (
        "membar #LoadStore|#StoreStore \n\t"
        "stx %[v], [%[rep_]] \n\t"
        :
        : [rep_] "r" (&rep_), [v] "r" (v)
        : "memory");
  }
  inline void* NoBarrier_Load() const { return rep_; }
  inline void NoBarrier_Store(void* v) { rep_ = v; }
};

// Atomic pointer based on ia64 acq/rel
/**
 * 在ia64下，利用acq和rel的指令直接实现相应的语义。
 */
#elif defined(__ia64) && defined(__GNUC__)
class AtomicPointer {
 private:
  void* rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* v) : rep_(v) { }
  inline void* Acquire_Load() const {
    void* val    ;
    __asm__ __volatile__ (
        "ld8.acq %[val] = [%[rep_]] \n\t"
        : [val] "=r" (val)
        : [rep_] "r" (&rep_)
        : "memory"
        );
    return val;
  }
  inline void Release_Store(void* v) {
    __asm__ __volatile__ (
        "st8.rel [%[rep_]] = %[v]  \n\t"
        :
        : [rep_] "r" (&rep_), [v] "r" (v)
        : "memory"
        );
  }
  inline void* NoBarrier_Load() const { return rep_; }
  inline void NoBarrier_Store(void* v) { rep_ = v; }
};

// We have neither MemoryBarrier(), nor <atomic>
#else
#error Please implement AtomicPointer for this platform.

#endif

#undef LEVELDB_HAVE_MEMORY_BARRIER
#undef ARCH_CPU_X86_FAMILY
#undef ARCH_CPU_ARM_FAMILY
#undef ARCH_CPU_ARM64_FAMILY
#undef ARCH_CPU_PPC_FAMILY

}  // namespace port
}  // namespace leveldb

#endif  // PORT_ATOMIC_POINTER_H_
