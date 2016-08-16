// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "leveldb/env.h"

namespace leveldb {

/**
 * posix的日志记录者
 */
class PosixLogger : public Logger {
 private:
  /**
   * 文件指针
   */
  FILE* file_;
  /**
   * 获得当前线程id的函数指针
   */
  uint64_t (*gettid_)();  // Return the thread id for the current thread
 public:
  /**
   * 构造函数
   */
  PosixLogger(FILE* f, uint64_t (*gettid)()) : file_(f), gettid_(gettid) { }
  /**
   * 析构函数
   *
   * 会调用fclose关闭文件
   */
  virtual ~PosixLogger() {
    fclose(file_);
  }
  /**
   * 以format为格式输出可变参数到log中
   *
   * 首先获取到线程id
   * 然后会做输出，在输出的过程中会做两次尝试，
   * 第一次使用栈变量做buffer，如果第一次不够用会在第二次使用堆变量做更大的buffer
   * 将日期和线程id先输出出来，然后将可变参数以format为格式输出出来
   * 然后再在最后面追加换行符，并调用fwrite和fflush输出到内核缓冲
   */
  virtual void Logv(const char* format, va_list ap) {
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      fwrite(base, 1, p - base, file_);
      fflush(file_);
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
