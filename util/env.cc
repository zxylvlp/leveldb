// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

namespace leveldb {

Env::~Env() {
}

Status Env::NewAppendableFile(const std::string& fname, WritableFile** result) {
  return Status::NotSupported("NewAppendableFile", fname);
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

Logger::~Logger() {
}

FileLock::~FileLock() {
}

/**
 * 打印日志
 *
 * 如果info_log不为空，则调用其Logv，根据format和可变参数打印日志
 */
void Log(Logger* info_log, const char* format, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

/**
 * 将data的内容写到名为fname的文件中
 *
 * 首先创建一个可写文件
 * 然后将data追加到文件中
 * 如果should_sync为真则将文件刷到磁盘
 * 然后将文件关闭
 * 并且析构创建的可写文件
 * 如果刚才有出错的情况，则删除该文件
 */
static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->DeleteFile(fname);
  }
  return s;
}

/**
 * 将data的内容写到名为fname的文件中
 * 调用DoWriteStringToFile实现不刷到磁盘
 */
Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

/**
 * 将data的内容写到名为fname的文件中
 * 调用DoWriteStringToFile实现刷到磁盘
 */
Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

/**
 * 从名为fname的文件中，读取所有内容保存到data中
 *
 * 首先清空data的内容
 * 然后创建一个名为fname的顺序读文件
 * 创建一个buffer
 * 循环调用循序读文件的读方法读到buffer中，并且将buffer追加到data中，如果出错或者读到长度为0则跳出循环
 * 析构buffer和顺序读文件
 */
Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {
}

}  // namespace leveldb
