// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "leveldb/dumpfile.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {
namespace {

/**
 * 标准输出打印者
 */
class StdoutPrinter : public WritableFile {
 public:
  /**
   * 向stdout输出data，并且返回ok状态
   */
  virtual Status Append(const Slice& data) {
    fwrite(data.data(), 1, data.size(), stdout);
    return Status::OK();
  }
  /**
   * 直接返回ok状态
   */
  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }
};

/**
 * dump文件列表中的所有文件到标准输出
 *
 * 首先创建一个标准输出打印者
 * 然后对所有文件调用DumpFile输出到标准输出
 * 如果中间出错则输出错误信息到标准错误输出并且返回假
 * 否则返回真
 */
bool HandleDumpCommand(Env* env, char** files, int num) {
  StdoutPrinter printer;
  bool ok = true;
  for (int i = 0; i < num; i++) {
    Status s = DumpFile(env, files[i], &printer);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      ok = false;
    }
  }
  return ok;
}

}  // namespace
}  // namespace leveldb

/**
 * 打印使用提示符
 */
static void Usage() {
  fprintf(
      stderr,
      "Usage: leveldbutil command...\n"
      "   dump files...         -- dump contents of specified files\n"
      );
}

/**
 * 主函数用于dump文件
 *
 * 当第1个参数是dump的时候调用HandleDumpCommand处理dump命令
 * 否则调用Usage打印使用提示符
 */
int main(int argc, char** argv) {
  leveldb::Env* env = leveldb::Env::Default();
  bool ok = true;
  if (argc < 2) {
    Usage();
    ok = false;
  } else {
    std::string command = argv[1];
    if (command == "dump") {
      ok = leveldb::HandleDumpCommand(env, argv+2, argc-2);
    } else {
      Usage();
      ok = false;
    }
  }
  return (ok ? 0 : 1);
}
