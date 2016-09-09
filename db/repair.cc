// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// We recover the contents of the descriptor from the other files we find.
// (1) Any log files are first converted to tables
// (2) We scan every table to compute
//     (a) smallest/largest for the table
//     (b) largest sequence number in the table
// (3) We generate descriptor contents:
//      - log number is set to zero
//      - next-file-number is set to 1 + largest file number we found
//      - last-sequence-number is set to largest sequence# found across
//        all tables (see 2c)
//      - compaction pointers are cleared
//      - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"

namespace leveldb {

namespace {

/**
 * 修复者对象
 */
class Repairer {
 public:
  /**
   * 构造函数
   */
  Repairer(const std::string& dbname, const Options& options)
      : dbname_(dbname),
        env_(options.env),
        icmp_(options.comparator),
        ipolicy_(options.filter_policy),
        options_(SanitizeOptions(dbname, &icmp_, &ipolicy_, options)),
        owns_info_log_(options_.info_log != options.info_log),
        owns_cache_(options_.block_cache != options.block_cache),
        next_file_number_(1) {
    // TableCache can be small since we expect each table to be opened once.
    table_cache_ = new TableCache(dbname_, &options_, 10);
  }

  /**
   * 析构函数
   */
  ~Repairer() {
    delete table_cache_;
    if (owns_info_log_) {
      delete options_.info_log;
    }
    if (owns_cache_) {
      delete options_.block_cache;
    }
  }

  /**
   * 运行修复
   *
   * 首先收集文件信息
   * 然后将所有log文件转换为表文件
   * 并且所有表文件提取meta信息
   * 最后将所有信息都保存到描述符文件中
   */
  Status Run() {
    Status status = FindFiles();
    if (status.ok()) {
      ConvertLogFilesToTables();
      ExtractMetaData();
      status = WriteDescriptor();
    }
    if (status.ok()) {
      unsigned long long bytes = 0;
      for (size_t i = 0; i < tables_.size(); i++) {
        bytes += tables_[i].meta.file_size;
      }
      Log(options_.info_log,
          "**** Repaired leveldb %s; "
          "recovered %d files; %llu bytes. "
          "Some data may have been lost. "
          "****",
          dbname_.c_str(),
          static_cast<int>(tables_.size()),
          bytes);
    }
    return status;
  }

 private:
  /**
   * 表信息结构体
   */
  struct TableInfo {
    FileMetaData meta;
    SequenceNumber max_sequence;
  };

  /**
   * 数据库名
   */
  std::string const dbname_;
  /**
   * 指向环境的指针
   */
  Env* const env_;
  /**
   * 内部键比较者
   */
  InternalKeyComparator const icmp_;
  /**
   * 内部filter策略
   */
  InternalFilterPolicy const ipolicy_;
  /**
   * 选项
   */
  Options const options_;
  /**
   * 选项中的信息日志属于自己
   */
  bool owns_info_log_;
  /**
   * 选项中的块缓存属于自己
   */
  bool owns_cache_;
  /**
   * 指向表缓存的指针
   */
  TableCache* table_cache_;
  /**
   * 版本编辑对象
   */
  VersionEdit edit_;

  /**
   * 描述符文件名列表
   */
  std::vector<std::string> manifests_;
  /**
   * 表文件号列表
   */
  std::vector<uint64_t> table_numbers_;
  /**
   * 日志文件号列表
   */
  std::vector<uint64_t> logs_;
  /**
   * 表信息列表
   */
  std::vector<TableInfo> tables_;
  /**
   * 下一个文件号
   */
  uint64_t next_file_number_;

  /**
   * 找到文件
   *
   * 首先找到dbname_目录下面的所有文件名放到文件名列表中
   * 如果文件名列表为空，返回出错
   * 遍历文件名列表，并且调用ParseFileName从文件名中解析得到文件号和文件类型
   * 如果是描述符文件类型，则添加文件名到描述符文件列表中继续循环
   * 如果文件号加1大于下一个文件号，则将下一个文件号设置为文件号加1
   * 如果是日志文件类型，则添加文件号到日志文件列表
   * 如果是表文件类型，则添加文件号到表号列表
   */
  Status FindFiles() {
    std::vector<std::string> filenames;
    Status status = env_->GetChildren(dbname_, &filenames);
    if (!status.ok()) {
      return status;
    }
    if (filenames.empty()) {
      return Status::IOError(dbname_, "repair found no files");
    }

    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        if (type == kDescriptorFile) {
          manifests_.push_back(filenames[i]);
        } else {
          if (number + 1 > next_file_number_) {
            next_file_number_ = number + 1;
          }
          if (type == kLogFile) {
            logs_.push_back(number);
          } else if (type == kTableFile) {
            table_numbers_.push_back(number);
          } else {
            // Ignore other files
          }
        }
      }
    }
    return status;
  }

  /**
   * 转换日志文件列表到表格
   *
   * 遍历日志文件号列表
   * 对每一个日志号调ConvertLogToTable转换为表
   * 最后调ArchiveFile将日志文件移动到lost目录
   */
  void ConvertLogFilesToTables() {
    for (size_t i = 0; i < logs_.size(); i++) {
      std::string logname = LogFileName(dbname_, logs_[i]);
      Status status = ConvertLogToTable(logs_[i]);
      if (!status.ok()) {
        Log(options_.info_log, "Log #%llu: ignoring conversion error: %s",
            (unsigned long long) logs_[i],
            status.ToString().c_str());
      }
      ArchiveFile(logname);
    }
  }

  /**
   * 转换日志文件到表格
   *
   * 首先声明一个日志报告者
   * 利用日志文件号构造文件名，然后以顺序读打开日志文件
   * 创建一个刚刚声明的日志报告者，将当前环境，当前选项中的info_log，和日志文件号分别传递进去
   * 利用这个顺序读日志文件构造一个读者，将报告者传递进去
   * 创建一个内存表
   * 然后循环利用读者读取记录，然后将其插入内存表
   * 最后析构日志文件
   * 首先创建一个文件meta，然后将他的号码设置为下一个文件号，并且将下一个文件号加1
   * 对内存表创建一个内存表的迭代器，利用这个迭代器调用BuildTable创建一个表文件
   * 然后析构迭代器，对内存表去引用，并且置为空
   * 最后将meta的文件号放到表序号列表中
   */
  Status ConvertLogToTable(uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env* env;
      Logger* info_log;
      uint64_t lognum;
      virtual void Corruption(size_t bytes, const Status& s) {
        // We print error messages for corruption, but continue repairing.
        Log(info_log, "Log #%llu: dropping %d bytes; %s",
            (unsigned long long) lognum,
            static_cast<int>(bytes),
            s.ToString().c_str());
      }
    };

    // Open the log file
    std::string logname = LogFileName(dbname_, log);
    SequentialFile* lfile;
    Status status = env_->NewSequentialFile(logname, &lfile);
    if (!status.ok()) {
      return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.lognum = log;
    // We intentionally make log::Reader do checksumming so that
    // corruptions cause entire commits to be skipped instead of
    // propagating bad information (like overly large sequence
    // numbers).
    log::Reader reader(lfile, &reporter, false/*do not checksum*/,
                       0/*initial_offset*/);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    MemTable* mem = new MemTable(icmp_);
    mem->Ref();
    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < 12) {
        reporter.Corruption(
            record.size(), Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      status = WriteBatchInternal::InsertInto(&batch, mem);
      if (status.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        Log(options_.info_log, "Log #%llu: ignoring %s",
            (unsigned long long) log,
            status.ToString().c_str());
        status = Status::OK();  // Keep going with rest of file
      }
    }
    delete lfile;

    // Do not record a version edit for this conversion to a Table
    // since ExtractMetaData() will also generate edits.
    FileMetaData meta;
    meta.number = next_file_number_++;
    Iterator* iter = mem->NewIterator();
    status = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    delete iter;
    mem->Unref();
    mem = NULL;
    if (status.ok()) {
      if (meta.file_size > 0) {
        table_numbers_.push_back(meta.number);
      }
    }
    Log(options_.info_log, "Log #%llu: %d ops saved to Table #%llu %s",
        (unsigned long long) log,
        counter,
        (unsigned long long) meta.number,
        status.ToString().c_str());
    return status;
  }

  /**
   * 提取元数据
   *
   * 遍历表序号列表中的所有表序号
   * 对其调用ScanTable扫描表的内容
   */
  void ExtractMetaData() {
    for (size_t i = 0; i < table_numbers_.size(); i++) {
      ScanTable(table_numbers_[i]);
    }
  }

  /**
   * 根据meta创建一个表迭代器
   *
   * 从表缓存里面拿到文件号对应的迭代器
   */
  Iterator* NewTableIterator(const FileMetaData& meta) {
    // Same as compaction iterators: if paranoid_checks are on, turn
    // on checksum verification.
    ReadOptions r;
    r.verify_checksums = options_.paranoid_checks;
    return table_cache_->NewIterator(r, meta.number, meta.file_size);
  }

  /**
   * 根据文件号扫描表
   *
   * 首先构建一个表信息结构体，将其中meta的文件号设置为number
   * 然后根据文件号获得表文件名，根据文件名查找到文件获得文件大小设置到meta中的文件大小上
   * 如果文件大小获取失败则调用ArchiveFile移动文件到lost子目录并且返回
   * 根据meta创建一个表的迭代器
   * 然后迭代表中的数据，给表信息结构体设置最大序列号和meta中的开始和结束
   * 最后析构迭代器
   * 如果刚才的过程没有出错则将表信息加入到表信息列表中，否则调用RepairTable修复表
   */
  void ScanTable(uint64_t number) {
    TableInfo t;
    t.meta.number = number;
    std::string fname = TableFileName(dbname_, number);
    Status status = env_->GetFileSize(fname, &t.meta.file_size);
    if (!status.ok()) {
      // Try alternate file name.
      fname = SSTTableFileName(dbname_, number);
      Status s2 = env_->GetFileSize(fname, &t.meta.file_size);
      if (s2.ok()) {
        status = Status::OK();
      }
    }
    if (!status.ok()) {
      ArchiveFile(TableFileName(dbname_, number));
      ArchiveFile(SSTTableFileName(dbname_, number));
      Log(options_.info_log, "Table #%llu: dropped: %s",
          (unsigned long long) t.meta.number,
          status.ToString().c_str());
      return;
    }

    // Extract metadata by scanning through table.
    int counter = 0;
    Iterator* iter = NewTableIterator(t.meta);
    bool empty = true;
    ParsedInternalKey parsed;
    t.max_sequence = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      if (!ParseInternalKey(key, &parsed)) {
        Log(options_.info_log, "Table #%llu: unparsable key %s",
            (unsigned long long) t.meta.number,
            EscapeString(key).c_str());
        continue;
      }

      counter++;
      if (empty) {
        empty = false;
        t.meta.smallest.DecodeFrom(key);
      }
      t.meta.largest.DecodeFrom(key);
      if (parsed.sequence > t.max_sequence) {
        t.max_sequence = parsed.sequence;
      }
    }
    if (!iter->status().ok()) {
      status = iter->status();
    }
    delete iter;
    Log(options_.info_log, "Table #%llu: %d entries %s",
        (unsigned long long) t.meta.number,
        counter,
        status.ToString().c_str());

    if (status.ok()) {
      tables_.push_back(t);
    } else {
      RepairTable(fname, t);  // RepairTable archives input file.
    }
  }

  /**
   * 根据文件名和表信息修复表
   *
   * 首先创建一个新文件，并且将下一个文件号加1
   * 然后创建新文件的表构建者
   * 然后迭代源文件中的所有数据，添加到构建者中，并且统计总数
   * 然后将源文件移动到lost目录中
   * 如果当前总数是0则调用构建者的取消方法
   * 否则调用构建者的完成方法，并且给meta设置构建者中的文件大小
   * 析构构建者，关闭新文件
   * 如果刚刚统计的内容总数大于0，将新文件名改成原文件名，并且添加到表信息列表中
   * 如果刚刚有出错的情况则删除新文件
   */
  void RepairTable(const std::string& src, TableInfo t) {
    // We will copy src contents to a new table and then rename the
    // new table over the source.

    // Create builder.
    std::string copy = TableFileName(dbname_, next_file_number_++);
    WritableFile* file;
    Status s = env_->NewWritableFile(copy, &file);
    if (!s.ok()) {
      return;
    }
    TableBuilder* builder = new TableBuilder(options_, file);

    // Copy data.
    Iterator* iter = NewTableIterator(t.meta);
    int counter = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      builder->Add(iter->key(), iter->value());
      counter++;
    }
    delete iter;

    ArchiveFile(src);
    if (counter == 0) {
      builder->Abandon();  // Nothing to save
    } else {
      s = builder->Finish();
      if (s.ok()) {
        t.meta.file_size = builder->FileSize();
      }
    }
    delete builder;
    builder = NULL;

    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (counter > 0 && s.ok()) {
      std::string orig = TableFileName(dbname_, t.meta.number);
      s = env_->RenameFile(copy, orig);
      if (s.ok()) {
        Log(options_.info_log, "Table #%llu: %d entries repaired",
            (unsigned long long) t.meta.number, counter);
        tables_.push_back(t);
      }
    }
    if (!s.ok()) {
      env_->DeleteFile(copy);
    }
  }

  /**
   * 写描述符文件
   *
   * 首先创建一个临时文件
   * 获取文件信息列表中所有文件的最大序列号
   * 设置edit_的日志号，下一个文件号，最大序列号和比较者
   * 将每一个文件信息分别拆开添加到edit_中
   * 对临时文件创建一个写者，并且将edit_编码并且保存到其中
   * 如果有出错的情况则将临时文件删除
   * 否则将所有描述符文件全部移动到lost目录中
   * 并且将刚才的临时文件重命名为描述符文件，并且设置current指向这个文件
   */
  Status WriteDescriptor() {
    std::string tmp = TempFileName(dbname_, 1);
    WritableFile* file;
    Status status = env_->NewWritableFile(tmp, &file);
    if (!status.ok()) {
      return status;
    }

    SequenceNumber max_sequence = 0;
    for (size_t i = 0; i < tables_.size(); i++) {
      if (max_sequence < tables_[i].max_sequence) {
        max_sequence = tables_[i].max_sequence;
      }
    }

    edit_.SetComparatorName(icmp_.user_comparator()->Name());
    edit_.SetLogNumber(0);
    edit_.SetNextFile(next_file_number_);
    edit_.SetLastSequence(max_sequence);

    for (size_t i = 0; i < tables_.size(); i++) {
      // TODO(opt): separate out into multiple levels
      const TableInfo& t = tables_[i];
      edit_.AddFile(0, t.meta.number, t.meta.file_size,
                    t.meta.smallest, t.meta.largest);
    }

    //fprintf(stderr, "NewDescriptor:\n%s\n", edit_.DebugString().c_str());
    {
      log::Writer log(file);
      std::string record;
      edit_.EncodeTo(&record);
      status = log.AddRecord(record);
    }
    if (status.ok()) {
      status = file->Close();
    }
    delete file;
    file = NULL;

    if (!status.ok()) {
      env_->DeleteFile(tmp);
    } else {
      // Discard older manifests
      for (size_t i = 0; i < manifests_.size(); i++) {
        ArchiveFile(dbname_ + "/" + manifests_[i]);
      }

      // Install new manifest
      status = env_->RenameFile(tmp, DescriptorFileName(dbname_, 1));
      if (status.ok()) {
        status = SetCurrentFile(env_, dbname_, 1);
      } else {
        env_->DeleteFile(tmp);
      }
    }
    return status;
  }

  /**
   * 将路径名为fname的文件，移动到lost子目录中
   *
   * 首先获得右边第一个/的位置
   * 如果找到了则将新目录名加上这个位置之前的目录部分，否则新目录名置空
   * 将新目录追加/lost，如果新目录不存在则创建
   * 然后将新目录加上文件名，组成新文件名
   * 最后将文件移动到新目录
   */
  void ArchiveFile(const std::string& fname) {
    // Move into another directory.  E.g., for
    //    dir/foo
    // rename to
    //    dir/lost/foo
    const char* slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != NULL) {
      new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir);  // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append((slash == NULL) ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file);
    Log(options_.info_log, "Archiving %s: %s\n",
        fname.c_str(), s.ToString().c_str());
  }
};
}  // namespace

/**
 * 修复数据库
 *
 * 首先创建一个修复着对象，然后调用它的Run进行处理
 */
Status RepairDB(const std::string& dbname, const Options& options) {
  Repairer repairer(dbname, options);
  return repairer.Run();
}

}  // namespace leveldb
