// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void  BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Constant after construction
  /**
   * 指向环境的指针
   */
  Env* const env_;
  /**
   * 内部键比较者
   */
  const InternalKeyComparator internal_comparator_;
  /**
   * 内部键filter策略
   */
  const InternalFilterPolicy internal_filter_policy_;
  /**
   * 用于控制数据库行为的选项
   */
  const Options options_;  // options_.comparator == &internal_comparator_
  /**
   * 是否拥有info_log
   */
  bool owns_info_log_;
  /**
   * 是否拥有缓存
   */
  bool owns_cache_;
  /**
   * 数据库名
   */
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  /**
   * 指向表缓存的指针
   */
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  /**
   * 指向数据库全局锁的指针
   */
  FileLock* db_lock_;

  // State below is protected by mutex_
  /**
   * 保护下面状态的互斥锁
   */
  port::Mutex mutex_;
  /**
   * 表示正在关闭
   */
  port::AtomicPointer shutting_down_;
  /**
   * 等后台完成的条件变量
   */
  port::CondVar bg_cv_;          // Signalled when background work finishes
  /**
   * 指向内存表的指针
   */
  MemTable* mem_;
  /**
   * 指向正在被合并的内存表的指针
   */
  MemTable* imm_;                // Memtable being compacted
  /**
   * 表示是否有正在合并的内存表
   */
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  /**
   * 指向日志文件的指针
   */
  WritableFile* logfile_;
  /**
   * 日志文件的文件号
   */
  uint64_t logfile_number_;
  /**
   * 指向日志写者的指针
   */
  log::Writer* log_;
  /**
   * 随机种子
   */
  uint32_t seed_;                // For sampling.

  // Queue of writers.
  /**
   * 写者队列
   */
  std::deque<Writer*> writers_;
  /**
   * 指向批量写的指针
   */
  WriteBatch* tmp_batch_;

  /**
   * 快照列表
   */
  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  /**
   * 不能被删除的表文件集合
   */
  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  /**
   * 后台合并是否被调度
   */
  bool bg_compaction_scheduled_;

  // Information for a manual compaction
  /**
   * 手工合并类
   */
  struct ManualCompaction {
    /**
     * 合并的层级
     */
    int level;
    /**
     * 是否完成
     */
    bool done;
    /**
     * 合并的开始键
     */
    const InternalKey* begin;   // NULL means beginning of key range
    /**
     * 合并的结束键
     */
    const InternalKey* end;     // NULL means end of key range
    /**
     * 跟踪合并过程的临时存储
     */
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  /**
   * 指向手工合并的指针
   */
  ManualCompaction* manual_compaction_;

  /**
   * 指向版本集合的指针
   */
  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  /**
   * 后台错误状态码
   */
  Status bg_error_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  /**
   * 合并统计信息类
   */
  struct CompactionStats {
    /**
     * 微秒数
     */
    int64_t micros;
    /**
     * 读取的字节数
     */
    int64_t bytes_read;
    /**
     * 写入的字节数
     */
    int64_t bytes_written;

    /**
     * 构造函数
     */
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

    /**
     * 把另一个合并统计信息对象加到自己上面来
     */
    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  /**
   * 保存每一层的合并统计信息
   */
  CompactionStats stats_[config::kNumLevels];

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  /**
   * 返回用户键比较者
   *
   * 从当前内部键比较者中拿到用户键比较者
   */
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
