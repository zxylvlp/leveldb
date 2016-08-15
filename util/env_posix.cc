// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <set>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

/**
 * 根据上下文和错误号返回一个Status表示的IO错误
 */
static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

/**
 * Posix顺序文件类，继承顺序文件接口类
 */
class PosixSequentialFile: public SequentialFile {
 private:
  /**
   * 存储文件名
   */
  std::string filename_;
  /**
   * 存储文件结构体指针
   */
  FILE* file_;

 public:
  /**
   * 构造函数
   */
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  /**
   * 析构函数
   *
   * 在其中调用fclose关闭打开的文件
   */
  virtual ~PosixSequentialFile() { fclose(file_); }

  /**
   * 读取文件
   *
   * 从文件中读取n个byte到scratch中，并且将result指向scratch中的内容
   * 如果实际读取到的内容长度小于n，并且没有遇到文件尾则返回一个错误
   */
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  /**
   * 跳过n个字节
   *
   * 调用fseek将当前文件指针向前移动n个字节
   * fseek返回值为状态码，如果fseek不出错返回0
   */
  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access
/**
 * Posix随机访问文件类，继承随机访问文件接口类
 */
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  /**
   * 文件名
   */
  std::string filename_;
  /**
   * 文件描述符
   */
  int fd_;

 public:
  /**
   * 构造函数
   */
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  /**
   * 析构函数
   *
   * 会调用close关闭fd_
   */
  virtual ~PosixRandomAccessFile() { close(fd_); }

  /**
   * 随机读取文件
   *
   * 调用pread从fd的offset处读取n个字节到scratch返回实际读取的字节数
   * 然后将Slice指向读取得到的内容
   * 如果返回的字节数是负数则返回读取出错
   */
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
/**
 * mmap调用次数限制器
 */
class MmapLimiter {
 public:
  // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
  /**
   * 构造函数
   *
   * 如果是64位则限制1000次
   * 如果是其他位数则限制不允许mmap
   */
  MmapLimiter() {
    SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  /**
   * 获取一个机会
   *
   * 首先调用GetAllowed检测是否有可用的机会，
   * 这里用acquire_load是为了防止下面的操作乱序上来和编译器优化，
   * 如果发现没有机会则直接返回false
   * 否则加锁之后重新调用acquire_load获得当前可用的机会数，其实可以改成no_barrier_load
   * 如果机会数小于等于0则返回false
   * 否则用release_store将机会数减1并返回ture
   */
  bool Acquire() {
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  /**
   * 释放一个机会
   *
   * 上锁后将机会加1即可
   * 里面用release_store是有理由的，
   * 因为锁上面的内容有可能进入临界区，
   * 之后与store操作乱序后可能会有问题
   * 里面用acquire_load其实可以改成no_barrier_load
   */
  void Release() {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  /**
   * 保护写的互斥锁
   */
  port::Mutex mu_;
  /**
   * 存放机会数的原子变量
   */
  port::AtomicPointer allowed_;

  /**
   * 获得当前的机会数
   *
   * 是对机会数原子变量acquire_load的一层封装
   */
  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  /**
   * 设置当前的机会数
   *
   * 是对机会数原子变量release_store的一层封装
   */
  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) {
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  /**
   * 禁止拷贝和赋值
   */
  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
/**
 * 基于mmap的Posix可读文件类继承随机访问文件接口类
 */
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  /**
   * 文件名
   */
  std::string filename_;
  /**
   * mmap的内存区域
   */
  void* mmapped_region_;
  /**
   * mmap的内存区域长度
   */
  size_t length_;
  /**
   * mmap调用次数限制器
   */
  MmapLimiter* limiter_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  /**
   * 构造函数
   */
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) {
  }

  /**
   * 析构函数
   *
   * 调用munmap取消内存映射
   * 并且释放mmap调用的调用机会
   */
  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  /**
   * 随机读取文件
   *
   * 如果offset+n大于mmap的映射长度
   * 则将result设置成一个空slice，并且返回一个错误
   * 否则将result的内容指向mmapped_region_的offset处，并且将长度设置成n个字节
   */
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};
/**
 * Posix可写文件类，继承可写文件类
 */
class PosixWritableFile : public WritableFile {
 private:
  /**
   * 文件名
   */
  std::string filename_;
  /**
   * 指向打开的文件的结构体的指针
   */
  FILE* file_;

 public:
  /**
   * 构造函数
   */
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  /**
   * 析构函数
   *
   * 如果file_不为空，则调用fclose关闭文件
   */
  ~PosixWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  /**
   * 将data中的内容添加到文件中
   *
   * 调用fwrite_unlocked将data写入文件，
   * 如果返回的长度与data的长度不一致，则返回写入出错
   */
  virtual Status Append(const Slice& data) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  /**
   * 关闭文件
   *
   * 调用fclose关闭文件，如果它的返回值不是零，则返回关闭出错
   * 函数返回前将文件指针置为空
   */
  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  /**
   * 将缓冲区中的内容刷新到内核缓冲区
   *
   * 调用fflush_unlocked，如果返回值不为0，则返回出错
   */
  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  /**
   * 如果当前文件是manifest文件，则对当前目录fsyc，让当前目录的元信息刷盘
   *
   * 首先从当前文件名中找到目录名和文件名
   * 然后判断文件名是否是manifest开头的，如果不是则直接返回
   * 否则只读打开目录名后调用fsync，这两步中有一步的返回值返回值小于0则返回出错
   * 调用close关闭打开的文件描述符
   */
  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  /**
   * 将文件刷盘
   *
   * 先调用SyncDirIfManifest，对manifest文件要先将其所在目录的元信息刷盘，如果调用出错则返回出错
   * 然后调用fflush_unlocked和fdatasync先将用户缓冲刷到内核缓冲再刷到磁盘中，这两步有一步返回值不为0则返回出错
   */
  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||
        fdatasync(fileno(file_)) != 0) {
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};

/**
 * 对文件描述符加锁或者解锁
 *
 * 首先将错误码清零，
 * 然后创建一个flock对象，并且将它清零，
 * 然后将他的类型设置成加锁或者解锁，
 * 然后让它的start和len都设置成0，说明对整个文件加锁
 * 最后调用fcntl对文件加锁或者解锁，返回他的返回值
 */
static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

/**
 * Posix文件锁类，继承文件锁接口类
 */
class PosixFileLock : public FileLock {
 public:
  /**
   * 锁住的文件描述符
   */
  int fd_;
  /**
   * 锁住的文件名
   */
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
/**
 * 存放posix lock的集合
 */
class PosixLockTable {
 private:
  /**
   * 保护集合的互斥锁
   */
  port::Mutex mu_;
  /**
   * 被锁文件的名字结合
   */
  std::set<std::string> locked_files_;
 public:
  /**
   * 将文件名fname插入到集合中
   *
   * 首先加锁，然后调用set的insert将fname添加进去，
   * 并且取出返回值的second返回
   * 返回值为true代表以前没有相同的元素，插入成功
   * 返回值为false代表以前有相同的元素，插入失败
   */
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  /**
   * 将文件名fname从集合中删除
   *
   * 首先加锁，然后调用set的erase将fname从集合中删除
   */
  void Remove(const std::string& fname) {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

/**
 * Posix环境类，继承环境接口类
 */
class PosixEnv : public Env {
 public:
  PosixEnv();
  /**
   * 析构函数
   *
   * 正常情况下这个类是单例的，不会调用析构函数
   * 首先输出错误信息到错误输出
   * 然后调用abort
   */
  virtual ~PosixEnv() {
    char msg[] = "Destroying Env::Default()\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
  }

  /**
   * 创建一个名为fname的顺序读文件类对象，并且将result的内容设置成顺序读文件类对象的指针
   *
   * 调用fopen只读打开名为fname的文件，如果打开失败返回出错
   * 然后new一个顺序读文件类的对象，将对象的指针存到result的内容中
   */
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  /**
   * 创建一个名为fname的随机读文件类对象，并且将result的内容设置成随机读文件类对象的指针
   *
   * 调用open只读打开名为fname的文件，如果打开失败返回出错
   * 然后尝试获取一个调用mmap的机会，如果获取失败则创建一个普通的随机访问文件类对象
   * 否则调用GetFileSize获得文件的大小，如果获取失败则调用close关闭打开的文件释放调用mmap的机会
   * 如果成功则调用mmap，如果返回失败则调用close关闭打开的文件释放调用mmap的机会并返回出错
   * 否则创建一个基于mmap的随机读文件类对象
   */
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (mmap_limit_.Acquire()) {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
    }
    return s;
  }

  /**
   * 创建一个可写文件类对象，并将result的内容设置成对象的指针
   *
   * 首先调用fopen只写打开一个名为fname的文件类对象
   * 如果打开失败则返回出错
   * 如果打开成功则创建一个可写文件类对象，并将result的内容设置成对象的指针
   */
  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, f);
    }
    return s;
  }

  /**
   * 创建一个可追加文件类对象，并将result的内容设置成对象的指针
   *
   * 首先调用fopen追加打开一个名为fname的文件类对象
   * 如果打开失败则返回出错
   * 如果打开成功则创建一个可写文件类对象，并将result的内容设置成对象的指针
   */
  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result) {
    Status s;
    FILE* f = fopen(fname.c_str(), "a");
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, f);
    }
    return s;
  }

  /**
   * 检测名为fname的文件是否存在
   *
   * 调用access函数实现，如果返回0说明存在，否则说明不存在
   */
  virtual bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  /**
   * 获取dir中所有孩子的名字，放到result中
   *
   * 首先清空result的内容
   * 调用根据dir调用opendir打开目录
   * 如果打开失败返回出错
   * 然后循环调用readdir获得一个元素，如果不为空则将它的名字添加到result中
   * 否则跳出循环
   * 最后调用closedir关闭目录
   */
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  /**
   * 删除名为fname的文件
   *
   * 调用unlink将名为fname的文件删除，如果返回值不为0则返回出错
   */
  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  /**
   * 创建名为name的目录
   *
   * 调用mkdir创建名为name的目录，如果返回值不为0则返回出错
   */
  virtual Status CreateDir(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  /**
   * 删除名为name的目录
   *
   * 调用rmdir删除名为name的目录，如果返回值不为0则返回出错
   */
  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  /**
   * 获得名为fname的文件的大小
   *
   * 调用stat获得一个stat结构体，如果返回值不为0则返回出错
   * 否则将size的内容设置成stat的st_size
   */
  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  /**
   * 将名为src的文件重命名为target
   *
   * 调用rename对文件重命名，如果返回值不为0则返回出错
   */
  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  /**
   * 对名为fname的文件加锁，并且让lock的内容指向新创建的文件锁
   *
   * 首先调用open读写打开名为fname的文件，如果打开失败则返回出错
   * 然后将fname添加到locks_中，如果发现里面已经存在则调用close关闭打开的文件，并且返回出错
   * 调用LockOrUnlock对fd加锁，如果失败了则先调用close关闭打开的文件，并且将fname从locks_中删除，并且返回出错
   * 创建一个文件锁对象，并且将fd和fname都送给他，最后让lock的内容指向它
   */
  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  /**
   * 将文件解锁
   *
   * 首先调用LockOrUnlock将文件描述符解锁
   * 然后将文件名从locks中删除
   * 最后调用close将文件描述符关闭，析构原来分配的文件锁
   */
  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  /**
   * 获得测试目录名称到result
   *
   * 首先调用getenv获得env设置到result中，如果获取不到则将result设置成默认目录
   * 然后调用CreateDir确保目录已经被创建
   */
  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  /**
   * 获取线程id
   *
   * 首先调用pthread_self获得pthread_t类型的线程id
   * 然后将pthread_t的大小和8字节中选一个小的作为拷贝长度
   * 将pthread_t类型的线程id拷贝到一个uint64
   * 返回uint64类型的线程id
   */
  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  /**
   * 创建一个新的Logger
   *
   * 首先调用fopen只写打开名为fname的文件，如果打开失败返回出错，
   * 否则创建一个Logger，将result的内容指向它
   */
  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  /**
   * 获得当前的微秒数
   *
   * 调用gettimeofday获得当前时间
   * 然后将秒数乘以1000000之后加上微秒数
   */
  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  /**
   * 睡眠指定的微秒数
   *
   * 调用usleep实现
   */
  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

 private:
  /**
   * 对pthread调用的结果进行检查，如果其结果出错，则在这里报错并且调用abort退出
   */
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  /**
   * 对BGThread的封装
   *
   * 让参数arg去调用BGThread
   */
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  /**
   * 保护后面几个变量的互斥锁
   */
  pthread_mutex_t mu_;
  /**
   * 用于唤醒后台线程的信号量
   */
  pthread_cond_t bgsignal_;
  /**
   * 用于存储后台线程
   */
  pthread_t bgthread_;
  /**
   * 用于存储后台线程是否已经开始
   */
  bool started_bgthread_;

  // Entry per Schedule() call
  /**
   * 需要在后台运行的事项
   *
   * 包含一个参数指针和一个函数指针
   */
  struct BGItem { void* arg; void (*function)(void*); };
  /**
   * 定义后台运行队列的数据结构
   */
  typedef std::deque<BGItem> BGQueue;
  /**
   * 用于存储后台运行队列
   */
  BGQueue queue_;

  /**
   * 存储上了文件锁的文件名的集合
   */
  PosixLockTable locks_;
  /**
   * 存储mmap调用次数限制器
   */
  MmapLimiter mmap_limit_;
};

/**
 * 构造函数
 *
 * 设置后台线程还没有启动
 * 并且初始化互斥锁和条件变量
 */
PosixEnv::PosixEnv() : started_bgthread_(false) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

/**
 * 给后台线程添加一个新任务
 *
 * 首先加上互斥锁
 * 然后判断后台线程是否已经启动了，如果没有启动则设置成启动了并调用pthread_create创建一个后台线程，并且指定一个函数和它的参数让它运行
 * 然后判断当前队列是否为空，如果为空则调用pthread_cond_signal唤醒后台线程
 * 并且给队列添加一个元素
 * 最后在退出函数之前解锁
 */
void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

/**
 * 后台线程执行的内容
 *
 * 最外层是一个while循环表示年复一年的运行
 * 下面描述内层的内容
 * 首先加锁
 * 然后在while循环中调用pthread_cond_wait，来等待队列不为空，并且防止假唤醒
 * 如果有人唤醒了，则从队列中取出第一个元素
 * 然后解锁
 * 最后调用从队列里面取出来的元素
 */
void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
/**
 * 用于存储开始线程的状态
 */
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}

/**
 * 开始线程的wrapper
 *
 * 调用传进来的StartThreadState
 * 并且析构它并且返回空
 */
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

/**
 * 开始一个线程执行传入的参数
 *
 * 首先将传入的参数构建一个StartThreadState，
 * 然后调用pthread_create创建一个线程并且让他执行StartThreadWrapper并且传入刚创建的StartThreadState
 */
void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

/**
 * 存储单例所需的pthread_once_t
 */
static pthread_once_t once = PTHREAD_ONCE_INIT;
/**
 * 默认环境，存储单例所需的实际内容
 */
static Env* default_env;
/**
 * 初始化默认环境，单例过程实际调用的函数
 */
static void InitDefaultEnv() { default_env = new PosixEnv; }

/**
 * 获得当前默认单例环境
 *
 * 利用pthread_once调用InitDefaultEnv
 * 并且返回default_env
 */
Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb
