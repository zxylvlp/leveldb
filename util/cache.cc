// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

/**
 * 实现Cache的析构函数
 */
Cache::~Cache() {
}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
/**
 * LRU中的一条数据项
 */
struct LRUHandle {
  /**
   * 指向k v对的value的指针
   */
  void* value;
  /**
   * k v对的析构函数
   */
  void (*deleter)(const Slice&, void* value);
  /**
   * 在hash表中，因为使用了开链方式，在其中当做一个链表处理
   */
  LRUHandle* next_hash;
  /**
   * 因为这是一条双链表，所以有前后指针
   */
  LRUHandle* next;
  LRUHandle* prev;
  /**
   * value实际占用的内存大小
   */
  size_t charge;      // TODO(opt): Only allow uint32_t?
  /**
   * key的长度
   */
  size_t key_length;
  /**
   * 表示是否在cache中，如果不在则在Unref的过程中要调用deleter析构k v对并且回收Handle占用的内存
   */
  bool in_cache;      // Whether entry is in the cache.
  /**
   * 当前这个Handle被引用的次数
   */
  uint32_t refs;      // References, including cache reference, if present.
  /**
   * key的hash值
   */
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  /**
   * key的实际数据，柔性数组
   */
  char key_data[1];   // Beginning of key

  /**
   * 利用key构造一个Slice对象返回，数据并不拷贝
   * 如果是next==this则表示这个Handle是为了快速查找构造的一个临时对象，在value中存key的slice
   */
  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
/**
 * 保存Handle的hash表
 */
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  /**
   * 从hash table中查找key, hash对应的handle的指针
   *
   * 首先调用FindPointer获得key, hash对应Handle的指针的地址，然后取出相应的指针返回
   */
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  /**
   * 将h插入到hash table中，
   * 如果hash table中没有h对应的元素则将元素插入到其中一个链表的末尾，并且返回NULL，
   * 如果有对应的元素，则将h插入到对应的元素的位置，并将对应的元素踢出来，并且返回对应的元素的指针
   *
   * 首先调用FindPointer获得h对应的Handle的指针的地址，取出其中的指针，传给old
   * 判断old是否为NULL？如果是null则说明以前没有h对应的Handle，否则说明以前有h对应的Handle
   * 然后把h插入old的位置上，如果old为null则将元素数加1，如果这个时候elems_数量大于length_则调用Resize
   * 最后返回old
   */
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  /**
   * 从hash table中删除key hash对应的Handle，
   * 如果删除成功(即存在)返回被删除的Handle的指针，
   * 否则返回NULL
   *
   * 首先调用FindPointer找到Handle的指针的地址
   * 将地址中的指针取出来，如果指针为空则返回NULL，
   * 否则取得指针指向的内容中的next_hash，将指针地址上的指针设置成它，并将elems_减1，
   * 返回刚刚取出来的指针，即被删除节点的指针
   */
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  /**
   * 链表数组的长度
   */
  uint32_t length_;
  /**
   * 总共有多少个元素
   */
  uint32_t elems_;
  /**
   * 保存了一个链表数组
   * 第一层指针就是数组地址
   * 第二次指针就是链表节点的指针
   */
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  /**
   * 返回lru table中hash与key都匹配的handle指针的地址(lru table中存指针)
   *
   * 首先利用hash sharding到一个链表，ptr是指向这个数组指定位置的指针，
   * 到后面ptr是上一个节点的next_hash的地址
   * 如果ptr的内容不为空(有一个不为空的链表或者下一个节点不为空)，
   * 并且ptr指向内容的内容里面hash和key都与传入参数比较，有一个不相等则将ptr置为ptr指向内容的内容里面next_hash的地址
   * 最终找到的是与key和hash相等的节点的上一个节点(有匹配)或最后一个节点(没有匹配)的next_hash域的地址（如果链表不为空）
   * 或者是指向list_中一个元素的地址(如果链表为空)
   */
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  /**
   * 完成hash table的resize即rehash
   *
   * 不停将new_length乘以2直到大于elem_
   * 然后创建一个new_length长的指针数据，并且初始化为0
   * 循环访问list_中的每一个链表
   * 对每一个链表节点的指针，先将它的next_hash缓存起来，
   * 然后将它sharding到新的指针数组中一个链表上，并且放到新链表的表头
   * 接着继续访问刚才缓存起来的next_hash指向的节点
   * 最后将老的指针数组list_析构掉
   * 新的指针数组设置为list_
   * 将新的指针数组的长度设置为length_
   */
  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
/**
 * LRUCache的实现
 */
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  /**
   * 设置容量
   */
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  /**
   * 获得使用了多少内存
   */
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  // Initialized before use.
  /**
   * 可以保存的内存的容量
   */
  size_t capacity_;

  // mutex_ protects the following state.
  /**
   * 保护后面列举的状态的互斥锁
   */
  mutable port::Mutex mutex_;
  /**
   * 当前使用了多少内存
   */
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  /**
   * lru链表的dummy头节点
   * lur.prev是最新的节点，lru.next是最旧的节点
   * 这里面保存的节点都是refs==1并且in_cache==true的
   */
  LRUHandle lru_;

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  /**
   * 正在使用的链表的dummy头结点
   * 这里面保存的节点都是客户端使用的节点，他们的refs>=2并且in_cache==true
   */
  LRUHandle in_use_;

  /**
   * hash表
   */
  HandleTable table_;
};

/**
 * 构造函数
 *
 * 将lru_和in_use_的next和prev指向自己
 * 构成空环
 */
LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

/**
 * 析构函数
 *
 * 这里只对lru_里面的元素处理，因为我们认为调用析构函数的时候in_use_里面就是空的了
 * 对lru_里面的每一个元素，将他们的in_cache设为false，并且调用Unref减低引用并且析构
 */
LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

/**
 * 引用e元素
 *
 * 如果refs==1并且in_cache==true则说明，
 * 在lru_里面，调用LRU_Remove将其从中删除，
 * 并且调用LRU_Append将其添加到in_use_中
 * 最后将refs+1
 */
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

/**
 * 减少e元素的引用
 *
 * 首先将refs减1
 * 判断当前的refs，如果为0，则调用deleter析构value，并且将e指向Handle的内存释放掉
 * 如果是1并且in_cache，则将e从in_use_中删掉加入到lru_中
 */
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {  // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

/**
 * 将e从所在链表中删掉
 *
 * 将下一个元素的前一个指针指向前一个元素
 * 将前一个元素的下一个指针指向下一个元素
 */
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

/**
 * 将e添加到list链表的头部
 *
 * 将e的下一个元素指针置为list
 * 将e的前一个元素指针置为list的前一个元素
 * 将e现在的前一个元素的下一个元素指针设置为e
 * 将e现在的下一个元素的前一个元素指针设置为e
 */
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

/**
 * 查找hash和key对应的Handle的指针
 *
 * 因为每一次查找都要修改链表，所以肯定要上锁
 * 然后去hash表中查找key，如果没找到则返回null
 * 调用Ref对查找到的handle指针增加一次引用
 * 并且返回查找到的handle指针
 */
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

/**
 * 释放拿到的Handle的指针
 *
 * 首先上锁，然后调用Unref减少一次引用
 */
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

/**
 * 将kv对插入Cache中
 *
 * 首先加锁，然后分配一个LRUHandle结构，将其中的内容进行设置，然后将in_cache设为false，将refs设为1，并且拷贝key到key_data
 *
 */
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } // else don't cache.  (Tests use capacity_==0 to turn off caching.)

  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != NULL, finish removing *e from the cache; it has already been removed
// from the hash table.  Return whether e != NULL.  Requires mutex_ held.
/**
 * 将e从lru_中删除
 *
 * 如果e为NULL则返回false
 * 否则将e从lru_中删除，
 * 然后将in_cache设置为false，
 * 并且将usage_减e中charge的大小，
 * 最后调用Unref释放内存，返回true
 *
 */
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != NULL;
}

/**
 * 从Cache中将hash key对应的元素删除
 *
 * 首先加锁，然后从table_中删除key handle*对，然后调用FinishErase从lru_中删除
 */
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

/**
 * 对lru_中的数据进行清空
 *
 * 首先加锁，然后遍历lru，
 * 从头开始，对每一个元素先从hash table中删除，
 * 然后调用FinishErase从lru中删除
 */
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
