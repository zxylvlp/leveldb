// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template<typename Key, class Comparator>
/**
 * 这个跳表，在一个时间点，只能有一个写者(有一个mutex)保证
 * 但是对于读者则没有限制，不需要加锁，使用atomic_pointer的release_store和acquire_load去同步
 */
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  /**
   * 跳表的迭代器的声明
   */
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    /**
     * 用于存放当前迭代器指向的跳表的指针
     */
    const SkipList* list_;
    /**
     * 用于存放当前迭代器指向的节点的指针
     */
    Node* node_;
    // Intentionally copyable
  };

 private:
  /**
   * 用于存放跳表中一个节点的最大高度
   */
  enum { kMaxHeight = 12 };

  // Immutable after construction
  /**
   * 用于存放跳表中各个节点key的比较函数
   */
  Comparator const compare_;
  /**
   * 用于存放跳表中节点内存分配器
   */
  Arena* const arena_;    // Arena used for allocations of nodes

  /**
   * 用于存放跳表的头结点，这个头结点是一个dummy节点
   */
  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  /**
   * 当前所有节点的中的最高节点的节点高度，被写者修改，但是读者读到脏值也是允许的，这是为什么呢？
   * 因为跳表最底下一层是链表，拿到大一点的高度可以加速查询，低一点顶多是不够快，没什么大不了的
   */
  port::AtomicPointer max_height_;   // Height of the entire list

  /**
   * 为什么用NoBarrier_Load的原因见上面的解释
   */
  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  /**
   * 插入元素的时候的生成随机高度的随机数生成器
   */
  Random rnd_;

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  /**
   * 判断a, b两个key是否相等
   */
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  /**
   * 禁止拷贝
   */
  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
/**
 * 跳表中节点的定义
 */
template<typename Key, class Comparator>
struct SkipList<Key,Comparator>::Node {
  /**
   * 跳表节点的构造方法，传入一个key
   */
  explicit Node(const Key& k) : key(k) { }

  /**
   * 跳表节点的key值
   */
  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  /**
   * 获得当前节点中，下一个节点指针数组中的第n指针，使用acquire语义来保证之下的代码不被乱序到读取之前，
   * 使读到的数据符合时间顺序
   */
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  /**
   * 设置当前节点中，下一个节点指针数组中的第n个指针为x，使用release语义来保证之上的代码不被乱序到设置之后
   */
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  /**
   * 获得当前节点中，下一个节点指针数组中的第n指针
   */
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  /**
   * 设置当前节点中，下一个节点指针数组中的第n个指针为x
   */
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  /**
   * 指向后面跳表节点的指针数组，柔性数组
   */
  port::AtomicPointer next_[1];
};

/**
 * 创建一个跳表节点
 *
 * 先分配内存，然后构造，这样搞是因为我们要自己搞一个柔性数组
 */
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  return new (mem) Node(key);
}

/**
 * 跳表迭代器的构造函数，仅仅将list_设置为传入的跳表
 */
template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = NULL;
}

/**
 * 验证当前迭代器指向的节点是否是有效的(不为null)
 */
template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

/**
 * 获得当前迭代器指向的节点的key
 *
 * 这个函数调用的前提是迭代器是有效的
 */
template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

/**
 * 将当前迭代器指向的节点置为下一个节点
 *
 * 直接找最下层的指针即可，这个函数调用的前提是迭代器是有效的
 */
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

/**
 * 将当前迭代器指向的节点置为前一个节点
 *
 * 因为跳表是没有办法向前移动的，
 * 所以我们调用FindLessThan获得小于当前节点的节点，如果找到的节点是跳表的head_即dummy，
 * 说明前面没有节点了，指向NULL即可
 */
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

/**
 * 将当前迭代器指向的节点置为，key大于等于target的节点
 *
 * 调用FindGreaterOrEqual找到并将其设置为当前指向的节点
 */
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

/**
 * 将迭代器指向第一个元素
 *
 * 即从dummy节点找到下一个节点就行了，如果跳表为空，则指向NULL
 */
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

/**
 * 将迭代器指向最后一个元素
 *
 * 调用FindLast获得最后一个元素的指针，
 * 如果最后一个元素是dummy说明跳表为空，则指向NULL
 */
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

/**
 * 返回一个随机高度
 *
 * 3/4的可能性高度为1，1/4*3/4的可能性高度为2，1/4*1/4*3/4的可能性高度为3....(1/4)^10*3/4的可能性高度为11，(1/4)^11的可能性高度为12
 * 就是说1层的元素数是2层的元素数的4倍，是3层的元素数的16倍
 * 返回值的值域是[1, 12]
 */
template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

/**
 * 判断Key是否在Node的Key的后面
 *
 * 如果Node是NULL则直接返回false，因为NULL被认为无限大(NULL在尾巴的后面)
 * 否则利用compare_判断两者的大小
 */
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

/**
 * 找到大于等于key的节点，prev是输出参数，内容是这个返回节点的前面节点的数组
 *
 * 从dummy节点出发，得到最高高度level，找到下一个节点next，
 * 如果目标key比next节点大，则将当前节点设置为下一个节点
 * 否则，先将prev[level]设置为当前节点
 * 然后判断是否当前是最低层，如果是最低层则返回next节点
 * 否则下降一层继续从当前节点出发找下一个节点next
 * 边界条件：如果跳表为空或者全部小于Key，则返回NULL
 */
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

/**
 * 找到小于key的节点
 *
 * 如果跳表为空或者找不到则返回dummy
 */
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

/**
 * 找到最后一个元素
 *
 * 从dummy出发，获得最高层的高度level，从最上面的一层找到尾巴(即它的next为NULL)，
 * 找到尾巴之后从最上层的尾巴节点开始，下降一层继续找尾巴，一直下去，直到第0层,返回第0层的尾巴。
 * 这里的边界是如果表为空则返回dummy。
 */
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

/**
 * 跳表的构造函数
 *
 * 除了传入必要的参数外，要创建一个dummy节点并且初始化它
 */
template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);
  }
}

/**
 * 插入一个key的函数
 *
 * 首先调用FindGreaterOrEqual找到大于key的节点和这个节点前面的所有节点
 * 因为不允许重复插入，所以要么这个节点找不到，要么这个节点的key不等于key
 * 调用RandomHeight获得要插入的节点的高度
 * 调用GetMaxHeight获得跳表当前的最大高度
 * 如果要插入的节点的高度大于跳表当前的最大高度的时候
 * 对prev的上面几层设置为dummy(因为前面没有那么高的)
 * 并将跳当前的最大高度设置为插入节点的高度
 * 创建一个新节点，将新节点的next数组设置为prev的next
 * 并将prev的每一个next设置为x
 *
 */
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

/**
 * 判断跳表中是否包含key
 *
 * 调用FindGreaterOrEqual获得大于等于它的节点，如果找不到则不包含
 * 如果找到的节点的key与key不相等则不包含
 * 否则是包含的
 */
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
