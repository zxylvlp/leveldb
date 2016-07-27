// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_TESTHARNESS_H_
#define STORAGE_LEVELDB_UTIL_TESTHARNESS_H_

#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "util/random.h"

namespace leveldb {
namespace test {
// 这里TEST宏用于生成测试用例，它会生成一个类，并且注册(通过RegisterTest)到全局
// RunAllTests可以运行所有注册了的测试用例
// ASSERT则是通过Tester对象实现的，每次先创建一个临时对象，然后调用它的成员函数判断，利用<<可以追加错误信息
// Test这个结构就是在全局存储注册的test信息的载体
// Run some of the tests registered by the TEST() macro.  If the
// environment variable "LEVELDB_TESTS" is not set, runs all tests.
// Otherwise, runs only the tests whose name contains the value of
// "LEVELDB_TESTS" as a substring.  E.g., suppose the tests are:
//    TEST(Foo, Hello) { ... }
//    TEST(Foo, World) { ... }
// LEVELDB_TESTS=Hello will run the first test
// LEVELDB_TESTS=o     will run both tests
// LEVELDB_TESTS=Junk  will run no tests
//
// Returns 0 if all tests pass.
// Dies or returns a non-zero value if some test fails.
extern int RunAllTests();

// Return the directory to use for temporary storage.
extern std::string TmpDir();

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
extern int RandomSeed();

// An instance of Tester is allocated to hold temporary state during
// the execution of an assertion.
// 在assert的时候创建的临时对象，可以持有一些临时状态
class Tester {
 private:
  // 这个测试是否成功
  bool ok_;
  // 当前的文件名
  const char* fname_;
  // 当前的行号
  int line_;
  // 待输出buffer
  std::stringstream ss_;

 public:
  // 构造函数，传入当前文件名和行号
  Tester(const char* f, int l)
      : ok_(true), fname_(f), line_(l) {
  }

  // 析构的时候如果检测到了错误才会报错并且退出
  // 因为对象是临时对象，用完了立马就会析构，所以放在这里也算是一个好主意
  ~Tester() {
    if (!ok_) {
      fprintf(stderr, "%s:%d:%s\n", fname_, line_, ss_.str().c_str());
      exit(1);
    }
  }

  // 这个函数检测b是否为true
  Tester& Is(bool b, const char* msg) {
    if (!b) {
      ss_ << " Assertion failure " << msg;
      ok_ = false;
    }
    return *this;
  }

  // 这个函数检测状态是否为ok
  Tester& IsOk(const Status& s) {
    if (!s.ok()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

// 这个宏也是相当有意思，利用它可以少写很多的代码，因为下面的多个算符的模板实现
// 只有名字和算符不同所以可以使用一个宏来搞
// 又为了这些算符能够接受任何参数所以利用了成员函数模板
#define BINARY_OP(name,op)                              \
  template <class X, class Y>                           \
  Tester& name(const X& x, const Y& y) {                \
    if (! (x op y)) {                                   \
      ss_ << " failed: " << x << (" " #op " ") << y;    \
      ok_ = false;                                      \
    }                                                   \
    return *this;                                       \
  }

  BINARY_OP(IsEq, ==)
  BINARY_OP(IsNe, !=)
  BINARY_OP(IsGe, >=)
  BINARY_OP(IsGt, >)
  BINARY_OP(IsLe, <=)
  BINARY_OP(IsLt, <)
#undef BINARY_OP

  // Attach the specified value to the error message if an error has occurred
  // 利用这个重载算符可以追加错误信息，这也是前面的测试都返回对象引用的原因
  template <class V>
  Tester& operator<<(const V& value) {
    if (!ok_) {
      ss_ << " " << value;
    }
    return *this;
  }
};

// 首先创建一个Tester的临时对象，然后调用相应的成员函数，创建临时对象是为了将__FILE__和__LINE__
// 这些参数绑定进去，然后根据不同的需求调用不同的成员函数
#define ASSERT_TRUE(c) ::leveldb::test::Tester(__FILE__, __LINE__).Is((c), #c)
#define ASSERT_OK(s) ::leveldb::test::Tester(__FILE__, __LINE__).IsOk((s))
#define ASSERT_EQ(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsEq((a),(b))
#define ASSERT_NE(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsNe((a),(b))
#define ASSERT_GE(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsGe((a),(b))
#define ASSERT_GT(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsGt((a),(b))
#define ASSERT_LE(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsLe((a),(b))
#define ASSERT_LT(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsLt((a),(b))

// a##b就是将两个宏参数连接起来
#define TCONCAT(a,b) TCONCAT1(a,b)
#define TCONCAT1(a,b) a##b

// 可以用它定义一个test case
// TEST这个宏非常的有意思，它可以把一段测试的类似函数的代码展开成一个类声明和实现，
// 并且在声明和实现中间调用函数注册这个测试
// 下面举例：
// TEST(ArenaTest, Empty) {
//   Arena arena;
// }
// 这段代码会展开成：
// class _Test_Empty : public ArenaTest {                              \
// public:                                                                \
//  void _Run();                                                          \
//  static void _RunIt() {                                                \
//    _Test_Empty t;                                             \
//    t._Run();                                                           \
//  }                                                                     \
// };                                                                      \
// bool _Test_ignored_Empty =                                     \
//  ::leveldb::test::RegisterTest("ArenaTest", "Empty", &_Test_Empty::_RunIt); \
// void _Test_Empty::_Run() {
//   Arena arena;
// }
// 将最先传进来的base作为继承的基类
// 然后将_Test_和name做连接，形成新的子类名字
// _Run函数是测试的具体函数需要自己实现
// _RunIt函数是测试的静态函数，因为是静态的所以可以当做函数指针直接注册
// 在_RunIt函数中会创建一个子类对象并且调用它的_Run函数进行测试
// 在做完类声明之后调用RegisterTest将_RunIt注册为一个test case
// 最后写上_Run实现的开头，后面由我们自己在宏后面写
#define TEST(base,name)                                                 \
class TCONCAT(_Test_,name) : public base {                              \
 public:                                                                \
  void _Run();                                                          \
  static void _RunIt() {                                                \
    TCONCAT(_Test_,name) t;                                             \
    t._Run();                                                           \
  }                                                                     \
};                                                                      \
bool TCONCAT(_Test_ignored_,name) =                                     \
  ::leveldb::test::RegisterTest(#base, #name, &TCONCAT(_Test_,name)::_RunIt); \
void TCONCAT(_Test_,name)::_Run()

// Register the specified test.  Typically not used directly, but
// invoked via the macro expansion of TEST.
extern bool RegisterTest(const char* base, const char* name, void (*func)());


}  // namespace test
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_TESTHARNESS_H_
