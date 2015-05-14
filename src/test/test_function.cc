// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <type_traits>

#include "common/cohort_function.h"
#include "count_new.h"
#include "min_allocator.h"
#include "gtest/gtest.h"

// Call tests from libc++
namespace static_invoke {
  template<typename T, int N>
  struct Array {
    typedef T type[N];
  };

  struct Type {
    Array<char, 1>::type& f1();
    Array<char, 2>::type& f2() const;

    Array<char, 1>::type& g1()&;
    Array<char, 2>::type& g2() const&;
    Array<char, 3>::type& g3()&&;
    Array<char, 4>::type& g4() const&&;
  };

  void checks() {
    static_assert(sizeof(cohort::invoke(&Type::f1,
					std::declval<Type>())) == 1, "");
    static_assert(sizeof(cohort::invoke(&Type::f2,
					std::declval<Type const>())) == 2, "");
    static_assert(sizeof(cohort::invoke(&Type::g1,
					std::declval<Type&>())) == 1, "");
    static_assert(sizeof(cohort::invoke(&Type::g2,
					std::declval<Type const &>())) == 2,
		  "");
    static_assert(sizeof(cohort::invoke(&Type::g3,
					std::declval<Type&&>())) == 3, "");
    static_assert(sizeof(cohort::invoke(&Type::g4,
					std::declval<Type const&&>())) == 4,
		  "");
  }

  struct meow {
    int mem1(int c) {
      return c;
    }
    int mem2() {
      return 5;
    }
    int operator()(int c) {
      return c * 2;
    }
  };

  int fun() {
    return 10;
  }

  TEST(Invoke, MemberFunctionWithArgs) {
    meow m;
    EXPECT_EQ(cohort::invoke(&meow::mem1, m, 5), 5);
  }

  TEST(Invoke, MemberFunctionWithArgsPtrObj) {
    meow m;
    EXPECT_EQ(cohort::invoke(&meow::mem1, &m, 5), 5);
  }

  TEST(Invoke, MemberFunctionNoArgs) {
    meow m;
    EXPECT_EQ(cohort::invoke(&meow::mem2, m), 5);
  }

  TEST(Invoke, MemberFunctionNoArgsPtrObj) {
    meow m;
    EXPECT_EQ(cohort::invoke(&meow::mem2, &m), 5);
  }

  TEST(Invoke, Functor) {
    meow m;
    EXPECT_EQ(cohort::invoke(m, 5), 10);
  }

  TEST(Invoke, Function) {
    EXPECT_EQ(cohort::invoke(fun), 10);
  }
};


// std::function tests from libc++ (basically we want to make sure we
// maintain full compatibility with std::function EXCEPT when we want
// not to.)

// Static typecheck tests

namespace static_function_type {
  template<typename T>
  class has_argument_type {
    typedef char yes;
    typedef long no;

    template <typename C>
    static yes check(typename C::argument_type*);
    template <typename C>
    static no check(...);
  public:
    enum {
      value = sizeof(check<T>(0)) == sizeof(yes)
    };
  };

  template<typename T>
  class has_first_argument_type {
    typedef char yes;
    typedef long no;

    template <typename C>
    static yes check(typename C::first_argument_type * );
    template <typename C>
    static no check(...);
  public:
    enum {
      value = sizeof(check<T>(0)) == sizeof(yes)
    };
  };


  template<typename T>
  class has_second_argument_type
  {
    typedef char yes;
    typedef long no;

    template <typename C>
    static yes check( typename C::second_argument_type *);
    template <typename C>
    static no check(...);
  public:
    enum {
      value = sizeof(check<T>(0)) == sizeof(yes)
    };
  };

  template<class F, class return_type>
  void test_nullary_function () {
    static_assert((std::is_same<typename F::result_type,
		   return_type>::value), "");
    static_assert((!has_argument_type<F>::value), "");
    static_assert((!has_first_argument_type<F>::value), "");
    static_assert((!has_second_argument_type<F>::value), "");
  }

  template<class F, class return_type, class arg_type>
  void test_unary_function () {
    static_assert((std::is_same<typename F::result_type, return_type>::value),
		  "");
    static_assert((std::is_same<typename F::argument_type,  arg_type>::value),
		  "");
    static_assert((!has_first_argument_type<F>::value), "");
    static_assert((!has_second_argument_type<F>::value), "");
  }

  template<class F, class return_type, class arg_type1, class arg_type2>
  void test_binary_function () {
    static_assert((std::is_same<typename F::result_type,
		   return_type>::value), "");
    static_assert((std::is_same<typename F::first_argument_type,
		   arg_type1>::value), "");
    static_assert((std::is_same<typename F::second_argument_type,
		   arg_type2>::value), "");
    static_assert((!has_argument_type<F>::value), "");
  }

  template<class F, class return_type>
  void test_other_function () {
    static_assert((std::is_same<typename F::result_type, return_type>::value),
		  "" );
    static_assert((!has_argument_type<F>::value), "" );
    static_assert((!has_first_argument_type<F>::value), "" );
    static_assert((!has_second_argument_type<F>::value), "" );
  }

  void checks() {
    test_nullary_function<cohort::function<int()>, int>();
    test_unary_function<cohort::function<double(int)>, double, int>();
    test_binary_function<cohort::function<double(int, char)>,
			 double, int, char>();
    test_other_function<cohort::function<double(int, char, double)>, double>();
  }
};

// Function swap tests

namespace function_swap {
  class A {
    int data[10];
  public:
    static int count;

    explicit A(int j) {
      ++count;
      data[0] = j;
    }

    A(const A& a) {
      ++count;
      for (int i = 0; i < 10; ++i)
	data[i] = a.data[i];
    }

    ~A() {
      --count;
    }

    int operator()(int i) const {
      for (int j = 0; j < 10; ++j)
	i += data[j];
      return i;
    }

    int id() const {
      return data[0];
    }
  };

  int A::count = 0;

  int g(int) {
    return 0;
  }
  int h(int) {
    return 1;
  }

  TEST(swap, SameFunctorClass) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f1 = A(1);
    cohort::function<int(int)> f2 = A(2);
    EXPECT_EQ(A::count, 2);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(2));
    EXPECT_EQ(f1.target<A>()->id(), 1);
    EXPECT_EQ(f2.target<A>()->id(), 2);
    swap(f1, f2);
    EXPECT_EQ(A::count, 2);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(2));
    EXPECT_EQ(f1.target<A>()->id(), 2);
    EXPECT_EQ(f2.target<A>()->id(), 1);
  }

  TEST(swap, FunctorAndFunction) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f1 = A(1);
    cohort::function<int(int)> f2 = g;
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_EQ(f1.target<A>()->id(), 1);
    EXPECT_TRUE(*f2.target<int(*)(int)>() == g);
    swap(f1, f2);
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(*f1.target<int(*)(int)>() == g);
    EXPECT_EQ(f2.target<A>()->id(), 1);
  }

  TEST(swap, FunctonAndFunctor) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f1 = g;
    cohort::function<int(int)> f2 = A(1);
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(*f1.target<int(*)(int)>() == g);
    EXPECT_TRUE(f2.target<A>()->id() == 1);
    swap(f1, f2);
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_EQ(f1.target<A>()->id(), 1);
    EXPECT_TRUE(*f2.target<int(*)(int)>() == g);
  }

  TEST(swap, FunctionAndFunction) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f1 = g;
    cohort::function<int(int)> f2 = h;
    EXPECT_EQ(A::count, 0);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(*f1.target<int(*)(int)>() == g);
    EXPECT_TRUE(*f2.target<int(*)(int)>() == h);
    swap(f1, f2);
    EXPECT_EQ(A::count, 0);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(*f1.target<int(*)(int)>() == h);
    EXPECT_TRUE(*f2.target<int(*)(int)>() == g);
  }
};

namespace test_bool {
  int g(int) {
    return 0;
  }

  TEST(TestBool, TestBool) {
    cohort::function<int(int)> f;
    ASSERT_FALSE(f);
    f = g;
    ASSERT_TRUE((bool)f);
  }
};

namespace FConstruct {
  class A {
    int data[10];
  public:
    static int count;

    A() {
      ++count;
      for (int i = 0; i < 10; ++i)
	data[i] = i;
    }

    A(const A&) {
      ++count;
    }

    ~A() {
      --count;
    }

    int operator()(int i) const {
      for (int j = 0; j < 10; ++j)
	i += data[j];
      return i;
    }

    int foo(int) const {
      return 1;
    }
  };

  int A::count = 0;

  int g(int) {
    return 0;
  }

  TEST(FConstruct, functor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = A();
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }

  TEST(FConstrust, functionAssign) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = g;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(FConstruct, NullFunction) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = (int (*)(int))0;
    EXPECT_FALSE(f);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(FConstruct, MemberFunction) {
    globalMemCounter.reset();
    cohort::function<int(const A*, int)> f = &A::foo;
    EXPECT_TRUE(!!f);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int (A::*)(int) const>() != 0);
  }

  TEST(FConstrust, functionConstruct) {
    cohort::function<int(int)> f(&g);
    EXPECT_TRUE(!!f);
    EXPECT_TRUE(f.target<int(*)(int)>() != 0);
    EXPECT_EQ(f(1), 0);
  }
};

namespace FAssign {
  class A {
    int data[10];
  public:
    static int count;

    A() {
      ++count;
      for (int i = 0; i < 10; ++i)
	data[i] = i;
    }

    A(const A&) {
      ++count;
    }

    ~A() {
      --count;
    }

    int operator()(int i) const {
      for (int j = 0; j < 10; ++j)
	i += data[j];
      return i;
    }

    int foo(int) const {
      return 1;
    }
  };

  int A::count = 0;

  int g(int) {
    return 0;
  }

  TEST(FAssign, Functor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f;
    f = A();
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }

  TEST(FAssign, Function) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f;
    f = g;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(FAssign, NullFunction) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f;
    f = (int (*)(int))0;
    EXPECT_FALSE(f);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(FAssign, MemFunction) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(const A*, int)> f;
    f = &A::foo;
    EXPECT_TRUE(!!f);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int (A::*)(int) const>() != 0);
  }

  TEST(FAssign, FunctionPtrCall) {
    cohort::function<int(int)> f;
    f = &g;
    EXPECT_TRUE(!!f);
    EXPECT_TRUE(f.target<int(*)(int)>() != 0);
    EXPECT_EQ(f(1), 0);
  }
};

namespace Incomplete {
  // Allow incomplete argument types in the __is_callable check

  struct X {
    typedef cohort::function<void(X&)> callback_type;
    virtual ~X() {}
  private:
    callback_type _cb;
  };
};

namespace Allocator {
  class A {
    int data[10];
  public:
    static int count;

    A() {
      ++count;
      for (int i = 0; i < 10; ++i)
	data[i] = i;
    }

    A(const A&) {
      ++count;
    }

    ~A() {
      --count;
    }

    int operator()(int i) const {
      for (int j = 0; j < 10; ++j)
	i += data[j];
      return i;
    }

    int foo(int) const {
      return 1;
    }
  };

  int A::count = 0;

  int g(int) {return 0;}

  class Foo {
  public:
    int bar(int k) {
      return k;
    }
  };

  TEST(Allocator, NullAllocatorConstruction) {
    cohort::function<int(int)> f(std::allocator_arg, min_allocator<int>());
    EXPECT_FALSE(f);
  }

  TEST(Allocator, Functor) {
    cohort::function<int(int)> f(std::allocator_arg, min_allocator<A>(), A());
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }

  TEST(Allocator, Function) {
    ASSERT_EQ(A::count, 0);
    cohort::function<int(int)> f(std::allocator_arg,
				 min_allocator<int(*)(int)>(), g);
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(Allocator, NullFunPtr) {
    cohort::function<int(int)> f(std::allocator_arg,
				 min_allocator<int(*)(int)>(),
				 (int (*)(int))0);
    EXPECT_FALSE(f);
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(Allocator, MemberFun) {
    cohort::function<int(const A*, int)> f(
      std::allocator_arg, min_allocator<int(A::*)(int)const>(),
      &A::foo);
    EXPECT_TRUE(!!f);
    EXPECT_TRUE(f.target<int (A::*)(int) const>() != 0);
  }

  TEST(Allocator, MemberFunBindAssign) {
    Foo f;
    cohort::function<int(int)> fun = std::bind(
      &Foo::bar, &f, std::placeholders::_1);
    EXPECT_TRUE(fun(10) == 10);
  }

  TEST(Allocator, FunctionPointer) {
    cohort::function<int(int)> fun(std::allocator_arg,
				   min_allocator<int(*)(int)>(),
				   &g);
    EXPECT_TRUE(!!fun);
    EXPECT_TRUE(fun.target<int(*)(int)>() != 0);
    EXPECT_EQ(fun(10), 0);
  }

  TEST(Allocator, PassFunctor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = A();
    EXPECT_EQ(A::count, 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    cohort::function<int(int)> f2(std::allocator_arg, min_allocator<A>(), f);
    EXPECT_EQ(A::count, 2);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(2));
    EXPECT_TRUE(f2.target<A>());
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
  }

  TEST(Allocator, PassFunction) {
    globalMemCounter.reset();
    ASSERT_EQ(A::count, 0);
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = g;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
    cohort::function<int(int)> f2(std::allocator_arg,
				  min_allocator<int(*)(int)>(), f);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>());
    EXPECT_TRUE(f2.target<A>() == 0);
  }

  TEST(Allocator, NonDefaultFunction) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    min_allocator<cohort::function<int(int)>> al;
    cohort::function<int(int)> f2(std::allocator_arg, al, g);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>());
    EXPECT_TRUE(f2.target<A>() == 0);
  }

  TEST(Allocator, EmptyPass) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    cohort::function<int(int)> f2(std::allocator_arg,
				  min_allocator<int>(), f);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f2.target<A>() == 0);
  }

  TEST(Allocator, NullPointer) {
    cohort::function<int(int)> f(std::allocator_arg,
				 min_allocator<int>(),
				 nullptr);
    EXPECT_FALSE(f);
  }

  TEST(Allocator, RValuePass) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = A();
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    cohort::function<int(int)> f2(std::allocator_arg,
				  min_allocator<A>(),
				  std::move(f));
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f2.target<A>());
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }

  TEST(Allocator, AssignAlloc) {
    ASSERT_TRUE(A::count == 0);
    {
      cohort::function<int(int)> f;
      f.assign(A(), min_allocator<A>());
      EXPECT_TRUE(A::count == 1);
      EXPECT_TRUE(f.target<A>());
      EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    }
    assert(A::count == 0);
  }
};

namespace Copy {
  class A {
    int data[10];
  public:
    static int count;

    A() {
      ++count;
      for (int i = 0; i < 10; ++i)
	data[i] = i;
    }

    A(const A&) {
      ++count;
    }

    ~A() {
      --count;
    }

    int operator()(int i) const {
      for (int j = 0; j < 10; ++j)
	i += data[j];
      return i;
    }
  };

  int A::count = 0;

  int g(int) {
    return 0;
  }

  TEST(Copy, Functor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    cohort::function<int(int)> f = A();
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    cohort::function<int(int)> f2 = f;
    EXPECT_TRUE(A::count == 2);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(2));
    EXPECT_TRUE(f2.target<A>());
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
  }

  TEST(Copy, Function) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = g;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
    cohort::function<int(int)> f2 = f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>());
    EXPECT_TRUE(f2.target<A>() == 0);
  }

  TEST(Copy, Empty) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    cohort::function<int(int)> f2 = f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f2.target<A>() == 0);
  }

  TEST(Copy, Empty2) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    EXPECT_TRUE(!f);
    cohort::function<long(int)> g = f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(g.target<long(*)(int)>() == 0);
    EXPECT_TRUE(g.target<A>() == 0);
    EXPECT_TRUE(!g);
  }

  TEST(Copy, MoveFunctor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = A();
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    cohort::function<int(int)> f2 = std::move(f);
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f2.target<A>());
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }

  TEST(Copy, AssignFunctor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = A();
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    cohort::function<int(int)> f2;
    f2 = f;
    EXPECT_TRUE(A::count == 2);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(2));
    EXPECT_TRUE(f2.target<A>());
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
  }
  TEST(Copy, AssignFunction) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = g;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
    cohort::function<int(int)> f2;
    f2 = f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>());
    EXPECT_TRUE(f2.target<A>() == 0);
  }

  TEST(Copy, AssignEmpty) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    cohort::function<int(int)> f2;
    f2 = f;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f2.target<A>() == 0);
    }

  TEST(Copy, MoveAssignFunctor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = A();
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
    cohort::function<int(int)> f2;
    f2 = std::move(f);
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f2.target<A>());
    EXPECT_TRUE(f2.target<int(*)(int)>() == 0);
    EXPECT_TRUE(f.target<A>() == 0);
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }

  TEST(Nothingness, Default) {
    cohort::function<int(int)> f;
    EXPECT_TRUE(!f);
  }

  TEST(Nothingness, Nullptr) {
    cohort::function<int(int)> f(nullptr);
    EXPECT_TRUE(!f);
  }

  TEST(Nothingness, EmptyAssignToFunctor) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = A();
    EXPECT_TRUE(A::count == 1);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(1));
    EXPECT_TRUE(f.target<A>());
    f = nullptr;
    EXPECT_TRUE(A::count == 0);
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<A>() == 0);
  }

  TEST(Nothingness, EmptyAssignToFunction) {
    globalMemCounter.reset();
    ASSERT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    ASSERT_TRUE(A::count == 0);
    cohort::function<int(int)> f = g;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>());
    EXPECT_TRUE(f.target<A>() == 0);
    f = nullptr;
    EXPECT_TRUE(globalMemCounter.checkOutstandingNewEq(0));
    EXPECT_TRUE(f.target<int(*)(int)>() == 0);
  }
};

namespace Call {
  int count = 0;

  // 1 arg, return void
  void f_void_1(int i) {
    count += i;
  }

  struct A_void_1 {
    void operator()(int i) {
      count += i;
    }

    void mem1() {
      ++count;
    }
    void mem2() const {
      ++count;
    }
  };

  TEST(Call, Testvoid1) {
    int save_count = count;
    // function
    {
      cohort::function<void (int)> r1(f_void_1);
      int i = 2;
      r1(i);
      EXPECT_TRUE(count == save_count+2);
      save_count = count;
    }
    // function pointer
    {
      void (*fp)(int) = f_void_1;
      cohort::function<void (int)> r1(fp);
      int i = 3;
      r1(i);
      EXPECT_TRUE(count == save_count+3);
      save_count = count;
    }
    // functor
    {
      A_void_1 a0;
      cohort::function<void (int)> r1(a0);
      int i = 4;
      r1(i);
      EXPECT_TRUE(count == save_count+4);
      save_count = count;
    }
    // member function pointer
    {
      void (A_void_1::*fp)() = &A_void_1::mem1;
      cohort::function<void (A_void_1)> r1(fp);
      A_void_1 a;
      r1(a);
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
      A_void_1* ap = &a;
      cohort::function<void (A_void_1*)> r2 = fp;
      r2(ap);
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
    }
    // const member function pointer
    {
      void (A_void_1::*fp)() const = &A_void_1::mem2;
      cohort::function<void (A_void_1)> r1(fp);
      A_void_1 a;
      r1(a);
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
      cohort::function<void (A_void_1*)> r2(fp);
      A_void_1* ap = &a;
      r2(ap);
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
    }
  }

  // 1 arg, return int

  int f_int_1(int i) {
    return i + 1;
  }

  struct A_int_1 {
    A_int_1() : data(5) {}
    int operator()(int i) {
      return i - 1;
    }

    int mem1() {
      return 3;
    }
    int mem2() const {
      return 4;
    }
    int data;
  };

  TEST(Call, TestInt1) {
    // function
    {
      cohort::function<int (int)> r1(f_int_1);
      int i = 2;
      EXPECT_TRUE(r1(i) == 3);
    }
    // function pointer
    {
      int (*fp)(int) = f_int_1;
      cohort::function<int (int)> r1(fp);
      int i = 3;
      EXPECT_TRUE(r1(i) == 4);
    }
    // functor
    {
      A_int_1 a0;
      cohort::function<int (int)> r1(a0);
      int i = 4;
      EXPECT_TRUE(r1(i) == 3);
    }
    // member function pointer
    {
      int (A_int_1::*fp)() = &A_int_1::mem1;
      cohort::function<int (A_int_1)> r1(fp);
      A_int_1 a;
      assert(r1(a) == 3);
      cohort::function<int (A_int_1*)> r2(fp);
      A_int_1* ap = &a;
      EXPECT_TRUE(r2(ap) == 3);
    }
    // const member function pointer
    {
      int (A_int_1::*fp)() const = &A_int_1::mem2;
      cohort::function<int (A_int_1)> r1(fp);
      A_int_1 a;
      EXPECT_TRUE(r1(a) == 4);
      cohort::function<int (A_int_1*)> r2(fp);
      A_int_1* ap = &a;
      EXPECT_TRUE(r2(ap) == 4);
    }
    // member data pointer
    {
      int A_int_1::*fp = &A_int_1::data;
      cohort::function<int& (A_int_1&)> r1(fp);
      A_int_1 a;
      EXPECT_TRUE(r1(a) == 5);
      r1(a) = 6;
      EXPECT_TRUE(r1(a) == 6);
      cohort::function<int& (A_int_1*)> r2(fp);
      A_int_1* ap = &a;
      EXPECT_TRUE(r2(ap) == 6);
      r2(ap) = 7;
      EXPECT_TRUE(r2(ap) == 7);
    }
  }

  // 2 arg, return void

  void f_void_2(int i, int j) {
    count += i+j;
  }

  struct A_void_2 {
    void operator()(int i, int j) {
      count += i+j;
    }

    void mem1(int i) {
      count += i;
    }
    void mem2(int i) const {
      count += i;
    }
  };

  TEST(Call, TestVoid2) {
    int save_count = count;
    // function
    {
      cohort::function<void (int, int)> r1(f_void_2);
      int i = 2;
      int j = 3;
      r1(i, j);
      EXPECT_TRUE(count == save_count+5);
      save_count = count;
    }
    // function pointer
    {
      void (*fp)(int, int) = f_void_2;
      cohort::function<void (int, int)> r1(fp);
      int i = 3;
      int j = 4;
      r1(i, j);
      EXPECT_TRUE(count == save_count+7);
      save_count = count;
    }
    // functor
    {
      A_void_2 a0;
      cohort::function<void (int, int)> r1(a0);
      int i = 4;
      int j = 5;
      r1(i, j);
      EXPECT_TRUE(count == save_count+9);
      save_count = count;
    }
    // member function pointer
    {
      void (A_void_2::*fp)(int) = &A_void_2::mem1;
      cohort::function<void (A_void_2, int)> r1(fp);
      A_void_2 a;
      int i = 3;
      r1(a, i);
      EXPECT_TRUE(count == save_count+3);
      save_count = count;
      cohort::function<void (A_void_2*, int)> r2(fp);
      A_void_2* ap = &a;
      r2(ap, i);
      EXPECT_TRUE(count == save_count+3);
      save_count = count;
    }
    // const member function pointer
    {
      void (A_void_2::*fp)(int) const = &A_void_2::mem2;
      cohort::function<void (A_void_2, int)> r1(fp);
      A_void_2 a;
      int i = 4;
      r1(a, i);
      EXPECT_TRUE(count == save_count+4);
      save_count = count;
      cohort::function<void (A_void_2*, int)> r2(fp);
      A_void_2* ap = &a;
      r2(ap, i);
      EXPECT_TRUE(count == save_count+4);
      save_count = count;
    }
  }

  // 2 arg, return int

  int f_int_2(int i, int j) {
    return i+j;
  }

  struct A_int_2 {
    int operator()(int i, int j) {
      return i+j;
    }

    int mem1(int i) {
      return i+1;
    }
    int mem2(int i) const {
      return i+2;
    }
  };

  TEST(Call, TestInt2) {
    // function
    {
      cohort::function<int(int, int)> r1(f_int_2);
      int i = 2;
      int j = 3;
      EXPECT_TRUE(r1(i, j) == i+j);
    }
    // function pointer
    {
      int (*fp)(int, int) = f_int_2;
      cohort::function<int(int, int)> r1(fp);
      int i = 3;
      int j = 4;
      EXPECT_TRUE(r1(i, j) == i+j);
    }
    // functor
    {
      A_int_2 a0;
      cohort::function<int (int, int)> r1(a0);
      int i = 4;
      int j = 5;
      EXPECT_TRUE(r1(i, j) == i+j);
    }
    // member function pointer
    {
      int(A_int_2::*fp)(int) = &A_int_2::mem1;
      cohort::function<int (A_int_2, int)> r1(fp);
      A_int_2 a;
      int i = 3;
      EXPECT_TRUE(r1(a, i) == i+1);
      cohort::function<int (A_int_2*, int)> r2(fp);
      A_int_2* ap = &a;
      EXPECT_TRUE(r2(ap, i) == i+1);
    }
    // const member function pointer
    {
      int (A_int_2::*fp)(int) const = &A_int_2::mem2;
      cohort::function<int (A_int_2, int)> r1(fp);
      A_int_2 a;
      int i = 4;
      EXPECT_TRUE(r1(a, i) == i+2);
      cohort::function<int (A_int_2*, int)> r2(fp);
      A_int_2* ap = &a;
      EXPECT_TRUE(r2(ap, i) == i+2);
    }
  }

  int f_int_0() {
    return 3;
  }

  struct A_int_0 {
    int operator()() {
      return 4;
    }
  };

  TEST(Call, TestInt0) {
      // function
    {
      cohort::function<int()> r1(f_int_0);
      EXPECT_TRUE(r1() == 3);
    }
    // function pointer
    {
      int (*fp)() = f_int_0;
      cohort::function<int()> r1(fp);
      EXPECT_TRUE(r1() == 3);
    }
    // functor
    {
      A_int_0 a0;
      cohort::function<int ()> r1(a0);
      assert(r1() == 4);
    }
  }

  void f_void_0() {
    ++count;
  }

  struct A_void_0 {
    void operator()() {
      ++count;
    }
  };

  TEST(Call, TestVoid0) {
    int save_count = count;
    // function
    {
      cohort::function<void()> r1(f_void_0);
      r1();
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
    }
    // function pointer
    {
      void (*fp)() = f_void_0;
      cohort::function<void()> r1(fp);
      r1();
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
    }
    // functor
    {
      A_void_0 a0;
      cohort::function<void ()> r1(a0);
      r1();
      EXPECT_TRUE(count == save_count+1);
      save_count = count;
    }
  }
};

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  static_invoke::checks();
  static_function_type::checks();

  return RUN_ALL_TESTS();
}
