// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// Imported from libc++ testing framework

#ifndef COUNT_NEW_HPP
#define COUNT_NEW_HPP

#include <cstdlib>
#include <cassert>
#include <new>

class MemCounter {
public:
  // Make MemCounter super hard to accidentally construct or copy.
  class MemCounterCtorArg {};
  explicit MemCounter(MemCounterCtorArg) {
    reset();
  }
  MemCounter(MemCounter const &) = delete;
  MemCounter& operator=(MemCounter const&) = delete;

  int outstanding_new;
  int new_called;
  int delete_called;
  int last_new_size;

  int outstanding_array_new;
  int new_array_called;
  int delete_array_called;
  int last_new_array_size;

public:
  void newCalled(std::size_t s) {
    assert(s);
    ++new_called;
    ++outstanding_new;
    last_new_size = s;
  }

  void deleteCalled(void* p) {
    assert(p);
    --outstanding_new;
    ++delete_called;
  }

  void newArrayCalled(std::size_t s) {
    assert(s);
    ++outstanding_array_new;
    ++new_array_called;
    last_new_array_size = s;
  }

  void deleteArrayCalled(void* p) {
    assert(p);
    --outstanding_array_new;
    ++delete_array_called;
  }

  void reset() {
    outstanding_new = 0;
    new_called = 0;
    delete_called = 0;
    last_new_size = 0;

    outstanding_array_new = 0;
    new_array_called = 0;
    delete_array_called = 0;
    last_new_array_size = 0;
  }

public:
  bool checkOutstandingNewEq(int n) const {
    return n == outstanding_new;
  }

  bool checkOutstandingNewNotEq(int n) const {
    return n != outstanding_new;
  }

  bool checkNewCalledEq(int n) const {
    return n == new_called;
  }

  bool checkNewCalledNotEq(int n) const {
    return n != new_called;
  }

  bool checkDeleteCalledEq(int n) const {
    return n == delete_called;
  }

  bool checkDeleteCalledNotEq(int n) const {
    return n != delete_called;
  }

  bool checkLastNewSizeEq(int n) const {
    return n == last_new_size;
  }

  bool checkLastNewSizeNotEq(int n) const {
    return n != last_new_size;
  }

  bool checkOutstandingArrayNewEq(int n) const {
    return n == outstanding_array_new;
  }

  bool checkOutstandingArrayNewNotEq(int n) const {
    return n != outstanding_array_new;
  }

  bool checkNewArrayCalledEq(int n) const {
    return n == new_array_called;
  }

  bool checkNewArrayCalledNotEq(int n) const {
    return n != new_array_called;
  }

  bool checkDeleteArrayCalledEq(int n) const {
    return n == delete_array_called;
  }

  bool checkDeleteArrayCalledNotEq(int n) const {
    return n != delete_array_called;
  }

  bool checkLastNewArraySizeEq(int n) const {
    return n == last_new_array_size;
  }

  bool checkLastNewArraySizeNotEq(int n) const {
    return n != last_new_array_size;
  }
};

MemCounter globalMemCounter((MemCounter::MemCounterCtorArg()));

void* operator new(std::size_t s)
{
  globalMemCounter.newCalled(s);
  return std::malloc(s);
}

void operator delete(void* p) noexcept
{
  globalMemCounter.deleteCalled(p);
  std::free(p);
}

void* operator new[](std::size_t s)
{
  globalMemCounter.newArrayCalled(s);
  return operator new(s);
}


void operator delete[](void* p) noexcept
{
  globalMemCounter.deleteArrayCalled(p);
  operator delete(p);
}

#endif /* COUNT_NEW_HPP */
