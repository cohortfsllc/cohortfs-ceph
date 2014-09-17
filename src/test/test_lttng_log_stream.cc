// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <limits>

#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#define TRACEPOINT_DEFINE
#include "log/LttngStream.h"


// lttng_stream example to test output of various built-in types
struct stream_test {
  bool b;
  char c;
  unsigned char uc;
  short s;
  unsigned short us;
  int i;
  unsigned int ui;
  long l;
  unsigned long ul;
  long long ll;
  unsigned long long ull;
  float f;
  double d;
  long double ld;
  const char *cstr;
  const unsigned char *ucstr;
  void *v;
  /* TODO: more complicated types
   * std::string str;
   * std::vector<int> vec;
   */
};

// templated stream output operator to support lttng_stream and std::ostream
template<class stream>
stream& operator<<(stream &out, const stream_test &t)
{
  return out << t.b << t.c << t.uc << t.s << t.us << t.i << t.ui << t.l << t.ul
    << t.ll << t.ull << t.f << t.d << t.ld << t.cstr << t.ucstr << t.v;
}

template <class T> T max() { return std::numeric_limits<T>::max(); }

int main(int argc, char *argv[])
{
  const unsigned char ucstr[] = { 0xFF, 0xFF, 0xFF, 0xFF, 0x00 };

  // initialize the stream_test with the maximum values for each type
  stream_test test = { true,
    max<char>(), max<unsigned char>(),
    max<short>(), max<unsigned short>(),
    max<int>(), max<unsigned int>(),
    max<long>(), max<unsigned long>(),
    max<long long>(), max<unsigned long long>(),
    max<float>(), max<double>(), max<long double>(),
    "foo", ucstr, (void*)max<uintptr_t>() };

  lttng_stream out(1, "a", 
	 1000, 2, 3, 4, 5);
  out << test << std::endl;
  std::cout << test << std::endl;
  return 0;
}
