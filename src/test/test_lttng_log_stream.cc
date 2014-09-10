// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "log/LttngStream.h"


struct stream_test {
  bool b;
  char c;
  int i;
  long l;
  float f;
  double d;
  const char *s;
};

// templated stream output operator to support lttng_stream and std::ostream
template<class stream>
stream& operator<<(stream &out, const stream_test &rhs)
{
  return out << rhs.b << rhs.c << rhs.i << rhs.l << rhs.f << rhs.d << rhs.s;
}

int main(int argc, char *argv[])
{
  stream_test test = { true, 'a', 5, 12, 1.5f, 6.666666666, "foo" };

  lttng_stream out;
  out << test << std::endl;
  std::cout << test << std::endl;
  return 0;
}
