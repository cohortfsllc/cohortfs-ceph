// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/PrebufferedStreambuf.h"
#include "gtest/gtest.h"

TEST(PrebufferedStreambuf, Empty)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);

  std::istream is(&sb);
  std::string out;
  getline(is, out);
  ASSERT_EQ("", out);
}

TEST(PrebufferedStreambuf, Simple)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);

  std::ostream os(&sb);
  os << "test";

  std::istream is(&sb);
  std::string out;
  getline(is, out);
  ASSERT_EQ("test", out);
}

TEST(PrebufferedStreambuf, SimpleNoPrealloc)
{
  std::string sbstr;
  PrebufferedStreambuf sb(sbstr);

  std::ostream os(&sb);
  os << "test";

  std::istream is(&sb);
  std::string out;
  getline(is, out);
  ASSERT_EQ("test", out);
}

TEST(PrebufferedStreambuf, Multiline)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);

  std::ostream os(&sb);
  const char *s = "this is a line\nanother line\nand a third\nwhee!\n";
  os << s;

  std::istream is(&sb);
  std::string out;
  getline(is, out, is.widen(0));
  ASSERT_EQ(s, out);
}

TEST(PrebufferedStreambuf, Withnull)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);

  std::ostream os(&sb);
  std::string s("null \0 and more", 15);
  os << s;

  std::istream is(&sb);
  std::string out;
  getline(is, out);
  ASSERT_EQ(s, out);
}

TEST(PrebufferedStreambuf, SimpleOverflow)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);

  std::ostream os(&sb);
  const char *s = "hello, this is longer than buf[10]";
  os << s;

  ASSERT_EQ(s, sb.str());

  std::istream is(&sb);
  std::string out;
  getline(is, out);
  ASSERT_EQ(s, out);
}

TEST(PrebufferedStreambuf, ManyOverflow)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);

  std::ostream os(&sb);
  const char *s = "hello, this way way way way way way way way way way way way way way way way way way way way way way way way way _way_ longer than buf[10]";
  os << s;

  ASSERT_EQ(s, sb.str());

  std::istream is(&sb);
  std::string out;
  getline(is, out);
  ASSERT_EQ(s, out);
}


TEST(PrebufferedStreambuf, Seek)
{
  std::string sbstr(10, '\0');
  PrebufferedStreambuf sb(sbstr);
  std::iostream stream(&sb);

  stream << "hello world";
  ASSERT_EQ(sb.str(), "hello world");

  // get current write position
  std::streampos pos = sb.pubseekoff(0, std::ios_base::cur, std::ios_base::out);
  ASSERT_EQ(pos, 11);
  // move read position to the second character, 'e'
  char c;
  pos = sb.pubseekpos(1, std::ios_base::in);
  ASSERT_EQ(pos, 1);
  stream >> c;
  ASSERT_EQ(c, 'e');

  // move write position to 'world', and overwrite with 'ceph!'
  pos = sb.pubseekoff(-5, std::ios_base::cur, std::ios_base::out);
  ASSERT_EQ(pos, 6);
  stream << "ceph!";
  ASSERT_EQ(sb.str(), "hello ceph!");

  // move read and write positions to the 'e' in hello
  pos = sb.pubseekoff(-10, std::ios_base::end,
    std::ios_base::in | std::ios_base::out);
  ASSERT_EQ(pos, 1);
  // replace 'e' with 'a' and read it back
  stream << 'a';
  stream >> c;
  ASSERT_EQ(c, 'a');
  ASSERT_EQ(sb.str(), "hallo ceph!");

  // invalid to do relative seek with both read and write position
  pos = sb.pubseekoff(0, std::ios_base::cur,
    std::ios_base::in | std::ios_base::out);
  ASSERT_EQ(pos, -1);
}
