// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "oid.h"
#include "common/Formatter.h"

static void append_escaped(string& out, const string &in)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out.push_back('%');
      out.push_back('p');
    } else if (*i == '.') {
      out.push_back('%');
      out.push_back('e');
    } else if (*i == '_') {
      out.push_back('%');
      out.push_back('u');
    } else {
      out.push_back(*i);
    }
  }
}

#define stringalong(x) {x, sizeof(x) - 1}

static const map<chunktype, string> typestrings = {
  {chunktype::entirety, "entirety"},
  {chunktype::data, "data"},
  {chunktype::ecc, "ecc"},
  {chunktype::terminus, "terminus"}
};

static const map<string, chunktype> stringtypes = {
  {"entirety", chunktype::entirety},
  {"data", chunktype::data},
  {"ecc", chunktype::ecc},
  {"terminus", chunktype::terminus}
};


// The appender must append a sequence of characters WIHOUT A NUL and
// return a pointer to the next character after the last appended. Or
// NULL if the buffer isn't big enough.
bool oid::append_c_str(
  char *orig, char sep, size_t len,
  char *(*appender)(char *dest, const char* src, size_t len)) const
{
  char *bound = orig + len;
  char *cursor  = (char *)memchr(orig, '\0', len);
  if (!cursor)
    return false;

  if (appender) {
    cursor = appender(orig, name.c_str(), bound - cursor);
    if (!cursor)
      return false;
  } else if ((ptrdiff_t)name.size() >= bound - cursor) {
    return false;
  } else {
    memcpy(cursor, name.c_str(), name.size());
    cursor += name.size();
  }

  if (cursor > bound)
    return false;
  *cursor++ = sep;

  assert(type < chunktype::terminus);
  if ((cursor > bound) ||
      ((bound - cursor) < (ptrdiff_t)typestrings.at(type).size()))
    return false;
  memcpy(cursor, typestrings.at(type).c_str(),
	 typestrings.at(type).size());
  cursor += typestrings.at(type).length();

  if (cursor > bound)
    return false;
  *cursor++ = sep;

  int writ = snprintf(cursor, bound - cursor, "%" PRIu32, stride);
  if (writ >= (bound - cursor))
    return false;

  return true;
}

void oid::append_str(
  string &orig, char sep,
  void (*appender)(string &dest, const string &src)) const
{
  orig.reserve(orig.size() + name.size() + 30);
  if (appender) {
    appender(orig, name);
  } else {
    orig.append(name);
  }
  orig.push_back(sep);
  // Should never be violated, compiler complains if you try it.
  assert(type < chunktype::terminus);
  orig.append(typestrings.at(type));
  orig.push_back(sep);
  orig.append(std::to_string(stride));
  orig.reserve(orig.length());
}


string oid::to_str() const
{
  string result;
  append_str(result, '.', append_escaped);
  return result;
}

oid::oid(const char *in, char sep,
	 bool (*appender)(string &dest, const char *begin,
			  const char *bound), const char *end)
{
  const char* cursor = in;
  const char* bound;

  if (end)
    bound = (char *)memchr(cursor, sep, end - cursor);
  else
    bound = strchr(in, sep);

  if (!bound)
    throw std::invalid_argument(in);

  name.reserve(bound - cursor);

  if (!appender(name, cursor, bound))
    throw std::invalid_argument(in);
  name.reserve(name.size());

  cursor = bound + 1;
  if (end && cursor >= end)
    throw std::invalid_argument(in);

  bound = strchr(cursor, sep);
  if (!bound)
    throw std::invalid_argument(in);
  string temp(cursor, bound - cursor);
  try {
    type = stringtypes.at(temp);
  } catch (std::out_of_range &e) {
    throw std::invalid_argument(in);
  }

  cursor = bound + 1;
  if (end && cursor >= end)
    throw std::invalid_argument(in);

  stride = strtoul(cursor, (char**) &bound, 10);
  if (*cursor == '\0' || (*bound != '\0' && bound != end))
    throw std::invalid_argument(in);
}

void oid::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(name, bl);
  ::encode(type, bl);
  ::encode(stride, bl);
  ENCODE_FINISH(bl);
}

void oid::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  ::decode(name, bl);
  ::decode(type, bl);
  ::decode(stride, bl);
  DECODE_FINISH(bl);
}

void oid::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", typestrings.at(type));
  f->dump_int("stride", stride);
}

void oid::generate_test_instances(list<oid*>& o)
{
  o.push_back(new oid);
  o.push_back(new oid("oname", chunktype::data, 97));
  o.push_back(new oid("oname3", chunktype::ecc, 31));
}

std::ostream& operator<<(std::ostream& out, const oid& o)
{
  out << o.to_str();
  return out;
}
