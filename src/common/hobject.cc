// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "hobject.h"
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

static const string typestrings[] = {
  [ENTIRETY] = "entirety",
  [DATA] = "data",
  [ECC] = "ecc",
  [TERMINUS] = "terminus"
};


// The appender must append a sequence of characters WIHOUT A NUL and
// return a pointer to the next character after the last appended. Or
// NULL if the buffer isn't big enough.
bool hobject_t::append_c_str(
  char *orig, char sep, size_t len,
  char *(*appender)(char *dest, const char* src, size_t len)) const
{
  char *bound = orig + len;
  char *cursor  = (char *)memchr(orig, '\0', len);
  if (!cursor)
    return false;

  if (appender) {
    cursor = appender(orig, oid.name.c_str(), bound - cursor);
    if (!cursor)
      return false;
  } else if ((ptrdiff_t)oid.name.size() >= bound - cursor) {
    return false;
  } else {
    memcpy(cursor, oid.name.c_str(), oid.name.size());
    cursor += oid.name.size();
  }

  if (cursor > bound)
    return false;
  *cursor++ = sep;

  assert(stripetype < TERMINUS);
  if ((cursor > bound) ||
      ((bound - cursor) < (ptrdiff_t)typestrings[stripetype].size()))
    return false;
  memcpy(cursor, typestrings[stripetype].c_str(),
	 typestrings[stripetype].size());
  cursor += typestrings[stripetype].length();

  if (cursor > bound)
    return false;
  *cursor++ = sep;

  int writ = snprintf(cursor, bound - cursor, "%" PRIu32, stripeno);
  if (writ >= (bound - cursor))
    return false;

  return true;
}

void hobject_t::append_str(
  string &orig, char sep,
  void (*appender)(string &dest, const string &src)) const
{
  if (appender) {
    appender(orig, oid.name);
  } else {
    orig.append(oid.name);
  }
  orig.push_back(sep);
  // Should never be violated, compiler complains if you try it.
  assert(stripetype < TERMINUS);
  orig.append(typestrings[stripetype]);
  orig.push_back(sep);
  orig.append(to_string(stripeno));
}


string hobject_t::to_str() const
{
  string result;
  append_str(result, '.', append_escaped);
  return result;
}

void hobject_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(oid, bl);
  ::encode(stripetype, bl);
  ::encode(stripeno, bl);
  ENCODE_FINISH(bl);
}

void hobject_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  ::decode(oid, bl);
  ::decode(stripetype, bl);
  ::decode(stripeno, bl);
  DECODE_FINISH(bl);
}

void hobject_t::dump(Formatter *f) const
{
  f->dump_string("oid", oid.name);
  f->dump_string("stripetype", typestrings[stripetype]);
  f->dump_int("stripeno", stripeno);
}

void hobject_t::generate_test_instances(list<hobject_t*>& o)
{
  o.push_back(new hobject_t);
  o.push_back(new hobject_t(object_t("oname"), DATA, 97));
  o.push_back(new hobject_t(object_t("oname3"), ECC, 31));
}

ostream& operator<<(ostream& out, const hobject_t& o)
{
  out << o.to_str();
  return out;
}
