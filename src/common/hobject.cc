// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "hobject.h"
#include "common/Formatter.h"

static void append_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == '_') {
      out->push_back('%');
      out->push_back('u');
    } else {
      out->push_back(*i);
    }
  }
}

static const string typestring(enum stripetype_t st) {
  switch (st) {
  case ENTIRETY:
    return "the whole shebang";
    break;

  case DATA:
    return "data";
    break;

  case ECC:
    return "ecc";
    break;
  }

  return "all is loosed and undone!";
}

string hobject_t::to_str() const
{
  string escaped;
  append_escaped(oid.name, &escaped);
  std::stringstream ss(escaped);

  ss << "." << typestring(stripetype) << setw(10) << stripeno;

  return ss.str();
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
  f->dump_string("stripetype", typestring(stripetype));
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
  out << o.oid << "(" << typestring(o.stripetype) << "."
      << "o.stripeno" << ")";
  return out;
}
