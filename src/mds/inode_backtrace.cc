// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include "inode_backtrace.h"

#include "common/Formatter.h"

/* inode_backpointer_t */

void inode_backpointer_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(dirino, bl);
  ::encode(dname, bl);
  ::encode(version, bl);
  ENCODE_FINISH(bl);
}

void inode_backpointer_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(dirino, bl);
  ::decode(dname, bl);
  ::decode(version, bl);
  DECODE_FINISH(bl);
}

void inode_backpointer_t::decode_old(bufferlist::iterator& bl)
{
  ::decode(dirino, bl);
  ::decode(dname, bl);
  ::decode(version, bl);
}

void inode_backpointer_t::dump(Formatter *f) const
{
  f->dump_unsigned("dirino", dirino);
  f->dump_string("dname", dname);
  f->dump_unsigned("version", version);
}

void inode_backpointer_t::generate_test_instances(list<inode_backpointer_t*>& ls)
{
  ls.push_back(new inode_backpointer_t);
  ls.push_back(new inode_backpointer_t);
  ls.back()->dirino = 1;
  ls.back()->dname = "foo";
  ls.back()->version = 123;
}


/*
 * inode_backtrace_t
 */

void inode_backtrace_t::encode(bufferlist& bl) const
{
  ENCODE_START(85, 85, bl);
  ::encode(ino, bl);
  ::encode(ancestors, bl);
  ::encode(volume, bl);
  ::encode(old_volumes, bl);
  ENCODE_FINISH(bl);
}

void inode_backtrace_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(85, 85, 4, bl);
  ::decode(ancestors, bl);
  ::decode(volume, bl);
  ::decode(old_volumes, bl);
  DECODE_FINISH(bl);
}

void inode_backtrace_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->open_array_section("ancestors");
  for (vector<inode_backpointer_t>::const_iterator p = ancestors.begin(); p != ancestors.end(); ++p) {
    f->open_object_section("backpointer");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_stream("volume") << volume;
  f->open_array_section("old_volumes");
  for (const auto& v : old_volumes) {
    f->dump_stream("uuid") << v;
  }
  f->close_section();
}

void inode_backtrace_t::generate_test_instances(list<inode_backtrace_t*>& ls)
{
  boost::uuids::string_generator parse;
  boost::uuids::uuid uuid1, uuid2, uuid3;

  uuid1 = parse("5a9e54a4-7740-4d03-b0fb-e1f3b899b185");
  uuid2 = parse("5edbdba8-af1a-4b48-8f2f-1ec5cf84efbe");
  uuid3 = parse("e9013f90-e7a3-4f69-bb85-bcf74559e68d");
  ls.push_back(new inode_backtrace_t);
  ls.push_back(new inode_backtrace_t);
  ls.back()->ino = 1;
  ls.back()->ancestors.push_back(inode_backpointer_t());
  ls.back()->ancestors.back().dirino = 123;
  ls.back()->ancestors.back().dname = "bar";
  ls.back()->ancestors.back().version = 456;
  ls.back()->volume = uuid1;
  ls.back()->old_volumes.insert(uuid2);
  ls.back()->old_volumes.insert(uuid3);
}

