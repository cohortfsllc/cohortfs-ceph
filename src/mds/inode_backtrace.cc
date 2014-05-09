// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

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
  ENCODE_START(5, 4, bl);
  ::encode(ino, bl);
  ::encode(ancestors, bl);
  ::encode(pool, bl);
  ::encode(old_pools, bl);
  ENCODE_FINISH(bl);
}

void inode_backtrace_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  if (struct_v < 3)
    return;  // sorry, the old data was crap
  ::decode(ino, bl);
  if (struct_v >= 4) {
    ::decode(ancestors, bl);
  } else {
    uint32_t n;
    ::decode(n, bl);
    while (n--) {
      ancestors.push_back(inode_backpointer_t());
      ancestors.back().decode_old(bl);
    }
  }
  if (struct_v >= 5) {
    ::decode(pool, bl);
    ::decode(old_pools, bl);
  }
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
  f->dump_int("pool", pool);
  f->open_array_section("old_pools");
  for (set<int64_t>::iterator p = old_pools.begin(); p != old_pools.end(); ++p) {
    f->dump_int("old_pool", *p);
  }
  f->close_section();
}

void inode_backtrace_t::generate_test_instances(list<inode_backtrace_t*>& ls)
{
  ls.push_back(new inode_backtrace_t);
  ls.push_back(new inode_backtrace_t);
  ls.back()->ino = 1;
  ls.back()->ancestors.push_back(inode_backpointer_t());
  ls.back()->ancestors.back().dirino = 123;
  ls.back()->ancestors.back().dname = "bar";
  ls.back()->ancestors.back().version = 456;
  ls.back()->pool = 0;
  ls.back()->old_pools.insert(10);
  ls.back()->old_pools.insert(7);
}

