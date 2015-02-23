// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */
#include <ctype.h>
#include <sstream>
#include "ObjectStore.h"
#include "common/Formatter.h"
#include "common/safe_io.h"

int ObjectStore::write_meta(const std::string& key,
			    const std::string& value)
{
  string v = value;
  v += "\n";
  int r = safe_write_file(path.c_str(), key.c_str(),
			  v.c_str(), v.length());
  if (r < 0)
    return r;
  return 0;
}

int ObjectStore::read_meta(const std::string& key,
			   std::string *value)
{
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
			 buf, sizeof(buf));
  if (r <= 0)
    return r;
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  *value = string(buf, r);
  return 0;
}




ostream& operator<<(ostream& out, const ObjectStore::Sequencer& s)
{
  return out << "osr(" << s.get_name() << " " << &s << ")";
}

unsigned ObjectStore::apply_transactions(Sequencer *osr,
					 list<Transaction*> &tls,
					 Context *ondisk)
{
  // use op pool
  Cond my_cond;
  Mutex my_lock;
  int r = 0;
  bool done;
  C_SafeCond *onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);

  queue_transactions(osr, tls, onreadable, ondisk);

  my_lock.Lock();
  while (!done)
    my_cond.Wait(my_lock);
  my_lock.Unlock();
  return r;
}

int ObjectStore::queue_transactions(
  Sequencer *osr,
  list<Transaction*>& tls,
  Context *onreadable,
  Context *oncommit,
  Context *onreadable_sync,
  Context *oncomplete,
  OpRequestRef op = OpRequestRef())
{
  RunOnDeleteRef _complete(new RunOnDelete(oncomplete));
  Context *_onreadable = new Wrapper<RunOnDeleteRef>(
    onreadable, _complete);
  Context *_oncommit = new Wrapper<RunOnDeleteRef>(
    oncommit, _complete);
  return queue_transactions(osr, tls, _onreadable, _oncommit,
			    onreadable_sync, op);
}

void ObjectStore::Transaction::dump(ceph::Formatter *f)
{
  f->open_array_section("ops");
  int op_num = 0;
  bool stop_looping = false;
  for (op_iterator i = begin(); i != end() && !stop_looping; ++i) {
    f->open_object_section("op");
    f->dump_int("op_num", op_num);

    switch (i->op) {
    case Transaction::OP_NOP:
      f->dump_string("op_name", "nop");
      break;
    case Transaction::OP_TOUCH:
      f->dump_string("op_name", "touch");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_WRITE:
      f->dump_string("op_name", "write");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_unsigned("length", i->len);
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("bufferlist length", i->data.length());
      break;

    case Transaction::OP_ZERO:
      f->dump_string("op_name", "zero");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("length", i->len);
      break;

    case Transaction::OP_TRIMCACHE:
      f->dump_string("op_name", "trim_cache");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("length", i->len);
      break;

    case Transaction::OP_TRUNCATE:
      f->dump_string("op_name", "truncate");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_unsigned("offset", i->off);
      break;

    case Transaction::OP_REMOVE:
      f->dump_string("op_name", "remove");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_SETATTR:
      f->dump_string("op_name", "setattr");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_string("name", i->name);
      f->dump_unsigned("length", i->data.length());
      break;

    case Transaction::OP_SETATTRS:
      f->dump_string("op_name", "setattrs");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->open_object_section("attr_lens");
      for (map<string,bufferptr>::iterator p = i->xattrs.begin();
	  p != i->xattrs.end(); ++p) {
	f->dump_unsigned(p->first.c_str(), p->second.length());
      }
      break;

    case Transaction::OP_RMATTR:
      f->dump_string("op_name", "rmattr");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_string("name", i->name);
      break;

    case Transaction::OP_RMATTRS:
      f->dump_string("op_name", "rmattrs");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_CLONE:
      f->dump_string("op_name", "clone");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("src_obj") << i->obj;
      f->dump_stream("dst_obj") << i->obj2;
      break;

    case Transaction::OP_CLONERANGE:
      f->dump_string("op_name", "clonerange");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("src_obj") << i->obj;
      f->dump_stream("dst_obj") << i->obj2;
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("len", i->len);
      break;

    case Transaction::OP_CLONERANGE2:
      f->dump_string("op_name", "clonerange2");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("src_obj") << i->obj;
      f->dump_stream("dst_obj") << i->obj2;
      f->dump_unsigned("src_offset", i->off);
      f->dump_unsigned("len", i->len);
      f->dump_unsigned("dst_offset", i->off2);
      break;

    case Transaction::OP_MKCOLL:
      f->dump_string("op_name", "mkcoll");
      f->dump_stream("collection") << i->cid;
      break;

    case Transaction::OP_RMCOLL:
      f->dump_string("op_name", "rmcoll");
      f->dump_stream("collection") << i->cid;
      break;

    case Transaction::OP_COLL_ADD:
      f->dump_string("op_name", "collection_add");
      f->dump_stream("src_collection") << i->cid;
      f->dump_stream("dst_collection") << i->cid2;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_COLL_REMOVE:
      f->dump_string("op_name", "collection_remove");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_COLL_MOVE:
      f->open_object_section("collection_move");
      f->dump_stream("src_collection") << i->cid;
      f->dump_stream("dst_collection") << i->cid2;
      f->dump_stream("obj") << i->obj;
      f->close_section();
      break;

    case Transaction::OP_COLL_SETATTR:
      f->dump_string("op_name", "collection_setattr");
      f->dump_stream("collection") << i->cid;
      f->dump_string("name", i->name);
      f->dump_unsigned("length", i->data.length());
      break;

    case Transaction::OP_COLL_RMATTR:
      f->dump_string("op_name", "collection_rmattr");
      f->dump_stream("collection") << i->cid;
      f->dump_string("name", i->name);
      break;

    case Transaction::OP_STARTSYNC:
      f->dump_string("op_name", "startsync");
      break;

    case Transaction::OP_COLL_RENAME:
      f->dump_string("op_name", "collection_rename");
      f->dump_stream("src_collection") << i->cid;
      f->dump_stream("dst_collection") << i->cid2;
      break;

    case Transaction::OP_OMAP_CLEAR:
      f->dump_string("op_name", "omap_clear");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_OMAP_SETKEYS:
      f->dump_string("op_name", "omap_setkeys");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->open_object_section("attr_lens");
      for (map<string, bufferlist>::iterator p = i->attrs.begin();
	  p != i->attrs.end(); ++p) {
	f->dump_unsigned(p->first.c_str(), p->second.length());
      }
      f->close_section();
      break;

    case Transaction::OP_OMAP_RMKEYS:
      f->dump_string("op_name", "omap_rmkeys");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      break;

    case Transaction::OP_OMAP_SETHEADER:
      f->dump_string("op_name", "omap_setheader");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_stream("header_length") << i->data.length();
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      f->dump_string("op_name", "op_omap_rmkeyrange");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_string("first", i->name);
      f->dump_string("last", i->name2);
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      f->dump_string("op_name", "op_coll_move_rename");
      f->dump_stream("old_collection") << i->cid;
      f->dump_stream("old_obj") << i->obj;
      f->dump_stream("new_collection") << i->cid2;
      f->dump_stream("new_obj") << i->obj2;
      break;

    case Transaction::OP_SETALLOCHINT:
      f->dump_string("op_name", "op_setallochint");
      f->dump_stream("collection") << i->cid;
      f->dump_stream("obj") << i->obj;
      f->dump_stream("expected_object_size") << i->value1;
      f->dump_stream("expected_write_size") << i->value2;
      break;

    default:
      f->dump_string("op_name", "unknown");
      f->dump_unsigned("op_code", i->op);
      stop_looping = true;
      break;
    }
    f->close_section();
    op_num++;
  }
  f->close_section();
}

void ObjectStore::Transaction::Op::encode(bufferlist &bl) const
{
  ::encode(op, bl);
  // encode only the fields relevant to this op
  switch (op) {
  case OP_TOUCH:
    ::encode(cid, bl);
    ::encode(obj, bl);
    break;
  case OP_WRITE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    ::encode(data, bl);
    break;
  case OP_ZERO:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    break;
  case OP_TRIMCACHE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    break;
  case OP_TRUNCATE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(off, bl);
    break;
  case OP_REMOVE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    break;
  case OP_SETATTR:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(name, bl);
    ::encode(data, bl);
    break;
  case OP_SETATTRS:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(xattrs, bl);
    break;
  case OP_RMATTR:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(name, bl);
    break;
  case OP_RMATTRS:
    ::encode(cid, bl);
    ::encode(obj, bl);
    break;
  case OP_CLONE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(obj2, bl);
    break;
  case OP_CLONERANGE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(obj2, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    break;
  case OP_CLONERANGE2:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(obj2, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    ::encode(off2, bl);
    break;
  case OP_RMCOLL:
    ::encode(cid, bl);
    break;
  case OP_COLL_ADD:
    ::encode(cid, bl);
    ::encode(cid2, bl);
    ::encode(obj, bl);
    break;
  case OP_COLL_REMOVE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    break;
  case OP_COLL_MOVE:
    ::encode(cid, bl);
    ::encode(cid2, bl);
    ::encode(obj, bl);
    break;
  case OP_COLL_MOVE_RENAME:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(cid2, bl);
    ::encode(obj2, bl);
    break;
  case OP_COLL_SETATTRS:
    ::encode(cid, bl);
    ::encode(xattrs, bl);
    break;
  case OP_COLL_RENAME:
    ::encode(cid, bl);
    ::encode(cid2, bl);
    break;
  case OP_OMAP_CLEAR:
    ::encode(cid, bl);
    ::encode(obj, bl);
    break;
  case OP_OMAP_SETKEYS:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(attrs, bl);
    break;
  case OP_OMAP_RMKEYS:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(keys, bl);
    break;
  case OP_OMAP_RMKEYRANGE:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(name, bl);
    ::encode(name2, bl);
    break;
  case OP_OMAP_SETHEADER:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(data, bl);
    break;
  case OP_SETALLOCHINT:
    ::encode(cid, bl);
    ::encode(obj, bl);
    ::encode(value1, bl);
    ::encode(value2, bl);
    break;
  }
}

void ObjectStore::Transaction::Op::decode(bufferlist::iterator &p)
{
  ::decode(op, p);
  switch (op) {
  case OP_TOUCH:
    ::decode(cid, p);
    ::decode(obj, p);
    break;
  case OP_WRITE:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(off, p);
    ::decode(len, p);
    ::decode(data, p);
    break;
  case OP_ZERO:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(off, p);
    ::decode(len, p);
    break;
  case OP_TRIMCACHE:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(off, p);
    ::decode(len, p);
    break;
  case OP_TRUNCATE:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(off, p);
    break;
  case OP_REMOVE:
    ::decode(cid, p);
    ::decode(obj, p);
    break;
  case OP_SETATTR:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(name, p);
    ::decode(data, p);
    break;
  case OP_SETATTRS:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(xattrs, p);
    break;
  case OP_RMATTR:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(name, p);
    break;
  case OP_RMATTRS:
    ::decode(cid, p);
    ::decode(obj, p);
    break;
  case OP_CLONE:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(obj2, p);
    break;
  case OP_CLONERANGE:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(obj2, p);
    ::decode(off, p);
    ::decode(len, p);
    break;
  case OP_CLONERANGE2:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(obj2, p);
    ::decode(off, p);
    ::decode(len, p);
    ::decode(off2, p);
    break;
  case OP_RMCOLL:
    ::decode(cid, p);
    break;
  case OP_COLL_ADD:
    ::decode(cid, p);
    ::decode(cid2, p);
    ::decode(obj, p);
    break;
  case OP_COLL_REMOVE:
    ::decode(cid, p);
    ::decode(obj, p);
    break;
  case OP_COLL_MOVE:
    ::decode(cid, p);
    ::decode(cid2, p);
    ::decode(obj, p);
    break;
  case OP_COLL_MOVE_RENAME:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(cid2, p);
    ::decode(obj2, p);
    break;
  case OP_COLL_SETATTRS:
    ::decode(cid, p);
    ::decode(xattrs, p);
    break;
  case OP_COLL_RENAME:
    ::decode(cid, p);
    ::decode(cid2, p);
    break;
  case OP_OMAP_CLEAR:
    ::decode(cid, p);
    ::decode(obj, p);
    break;
  case OP_OMAP_SETKEYS:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(attrs, p);
    break;
  case OP_OMAP_RMKEYS:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(keys, p);
    break;
  case OP_OMAP_RMKEYRANGE:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(name, p);
    ::decode(name2, p);
    break;
  case OP_OMAP_SETHEADER:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(data, p);
    break;
  case OP_SETALLOCHINT:
    ::decode(cid, p);
    ::decode(obj, p);
    ::decode(value1, p);
    ::decode(value2, p);
    break;
  }
}

void ObjectStore::Transaction::generate_test_instances(list<ObjectStore::Transaction*>& o)
{
  o.push_back(new Transaction);

  Transaction *t = new Transaction;
  t->nop();
  o.push_back(t);

  t = new Transaction;
  coll_t c("foocoll");
  coll_t c2("foocoll2");
  oid o1(oid("obj", chunktype::data, 91));
  oid o2(oid("obj2", chunktype::ecc, 456));
  oid o3(oid("obj3", chunktype::data, 456));
  t->touch(c, o1);
  bufferlist bl;
  bl.append("some data");
  t->write(c, o1, 1, bl.length(), bl);
  t->zero(c, o1, 22, 33);
  t->truncate(c, o1, 99);
  t->remove(c, o1);
  o.push_back(t);

  t = new Transaction;
  t->setattr(c, o1, "key", bl);
  map<string,bufferptr> m;
  m["a"] = buffer::copy("this", 4);
  m["b"] = buffer::copy("that", 4);
  t->setattrs(c, o1, m);
  t->rmattr(c, o1, "b");
  t->rmattrs(c, o1);

  t->clone(c, o1, o2);
  t->clone(c, o1, o3);
  t->clone_range(c, o1, o2, 1, 12, 99);

  t->create_collection(c);
  t->collection_add(c, c2, o1);
  t->collection_add(c, c2, o2);
  t->collection_move(c, c2, o3);
  t->remove_collection(c);
  t->collection_setattr(c, "this", bl);
  t->collection_rmattr(c, "foo");
  t->collection_setattrs(c, m);
  t->collection_rename(c, c2);
  o.push_back(t);
}

int ObjectStore::collection_list(const coll_t &c, vector<oid>& o)
{
  vector<oid> go;
  int ret = collection_list(c, go);
  if (ret == 0) {
    o.reserve(go.size());
    for (vector<oid>::iterator i = go.begin(); i != go.end() ; ++i)
      o.push_back(*i);
  }
  return ret;
}

int ObjectStore::collection_list_partial(const coll_t &c, oid start,
					 int min, int max,
					 vector<oid> *ls, oid *next)
{
  vector<oid> go;
  oid gnext, gstart(start);
  int ret = collection_list_partial(c, gstart, min, max, &go, &gnext);
  if (ret == 0) {
    *next = gnext;
    ls->reserve(go.size());
    for (vector<oid>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(*i);
  }
  return ret;
}

int ObjectStore::collection_list_range(const coll_t &c, oid start,
				       oid end, vector<oid> *ls)
{
  vector<oid> go;
  oid gstart(start), gend(end);
  int ret = collection_list_range(c, gstart, gend, &go);
  if (ret == 0) {
    ls->reserve(go.size());
    for (vector<oid>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(*i);
  }
  return ret;
}
