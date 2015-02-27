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
			   std::string* value)
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

unsigned ObjectStore::apply_transactions(list<Transaction*> &tls,
					 Context *ondisk)
{
  // use op pool
  std::condition_variable my_cond;
  std::mutex my_lock;
  int r = 0;
  bool done;
  C_SafeCond* onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);

  queue_transactions(tls, onreadable, ondisk);

  std::unique_lock<std::mutex> myl(my_lock);
  while (!done)
    my_cond.wait(myl);
  myl.unlock();
  return r;
}

int ObjectStore::queue_transactions(list<Transaction*>& tls,
                                    Context *onreadable, Context *oncommit,
                                    Context *onreadable_sync,
                                    Context *oncomplete,
                                    OpRequestRef op = OpRequestRef())
{
  RunOnDeleteRef _complete(new RunOnDelete(oncomplete));
  Context *_onreadable = new Wrapper<RunOnDeleteRef>(onreadable, _complete);
  Context *_oncommit = new Wrapper<RunOnDeleteRef>(oncommit, _complete);
  return queue_transactions(tls, _onreadable, _oncommit, onreadable_sync, op);
}

void ObjectStore::Transaction::dump(ceph::Formatter* f)
{
  using std::get;

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
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_WRITE:
      f->dump_string("op_name", "write");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_unsigned("length", i->len);
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("bufferlist length", i->data.length());
      break;

    case Transaction::OP_ZERO:
      f->dump_string("op_name", "zero");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("length", i->len);
      break;

    case Transaction::OP_TRIMCACHE:
      f->dump_string("op_name", "trim_cache");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("length", i->len);
      break;

    case Transaction::OP_TRUNCATE:
      f->dump_string("op_name", "truncate");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_unsigned("offset", i->off);
      break;

    case Transaction::OP_REMOVE:
      f->dump_string("op_name", "remove");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_SETATTR:
      f->dump_string("op_name", "setattr");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_string("name", i->name);
      f->dump_unsigned("length", i->data.length());
      break;

    case Transaction::OP_SETATTRS:
      f->dump_string("op_name", "setattrs");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->open_object_section("attr_lens");
      for (map<string,bufferptr>::iterator p = i->xattrs.begin();
	  p != i->xattrs.end(); ++p) {
	f->dump_unsigned(p->first.c_str(), p->second.length());
      }
      break;

    case Transaction::OP_RMATTR:
      f->dump_string("op_name", "rmattr");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_string("name", i->name);
      break;

    case Transaction::OP_RMATTRS:
      f->dump_string("op_name", "rmattrs");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_CLONE:
      f->dump_string("op_name", "clone");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("src_oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_stream("dst_oid") << get<1>(obj_slots[i->o2_ix]);
      break;

    case Transaction::OP_CLONERANGE:
      f->dump_string("op_name", "clonerange");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("src_oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_stream("dst_oid") << get<1>(obj_slots[i->o2_ix]);
      f->dump_unsigned("offset", i->off);
      f->dump_unsigned("len", i->len);
      break;

    case Transaction::OP_CLONERANGE2:
      f->dump_string("op_name", "clonerange2");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("src_oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_stream("dst_oid") << get<1>(obj_slots[i->o2_ix]);
      f->dump_unsigned("src_offset", i->off);
      f->dump_unsigned("len", i->len);
      f->dump_unsigned("dst_offset", i->off2);
      break;

    case Transaction::OP_MKCOLL:
      f->dump_string("op_name", "mkcoll");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      break;

    case Transaction::OP_RMCOLL:
      f->dump_string("op_name", "rmcoll");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      break;

    case Transaction::OP_COLL_ADD:
      f->dump_string("op_name", "collection_add");
      f->dump_stream("src_collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("dst_collection") << get<1>(col_slots[i->c2_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_COLL_REMOVE:
      f->dump_string("op_name", "collection_remove");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_COLL_MOVE:
      f->open_object_section("collection_move");
      f->dump_stream("src_collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("dst_collection") << get<1>(col_slots[i->c2_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->close_section();
      break;

    case Transaction::OP_COLL_SETATTR:
      f->dump_string("op_name", "collection_setattr");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_string("name", i->name);
      f->dump_unsigned("length", i->data.length());
      break;

    case Transaction::OP_COLL_RMATTR:
      f->dump_string("op_name", "collection_rmattr");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_string("name", i->name);
      break;

    case Transaction::OP_STARTSYNC:
      f->dump_string("op_name", "startsync");
      break;

    case Transaction::OP_COLL_RENAME:
      f->dump_string("op_name", "collection_rename");
      f->dump_stream("src_collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("dst_collection") << get<1>(col_slots[i->c2_ix]);
      break;

    case Transaction::OP_OMAP_CLEAR:
      f->dump_string("op_name", "omap_clear");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_OMAP_SETKEYS:
      f->dump_string("op_name", "omap_setkeys");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->open_object_section("attr_lens");
      for (map<string, bufferlist>::iterator p = i->attrs.begin();
	  p != i->attrs.end(); ++p) {
	f->dump_unsigned(p->first.c_str(), p->second.length());
      }
      f->close_section();
      break;

    case Transaction::OP_OMAP_RMKEYS:
      f->dump_string("op_name", "omap_rmkeys");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      break;

    case Transaction::OP_OMAP_SETHEADER:
      f->dump_string("op_name", "omap_setheader");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_stream("header_length") << i->data.length();
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      f->dump_string("op_name", "op_omap_rmkeyrange");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_string("first", i->name);
      f->dump_string("last", i->name2);
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      f->dump_string("op_name", "op_coll_move_rename");
      f->dump_stream("old_collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("old_oid") << get<1>(obj_slots[i->o1_ix]);
      f->dump_stream("new_collection") << get<1>(col_slots[i->c2_ix]);
      f->dump_stream("new_oid") << get<1>(obj_slots[i->o2_ix]);
      break;

    case Transaction::OP_SETALLOCHINT:
      f->dump_string("op_name", "op_setallochint");
      f->dump_stream("collection") << get<1>(col_slots[i->c1_ix]);
      f->dump_stream("oid") << get<1>(obj_slots[i->o1_ix]);
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

void ObjectStore::Transaction::Op::encode(bufferlist& bl) const
{
  ::encode(op, bl);
  // encode only the fields relevant to this op
  switch (op) {
  case OP_TOUCH:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_WRITE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    ::encode(data, bl);
    break;
  case OP_ZERO:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    break;
  case OP_TRIMCACHE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    break;
  case OP_TRUNCATE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(off, bl);
    break;
  case OP_REMOVE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_SETATTR:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(name, bl);
    ::encode(data, bl);
    break;
  case OP_SETATTRS:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(xattrs, bl);
    break;
  case OP_RMATTR:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(name, bl);
    break;
  case OP_RMATTRS:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_CLONE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(o2_ix, bl);
    break;
  case OP_CLONERANGE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(o2_ix, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    break;
  case OP_CLONERANGE2:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(o2_ix, bl);
    ::encode(off, bl);
    ::encode(len, bl);
    ::encode(off2, bl);
    break;
  case OP_RMCOLL:
    ::encode(c1_ix, bl);
    break;
  case OP_COLL_ADD:
    ::encode(c1_ix, bl);
    ::encode(c2_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_COLL_REMOVE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_COLL_MOVE:
    ::encode(c1_ix, bl);
    ::encode(c2_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_COLL_MOVE_RENAME:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(c2_ix, bl);
    ::encode(o2_ix, bl);
    break;
  case OP_COLL_SETATTRS:
    ::encode(c1_ix, bl);
    ::encode(xattrs, bl);
    break;
  case OP_COLL_RENAME:
    ::encode(c1_ix, bl);
    ::encode(c2_ix, bl);
    break;
  case OP_OMAP_CLEAR:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    break;
  case OP_OMAP_SETKEYS:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(attrs, bl);
    break;
  case OP_OMAP_RMKEYS:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(keys, bl);
    break;
  case OP_OMAP_RMKEYRANGE:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(name, bl);
    ::encode(name2, bl);
    break;
  case OP_OMAP_SETHEADER:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(data, bl);
    break;
  case OP_SETALLOCHINT:
    ::encode(c1_ix, bl);
    ::encode(o1_ix, bl);
    ::encode(value1, bl);
    ::encode(value2, bl);
    break;
  }
}

void ObjectStore::Transaction::Op::decode(bufferlist::iterator& p)
{
  coll_t cid;
  ::decode(op, p);
  switch (op) {
  case OP_TOUCH:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    break;
  case OP_WRITE:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(off, p);
    ::decode(len, p);
    ::decode(data, p);
    break;
  case OP_ZERO:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(off, p);
    ::decode(len, p);
    break;
  case OP_TRIMCACHE:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(off, p);
    ::decode(len, p);
    break;
  case OP_TRUNCATE:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(off, p);
    break;
  case OP_REMOVE:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    break;
  case OP_SETATTR:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(name, p);
    ::decode(data, p);
    break;
  case OP_SETATTRS:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(xattrs, p);
    break;
  case OP_RMATTR:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(name, p);
    break;
  case OP_RMATTRS:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    break;
  case OP_CLONE:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(o2_ix, p);
    break;
  case OP_CLONERANGE:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(o2_ix, p);
    ::decode(off, p);
    ::decode(len, p);
    break;
  case OP_CLONERANGE2:
    ::decode(cid, p);
    ::decode(o1_ix, p);
    ::decode(o2_ix, p);
    ::decode(off, p);
    ::decode(len, p);
    ::decode(off2, p);
    break;
  case OP_RMCOLL:
    ::decode(cid, p);
    break;
  case OP_COLL_ADD:
    ::decode(c1_ix, p);
    ::decode(c2_ix, p);
    ::decode(o1_ix, p);
    break;
  case OP_COLL_REMOVE:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    break;
  case OP_COLL_MOVE:
    ::decode(c1_ix, p);
    ::decode(c2_ix, p);
    ::decode(o1_ix, p);
    break;
  case OP_COLL_MOVE_RENAME:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    ::decode(c2_ix, p);
    ::decode(o2_ix, p);
    break;
  case OP_COLL_SETATTRS:
    ::decode(c1_ix, p);
    ::decode(xattrs, p);
    break;
  case OP_COLL_RENAME:
    ::decode(c1_ix, p);
    ::decode(c2_ix, p);
    break;
  case OP_OMAP_CLEAR:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    break;
  case OP_OMAP_SETKEYS:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    ::decode(attrs, p);
    break;
  case OP_OMAP_RMKEYS:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    ::decode(keys, p);
    break;
  case OP_OMAP_RMKEYRANGE:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    ::decode(name, p);
    ::decode(name2, p);
    break;
  case OP_OMAP_SETHEADER:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    ::decode(data, p);
    break;
  case OP_SETALLOCHINT:
    ::decode(c1_ix, p);
    ::decode(o1_ix, p);
    ::decode(value1, p);
    ::decode(value2, p);
    break;
  }
}

void ObjectStore::Transaction::generate_test_instances(
  list<ObjectStore::Transaction*>& o)
{
  o.push_back(new Transaction);

  Transaction* t = new Transaction;
  t->nop();
  o.push_back(t);

  coll_t c("foocoll");
  coll_t c2("foocoll2");

  hoid_t o1(oid_t("oid", chunktype::data, 91));
  hoid_t o2(oid_t("obj2", chunktype::ecc, 456));
  hoid_t o3(oid_t("obj3", chunktype::data, 456));

  t = new Transaction;
  t->push_cid(c);
  t->push_cid(c2);
  t->push_oid(o1);
  t->push_oid(o2);
  t->push_oid(o3);

  t->touch(0, 0);
  bufferlist bl;
  bl.append("some data");
  t->write(0, 0, 1, bl.length(), bl);
  t->zero(0, 0, 22, 33);
  t->truncate(0, 0, 99);
  t->remove(0, 0);
  o.push_back(t);

  t = new Transaction;
  t->push_cid(c);
  t->push_cid(c2);
  t->push_oid(o1);
  t->push_oid(o2);
  t->push_oid(o3);
  
  t->setattr(0, 0, "key", bl);
  map<string,bufferptr> m;
  m["a"] = ceph::buffer::copy("this", 4);
  m["b"] = ceph::buffer::copy("that", 4);
  t->setattrs(0, 0, m);
  t->rmattr(0, 0, "b");
  t->rmattrs(0, 0);

  t->clone(0, 0, 1);
  t->clone(0, 0, 2);
  t->clone_range(0, 0, 1, 1, 12, 99);

  // XXX: so, should the previous operations on c have failed?
  int ix = t->create_collection(c);
  t->collection_add(ix, 1, 0);
  t->collection_add(ix, 1, 1);
  t->collection_move(ix, 1, 2);
  t->remove_collection(ix);
  t->collection_setattr(ix, "this", bl);
  t->collection_rmattr(ix, "foo");
  t->collection_setattrs(ix, m);
  t->collection_rename(ix, 1);
  o.push_back(t);
}

int ObjectStore::collection_list(CollectionHandle ch,
				 vector<hoid_t>& o)
{
  vector<hoid_t> go;
  int ret = collection_list(ch, go);
  if (ret == 0) {
    o.reserve(go.size());
    for (vector<hoid_t>::iterator i = go.begin(); i != go.end() ; ++i)
      o.push_back(*i);
  }
  return ret;
}

int ObjectStore::collection_list_partial(CollectionHandle ch,
					 hoid_t start,
					 int min, int max,
					 vector<hoid_t>* ls,
					 hoid_t* next)
{
  vector<hoid_t> go;
  hoid_t gnext, gstart(start);
  int ret = collection_list_partial(ch, gstart, min, max, &go, &gnext);
  if (ret == 0) {
    *next = gnext;
    ls->reserve(go.size());
    for (vector<hoid_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(*i);
  }
  return ret;
}

int ObjectStore::collection_list_range(CollectionHandle ch,
				       hoid_t start,
				       hoid_t end,
				       vector<hoid_t>* ls)
{
  vector<hoid_t> go;
  hoid_t gstart(start), gend(end);
  int ret = collection_list_range(ch, gstart, gend, &go);
  if (ret == 0) {
    ls->reserve(go.size());
    for (vector<hoid_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(*i);
  }
  return ret;
}
