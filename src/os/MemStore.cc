// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "MemStore.h"
#include <map>

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "memstore(" << path << ") "

using std::min;

/* Factory method */
ObjectStore* MemStore_factory(CephContext* cct,
			      const std::string& data,
			      const std::string& journal)
{
  return new MemStore(cct, data);
}

/* DLL machinery */
extern "C" {
  void* objectstore_dllinit()
  {
    return reinterpret_cast<void*>(MemStore_factory);
  }
} /* extern "C" */


int MemStore::peek_journal_fsid(boost::uuids::uuid *fsid)
{
  *fsid = boost::uuids::nil_uuid();
  return 0;
}

int MemStore::mount()
{
  int r = _load();
  if (r < 0)
    return r;
  tx_tp.start();
  finisher.start();
  return 0;
}

int MemStore::umount()
{
  tx_tp.stop();
  finisher.stop();
  return _save();
}

int MemStore::_save()
{
  dout(10) << __func__ << dendl;
  Mutex::Locker l(apply_lock); // block any writer
  dump_all();
  set<coll_t> collections;
  for (map<coll_t,MemCollection*>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(20) << __func__ << " coll " << p->first << " " << p->second << dendl;
    collections.insert(p->first);
    bufferlist bl;
    assert(p->second);
    p->second->encode(bl);
    string fn = path + "/" + stringify(p->first);
    int r = bl.write_file(fn.c_str());
    if (r < 0)
      return r;
  }

  string fn = path + "/collections";
  bufferlist bl;
  ::encode(collections, bl);
  int r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  return 0;
}

void MemStore::dump_all()
{
  Formatter *f = new_formatter("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(0) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}

void MemStore::dump(Formatter *f)
{
  f->open_array_section("collections");
  for (map<coll_t,MemCollection*>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->open_array_section("xattrs");
    for (map<string,bufferptr>::iterator q = p->second->xattr.begin();
	 q != p->second->xattr.end();
	 ++q) {
      f->open_object_section("xattr");
      f->dump_string("name", q->first);
      f->dump_int("length", q->second.length());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("objects");
    for (map<hobject_t,ObjectRef>::iterator q = p->second->object_map.begin();
	 q != p->second->object_map.end();
	 ++q) {
      f->open_object_section("object");
      f->dump_string("name", stringify(q->first));
      if (q->second)
	q->second->dump(f);
      f->close_section();
    }
    f->close_section();

    f->close_section();
  }
  f->close_section();
}

int MemStore::_load()
{
  dout(10) << __func__ << dendl;
  bufferlist bl;
  string fn = path + "/collections";
  string err;
  int r = bl.read_file(fn.c_str(), &err);
  if (r < 0)
    return r;

  set<coll_t> collections;
  bufferlist::iterator p = bl.begin();
  ::decode(collections, p);

  for (set<coll_t>::iterator q = collections.begin();
       q != collections.end();
       ++q) {
    string fn = path + "/" + stringify(*q);
    bufferlist cbl;
    int r = cbl.read_file(fn.c_str(), &err);
    if (r < 0)
      return r;
    MemCollection* c(new MemCollection(*q));
    bufferlist::iterator p = cbl.begin();
    c->decode(p);
    coll_map[*q] = c;
  }

  dump_all();

  return 0;
}

void MemStore::set_fsid(const boost::uuids::uuid& u)
{
  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

boost::uuids::uuid MemStore::get_fsid()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  assert(r >= 0);
  boost::uuids::string_generator parse;
  return parse(fsid_str);
}

int MemStore::mkfs()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  if (r == -ENOENT) {
    boost::uuids::uuid fsid = boost::uuids::random_generator()();
    fsid_str = stringify(fsid);
    r = write_meta("fs_fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else {
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  return 0;
}

int MemStore::statfs(struct statfs *st)
{
  dout(10) << __func__ << dendl;
  // make some shit up.	 these are the only fields that matter.
  st->f_bsize = 1024;
  st->f_blocks = 1000000;
  st->f_bfree =	 1000000;
  st->f_bavail = 1000000;
  return 0;
}

objectstore_perf_stat_t MemStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}


// ---------------
// read operations

bool MemStore::exists(CollectionHandle ch, const hobject_t& oid)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oid << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);

  // Perform equivalent of c->get_object_(oid) != NULL. In C++11 the
  // shared_ptr needs to be compared to nullptr.
  return (bool)c->get_object(oid);
}

int MemStore::stat(
    CollectionHandle ch,
    const ObjectHandle oh,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  st->st_size = o->data_len;
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int MemStore::read(
    CollectionHandle ch,
    const ObjectHandle oh,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << " "
	   << offset << "~" << len << dendl;

  Object* o = static_cast<Object*>(oh);
  if (offset >= o->data_len)
    return 0;
  size_t l = len;
  if (l == 0)  // note: len == 0 means read the entire object
    l = o->data_len;
  else if (offset + l > o->data_len)
    l = o->data_len - offset;
  bl.clear();
  return _read_pages(o->data, offset, l, bl);
}

int MemStore::_read_pages(page_set &pages, unsigned offset, size_t len,
			  bufferlist &bl)
{
  const unsigned end = offset + len;
  size_t remaining = len;

  page_set::const_iterator page = pages.first_page_containing(offset, len);
  while (remaining) {
    // no more pages in range
    if (page == pages.end() || page->offset >= end) {
      bl.append_zero(remaining);
      break;
    }

    // fill any holes between pages with zeroes
    if (page->offset > offset) {
      const size_t count = min(remaining, page->offset - offset);
      bl.append_zero(count);
      remaining -= count;
      offset = page->offset;
      if (!remaining)
	break;
    }

    // read from page
    const uint64_t page_offset = offset - page->offset;
    const size_t count = min(remaining, PageSize - page_offset);

    bl.append(page->data + page_offset, count);

    remaining -= count;
    offset += count;
    ++page;
  }
  return len;
}

int MemStore::fiemap(CollectionHandle ch, const ObjectHandle oh,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid()
	   << " " << offset << "~" << len << dendl;

  Object* o = static_cast<Object*>(oh);
  if (offset >= o->data_len)
    return 0;
  size_t l = len;
  if (offset + l > o->data_len)
    l = o->data_len - offset;
  map<uint64_t, uint64_t> m;
  m[offset] = l;
  ::encode(m, bl);
  return 0;
}

int MemStore::getattr(CollectionHandle ch, const ObjectHandle oh,
		      const char *name, bufferptr& value)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid()
	   << " " << name << dendl;

  Object* o = static_cast<Object*>(oh);
  string k(name);
  if (!o->xattr.count(k)) {
    return -ENODATA;
  }
  value = o->xattr[k];
  return 0;
}

int MemStore::getattrs(CollectionHandle ch, const ObjectHandle oh,
		       map<string,bufferptr>& aset, bool user_only)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  if (user_only) {
    for (map<string,bufferptr>::iterator p = o->xattr.begin();
	 p != o->xattr.end();
	 ++p) {
      if (p->first.length() > 1 && p->first[0] == '_') {
	aset[p->first.substr(1)] = p->second;
      }
    }
  } else {
    aset = o->xattr;
  }
  return 0;
}

int MemStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << dendl;
  RWLock::RLocker l(coll_lock);
  for (map<coll_t,MemCollection*>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

ObjectStore::CollectionHandle MemStore::open_collection(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  map<coll_t,MemCollection*>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return NULL;
  return cp->second;
}

int MemStore::close_collection(CollectionHandle ch)
{
  // XXX do nothing
  return 0;
}

bool MemStore::collection_exists(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::RLocker l(coll_lock);

  return coll_map.count(cid);
}

int MemStore::collection_getattr(CollectionHandle ch, const char *name,
				 void *value, size_t size)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << name << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker lc(c->lock);

  if (!c->xattr.count(name))
    return -ENOENT;
  bufferlist bl;
  bl.append(c->xattr[name]);
  size_t l = MIN(size, bl.length());
  bl.copy(0, size, (char *)value);
  return l;
}

int MemStore::collection_getattr(CollectionHandle ch, const char *name,
				 bufferlist& bl)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << name << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker l(c->lock);
  
  if (!c->xattr.count(name))
    return -ENOENT;
  bl.clear();
  bl.append(c->xattr[name]);
  return bl.length();
}

int MemStore::collection_getattrs(CollectionHandle ch,
				  map<string,bufferptr> &aset)
{
  dout(10) << __func__ << " " << ch->get_cid() << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker l(c->lock);

  aset = c->xattr;
  return 0;
}

bool MemStore::collection_empty(CollectionHandle ch)
{
  dout(10) << __func__ << " " << ch->get_cid() << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker l(c->lock);

  return c->object_map.empty();
}

int MemStore::collection_list(CollectionHandle ch, vector<hobject_t>& o)
{
  dout(10) << __func__ << " " << ch->get_cid() << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker l(c->lock);

  for (map<hobject_t,ObjectRef>::iterator p = c->object_map.begin();
       p != c->object_map.end();
       ++p)
    o.push_back(p->first);
  return 0;
}

int MemStore::collection_list_partial(CollectionHandle ch,
				      hobject_t start, int min, int max,
				      vector<hobject_t> *ls, hobject_t *next)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << start << " " << min
	   << "-" << max << " " << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker l(c->lock);

  map<hobject_t,ObjectRef>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 ls->size() < (unsigned)max) {
    ls->push_back(p->first);
    ++p;
  }
  if (p != c->object_map.end())
    *next = p->first;

  return 0;
}

int MemStore::collection_list_range(CollectionHandle ch,
				    hobject_t start, hobject_t end,
				    vector<hobject_t> *ls)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << start << " " << end
	   << dendl;

  MemCollection* c = static_cast<MemCollection*>(ch);
  RWLock::RLocker l(c->lock);

  map<hobject_t,ObjectRef>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 p->first < end) {
    ls->push_back(p->first);
    ++p;
  }
  return 0;
}

int MemStore::omap_get(
    CollectionHandle ch, ///< [in] Collection containing oid
    const ObjectHandle oh,   ///< [in] Object containing omap
    bufferlist *header,	    ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    )
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  *header = o->omap_header;
  *out = o->omap;
  return 0;
}

int MemStore::omap_get_header(
    CollectionHandle ch, ///< [in] Collection containing oid
    const ObjectHandle oh,   ///< [in] Object containing omap
    bufferlist *header,	    ///< [out] omap header
    bool allow_eio	    ///< [in] don't assert on eio
    )
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  if (!o)
    return -ENOENT;
  *header = o->omap_header;
  return 0;
}

int MemStore::omap_get_keys(
    CollectionHandle ch, ///< [in] Collection containing oid
    const ObjectHandle oh,   ///< [in] Object containing omap
    set<string> *keys	    ///< [out] Keys defined on oid
    )
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  for (map<string,bufferlist>::iterator p = o->omap.begin();
       p != o->omap.end();
       ++p)
    keys->insert(p->first);
  return 0;
}

int MemStore::omap_get_values(
    CollectionHandle ch, ///< [in] Collection containing oid
    const ObjectHandle oh,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    )
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*q);
  }
  return 0;
}

int MemStore::omap_check_keys(
    CollectionHandle ch, ///< [in] Collection containing oid
    const ObjectHandle oh,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out	    ///< [out] Subset of keys defined on oid
    )
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*p);
  }
  return 0;
}

ObjectMap::ObjectMapIterator
MemStore::get_omap_iterator(CollectionHandle ch, const ObjectHandle oh)
{
  dout(10) << __func__ << " " << ch->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(o));
}


// ---------------
// write operations

int MemStore::queue_transactions(Sequencer *osr,
				 list<Transaction*>& tls,
				 OpRequestRef op,
				 ThreadPool::TPHandle *handle)
{
  // fixme: ignore the Sequencer and serialize everything.
  Mutex::Locker l(apply_lock);

  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p)
    tx_wq.queue(*p);

  return 0;
}

void MemStore::_finish_transaction(Transaction &t)
{
  Context *on_apply_sync = t.get_on_applied_sync();
  Context *on_apply = t.get_on_applied();
  Context *on_commit = t.get_on_commit();

  if (on_apply_sync)
    on_apply_sync->complete(0);
  if (on_apply)
    finisher.queue(on_apply);
  if (on_commit)
    finisher.queue(on_commit);
}

void MemStore::_do_transaction(Transaction& t, ThreadPool::TPHandle &handle)
{
  int pos = 0;

  for (Transaction::op_iterator i = t.begin(); i != t.end(); ++i) {

    int r = 0;
    MemCollection* c = nullptr;
    ObjectHandle oh2, oh;

    switch (i->op) {
    case Transaction::OP_NOP:
      break;

    case Transaction::OP_TOUCH:
      // may create o
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _touch(c, oh);
	}
      }
      break;

    case Transaction::OP_WRITE:
      // may create o
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _write(c, oh, i->off, i->len, i->data, t.get_replica());
	}
      }
      break;

    case Transaction::OP_ZERO:
      // may create o
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _zero(c, oh, i->off, i->len);
	}
      }
      break;

    case Transaction::OP_TRUNCATE:
      // may create o
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _truncate(c, oh, i->off);
	}
      }
      break;

    case Transaction::OP_REMOVE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  r = _remove(c, oh);
	}
      }
      break;

    case Transaction::OP_SETATTR:
      {
	r = -ENOENT;
	c = get_slot_collection(t, i->c1_ix);
	if (c) {
	  oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	  if (oh) {
	    bufferlist &bl = i->data;
	    map<string, bufferptr> to_set;
	    to_set[i->name] = bufferptr(bl.c_str(), bl.length());
	    r = _setattrs(c, oh, to_set);
	  }
	}
      }
      break;

    case Transaction::OP_SETATTRS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _setattrs(c, oh, i->xattrs);
	}
      }
      break;

    case Transaction::OP_RMATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  r = _rmattr(c, oh, i->name.c_str());
	}
      }
      break;

    case Transaction::OP_RMATTRS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  r = _rmattrs(c, oh);
	}
      }
      break;

    case Transaction::OP_CLONE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  oh2 = get_slot_object(t, c, i->o2_ix, true /* create */);
	  if (oh2) {
	    r = _clone(c, oh, oh2);
	  }
	}
      }
      break;

    case Transaction::OP_CLONERANGE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  oh2 = get_slot_object(t, c, i->o2_ix, true /* create */);
	  if (oh2) {
	    r = _clone_range(c, oh, oh2, i->off, i->len, i->off);
	  }
	}
      }
      break;

    case Transaction::OP_CLONERANGE2:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  oh2 = get_slot_object(t, c, i->o2_ix, true /* create */);
	  if (oh2) {
	    r = _clone_range(c, oh, oh2, i->off, i->len, i->off2);
	  }
	}
      }
      break;

    case Transaction::OP_MKCOLL:
      r = _create_collection(std::get<1>(t.c_slot(i->c1_ix)));
      if (!r) {
	(void) get_slot_collection(t, i->c1_ix);
      }
      break;

    case Transaction::OP_RMCOLL:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	r = _destroy_collection(c);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	r = _collection_setattr(c, i->name.c_str(),
				i->data.c_str(), i->data.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	r = _collection_rmattr(c, i->name.c_str());
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  r = _omap_clear(c, oh);
	}
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _omap_setkeys(c, oh, i->attrs);
	}
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  r = _omap_rmkeys(c, oh, i->keys);
	}
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (oh) {
	  r = _omap_rmkeyrange(c, oh, i->name, i->name2);
	}
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	oh = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (oh) {
	  r = _omap_setheader(c, oh, i->data);
	}
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      // nop
      break;
#if 0
    case Transaction::OP_COLL_ADD:
      r = -EINVAL; // removed
      break;

    case Transaction::OP_COLL_REMOVE:
      r = -EINVAL; // removed
      break;

    case Transaction::OP_COLL_MOVE:
      r = -EINVAL; // removed
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      r = -EINVAL; // removed
      break;

    case Transaction::OP_COLL_RENAME:
      r = -EINVAL; // removed
      break;
#endif
    default:
      derr << "bad op " << i->op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(i->op == Transaction::OP_CLONERANGE ||
			    i->op == Transaction::OP_CLONE ||
			    i->op == Transaction::OP_CLONERANGE2))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (i->op == Transaction::OP_CLONERANGE ||
			     i->op == Transaction::OP_CLONE ||
			     i->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	  dump_all();
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation "
		<< i->op << " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;

    handle.reset_tp_timeout();
  }
}

int MemStore::_touch(MemCollection* c, ObjectHandle oh)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  c->get_or_create_object(oh->get_oid());
  return 0;
}

int MemStore::_write(MemCollection* c, ObjectHandle oh,
		     uint64_t offset, size_t len, const bufferlist& bl,
		     bool replica)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << " "
	   << offset << "~" << len << dendl;
  
  assert(len == bl.length());

  Object* o = static_cast<Object*>(oh);
  _write_pages(bl, offset, o);

  // extend the length
  if (o->data_len < offset + len)
    o->data_len = offset + len;
  return 0;
}

void MemStore::_write_pages(const bufferlist& src, unsigned offset,
			    Object* o)
{
  unsigned len = src.length();

  // count the overlapping pages
  size_t page_count = 0;
  if (offset % PageSize) {
    page_count++;
    size_t rem = PageSize - offset % PageSize;
    len = len <= rem ? 0 : len - rem;
  }
  page_count += len / PageSize;
  if (len % PageSize)
    page_count++;

  // allocate a vector for page pointers
  // TODO: preallocate page vectors for each worker thread
  typedef vector<page_set::page_type*> page_vec;
  page_vec pages;
  pages.reserve(page_count);

  o->alloc_lock.lock();

  // make sure the page range is allocated
  page_set::iterator p = o->data.alloc_range(offset, src.length());
  // flatten the range into a vector while we hold the lock
  for (size_t i = 0; i < page_count; i++) {
    pages.push_back(&*p);
    p->get();
    ++p;
  }

  o->alloc_lock.unlock();

  bufferlist* ncbl = const_cast<bufferlist*>(&src);
  page_vec::iterator page = pages.begin();

  buffer::list::iterator bl_iter = ncbl->begin();
  while (! bl_iter.end()) {
    char *data = bl_iter.get_bytes(&len);
    unsigned page_offset = offset - (*page)->offset;
    unsigned pageoff = PageSize - page_offset;
    unsigned count = min((size_t)len, (size_t) pageoff);
    memcpy((*page)->data + page_offset, data, count);
    offset += count;
    if (count == pageoff)
      ++page;
    bl_iter.advance(count);
  }

  // drop page refs
  for (size_t i = 0; i < page_count; i++)
    pages[i]->put();
}

int MemStore::_zero(MemCollection* c, ObjectHandle oh,
		    uint64_t offset, size_t len)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid()
	   << " " << offset << "~" << len << dendl;
  bufferptr bp(len);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  return _write(c, oh, offset, len, bl);
}

int MemStore::_truncate(MemCollection* c, ObjectHandle oh,
			uint64_t size)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid()
	   << " " << size << dendl;

  Object* o = static_cast<Object*>(oh);
  if (o->data_len > size) {
    o->alloc_lock.lock();
    o->data.free_pages_after(size);
    o->alloc_lock.unlock();
  }
  o->data_len = size;
  return 0;
}

int MemStore::_remove(MemCollection* c, ObjectHandle oh)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  c->object_map.erase(oh->get_oid());
  c->object_hash.erase(oh->get_oid());
  return 0;
}

int MemStore::_setattrs(MemCollection* c, ObjectHandle oh,
			map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  for (map<string,bufferptr>::const_iterator p = aset.begin(); p != aset.end();
       ++p)
    o->xattr[p->first] = p->second;
  return 0;
}

int MemStore::_rmattr(MemCollection* c, ObjectHandle oh,
		      const char *name)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid()
	   << " " << name << dendl;

  Object* o = static_cast<Object*>(oh);
  map<string,bufferptr>::iterator iter = o->xattr.find(name);
  if (iter != o->xattr.end())
    o->xattr.erase(iter);
  else
    return -ENODATA;
  return 0;
}

int MemStore::_rmattrs(MemCollection* c, ObjectHandle oh)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  o->xattr.clear();
  return 0;
}

int MemStore::_clone(MemCollection* c, ObjectHandle oh,
		     ObjectHandle noh)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid()
	   << " -> " << noh->get_oid() << dendl;

  // XXX: hold lock over both calls
  Object* o = static_cast<Object*>(oh);
  Object* n = static_cast<Object*>(noh);
  return -ENOTSUP; // TODO: clone
}

int MemStore::_clone_range(MemCollection* c, ObjectHandle oh,
			   ObjectHandle noh, uint64_t srcoff,
			   uint64_t len, uint64_t dstoff)
{
  dout(10) << __func__ << " " << c->get_cid() << " "
	   << oh->get_oid() << " " << srcoff << "~" << len << " -> "
	   << noh->get_oid() << " " << dstoff << "~" << len
	   << dendl;
  
  // XXX: hold lock over both calls
  Object* o = static_cast<Object*>(oh);
  Object* n = static_cast<Object*>(noh);

  if (srcoff >= o->data_len)
    return 0;

  return -ENOTSUP; // TODO: clone
}

int MemStore::_omap_clear(MemCollection* c, ObjectHandle oh)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  o->omap.clear();
  return 0;
}

int MemStore::_omap_setkeys(MemCollection* c, ObjectHandle oh,
			    const map<string, bufferlist> &aset)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  for (map<string,bufferlist>::const_iterator p = aset.begin(); p != aset.end();
       ++p)
    o->omap[p->first] = p->second;
  return 0;
}

int MemStore::_omap_rmkeys(MemCollection* c, ObjectHandle oh,
			   const set<string> &keys)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p)
    o->omap.erase(*p);
  return 0;
}

int MemStore::_omap_rmkeyrange(MemCollection* c,
			       ObjectHandle oh, const string& first,
			       const string& last)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid()
	   << " " << first << " " << last << dendl;

  Object* o = static_cast<Object*>(oh);
  map<string,bufferlist>::iterator p = o->omap.upper_bound(first);
  map<string,bufferlist>::iterator e = o->omap.lower_bound(last);
  while (p != e)
    o->omap.erase(p++);
  return 0;
}

int MemStore::_omap_setheader(MemCollection* c,
			      ObjectHandle oh, const bufferlist &bl)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << oh->get_oid() << dendl;

  Object* o = static_cast<Object*>(oh);
  o->omap_header = bl;
  return 0;
}

int MemStore::_create_collection(const coll_t &cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::WLocker l(coll_lock);
  map<coll_t,MemCollection*>::iterator cp = coll_map.find(cid);
  if (cp != coll_map.end())
    return -EEXIST;
  coll_map[cid] = new MemCollection(cid);
  return 0;
}

int MemStore::_destroy_collection(MemCollection* c)
{
  dout(10) << __func__ << " " << c->get_cid() << dendl;
  RWLock::WLocker l(coll_lock);
  map<coll_t,MemCollection*>::iterator cp = coll_map.find(c->get_cid());
  if (cp == coll_map.end())
    return -ENOENT;
  {
    RWLock::RLocker l2(cp->second->lock);
    if (!cp->second->object_map.empty())
      return -ENOTEMPTY; // XXXX does this prevent destruction in general?
  }
  coll_map.erase(cp);
  // XXX delete c?
  return 0;
}

int MemStore::_collection_setattr(MemCollection* c, const char *name,
				  const void *value, size_t size)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << name << dendl;
  
  RWLock::WLocker l(c->lock);
  c->xattr[name] = bufferptr((const char *)value, size);
  return 0;
}

int MemStore::_collection_setattrs(MemCollection* c,
				   map<string,bufferptr> &aset)
{
  dout(10) << __func__ << " " << c->get_cid() << dendl;

  RWLock::WLocker l(c->lock);
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end();
       ++p) {
    c->xattr[p->first] = p->second;
  }
  return 0;
}

int MemStore::_collection_rmattr(MemCollection* c, const char *name)
{
  dout(10) << __func__ << " " << c->get_cid() << " " << name << dendl;

  RWLock::WLocker l(c->lock);
  if (c->xattr.count(name) == 0) // XXX can't we just erase?  nothrow?
    return -ENODATA;
  c->xattr.erase(name);
  return 0;
}
