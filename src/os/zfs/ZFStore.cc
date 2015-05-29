// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ZFStore.h"

#include "include/stringify.h"
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_generators.hpp>

static std::mutex mtx;
static std::atomic<bool> initialized;
std::atomic<uint32_t> ZFStore::n_instances;
lzfw_handle_t* ZFStore::zhd;

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "zfstore(" << path << ") "

/* Factory method */
ObjectStore* ZFStore_factory(CephContext* cct,
			     const std::string& data,
			     const std::string& journal)
{
  return new ZFStore(cct, data);
}

/* DLL machinery */
extern "C" {
  void* objectstore_dllinit()
  {
    return reinterpret_cast<void*>(ZFStore_factory);
  }
} /* extern "C" */


ZFStore::ZFStore(CephContext* cct, const std::string& path)
  : ObjectStore(cct, path), zhfs(nullptr)
{
  if (!initialized) {
    std::unique_lock<std::mutex> l(mtx);
    if (!initialized) {
      zhd = lzfw_init();
      if (!zhd) {
	dout(-1) << "lzfw_init() failed" << dendl;
      }
      initialized = true;
    }
  }
  ++n_instances;
}

/* make a root "osdfs" filesystem on the device */
int ZFStore::mkfs()
{
  std::string osdfs = path + "/osdfs";

  abort();
  return 0;
}

int ZFStore::statfs(struct statfs* st)
{
  int err;
  struct statvfs stv;

  assert(zhfs);

  err = lzfw_statfs(zhfs, &stv);
  if (!err) {
    memset(st, 0, sizeof(struct statfs));
    st->f_bsize = stv.f_bsize;
    st->f_blocks = stv.f_blocks;
    st->f_bfree = stv.f_bfree;
    st->f_bavail = stv.f_bavail;
    st->f_files = stv.f_files;
    st->f_ffree = stv.f_ffree;
    st->f_namelen = stv.f_namemax;
  }

  return err;
} /* statfs */

int ZFStore::mount() {
  assert(zhd);

  /* path -> pool */
  zhfs = lzfw_mount(path.c_str(), "/tank" /* XXX */, "" /* XXX */);
  if (!zhfs) {
    dout(-1) << "lzfw_mount() failed"
	     << " path=" << path << " dir=" << "/tank"
	     << " opts=" << "" << dendl;
    return -EINVAL;
  }
  return 0;
}

int ZFStore::umount() {
  assert(zhfs);
  int r = lzfw_umount(zhfs, true /* XXX force */);
  zhfs = nullptr;
  return -r;
}

bool ZFStore::exists(CollectionHandle ch, const hoid_t& oid)
{
  ZCollection* zc = static_cast<ZCollection*>(ch);
  return zc->index.lookup(oid) == 0;
}

int ZFStore::stat(CollectionHandle ch, ObjectHandle oh,
		  struct stat* st, bool allow_eio)
{
  abort();
  return 0;
} /* stat */

int ZFStore::read(CollectionHandle ch, ObjectHandle oh,
		  uint64_t offset, size_t len, bufferlist& bl,
		  bool allow_eio)
{
  abort();
  return 0;
} /* read */

int ZFStore::fiemap(CollectionHandle ch, ObjectHandle oh,
		    uint64_t offset, size_t len, bufferlist& bl)
{
  abort();
  return 0;
}

int ZFStore::getattr(CollectionHandle ch, ObjectHandle oh,
		     const char* name, bufferptr& value)
{
  abort();
  return 0;
} /* getattr */

int ZFStore::getattrs(CollectionHandle ch, ObjectHandle oh,
		      map<std::string,bufferptr>& aset,
		      bool user_only)
{
  abort();
  return 0;
}

int ZFStore::list_collections(vector<coll_t>& ls)
{
  abort();
  return 0;
}

CollectionHandle ZFStore::open_collection(const coll_t& c)
{
  abort();
  return nullptr;
}

int ZFStore::close_collection(CollectionHandle ch)
{
  abort();
  return 0;
}

bool ZFStore::collection_exists(const coll_t& c)
{
  abort();
  return true;
}

int ZFStore::collection_getattr(CollectionHandle ch, const char* name,
				void* value, size_t size)
{
  abort();
  return 0;
}

int ZFStore::collection_getattr(CollectionHandle ch, const char* name,
				bufferlist& bl)
{
  abort();
  return 0;
}

int ZFStore::collection_getattrs(CollectionHandle ch,
				 map<std::string,bufferptr>& aset)
{
  abort();
  return 0;
}

bool ZFStore::collection_empty(CollectionHandle ch)
{
  abort();
  return true;
}

int ZFStore::collection_list(CollectionHandle ch, vector<hoid_t>& o)
{
  abort();
  return 0;
}

int ZFStore::collection_list_partial(CollectionHandle ch, hoid_t start,
				     int min, int max, vector<hoid_t>* ls,
				     hoid_t* next)
{
  abort();
  return 0;
}

int ZFStore::collection_list_range(CollectionHandle ch, hoid_t start,
				   hoid_t end, vector<hoid_t>* ls)
{
  abort();
  return 0;
}

int ZFStore::collection_list_partial2(CollectionHandle ch,
				      int min, int max,
				      vector<hoid_t> *vs,
				      CLPCursor& cursor)
{
  abort();
  return 0;
}

int ZFStore::omap_get(CollectionHandle ch, ObjectHandle oh,
		      bufferlist* header, map<std::string, bufferlist>* out)
{
  abort();
  return 0;
}

int ZFStore::omap_get_header(CollectionHandle ch, ObjectHandle oh,
			     bufferlist* header, bool allow_eio)
{
  abort();
  return 0;
}

int ZFStore::omap_get_keys(CollectionHandle ch, ObjectHandle oh,
			   set<std::string>* keys)
{
  abort();
  return 0;
}

int ZFStore::omap_get_values(CollectionHandle ch, ObjectHandle oh,
			     const set<std::string>& keys,
			     map<std::string, bufferlist>* out)
{
  abort();
  return 0;
}

int ZFStore::omap_check_keys(CollectionHandle ch, ObjectHandle oh,
			     const set<std::string>& keys,
			     set<std::string>* out)
{
  abort();
  return 0;
}

ObjectMap::ObjectMapIterator ZFStore::get_omap_iterator(CollectionHandle ch,
							ObjectHandle oh)
{
  ZObject* o = static_cast<ZObject*>(oh);
  return ObjectMap::ObjectMapIterator(new ZOmapIterator(o));
}

void ZFStore::set_fsid(const boost::uuids::uuid& u)
{
  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

boost::uuids::uuid ZFStore::get_fsid()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  assert(r >= 0);
  boost::uuids::string_generator parse;
  return parse(fsid_str);
}

objectstore_perf_stat_t ZFStore::get_cur_stats()
{
  return objectstore_perf_stat_t();
}

int ZFStore::queue_transactions(list<Transaction*>& tls,
				OpRequestRef op)
{
  abort();
  return 0;
}

ZFStore::~ZFStore()
{
  --n_instances;
  if (n_instances == 0) {
    std::unique_lock<std::mutex> l(mtx);
    if (n_instances == 0) {
      lzfw_exit(zhd);
      zhd = nullptr;
      initialized = false;
    }
  }
}
