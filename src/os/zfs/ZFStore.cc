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

#include <algorithm>
#include <sys/uio.h>
#include "include/stringify.h"
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/algorithm/string/split.hpp>
#include "common/zipkin_trace.h"
#include "ZFSHelper.h"

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

using namespace cohort_zfs;

ZFStore::ZFStore(CephContext* cct, const std::string& path)
  : ObjectStore(cct, path),
    root_ds(path + "/osdfs"),
    zhfs(nullptr),
    meta_ino{0,0}, meta_vno(nullptr),
    trace_endpoint("0.0.0.0", 0, NULL)
{
  /* global init */
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
  /* instance init */
}

/* make a root osd filesystem on the device */
int ZFStore::mkfs()
{
  const char* lzw_err;
  zp_desc_map zpm;
  int sz, err = 0;

  using std::get;

  assert(zhd);
  assert(!zhfs);

  // XXXX implement
  //err = lzfw_zpool_stat(zhd, path.c_str(), struct lzfs_pstat);
  if (err && (err == ENOENT)) {
    if (cct->_conf->zfstore_zpool_create) {
      // look for a matching pool in zfstore_zpool_devices
      std::string zp_devs = cct->_conf->zfstore_zpool_devices;
      sz = parse_zp_desc(zp_devs, zpm);
      zp_desc_map::iterator zpi = zpm.find(path);
      if (zpi != zpm.end()) {
	// we have the parameters to create zpool
	zp_desc_type& zpd = zpi->second;
	std::vector<std::string>& zp_devs = get<2>(zpd);
	// API is a pain in the a**
	char** zp_devs_arr = static_cast<char**>(
			         alloca(zp_devs.size()*sizeof(char*)));
	for (int dev_ix = 0; dev_ix < zp_devs.size(); ++dev_ix) {
	  zp_devs_arr[dev_ix] = const_cast<char*>(zp_devs[dev_ix].c_str());
	}
	err = lzfw_zpool_create(zhd, path.c_str(), get<1>(zpd).c_str(),
				(const char**) zp_devs_arr, zp_devs.size(),
				&lzw_err);
	if (!!err) {
	  dout(-1) << "lzfw_zpool_create failed " << " path=" << path << dendl;
	  goto out;
	}
      }
    }
  }
  // err = lzfw_dataset_stat(zhd, osdfs.c_str(), struct lzfw_dstat);
  if (err && (err == ENOENT)) {
    // create dataset
    char* ds;
    zfsh_adup(root_ds, ds); // spa routines chan change ds
    err = lzfw_dataset_create(zhd, ds, ZFS_TYPE_FILESYSTEM, &lzw_err);
  }
 out:
  return err;
} /* mkfs */

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

int ZFStore::attach_meta() {
  int err = 0;
  if (! zhfs) {
    std::unique_lock<std::mutex> lck(mtx);
    if (! zhfs) {
      zhfs = lzfw_mount(path.c_str(), "/osdfs", "" /* mount options */);
      if (! zhfs)
	return -EIO;
      /* mount it */
      err = lzfw_getroot(zhfs, &meta_ino);
      if (!!err)
	return err;
      /* open root */
      err = lzfw_opendir(zhfs, &cred, meta_ino, &meta_vno);
    }
  }
  return err;
} /* attach_meta() */

  // read and write key->value pairs to UNMOUNTED instance
int ZFStore::read_meta(const std::string& key, std::string* value)
{
  attach_meta();

  int err;
  unsigned flags = 0;
  lzfw_vnode_t* vno = nullptr;

  assert(key.length() <= 254); /* ZFS internal limit */

  err = lzfw_openat(zhfs, &cred, meta_vno, key.c_str(), O_RDWR|O_CREAT, 644,
		    &flags, &vno);
  if (!!err)
    return -err;

  const int BUFSZ = 1024;

  char buf[BUFSZ];
  struct iovec iov;
  struct stat st;
  size_t size;
  off_t off;

  err = lzfw_stat(zhfs, &cred, vno, &st);
  if (err)
    goto out;

  off = 0;
  size = st.st_size;
  value->clear();

  while (size > 0) {
    iov.iov_base = buf;
    iov.iov_len = MIN(BUFSZ, size); // lexical min
    err = lzfw_preadv(zhfs, &cred, vno, &iov, 1, off);
    if (!err) {
      err = EIO;
      break;
    }
    value->append(static_cast<char*>(iov.iov_base), err);
    off += err;
    size -= err;
  }

 out:
  (void) lzfw_close(zhfs, &cred, vno, O_RDWR|O_CREAT);
  return -err;
} /* read_meta */

int ZFStore::write_meta(const std::string& key, const std::string& value)
{
  attach_meta();

  int err;
  unsigned flags = 0;
  lzfw_vnode_t* vno = nullptr;

  assert(key.length() <= 254); /* ZFS internal limit */

  err = lzfw_openat(zhfs, &cred, meta_vno, key.c_str(), O_RDWR|O_CREAT, 644,
		    &flags, &vno);
  if (!!err)
    return -err;

  struct iovec iov;
  iov.iov_base = const_cast<char*>(value.c_str());
  iov.iov_len = value.length()+1;

  assert(meta_vno);
  err = lzfw_pwritev(zhfs, &cred, meta_vno, &iov, 1, 0 /* offset */);

  /* close it */
  (void) lzfw_close(zhfs, &cred, vno, O_RDWR|O_CREAT);
  return -err;
} /* write meta */

int ZFStore::mount() {
  assert(zhd);
  attach_meta(); /* attach osdfs, if not already */
  if (!zhfs) {
    dout(-1) << "lzfw_mount() failed"
	     << " path=" << path << " ds=" << root_ds
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

static constexpr uint16_t n_iovs = 16;
thread_local std::pair<iovec[n_iovs], iovec[n_iovs]> tls_iovs;

int ZFStore::read(CollectionHandle ch, ObjectHandle oh,
		  uint64_t offset, size_t len, bufferlist& bl,
		  bool allow_eio)
{
  iovec* iovs1 = tls_iovs.first;
  iovec* iovs2 = tls_iovs.second;
  uint16_t iovcnt;
  int err;

  if (len == 0)
    return 0;

  ZObject* o = static_cast<ZObject*>(oh);

  if (len == CEPH_READ_ENTIRE) {
    struct stat st;
    err = lzfw_stat(zhfs, &cred, o->vno, &st);
    if (!!err)
      return -err;
    len = st.st_size;
  }

  /* read chunkwise */
  int ix;
  ssize_t resid = len;
  iovec* iov1;
  iovec* iov2;

  while (resid) {
    ssize_t bytes_read;
    for (ix = 0; ix < n_iovs; ++ix) {
      iov1 = &iovs1[ix];
      iov2 = &iovs2[ix];
      // XXX get page
      iov1->iov_base = malloc(65536*sizeof(char));
      iov2->iov_base = iov1->iov_base;
      iov1->iov_len = MIN(resid, 65536);
    } /* ix */
    iovcnt = ix+1;
    bytes_read = lzfw_preadv(zhfs, &cred, o->vno, iovs1, iovcnt, offset);
    if (bytes_read <= 0)
      return bytes_read;
    /* transfer filled buffers -- ZFS/Solaris burns down iov->iov_cnt */
    for (ix = 0; ix < iovcnt; ++ix) {
      iov1 = &iovs1[ix];
      iov2 = &iovs2[ix];
      int iov_len = 65536 - iov1->iov_len;
      if (unlikely(iov_len == 0))
	goto out;
      else {
	bufferptr bp =
	  buffer::create_static(iov_len, static_cast<char*>(iov2->iov_base));
	bl.push_back(bp);
	/* short read? */
	if (iov1->iov_len != 0)
	  goto out;
      }
    }
    resid -= bytes_read;
    offset += bytes_read;
  } /* len  */

 out:
  return 0;
} /* read */

int ZFStore::fiemap(CollectionHandle ch, ObjectHandle oh,
		    uint64_t offset, size_t len, bufferlist& bl)
{
  abort();
  return 0;
}

int ZFStore::getattr(CollectionHandle ch, ObjectHandle oh,
		     const char* name, bufferptr& val)
{
  dout(15) << "getattr " << ch->get_cid() << "/" << oh->get_oid()
	   << " '" << name << "'" << dendl;

  ZCollection* c = static_cast<ZCollection*>(ch);
  ZObject* o = static_cast<ZObject*>(oh);
  size_t size;
  int r;

  /* XXX this interface is sub-optimal, needs atomicity */
  r = lzfw_getxattrat(c->zhfs, &cred, o->vno, name, nullptr, &size);
  if (!!r || size == 0)
    goto out;

  val = buffer::create(size);
  r = lzfw_getxattrat(c->zhfs, &cred, o->vno, name, val.c_str(), &size);

 out:
  return -r;
} /* getattr */

/* lzfw_listxattrs2 iterator callback (accumulates) */
static int lsxattr_cb(lzfw_vnode_t *vnode, inogen_t object, creden_t *cred,
		      const char *name, void *arg)
{
  std::list<std::string>* xattr_names =
    static_cast<std::list<std::string>*>(arg);
  xattr_names->push_back(name);
  return 0;
}

int ZFStore::getattrs(CollectionHandle ch, ObjectHandle oh,
		      map<std::string,bufferptr>& aset,
		      bool user_only)
{
  int r;
  ZCollection* c = static_cast<ZCollection*>(ch);
  ZObject* o = static_cast<ZObject*>(oh);

  std::list<std::string> xattr_names;
  r = lzfw_listxattr2(c->zhfs, &cred, o->ino, lsxattr_cb, &xattr_names);

  for (auto& iter : xattr_names) {
    buffer::ptr bp;
    r = getattr(ch, oh, iter.c_str(), bp);
    if (!!r)
      return -r;
    aset.insert(map<std::string,bufferptr>::value_type(iter, bp));
  }
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
  std::string fsid_str;
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
} /* queue_transactions */

ZFStore::~ZFStore()
{
  int err;

  if (zhfs) {
    std::unique_lock<std::mutex> lck(mtx);
    // close root vnode
    err = lzfw_closedir(zhfs, &cred, meta_vno);
    meta_vno = nullptr;
  }

  --n_instances;
  if (n_instances == 0) {
    std::unique_lock<std::mutex> l(mtx);
    if (n_instances == 0) {
      lzfw_exit(zhd);
      zhd = nullptr;
      initialized = false;
    }
  }
} /* ~ZFStore */

/* ZFStore */

int ZFStore::do_transactions(list<Transaction*> &tls, uint64_t op_seq,
			     ZTracer::Trace &trace)
{
  int r = 0;

  trace.event("op_apply_start");
  trace.event("do_transactions");

  int ix = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end(); ++p, ix++) {
    r = do_transaction(**p, op_seq, ix);
    if (r < 0)
      break;
  }

  trace.event("op_apply_finish");

  return r;
} /* do_transactions */

int ZFStore::do_transaction(Transaction& t, uint64_t op_seq,
			    int trans_num)
{
  for (Transaction::op_iterator i = t.begin(); i != t.end(); ++i) {
    int r = 0;

    ZCollection* c = nullptr;
    ZObject* o, *o2;

    switch (i->op) {
    case Transaction::OP_NOP:
      break;

    case Transaction::OP_TOUCH:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o)
	  r = touch(c, o);
      }
      break;

    case Transaction::OP_WRITE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o)
	  r = write(c, o, i->off, i->len, i->data, t.get_replica());
      }
      break;

    case Transaction::OP_ZERO:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o)
	  r = zero(c, o, i->off, i->len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      // deprecated, no-op
      break;

    case Transaction::OP_TRUNCATE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o)
	  r = truncate(c, o, i->off);
      }
      break;

    case Transaction::OP_REMOVE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (o)
	  r = remove(c, o);
      }
      break;

    case Transaction::OP_SETATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o) {
	  bufferlist &bl = i->data;
	  thread_local map<std::string, bufferptr> to_set;
	  to_set.clear();
	  to_set[i->name] = buffer::ptr(bl.c_str(), bl.length());
	  r = setattrs(c, o, to_set);
	}
      }
      break;

    case Transaction::OP_SETATTRS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o)
	  r = setattrs(c, o, i->xattrs);
      }
      break;

    case Transaction::OP_RMATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (o)
	    r = rmattr(c, o, i->name);
      }
      break;

    case Transaction::OP_RMATTRS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* create */);
	if (o)
	    r = rmattrs(c, o);
      }
      break;

    case Transaction::OP_CLONE: // I hate you
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o) {
	  o2 = get_slot_object(t, c, i->o2_ix, true /* create */);
	  if (o2)
	    r = clone(c, o, o2);
	}
      }
      break;

    case Transaction::OP_CLONERANGE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o) {
	  o2 = get_slot_object(t, c, i->o2_ix, true /* create */);
	  if (o2)
	    r = clone_range(c, o, o2, i->off, i->len, i->off);
	}
      }
      break;

    case Transaction::OP_CLONERANGE2:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o) {
	  o2 = get_slot_object(t, c, i->o2_ix, true /* create */);
	  if (o2)
	    r = clone_range(c, o, o2, i->off, i->len, i->off2);
	}
      }
      break;

    case Transaction::OP_MKCOLL:
      r = create_collection(std::get<1>(t.c_slot(i->c1_ix)));
      if (!r) {
	(void) get_slot_collection(t, i->c1_ix);
      }
      break;

    case Transaction::OP_RMCOLL:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c)
	r = destroy_collection(c);
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	bufferlist &bl = i->data;
	r = c->setattr(i->name, buffer::ptr(bl.c_str(), bl.length()));
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c)
	r = c->rmattr(i->name);
      break;

    case Transaction::OP_OMAP_CLEAR:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* !create */);
	if (o)
	  r = omap_clear(c, o);
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* create */);
	if (o)
	  r = omap_setkeys(c, o, i->attrs);
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* !create */);
	if (o)
	  r = omap_rmkeys(c, o, i->keys);
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* !create */);
	if (o)
	  r = omap_rmkeyrange(c, o, i->name, i->name2);
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, true /* !create */);
	if (o)
	  r = omap_setheader(c, o, i->data);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      r = -ENOENT;
      c = get_slot_collection(t, i->c1_ix);
      if (c) {
	o = get_slot_object(t, c, i->o1_ix, false /* !create */);
	if (o)
	  r = set_alloc_hint(c, o, i->value1, i->value2);
      }
      break;

    case Transaction::OP_STARTSYNC:
      // no-op
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
	// -ENOENT is normally okay
	// ...including on a replayed OP_RMCOLL with checkpoint mode
	ok = true;

      if (r == -ENODATA)
	ok = true;

      if (i->op == Transaction::OP_SETALLOCHINT)
	// Either EOPNOTSUPP or EINVAL most probably.  EINVAL in most
	// cases means invalid hint size (e.g. too big, not a multiple
	// of block size, etc) or, at least on xfs, an attempt to set
	// or change it when the file is not empty.  However,
	// OP_SETALLOCHINT is advisory, so ignore all errors.
	ok = true;

      if (!ok) {
	const char* msg = "unexpected error code";

	if (r == -ENOENT && (i->op == Transaction::OP_CLONERANGE ||
			     i->op == Transaction::OP_CLONE ||
			     i->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation "
		<< i->op << dendl;
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

  } /* foreach op */

  return 0;  // FIXME count errors
} /* do_transaction */

int ZFStore::touch(ZCollection* c, ZObject* o)
{
  // no-op on object now instantiated
  dout(15) << "touch " << c->get_cid() << "/" << o->get_oid() << dendl;
  return 0;
} /* touch */

int ZFStore::write(ZCollection* c, ZObject* o, off_t offset, size_t len,
		   const bufferlist& bl, bool replica)
{

  dout(15) << "write " << c->get_cid() << "/" << o->get_oid() << " "
	   << offset << "~" << len << dendl;
  int r = 0;
  int bytes_written = 0;
  iovec *iovs = tls_iovs.first;
  uint16_t iov_ix;

  auto pb = bl.buffers().begin();
  while (pb != bl.buffers().end()) {
    int iov_len = 0;
    for (iov_ix = 0; (iov_ix < n_iovs) && (pb != bl.buffers().end());
	 ++iov_ix, ++pb) {
      iovs[iov_ix].iov_base = const_cast<char*>(pb->c_str());
      iovs[iov_ix].iov_len = pb->length();
      iov_len += iovs[iov_ix].iov_len;
    }
    r = lzfw_pwritev(zhfs, &cred, o->vno, iovs, iov_ix, offset);
    if (r < 0) {
      r = -EIO;
      goto out;
    }
    offset += iov_len; // XXX do short pwritevs happen?
  }

 out:
  return r;
} /* write */

int ZFStore::zero(ZCollection* c, ZObject* o, off_t offset, size_t len)
{
  dout(15) << "zero " << c->get_cid() << "/" << o->get_oid() << " "
	   << offset << "~" << len << dendl;

  /* call the method exposing VOP_SPACE */
  int r = lzfw_zero(zhfs, &cred, o->vno, offset, len);

  return r;
} /* zero */

int ZFStore::truncate(ZCollection* c, ZObject* o, uint64_t size)
{
  dout(15) << "truncate " << c->get_cid() << "/" << o->get_oid()
	   << " size " << size << dendl;

  int r = lzfw_truncate(zhfs, &cred, o->ino, size);

  return r;
} /* truncate */

int ZFStore::remove(ZCollection* c, ZObject* o)
{
  const hoid_t &oid = o->get_oid();
  dout(15) << "remove " << c->get_cid() << "/" << oid << dendl;
  return c->index.unlink(oid);

} /* remove */

int ZFStore::setattr(ZCollection* c, ZObject* o, const std::string& k,
		     const buffer::ptr& v)
{
  dout(15) << "setattr " << c->get_cid() << "/" << o->get_oid()
	   << " " << k << dendl;

  int r = lzfw_setxattrat(zhfs, &cred, o->vno, k.c_str(), v.c_str());
  return -r;
} /* setattr */

int ZFStore::setattrs(ZCollection* c, ZObject* o,
		      const map<std::string,buffer::ptr>& aset)
{
  int r = 0;
  for (auto& aset_iter : aset) {
    r = setattr(c, o, aset_iter.first, aset_iter.second);
    if (!!r)
      goto out;
  }
 out:
  return r;
} /* setattrs */

int ZFStore::rmattr(ZCollection* c, ZObject* o, const std::string& name)
{
  dout(15) << "rmattr " << c->get_cid() << "/" << o->get_oid()
	   << " '" << name << "'" << dendl;

  int r = lzfw_removexattr(zhfs, &cred, o->ino, name.c_str());
  return -r;
} /* rmattr */

/* lzfw_listxattrs2 iterator callback (removes each xattr) */
static int rmxattr_cb(lzfw_vnode_t *vnode, inogen_t object, creden_t *cred,
		      const char *name, void *arg)
{
  return lzfw_removexattr((lzfw_vfs_t *) arg, cred, object, name);
}

int ZFStore::rmattrs(ZCollection* c, ZObject* o)
{
  int r = lzfw_listxattr2(zhfs, &cred, o->ino, rmxattr_cb, zhfs);
  return -r;
} /* rmattrs */

int ZFStore::clone(ZCollection* c, ZObject* o  /* old */, ZObject* o2 /* new */)
{
  dout(15) << "clone " << c->get_cid() << "/" << o->get_oid()
	   << " -> " << c->get_cid() << "/" << o2->get_oid()
	   << dendl;

  return clone_range(c, o, o2, 0, CEPH_READ_ENTIRE, 0);
} /* clone */

int ZFStore::clone_range(ZCollection* c,
			 ZObject* o  /* old */,
			 ZObject* o2 /* new */,
			 off_t srcoff, size_t len, off_t dstoff)
{
  uint16_t iovcnt;
  int r;

  if (len == 0)
    return 0;

  if (len == CEPH_READ_ENTIRE) {
    struct stat st;
    r = lzfw_stat(zhfs, &cred, o->vno, &st);
    if (!!r)
      return -r;
    len = st.st_size;
  }

  /* read chunkwise */
  iovec* iovs1 = tls_iovs.first;
  iovec* iovs2 = tls_iovs.second;

  int ix;
  ssize_t resid = len;

  iovec* iov1;
  iovec* iov2;

  while (resid) {
    ssize_t bytes_read;
    for (ix = 0; ix < n_iovs; ++ix) {
      iov1 = &iovs1[ix];
      iov2 = &iovs2[ix];
      // XXX get page
      iov1->iov_base = malloc(65536*sizeof(char));
      iov2->iov_base = iov1->iov_base;
      iov1->iov_len = MIN(resid, 65536);
    } /* ix */
    iovcnt = ix+1;
    bytes_read = lzfw_preadv(zhfs, &cred, o->vno, iovs1, iovcnt, srcoff);
    if (bytes_read <= 0)
      return bytes_read;
    /* transfer filled buffers -- ZFS/Solaris burns down iov->iov_cnt */
    for (ix = 0; ix < iovcnt; ++ix) {
      iov1 = &iovs1[ix];
      iov2 = &iovs2[ix];
      int iov_len = 65536 - iov1->iov_len;
      if (unlikely(iov_len == 0))
	goto out;
      else {
	/* XXX free iov2->iov_base on exit from this block (TODO: recycle) */
	bufferptr bp =
	  buffer::create_static(iov_len, static_cast<char*>(iov2->iov_base));
	/* write it to o2 at dstoff */
	iovec wr_iov;
	wr_iov.iov_base = iov2->iov_base;
	wr_iov.iov_len = iov_len;
	r = lzfw_pwritev(zhfs, &cred, o2->vno, &wr_iov, 1, dstoff);
	/* short read? */
	if (iov1->iov_len != 0)
	  goto out;
      }
    }
    resid -= bytes_read;
    srcoff += bytes_read;
    dstoff += bytes_read;
  } /* len  */

 out:
  return 0;
} /* clone_range */

int ZFStore::create_collection(const coll_t& cid)
{
  int r;

  dout(15) << "create_collection " << cid << dendl;

  /* CohortFS FileStore initializes a FragTreeIndex on the
   * stack at the given path. */
  ZCollection zc(this, cid, r, true /* create */);

  if (!!r)
    return -r;

  return 0;
} /* create_collection */

int ZFStore::destroy_collection(ZCollection* c)
{
  dout(15) << "destroy_collection " << c->get_cid() << " = " << dendl;
  std::string ds_short_name = std::string("/") + c->get_cid().c_str();
  int r = c->index.destroy(ds_short_name);

  // XXX how do we destroy an attached ZCollection?  Casey?
  return r;
}

/* ZCollection */

ZFStore::ZCollection::ZCollection(ZFStore* zs, const coll_t& cid, int& r,
				  bool create)
  : ceph::os::Collection(zs, cid), cct(zs->cct), path(zs->path),
    index(zs->cct, zs->cct->_conf->fragtreeindex_initial_split),
    ds_name(zs->root_ds + cid.c_str()),
    root_ino{0,0}, meta_ino{0,0}, meta_vno(nullptr)
{
  if (create) {
    /* create a dataset */
    char* ds;
    const char* lzw_err;
    zfsh_adup(ds_name, ds); // spa routines chan change ds
    r = lzfw_dataset_create(zhd, ds, ZFS_TYPE_FILESYSTEM, &lzw_err);
    if (!!r)
      return;
  }

  /* mount our dataset */
  std::string ds_short_name = std::string("/") + cid.c_str();
  zhfs = lzfw_mount(zs->path.c_str(), ds_short_name.c_str(),
		    "" /* mount options */);
  if (!zhfs) {
    derr << "ZFStore::create_collection(" << cid << "): "
	 << "lzfw_mount failed" << dendl;
    r = ENODEV;
    return;
  }

  index.set_zhfs(zhfs);

  /* if we created it, do initial setup */
  if (create) {
    r = index.init(std::string("/frag_tree"));
    r = index.mount(std::string("/frag_tree"));

    /* create collection attributes meta file */
    r = lzfw_getroot(zhfs, &root_ino);
    r = lzfw_create(zhfs, &cred, root_ino, META_FILE, 644, &meta_ino);
    
  } else /* otherwise, mount it */ {
    r = index.mount(std::string("/frag_tree"));
    r = lzfw_open(zhfs, &cred, meta_ino, O_RDWR, &meta_vno);
    assert(r == 0);
  }
} /* ZCollection */

int ZFStore::ZCollection::setattr(const std::string& k, const buffer::ptr& v)
{
  dout(15) << "ZCollection::setattr " << cid << " " << v << dendl;

  assert(meta_vno);

  int r = lzfw_setxattrat(zhfs, &cred, meta_vno, k.c_str(), v.c_str());
  return r;
} /* setattr */

int ZFStore::ZCollection::rmattr(const std::string& k)
{
  dout(15) << "ZCollection::rmattr " << cid << " " << k << dendl;

  int r = lzfw_removexattr(zhfs, &cred, meta_ino, k.c_str());
  return -r;
} /* rmattr */
