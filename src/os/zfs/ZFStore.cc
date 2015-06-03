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
  : ObjectStore(cct, path), root_ds(path + "/osdfs"), zhfs(nullptr),
    meta_ino{0, 0}, meta_vno(nullptr)
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

int ZFStore::read(CollectionHandle ch, ObjectHandle oh,
		  uint64_t off, size_t len, bufferlist& bl,
		  bool allow_eio)
{
  static constexpr uint16_t n_iovs = 16;
  thread_local std::pair<iovec[n_iovs],
			 iovec[n_iovs]> iovs;
  iovec* iovs1 = iovs.first;
  iovec* iovs2 = iovs.second;
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
    bytes_read = lzfw_preadv(zhfs, &cred, o->vno, iovs1, iovcnt, off);
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
}
