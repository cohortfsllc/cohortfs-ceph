// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <cassert>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <errno.h>
#include <dirent.h>

#include <iostream>
#include <map>

#include "include/compat.h"

#include <fstream>
#include <sstream>

#include "KeyValueStore.h"
#include "common/BackTrace.h"
#include "include/types.h"

#include "osd/osd_types.h"
#include "include/color.h"
#include "include/buffer.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/sync_filesystem.h"
#include "LevelDBStore.h"

#include "common/ceph_crypto.h"
using ceph::crypto::SHA1;


#include "common/config.h"

#define dout_subsys ceph_subsys_keyvaluestore

/* Factory method */
ObjectStore* KVStore_factory(CephContext* cct,
			     const std::string& data,
			     const std::string& journal)
{
  return new KeyValueStore(cct, data);
}

/* DLL machinery */
extern "C" {
  void* objectstore_dllinit()
  {
    return reinterpret_cast<void*>(KVStore_factory);
  }
} /* extern "C" */

const string KeyValueStore::OBJECT_STRIP_PREFIX = "_STRIP_";
const string KeyValueStore::OBJECT_XATTR = "__OBJATTR__";
const string KeyValueStore::OBJECT_OMAP = "__OBJOMAP__";
const string KeyValueStore::OBJECT_OMAP_HEADER = "__OBJOMAP_HEADER__";
const string KeyValueStore::OBJECT_OMAP_HEADER_KEY = "__OBJOMAP_HEADER__KEY_";
const string KeyValueStore::COLLECTION = "__COLLECTION__";
const string KeyValueStore::COLLECTION_ATTR = "__COLL_ATTR__";

// ============== StripObjectMap Implementation =================

void StripObjectMap::sync_wrap(StripObjectHeader& strip_header,
			       KeyValueDB::Transaction t,
			       const SequencerPosition& spos)
{
  dout(10) << __func__ << " cid: " << strip_header.cid << "oid: "
	   << strip_header.oid << " setting spos to " << strip_header.spos
	   << dendl;
  strip_header.spos = spos;
  strip_header.header->data.clear();
  ::encode(strip_header, strip_header.header->data);

  sync(strip_header.header, t);
}

bool StripObjectMap::check_spos(const StripObjectHeader& header,
				const SequencerPosition& spos)
{
  if (spos > header.spos) {
    stringstream out;
    dout(10) << "cid: " << "oid: " << header.oid
	     << " not skipping op, *spos " << spos << dendl;
    dout(10) << " > header.spos " << header.spos << dendl;
    return false;
  } else {
    dout(10) << "cid: " << "oid: " << header.oid << " skipping op, spos "
	     << spos << " <= header.spos " << header.spos << dendl;
    return true;
  }
}

int StripObjectMap::save_strip_header(StripObjectHeader& strip_header,
				      const SequencerPosition& spos,
				      KeyValueDB::Transaction t)
{
  strip_header.spos = spos;
  strip_header.header->data.clear();
  ::encode(strip_header, strip_header.header->data);

  set_header(strip_header.cid, strip_header.oid, *(strip_header.header), t);
  return 0;
}

int StripObjectMap::create_strip_header(const coll_t& cid,
					const hoid_t& oid,
					StripObjectHeader& strip_header,
					KeyValueDB::Transaction t)
{
  Header header = lookup_create_header(cid, oid, t);
  if (!header)
    return -EINVAL;

  strip_header.oid = oid;
  strip_header.cid = cid;
  strip_header.header = header;

  return 0;
}

int StripObjectMap::lookup_strip_header(const coll_t& cid,
					const hoid_t& oid,
					StripObjectHeader& strip_header)
{
  Header header = lookup_header(cid, oid);

  if (!header) {
    dout(20) << "lookup_strip_header failed to get strip_header "
	     << " cid " << cid <<" oid " << oid << dendl;
    return -ENOENT;
  }

  if (header->data.length()) {
    bufferlist::iterator bliter = header->data.begin();
    ::decode(strip_header, bliter);
  }

  if (strip_header.strip_size == 0)
    strip_header.strip_size = default_strip_size;

  strip_header.oid = oid;
  strip_header.cid = cid;
  strip_header.header = header;

  dout(10) << "lookup_strip_header done " << " cid " << cid << " oid "
	   << oid << dendl;
  return 0;
}

int StripObjectMap::file_to_extents(uint64_t offset, size_t len,
				    uint64_t strip_size,
				    vector<StripExtent>& extents)
{
  if (len == 0)
    return 0;

  uint64_t start, end, strip_offset, extent_offset, extent_len;
  start = offset / strip_size;
  end = (offset + len) / strip_size;
  strip_offset = start * strip_size;

  // "offset" may in the middle of first strip object
  if (offset > strip_offset) {
    extent_offset = offset - strip_offset;
    if (extent_offset + len <= strip_size)
      extent_len = len;
    else
      extent_len = strip_size - extent_offset;
    extents.push_back(StripExtent(start, extent_offset, extent_len));
    start++;
    strip_offset += strip_size;
  }

  for (; start < end; ++start) {
    extents.push_back(StripExtent(start, 0, strip_size));
    strip_offset += strip_size;
  }

  // The end of strip object may be partial
  if (offset + len > strip_offset)
    extents.push_back(StripExtent(start, 0, offset+len-strip_offset));

  assert(extents.size());
  return 0;
}

void StripObjectMap::clone_wrap(StripObjectHeader& old_header,
				const coll_t& cid, const hoid_t& oid,
				KeyValueDB::Transaction t,
				StripObjectHeader *origin_header,
				StripObjectHeader *target_header)
{
  Header new_origin_header;

  if (target_header)
    *target_header = old_header;
  if (origin_header)
    *origin_header = old_header;

  clone(old_header.header, cid, oid, t, &new_origin_header,
	&target_header->header);

  if(origin_header)
    origin_header->header = new_origin_header;

  if (target_header) {
    target_header->oid = oid;
    target_header->cid = cid;
  }
}

void StripObjectMap::rename_wrap(const coll_t& cid, const hoid_t& oid,
				 KeyValueDB::Transaction t,
				 StripObjectHeader *header)
{
  assert(header);
  rename(header->header, cid, oid, t);

  if (header) {
    header->oid = oid;
    header->cid = cid;
  }
}

int StripObjectMap::get_values_with_header(const StripObjectHeader& header,
					   const string& prefix,
					   const set<string>& keys,
					   map<string, bufferlist> *out)
{
  return scan(header.header, prefix, keys, 0, out);
}

int StripObjectMap::get_keys_with_header(const StripObjectHeader& header,
					 const string& prefix,
					 set<string> *keys)
{
  ObjectMap::ObjectMapIterator iter = _get_iterator(header.header, prefix);
  for (; iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    keys->insert(iter->key());
  }
  return 0;
}

int StripObjectMap::get_with_header(const StripObjectHeader& header,
			const string& prefix, map<string, bufferlist> *out)
{
  ObjectMap::ObjectMapIterator iter = _get_iterator(header.header, prefix);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    out->insert(make_pair(iter->key(), iter->value()));
  }

  return 0;
}


// ========= KeyValueStore::BufferTransaction Implementation ============

int KeyValueStore::BufferTransaction::lookup_cached_header(
    const coll_t& cid, const hoid_t& oid,
    StripObjectMap::StripObjectHeader **strip_header,
    bool create_if_missing)
{
  StripObjectMap::StripObjectHeader header;
  int r = 0;

  StripHeaderMap::iterator it = strip_headers.find(make_pair(cid, oid));
  if (it != strip_headers.end()) {
    if (it->second.deleted)
      return -ENOENT;

    if (strip_header)
      *strip_header = &it->second;
    return 0;
  }

  r = store->backend->lookup_strip_header(cid, oid, header);
  if (r < 0 && create_if_missing) {
    r = store->backend->create_strip_header(cid, oid, header, t);
  }

  if (r < 0) {
    ldout(store->cct, 10) << __func__  << " " << cid << "/" << oid << " "
			  << " r = " << r << dendl;
    return r;
  }

  strip_headers[make_pair(cid, oid)] = header;
  if (strip_header)
    *strip_header = &strip_headers[make_pair(cid, oid)];
  return r;
}

int KeyValueStore::BufferTransaction::get_buffer_keys(
    StripObjectMap::StripObjectHeader& strip_header, const string& prefix,
    const set<string>& keys, map<string, bufferlist> *out)
{
  set<string> need_lookup;

  for (set<string>::iterator it = keys.begin(); it != keys.end(); ++it) {
    map<pair<string, string>, bufferlist>::iterator i =
	strip_header.buffers.find(make_pair(prefix, *it));

    if (i != strip_header.buffers.end()) {
      (*out)[*it].swap(i->second);
    } else {
      need_lookup.insert(*it);
    }
  }

  if (!need_lookup.empty()) {
    int r = store->backend->get_values_with_header(strip_header, prefix,
						   need_lookup, out);
    if (r < 0) {
      ldout(store->cct, 10) << __func__  << " " << strip_header.cid << "/"
			    << strip_header.oid << " " << " r = " << r
			    << dendl;
      return r;
    }
  }

  return 0;
}

void KeyValueStore::BufferTransaction::set_buffer_keys(
     StripObjectMap::StripObjectHeader& strip_header,
     const string& prefix, map<string, bufferlist>& values)
{
  store->backend->set_keys(strip_header.header, prefix, values, t);

  for (map<string, bufferlist>::iterator iter = values.begin();
       iter != values.end(); ++iter) {
    strip_header.buffers[make_pair(prefix, iter->first)].swap(iter->second);
  }
}

int KeyValueStore::BufferTransaction::remove_buffer_keys(
     StripObjectMap::StripObjectHeader& strip_header, const string& prefix,
     const set<string>& keys)
{
  for (set<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
    strip_header.buffers[make_pair(prefix, *iter)] = bufferlist();
  }

  return store->backend->rm_keys(strip_header.header, prefix, keys, t);
}

void KeyValueStore::BufferTransaction::clear_buffer_keys(
     StripObjectMap::StripObjectHeader& strip_header, const string& prefix)
{
  for (map<pair<string, string>, bufferlist>::iterator iter = strip_header.buffers.begin();
       iter != strip_header.buffers.end(); ++iter) {
    if (iter->first.first == prefix)
      iter->second = bufferlist();
  }
}

int KeyValueStore::BufferTransaction::clear_buffer(
     StripObjectMap::StripObjectHeader& strip_header)
{
  strip_header.deleted = true;

  return store->backend->clear(strip_header.header, t);
}

void KeyValueStore::BufferTransaction::clone_buffer(
    StripObjectMap::StripObjectHeader& old_header,
    const coll_t& cid, const hoid_t& oid)
{
  // Remove target ahead to avoid dead lock
  strip_headers.erase(make_pair(cid, oid));

  StripObjectMap::StripObjectHeader new_origin_header, new_target_header;

  store->backend->clone_wrap(old_header, cid, oid, t,
			     &new_origin_header, &new_target_header);

  // FIXME: Lacking of lock for origin header(now become parent), it will
  // cause other operation can get the origin header while submitting
  // transactions
  strip_headers[make_pair(cid, old_header.oid)] = new_origin_header;
  strip_headers[make_pair(cid, oid)] = new_target_header;
}

void KeyValueStore::BufferTransaction::rename_buffer(
    StripObjectMap::StripObjectHeader& old_header,
    const coll_t& cid, const hoid_t& oid)
{
  if (store->backend->check_spos(old_header, spos))
    return ;

  // FIXME: Lacking of lock for origin header, it will cause other operation
  // can get the origin header while submitting transactions
  store->backend->rename_wrap(cid, oid, t, &old_header);

  strip_headers.erase(make_pair(old_header.cid, old_header.oid));
  strip_headers[make_pair(cid, oid)] = old_header;
}

int KeyValueStore::BufferTransaction::submit_transaction()
{
  int r = 0;

  for (StripHeaderMap::iterator header_iter = strip_headers.begin();
       header_iter != strip_headers.end(); ++header_iter) {
    StripObjectMap::StripObjectHeader header = header_iter->second;

    if (store->backend->check_spos(header, spos))
      continue;

    if (header.deleted)
      continue;

    r = store->backend->save_strip_header(header, spos, t);
    if (r < 0) {
      ldout(store->cct, 10) << __func__ << " save strip header failed " << dendl;
      goto out;
    }
  }

out:

  ldout(store->cct, 5) << __func__ << " r = " << r << dendl;
  return store->backend->submit_transaction(t);
}

// =========== KeyValueStore Intern Helper Implementation ==============

int KeyValueStore::_create_current()
{
  struct stat st;
  int ret = ::stat(current_fn.c_str(), &st);
  if (ret == 0) {
    // current/ exists
    if (!S_ISDIR(st.st_mode)) {
      dout(0) << "_create_current: current/ exists but is not a directory" << dendl;
      ret = -EINVAL;
    }
  } else {
    ret = ::mkdir(current_fn.c_str(), 0755);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "_create_current: mkdir " << current_fn << " failed: "<< cpp_strerror(ret) << dendl;
    }
  }

  return ret;
}



// =========== KeyValueStore API Implementation ==============

KeyValueStore::KeyValueStore(CephContext *cct, const std::string& base,
			     const char *name, bool do_update) :
  ObjectStore(cct, base),
  basedir(base),
  fsid_fd(-1), op_fd(-1), current_fd(-1),
  kv_type(KV_TYPE_NONE),
  backend(NULL),
  ondisk_finisher(cct),
  op_finisher(cct),
  m_keyvaluestore_queue_max_ops(cct->_conf->keyvaluestore_queue_max_ops),
  m_keyvaluestore_queue_max_bytes(cct->_conf->keyvaluestore_queue_max_bytes),
  do_update(do_update),
  submit_op_seq(0)
{
  ostringstream oss;
  oss << basedir << "/current";
  current_fn = oss.str();

  ostringstream sss;
  sss << basedir << "/current/commit_op_seq";
  current_op_seq_fn = sss.str();

  cct->_conf->add_observer(this);
}

KeyValueStore::~KeyValueStore()
{
  cct->_conf->remove_observer(this);
}

int KeyValueStore::statfs(struct statfs *buf)
{
  if (::statfs(basedir.c_str(), buf) < 0) {
    int r = -errno;
    return r;
  }
  return 0;
}

int KeyValueStore::mkfs()
{
  int ret = 0;
  char fsid_fn[PATH_MAX];
  boost::uuids::uuid old_fsid;

  dout(1) << "mkfs in " << basedir << dendl;

  // open+lock fsid
  snprintf(fsid_fn, sizeof(fsid_fn), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fsid_fn, O_RDWR|O_CREAT, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "mkfs: failed to open " << fsid_fn << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  if (lock_fsid() < 0) {
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  if (read_fsid(fsid_fd, &old_fsid) < 0 || old_fsid.is_nil()) {
    if (fsid.is_nil()) {
      fsid = boost::uuids::random_generator()();
      dout(1) << "mkfs generated fsid " << fsid << dendl;
    } else {
      dout(1) << "mkfs using provided fsid " << fsid << dendl;
    }

    char fsid_str[40];
    strcpy(fsid_str, to_string(fsid).c_str());
    strcat(fsid_str, "\n");
    ret = ::ftruncate(fsid_fd, 0);
    if (ret < 0) {
      ret = -errno;
      derr << "mkfs: failed to truncate fsid: " << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    ret = safe_write(fsid_fd, fsid_str, strlen(fsid_str));
    if (ret < 0) {
      derr << "mkfs: failed to write fsid: " << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    if (::fsync(fsid_fd) < 0) {
      ret = errno;
      derr << "mkfs: close failed: can't write fsid: "
	   << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    dout(10) << "mkfs fsid is " << fsid << dendl;
  } else {
    if (!fsid.is_nil() && fsid != old_fsid) {
      derr << "mkfs on-disk fsid " << old_fsid << " != provided " << fsid << dendl;
      ret = -EINVAL;
      goto close_fsid_fd;
    }
    fsid = old_fsid;
    dout(1) << "mkfs fsid is already set to " << fsid << dendl;
  }

  // version stamp
  ret = write_version_stamp();
  if (ret < 0) {
    derr << "mkfs: write_version_stamp() failed: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  ret = _create_current();
  if (ret < 0) {
    derr << "mkfs: failed to create current/ " << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  if (_detect_backend()) {
    derr << "KeyValueStore::mkfs error in _detect_backend" << dendl;
    ret = -1;
    goto close_fsid_fd;
  }

  {
    KeyValueDB *store;
    if (kv_type == KV_TYPE_LEVELDB) {
      store = new LevelDBStore(cct, current_fn);
    } else {
      derr << "KeyValueStore::mkfs error: unknown backend type" << kv_type << dendl;
      ret = -1;
      goto close_fsid_fd;
    }

    store->init();
    stringstream err;
    if (store->create_and_open(err)) {
      derr << "KeyValueStore::mkfs failed to create keyvaluestore backend: "
	   << err.str() << dendl;
      ret = -1;
      delete store;
      goto close_fsid_fd;
    } else {
      delete store;
      dout(1) << "keyvaluestore backend exists/created" << dendl;
    }
  }

  dout(1) << "mkfs done in " << basedir << dendl;
  ret = 0;

 close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
  return ret;
}

int KeyValueStore::read_fsid(int fd, boost::uuids::uuid *id)
{
  boost::uuids::string_generator parse;
  char fsid_str[40];
  int ret = safe_read(fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;

  if (ret > 36)
    fsid_str[36] = 0;

  try {
    *id = parse(fsid_str);
  } catch (std::runtime_error& e) {
    return -EINVAL;
  }
  
  return 0;
}

int KeyValueStore::lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    int err = errno;
    dout(0) << "lock_fsid failed to lock " << basedir
	    << "/fsid, is another ceph-osd still running? "
	    << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool KeyValueStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << dendl;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // verify fs isn't in use

  fsid_fd = ::open(fn, O_RDWR, 0644);
  if (fsid_fd < 0)
    return 0;	// no fsid, ok.
  bool inuse = lock_fsid() < 0;
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
  return inuse;
}

int KeyValueStore::update_version_stamp()
{
  return write_version_stamp();
}

int KeyValueStore::version_stamp_is_valid(uint32_t *version)
{
  bufferptr bp(PATH_MAX);
  int ret = safe_read_file(basedir.c_str(), "store_version",
      bp.c_str(), bp.length());
  if (ret < 0) {
    if (ret == -ENOENT)
      return 0;
    return ret;
  }
  bufferlist bl;
  bl.push_back(bp);
  bufferlist::iterator i = bl.begin();
  ::decode(*version, i);
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int KeyValueStore::write_version_stamp()
{
  bufferlist bl;
  ::encode(target_version, bl);

  return safe_write_file(basedir.c_str(), "store_version",
      bl.c_str(), bl.length());
}

int KeyValueStore::mount()
{
  int ret;
  char buf[PATH_MAX];

  dout(5) << "basedir " << basedir << dendl;

  // make sure global base dir exists
  if (::access(basedir.c_str(), R_OK | W_OK)) {
    ret = -errno;
    derr << "KeyValueStore::mount: unable to access basedir '" << basedir
	 << "': " << cpp_strerror(ret) << dendl;
    goto done;
  }

  // get fsid
  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_RDWR, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: error opening '" << buf << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  ret = read_fsid(fsid_fd, &fsid);
  if (ret < 0) {
    derr << "KeyValueStore::mount: error reading fsid_fd: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  if (lock_fsid() < 0) {
    derr << "KeyValueStore::mount: lock_fsid failed" << dendl;
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  dout(10) << "mount fsid is " << fsid << dendl;

  uint32_t version_stamp;
  ret = version_stamp_is_valid(&version_stamp);
  if (ret < 0) {
    derr << "KeyValueStore::mount : error in version_stamp_is_valid: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  } else if (ret == 0) {
    if (do_update) {
      derr << "KeyValueStore::mount : stale version stamp detected: "
	   << version_stamp << ". Proceeding, do_update "
	   << "is set, performing disk format upgrade." << dendl;
    } else {
      ret = -EINVAL;
      derr << "KeyValueStore::mount : stale version stamp " << version_stamp
	   << ". Please run the KeyValueStore update script before starting "
	   << "the OSD, or set keyvaluestore_update_to to " << target_version
	   << dendl;
      goto close_fsid_fd;
    }
  }

  current_fd = ::open(current_fn.c_str(), O_RDONLY);
  if (current_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: error opening: " << current_fn << ": "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  assert(current_fd >= 0);

  if (_detect_backend()) {
    derr << "KeyValueStore::mount error in _detect_backend" << dendl;
    ret = -1;
    goto close_current_fd;
  }

  {
    KeyValueDB *store;
    if (kv_type == KV_TYPE_LEVELDB) {
      store = new LevelDBStore(cct, current_fn);
    } else {
      derr << "KeyValueStore::mount error: unknown backend type" << kv_type
	   << dendl;
      ret = -1;
      goto close_current_fd;
    }

    store->init();
    stringstream err;
    if (store->open(err)) {
      derr << "KeyValueStore::mount Error initializing keyvaluestore backend: "
	   << err.str() << dendl;
      ret = -1;
      delete store;
      goto close_current_fd;
    }

    StripObjectMap *dbomap = new StripObjectMap(cct, store);
    ret = dbomap->init(do_update);
    if (ret < 0) {
      delete dbomap;
      derr << "Error initializing StripObjectMap: " << ret << dendl;
      goto close_current_fd;
    }
    stringstream err2;

    if (cct->_conf->keyvaluestore_debug_check_backend && !dbomap->check(err2)) {
      derr << err2.str() << dendl;;
      delete dbomap;
      ret = -EINVAL;
      goto close_current_fd;
    }

    backend.reset(dbomap);
  }

  op_finisher.start();
  ondisk_finisher.start();

  // all okay.
  return 0;

close_current_fd:
  VOID_TEMP_FAILURE_RETRY(::close(current_fd));
  current_fd = -1;
close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
done:
  return ret;
}

int KeyValueStore::umount()
{
  dout(5) << "umount " << basedir << dendl;

  op_finisher.stop();
  ondisk_finisher.stop();

  if (fsid_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
  }
  if (op_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(op_fd));
    op_fd = -1;
  }
  if (current_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(current_fd));
    current_fd = -1;
  }

  backend.reset();

  // nothing
  return 0;
}

int KeyValueStore::get_max_object_name_length()
{
  unique_lock l(lock);
  int ret = pathconf(basedir.c_str(), _PC_NAME_MAX);
  if (ret < 0) {
    int err = errno;
    l.unlock();
    if (err == 0)
      return -EDOM;
    return -err;
  }
  l.unlock();
  return ret;
}

int KeyValueStore::queue_transactions(list<Transaction*> &tls,
				      OpRequestRef osd_op)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  Transaction::collect_contexts(tls, &onreadable, &ondisk, &onreadable_sync);

  const uint64_t op = ++submit_op_seq;

  dout(5) << "queue_transactions seq " << op << dendl;
  int r = do_transactions(tls, op);
  if (r < 0)
    delete ondisk;
  else if (ondisk)
    ondisk_finisher.queue(ondisk, r);

  if (onreadable_sync)
    onreadable_sync->complete(0);
  op_finisher.queue(onreadable);

  return 0;
}

// Combine all the ops in the same transaction using "BufferTransaction" and
// cache the middle results in order to make visible to the following ops.
//
// Lock: KeyValueStore use "in_use" in GenericObjectMap to avoid concurrent
// operation on the same object. Not sure ReadWrite lock should be applied to
// improve concurrent performance. In the future, I'd like to remove apply_lock
// on "osr" and introduce PG RWLock.
int KeyValueStore::do_transactions(list<Transaction*> &tls, uint64_t op_seq)
{
  int r = 0;

  int trans_num = 0;
  SequencerPosition spos(op_seq, trans_num, 0);
  BufferTransaction bt(this, spos);

  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p, trans_num++) {
    r = do_transaction(**p, bt, spos);
    if (r < 0)
      break;
  }

  r = bt.submit_transaction();
  if (r < 0) {
    assert(0 == "unexpected error");  // FIXME
  }

  return r;
}

unsigned KeyValueStore::do_transaction(Transaction& tr,
                                       BufferTransaction &t,
                                       SequencerPosition& spos)
{
  dout(10) << "do_transaction on " << &tr << dendl;

  typedef Transaction::op_iterator op_iterator;
  for (op_iterator i = tr.begin(); i != tr.end(); ++i) {

    int r = 0;

    CollectionHandle ch = nullptr;
    ObjectHandle oh, oh2;

    switch (i->op) {
  case Transaction::OP_NOP:
    break;

    case Transaction::OP_TOUCH:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  r = _touch(ch->get_cid(), oh->get_oid(), t);
	}
      }
      break;

    case Transaction::OP_WRITE:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  r = _write(ch->get_cid(), oh->get_oid(), i->off, i->len, i->data,
		     t, tr.get_replica());
	}
      }
      break;

    case Transaction::OP_ZERO:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  r = _zero(ch->get_cid(), oh->get_oid(), i->off, i->len, t);
	}
      }
      break;

    case Transaction::OP_TRIMCACHE:
      // deprecated, no-op
      break;

    case Transaction::OP_TRUNCATE:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  r = _truncate(ch->get_cid(), oh->get_oid(), i->off, t);
	}
      }
      break;

    case Transaction::OP_REMOVE:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, false /* create */);
	if (oh) {
	  r = _remove(ch->get_cid(), oh->get_oid(), t);
	}
      }
      break;

    case Transaction::OP_SETATTR:
      {
	r = -ENOENT;
	ch = get_slot_collection(tr, i->c1_ix);
	if (ch) {
	  oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	  if (oh) {
	    map<string, bufferptr> to_set;
	    to_set[i->name] = bufferptr(i->data.c_str(), i->data.length());
	    r = _setattrs(ch->get_cid(), oh->get_oid(), to_set, t);
	    if (r == -ENOSPC)
	      dout(0) << " ENOSPC on setxattr on " << ch << "/" << oh->get_oid()
		      << " name " << i->name << " size " << i->data.length()
		      << dendl;
	  }
	}
      }
      break;

    case Transaction::OP_SETATTRS:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  r = _setattrs(ch->get_cid(), oh->get_oid(), i->xattrs, t);
	  if (r == -ENOSPC)
	    dout(0) << " ENOSPC on setxattrs on " << ch << "/" << oh->get_oid()
		    << dendl;
	}
      }
      break;

    case Transaction::OP_RMATTR:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, false /* create */);
	if (oh) {
	  r = _rmattr(ch->get_cid(), oh->get_oid(), i->name.c_str(), t);
	}
      }
      break;

    case Transaction::OP_RMATTRS:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, false /* create */);
	if (oh) {
	  r = _rmattrs(ch->get_cid(), oh->get_oid(), t);
	}
      }
      break;

    case Transaction::OP_CLONE:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  oh2 = get_slot_object(tr, ch, i->o2_ix, true /* create */);
	  if (oh2) {
	    r = _clone(ch->get_cid(), oh->get_oid(), oh2->get_oid(), t);
	  }
	}
      }
      break;

    case Transaction::OP_CLONERANGE:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  oh2 = get_slot_object(tr, ch, i->o2_ix, true /* create */);
	  if (oh2) {
	    r = _clone_range(ch->get_cid(), oh->get_oid(), oh2->get_oid(),
			     i->off, i->len, i->off, t);
	  }
	}
      }    
      break;

    case Transaction::OP_CLONERANGE2:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  oh2 = get_slot_object(tr, ch, i->o2_ix, true /* create */);
	  if (oh2) {
	    r = _clone_range(ch->get_cid(), oh->get_oid(), oh2->get_oid(),
			     i->off, i->len, i->off2, t);
	  }
	}
      }
      break;

    case Transaction::OP_MKCOLL:
      r = _create_collection(ch->get_cid(), t);
      if (!r) {
	(void) get_slot_collection(tr, i->c1_ix);
      }
      break;

    case Transaction::OP_RMCOLL:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	r = _destroy_collection(ch->get_cid(), t);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	r = _collection_setattr(ch->get_cid(), i->name.c_str(),
				i->data.c_str(), i->data.length(), t);
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	r = _collection_rmattr(ch->get_cid(), i->name.c_str(), t);
      }
      break;

    case Transaction::OP_STARTSYNC:
      {
	start_sync();
	break;
      }

    case Transaction::OP_OMAP_CLEAR:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, false /* !create */);
	if (oh) {
	  r = _omap_clear(ch->get_cid(), oh->get_oid(), t);
	}
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* create */);
	if (oh) {
	  r = _omap_setkeys(ch->get_cid(), oh->get_oid(), i->attrs, t);
	}
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, false /* !create */);
	if (oh) {
	  r = _omap_rmkeys(ch->get_cid(), oh->get_oid(), i->keys, t);
	}
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, false /* !create */);
	if (oh) {
	  r = _omap_rmkeyrange(ch->get_cid(), oh->get_oid(), i->name,
			       i->name2, t);
	}
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      r = -ENOENT;
      ch = get_slot_collection(tr, i->c1_ix);
      if (ch) {
	oh = get_slot_object(tr, ch, i->o1_ix, true /* !create */);
	if (oh) {
	  r = _omap_setheader(ch->get_cid(), oh->get_oid(), i->data, t);
	}
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      // TODO: can kvstore make use of the hint?
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
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation "
		<< i->op << " (" << spos << ", or op " << spos.op
		<< ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	tr.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");

	if (r == -EMFILE) {
	  dump_open_fds(cct);
	}
      }
    }

    spos.op++;
  }

  return 0;  // FIXME count errors
}


// =========== KeyValueStore Op Implementation ==============
// objects

bool KeyValueStore::exists(CollectionHandle ch, const hoid_t& oid)
{
  const coll_t& cid = ch->get_cid();
  
  dout(10) << __func__ << "collection: " << cid << " object: " << oid
	   << dendl;
  int r;
  StripObjectMap::StripObjectHeader header;

  r = backend->lookup_strip_header(cid, oid, header);
  if (r < 0) {
    return false;
  }

  return true;
}

/* XXXX definitely incorrect */

ObjectHandle KeyValueStore::get_object(
  CollectionHandle ch, const hoid_t& oid) {
  return new KVObject(ch, oid);
}

ObjectHandle KeyValueStore::get_object(
  CollectionHandle ch, const hoid_t& oid, bool create) {
  return new KVObject(ch, oid);
}

void put_object(ObjectHandle oh) {
  delete oh;
}

int KeyValueStore::stat(CollectionHandle ch, ObjectHandle oh,
			struct stat *st, bool allow_eio)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& oid = oh->get_oid();

  dout(10) << "stat " << cid << "/" << oid << dendl;

  StripObjectMap::StripObjectHeader header;

  int r = backend->lookup_strip_header(cid, oid, header);
  if (r < 0) {
    dout(10) << "stat " << cid << "/" << oid << "=" << r << dendl;
    return -ENOENT;
  }

  st->st_blocks = header.max_size / header.strip_size;
  if (header.max_size % header.strip_size)
    st->st_blocks++;
  st->st_nlink = 1;
  st->st_size = header.max_size;
  st->st_blksize = header.strip_size;

  return r;
}

int KeyValueStore::_generic_read(StripObjectMap::StripObjectHeader& header,
				 uint64_t offset, size_t len, bufferlist& bl,
				 bool allow_eio, BufferTransaction *bt)
{
  if (header.max_size < offset) {
    dout(10) << __func__ << " " << header.cid << "/" << header.oid << ")"
	     << " offset exceed the length of bl"<< dendl;
    return 0;
  }

  if (len == 0)
    len = header.max_size - offset;

  if (offset + len > header.max_size)
    len = header.max_size - offset;

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header.strip_size,
				  extents);
  map<string, bufferlist> out;
  set<string> keys;

  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    bufferlist old;
    string key = strip_object_key(iter->no);

    if (bt && header.buffers.count(make_pair(OBJECT_STRIP_PREFIX, key))) {
      // use strip_header buffer
      assert(header.bits[iter->no]);
      out[key] = header.buffers[make_pair(OBJECT_STRIP_PREFIX, key)];
    } else if (header.bits[iter->no]) {
      keys.insert(key);
    }
  }

  int r = backend->get_values_with_header(header, OBJECT_STRIP_PREFIX, keys, &out);
  if (r < 0) {
    dout(10) << __func__ << " " << header.cid << "/" << header.oid << " "
	     << offset << "~" << len << " = " << r << dendl;
    return r;
  } else if (out.size() != keys.size()) {
    dout(0) << __func__ << " broken header or missing data in backend "
	    << header.cid << "/" << header.oid << " " << offset << "~"
	    << len << " = " << r << dendl;
    return -EBADF;
  }

  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    string key = strip_object_key(iter->no);

    if (header.bits[iter->no]) {
      if (iter->len == header.strip_size) {
	bl.claim_append(out[key]);
      } else {
	out[key].copy(iter->offset, iter->len, bl);
      }
    } else {
      bl.append_zero(iter->len);
    }
  }

  dout(10) << __func__ << " " << header.cid << "/" << header.oid << " "
	   << offset << "~" << bl.length() << "/" << len << " r = " << r
	   << dendl;

  return bl.length();
}


int KeyValueStore::read(CollectionHandle ch, ObjectHandle oh,
			uint64_t offset, size_t len, bufferlist& bl,
			bool allow_eio)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& oid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
	   << len << dendl;

  StripObjectMap::StripObjectHeader header;

  int r = backend->lookup_strip_header(cid, oid, header);

  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
	      << len << " header isn't exist: r = " << r << dendl;
    return r;
  }

  return _generic_read(header, offset, len, bl, allow_eio);
}

int KeyValueStore::fiemap(CollectionHandle ch, ObjectHandle oh,
			  uint64_t offset, size_t len, bufferlist& bl)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& oid = oh->get_oid();

  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  int r;
  StripObjectMap::StripObjectHeader header;

  r = backend->lookup_strip_header(cid, oid, header);
  if (r < 0) {
    dout(10) << "fiemap " << cid << "/" << oid << " " << offset << "~" << len
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header.strip_size,
				  extents);

  map<uint64_t, uint64_t> m;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    m[iter->offset] = iter->len;
  }
  ::encode(m, bl);
  return 0;
}

int KeyValueStore::_remove(const coll_t& cid, const hoid_t& oid,
			   BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " "
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  r = t.clear_buffer(*header);

  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_truncate(const coll_t& cid, const hoid_t& oid,
			     uint64_t size, BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " size " << size
	   << dendl;

  int r;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << size
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  if (header->max_size == size)
    return 0;

  if (header->max_size > size) {
    vector<StripObjectMap::StripExtent> extents;
    StripObjectMap::file_to_extents(size, header->max_size-size,
				    header->strip_size, extents);
    assert(extents.size());

    vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
    if (header->bits[iter->no] && iter->offset != 0) {
      bufferlist value;
      map<string, bufferlist> values;
      set<string> lookup_keys;
      string key = strip_object_key(iter->no);

      lookup_keys.insert(key);
      r = t.get_buffer_keys(*header, OBJECT_STRIP_PREFIX,
			    lookup_keys, &values);
      if (r < 0) {
	dout(10) << __func__ << " " << cid << "/" << oid << " "
		 << size << " = " << r << dendl;
	return r;
      } else if (values.size() != lookup_keys.size()) {
	dout(0) << __func__ << " broken header or missing data in backend "
		<< header->cid << "/" << header->oid << " size " << size
		<<  " r = " << r << dendl;
	return -EBADF;
      }

      values[key].copy(0, iter->offset, value);
      value.append_zero(header->strip_size-iter->offset);
      assert(value.length() == header->strip_size);
      value.swap(values[key]);

      t.set_buffer_keys(*header, OBJECT_STRIP_PREFIX, values);
      ++iter;
    }

    set<string> keys;
    for (; iter != extents.end(); ++iter) {
      if (header->bits[iter->no]) {
	keys.insert(strip_object_key(iter->no));
	header->bits[iter->no] = 0;
      }
    }
    r = t.remove_buffer_keys(*header, OBJECT_STRIP_PREFIX, keys);
    if (r < 0) {
      dout(10) << __func__ << " " << cid << "/" << oid << " "
	       << size << " = " << r << dendl;
      return r;
    }
  }

  header->bits.resize(size/header->strip_size+1);
  header->max_size = size;

  dout(10) << __func__ << " " << cid << "/" << oid << " size " << size << " = "
	   << r << dendl;
  return r;
}

int KeyValueStore::_touch(const coll_t& cid, const hoid_t& oid,
			  BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(cid, oid, &header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " "
	     << " failed to get header: r = " << r << dendl;
    r = -EINVAL;
    return r;
  }

  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_generic_write(StripObjectMap::StripObjectHeader& header,
				  uint64_t offset, size_t len,
				  const bufferlist& bl,
				  BufferTransaction& t,
				  bool replica)
{
  if (len > bl.length())
    len = bl.length();

  if (len + offset > header.max_size) {
    header.max_size = len + offset;
    header.bits.resize(header.max_size/header.strip_size+1);
  }

  vector<StripObjectMap::StripExtent> extents;
  StripObjectMap::file_to_extents(offset, len, header.strip_size,
				  extents);

  map<string, bufferlist> out;
  set<string> keys;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    if (header.bits[iter->no] && !(iter->offset == 0 &&
				   iter->len == header.strip_size))
      keys.insert(strip_object_key(iter->no));
  }

  int r = t.get_buffer_keys(header, OBJECT_STRIP_PREFIX, keys, &out);
  if (r < 0) {
    dout(10) << __func__ << " failed to get value " << header.cid << "/"
	      << header.oid << " " << offset << "~" << len << " = " << r
	      << dendl;
    return r;
  } else if (keys.size() != out.size()) {
    // Error on header.bits or the corresponding key/value pair is missing
    dout(0) << __func__ << " broken header or missing data in backend "
	    << header.cid << "/" << header.oid << " " << offset << "~"
	    << len << " = " << r << dendl;
    return -EBADF;
  }

  uint64_t bl_offset = 0;
  map<string, bufferlist> values;
  for (vector<StripObjectMap::StripExtent>::iterator iter = extents.begin();
       iter != extents.end(); ++iter) {
    bufferlist value;
    string key = strip_object_key(iter->no);
    if (header.bits[iter->no]) {
      if (iter->offset == 0 && iter->len == header.strip_size) {
	bl.copy(bl_offset, iter->len, value);
	bl_offset += iter->len;
      } else {
	assert(out[key].length() == header.strip_size);

	out[key].copy(0, iter->offset, value);
	bl.copy(bl_offset, iter->len, value);
	bl_offset += iter->len;

	if (value.length() != header.strip_size)
	  out[key].copy(value.length(), header.strip_size-value.length(),
			value);
      }
    } else {
      if (iter->offset)
	value.append_zero(iter->offset);
      bl.copy(bl_offset, iter->len, value);
      bl_offset += iter->len;

      if (value.length() < header.strip_size)
	value.append_zero(header.strip_size-value.length());

      header.bits[iter->no] = 1;
    }
    assert(value.length() == header.strip_size);
    values[key].swap(value);
  }
  assert(bl_offset == len);

  t.set_buffer_keys(header, OBJECT_STRIP_PREFIX, values);
  dout(10) << __func__ << " " << header.cid << "/" << header.oid << " "
	   << offset << "~" << len << " = " << r << dendl;

  return r;
}

int KeyValueStore::_write(const coll_t& cid, const hoid_t& oid,
			  uint64_t offset, size_t len, const bufferlist& bl,
			  BufferTransaction& t, bool replica)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
	   << len << dendl;

  int r;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(cid, oid, &header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oid << " " << offset
	     << "~" << len << " failed to get header: r = " << r << dendl;
    return r;
  }

  return _generic_write(*header, offset, len, bl, t, replica);
}

int KeyValueStore::_zero(const coll_t& cid, const hoid_t& oid,
			 uint64_t offset, size_t len, BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " " << offset << "~" << len << dendl;

  bufferptr bp(len);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  int r = _write(cid, oid, offset, len, bl, t);

  dout(10) << __func__ << " " << cid << "/" << oid << " " << offset << "~"
	   << len << " = " << r << dendl;
  return r;
}

int KeyValueStore::_clone(const coll_t& cid, const hoid_t& oldoid,
			  const hoid_t& newoid, BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	   << newoid << dendl;

  if (oldoid == newoid)
    return 0;

  int r;
  StripObjectMap::StripObjectHeader *old_header;

  r = t.lookup_cached_header(cid, oldoid, &old_header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	     << newoid << " = " << r << dendl;
    return r;
  }

  t.clone_buffer(*old_header, cid, newoid);

  dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	   << newoid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_clone_range(const coll_t& cid, const hoid_t& oldoid,
				const hoid_t& newoid, uint64_t srcoff,
				uint64_t len, uint64_t dstoff,
				BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	   << newoid << " " << srcoff << "~" << len << " to " << dstoff
	   << dendl;

  int r;
  bufferlist bl;

  StripObjectMap::StripObjectHeader *old_header, *new_header;

  r = t.lookup_cached_header(cid, oldoid, &old_header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	   << newoid << " " << srcoff << "~" << len << " to " << dstoff
	   << " header isn't exist: r = " << r << dendl;
    return r;
  }

  r = t.lookup_cached_header(cid, newoid, &new_header, true);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	   << newoid << " " << srcoff << "~" << len << " to " << dstoff
	   << " can't create header: r = " << r << dendl;
    return r;
  }

  r = _generic_read(*old_header, srcoff, len, bl, false, &t);
  if (r < 0)
    goto out;

  r = _generic_write(*new_header, dstoff, len, bl, t);

 out:
  dout(10) << __func__ << " " << cid << "/" << oldoid << " -> " << cid << "/"
	   << newoid << " " << srcoff << "~" << len << " to " << dstoff
	   << " = " << r << dendl;
  return r;
}

// attrs

int KeyValueStore::getattr(CollectionHandle ch, ObjectHandle oh,
			   const char *name, bufferptr& bp)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& oid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << oid << " '" << name << "'"
	   << dendl;

  int r;
  map<string, bufferlist> got;
  set<string> to_get;

  to_get.insert(string(name));
  r = backend->get_values(cid, oid, OBJECT_XATTR, to_get, &got);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " get_xattrs err r =" << r << dendl;
    goto out;
  }
  if (got.empty()) {
    dout(10) << __func__ << " got.size() is 0" << dendl;
    return -ENODATA;
  }
  bp = bufferptr(got.begin()->second.c_str(),
		 got.begin()->second.length());
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << "/" << oid << " '" << name << "' = "
	   << r << dendl;
  return r;
}

int KeyValueStore::getattrs(CollectionHandle ch, ObjectHandle oh,
			    map<string,bufferptr>& aset, bool user_only)
{
  int r;
  map<string, bufferlist> attr_aset;

  const coll_t& cid = ch->get_cid();
  const hoid_t& oid = oh->get_oid();

  r = backend->get(cid, oid, OBJECT_XATTR, &attr_aset);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get attrs r = " << r << dendl;
    goto out;
  }

  if (r == -ENOENT)
    r = 0;

  for (map<string, bufferlist>::iterator i = attr_aset.begin();
       i != attr_aset.end(); ++i) {
    string key;
    if (user_only) {
      if (i->first[0] != '_')
	continue;
      if (i->first == "_")
	continue;
      key = i->first.substr(1, i->first.size());
    } else {
      key = i->first;
    }
    aset.insert(make_pair(key,
		bufferptr(i->second.c_str(), i->second.length())));
  }

 out:
  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;

  return r;
}

int KeyValueStore::_setattrs(const coll_t& cid, const hoid_t& oid,
			     map<string, bufferptr>& aset,
			     BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;

  StripObjectMap::StripObjectHeader *header;
  map<string, bufferlist> attrs;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0)
    goto out;

  for (map<string, bufferptr>::iterator it = aset.begin();
       it != aset.end(); ++it) {
    attrs[it->first].push_back(it->second);
  }

  t.set_buffer_keys(*header, OBJECT_XATTR, attrs);

out:
  dout(10) << __func__ << " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}


int KeyValueStore::_rmattr(const coll_t& cid, const hoid_t& oid,
			   const char *name, BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << " '" << name << "'"
	   << dendl;

  int r;
  set<string> to_remove;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r
	     << dendl;
    return r;
  }

  to_remove.insert(string(name));
  r = t.remove_buffer_keys(*header, OBJECT_XATTR, to_remove);

  dout(10) << __func__ << " " << cid << "/" << oid << " '" << name << "' = "
	   << r << dendl;
  return r;
}

int KeyValueStore::_rmattrs(const coll_t& cid, const hoid_t& oid,
			    BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << oid << dendl;

  int r;
  set<string> attrs;

  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(cid, oid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r
	     << dendl;
    return r;
  }

  r = backend->get_keys_with_header(*header, OBJECT_XATTR, &attrs);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get attrs r = " << r << dendl;
    return r;
  }

  r = t.remove_buffer_keys(*header, OBJECT_XATTR, attrs);
  t.clear_buffer_keys(*header, OBJECT_XATTR);

  dout(10) << __func__ <<  " " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

// collection attrs

int KeyValueStore::collection_getattr(CollectionHandle ch,
				      const char *name, void *value,
				      size_t size)
{
  const coll_t& c = ch->get_cid();

  dout(15) << __func__ << " " << c.to_str() << " '" << name << "' len "
	   << size << dendl;

  bufferlist bl;
  int r;

  r = collection_getattr(ch, name, bl);
  if (r < 0)
      goto out;

  if (bl.length() < size) {
    r = bl.length();
    bl.copy(0, bl.length(), static_cast<char*>(value));
  } else {
    r = size;
    bl.copy(0, size, static_cast<char*>(value));
  }

out:
  dout(10) << __func__ << " " << c.to_str() << " '" << name << "' len "
	   << size << " = " << r << dendl;
  return r;
}

int KeyValueStore::collection_getattr(CollectionHandle ch,
				      const char *name, bufferlist& bl)
{
  const coll_t& c = ch->get_cid();

  dout(15) << __func__ << " " << c.to_str() << " '" << name
	   << "'" << dendl;

  set<string> keys;
  map<string, bufferlist> out;
  keys.insert(string(name));

  int r = backend->get_values(get_coll_for_coll(), make_ghobject_for_coll(c),
			      COLLECTION_ATTR, keys, &out);
  if (r < 0) {
    dout(10) << __func__ << " could not get key" << string(name) << dendl;
    r = -EINVAL;
  }

  assert(out.size());
  bl.swap(out.begin()->second);

  dout(10) << __func__ << " " << c.to_str() << " '" << name << "' len "
	   << bl.length() << " = " << r << dendl;
  return bl.length();
}

int KeyValueStore::collection_getattrs(CollectionHandle ch,
				       map<string, bufferptr>& aset)
{
  const coll_t& cid = ch->get_cid();

  dout(10) << __func__ << " " << cid.to_str() << dendl;

  map<string, bufferlist> out;
  set<string> keys;

  for (map<string, bufferptr>::iterator it = aset.begin();
       it != aset.end(); ++it) {
      keys.insert(it->first);
  }

  int r = backend->get_values(get_coll_for_coll(), make_ghobject_for_coll(cid),
			      COLLECTION_ATTR, keys, &out);
  if (r < 0) {
    dout(10) << __func__ << " could not get keys" << dendl;
    r = -EINVAL;
    goto out;
  }

  for (map<string, bufferlist>::iterator it = out.begin(); it != out.end();
       ++it) {
    bufferptr ptr(it->second.c_str(), it->second.length());
    aset.insert(make_pair(it->first, ptr));
  }

 out:
  dout(10) << __func__ << " " << cid.to_str() << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_setattr(const coll_t& c, const char *name,
				       const void *value, size_t size,
				       BufferTransaction& t)
{
  dout(10) << __func__ << " " << c << " '" << name << "' len "
	   << size << dendl;

  int r;
  bufferlist bl;
  map<string, bufferlist> out;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(get_coll_for_coll(),
			     make_ghobject_for_coll(c),
			     &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r << dendl;
    return r;
  }

  bl.append(reinterpret_cast<const char*>(value), size);
  out.insert(make_pair(string(name), bl));

  t.set_buffer_keys(*header, COLLECTION_ATTR, out);

  dout(10) << __func__ << " " << c << " '"
	   << name << "' len " << size << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_rmattr(const coll_t& c,
				      const char *name,
				      BufferTransaction& t)
{
  dout(15) << __func__ << " " << c << dendl;

  bufferlist bl;
  set<string> out;
  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(get_coll_for_coll(),
				 make_ghobject_for_coll(c), &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r << dendl;
    return r;
  }

  out.insert(string(name));
  r = t.remove_buffer_keys(*header, COLLECTION_ATTR, out);

  dout(10) << __func__ << " " << c << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_setattrs(const coll_t& cid,
					map<string,bufferptr>& aset,
					BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << dendl;

  map<string, bufferlist> attrs;
  StripObjectMap::StripObjectHeader *header;
  int r = t.lookup_cached_header(get_coll_for_coll(),
				 make_ghobject_for_coll(cid),
				 &header, false);
  if (r < 0) {
    dout(10) << __func__ << " could not find header r = " << r << dendl;
    return r;
  }

  for (map<string, bufferptr>::iterator it = aset.begin(); it != aset.end();
       ++it) {
    attrs[it->first].push_back(it->second);
  }

  t.set_buffer_keys(*header, COLLECTION_ATTR, attrs);

  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}


// collections

int KeyValueStore::_create_collection(const coll_t& c,
				      BufferTransaction& t)
{
  dout(15) << __func__ << " " << c << dendl;

  int r;
  StripObjectMap::StripObjectHeader *header;
  bufferlist bl;

  r = t.lookup_cached_header(get_coll_for_coll(),
			     make_ghobject_for_coll(c), &header,
			     false);
  if (r == 0) {
    r = -EEXIST;
    return r;
  }

  r = t.lookup_cached_header(get_coll_for_coll(),
			     make_ghobject_for_coll(c), &header,
			     true);

  dout(10) << __func__ << " cid " << c << " r = " << r << dendl;
  return r;
}

int KeyValueStore::_destroy_collection(const coll_t& c,
				       BufferTransaction& t)
{
  dout(15) << __func__ << " " << c << dendl;

  int r;
  uint64_t modified_object = 0;
  StripObjectMap::StripObjectHeader *header;
  vector<hoid_t> objs;

  r = t.lookup_cached_header(get_coll_for_coll(), make_ghobject_for_coll(c),
			     &header, false);
  if (r < 0) {
    goto out;
  }

  // All modified objects are marked deleted
  for (BufferTransaction::StripHeaderMap::iterator iter = t.strip_headers.begin();
       iter != t.strip_headers.end(); ++iter) {
    // sum the total modified object in this PG
    if (iter->first.first != c)
      continue;

    modified_object++;
    if (!iter->second.deleted) {
      r = -ENOTEMPTY;
      goto out;
    }
  }

  r = backend->list_objects(c, hoid_t(), modified_object+1, &objs,
			    0);
  // No other object
  if (objs.size() != modified_object && objs.size() != 0) {
    r = -ENOTEMPTY;
    goto out;
  }

  for(vector<hoid_t>::iterator iter = objs.begin();
      iter != objs.end(); ++iter) {
    if (!t.strip_headers.count(make_pair(c, *iter))) {
      r = -ENOTEMPTY;
      goto out;
    }
  }

  r = t.clear_buffer(*header);

out:
  dout(10) << __func__ << " " << c << " = " << r << dendl;
  return r;
}


int KeyValueStore::_collection_add(const coll_t& c,
				   const coll_t& oldcid,
				   const hoid_t& o,
				   BufferTransaction& t)
{
  dout(15) << __func__ <<  " " << c << "/" << o << " from " << oldcid << "/"
	   << o << dendl;

  bufferlist bl;
  StripObjectMap::StripObjectHeader *header, *old_header;

  int r = t.lookup_cached_header(oldcid, o, &old_header, false);
  if (r < 0) {
    goto out;
  }

  r = t.lookup_cached_header(c, o, &header, false);
  if (r == 0) {
    r = -EEXIST;
    dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
	     << o << " already exist " << dendl;
    goto out;
  }

  r = _generic_read(*old_header, 0, old_header->max_size, bl, false, &t);
  if (r < 0) {
    r = -EINVAL;
    goto out;
  }

  r = _generic_write(*header, 0, bl.length(), bl, t);
  if (r < 0) {
    r = -EINVAL;
  }

out:
  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
	   << o << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_move_rename(const coll_t& oldcid,
					   const hoid_t& oldoid,
					   const coll_t& c,
					   const hoid_t& o,
					   BufferTransaction& t)
{
  dout(15) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
	   << oldoid << dendl;
  int r;
  StripObjectMap::StripObjectHeader *header;

  r = t.lookup_cached_header(c, o, &header, false);
  if (r == 0) {
    dout(10) << __func__ << " " << oldcid << "/" << oldoid << " -> " << c
	     << "/" << o << " = " << r << dendl;
    return -EEXIST;
  }

  r = t.lookup_cached_header(oldcid, oldoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << oldcid << "/" << oldoid << " -> " << c
	     << "/" << o << " = " << r << dendl;
    return r;
  }

  t.rename_buffer(*header, c, o);

  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
	   << oldoid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_remove_recursive(const coll_t& cid,
						BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << dendl;

  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(get_coll_for_coll(),
				 make_ghobject_for_coll(cid),
				 &header, false);
  if (r < 0) {
    return 0;
  }

  vector<hoid_t> objects;
  hoid_t place;
  do {
    r = backend->list_objects(cid, place, 300, &objects, &place);
    if (r < 0)
      return r;

    if (objects.empty())
      break;

    for (vector<hoid_t>::iterator i = objects.begin();
	 i != objects.end(); ++i) {
      r = _remove(cid, *i, t);

      if (r < 0)
	return r;
    }
  } while (!objects.empty());

  r = t.clear_buffer(*header);

  dout(10) << __func__ << " " << cid  << " r = " << r << dendl;
  return 0;
}

int KeyValueStore::_collection_rename(const coll_t& cid,
				      const coll_t& ncid,
				      BufferTransaction& t)
{
  dout(10) << __func__ << " origin cid " << cid << " new cid " << ncid
	   << dendl;

  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(get_coll_for_coll(),
				 make_ghobject_for_coll(ncid),
				 &header, false);
  if (r == 0) {
    dout(2) << __func__ << ": " << ncid << " DNE" << dendl;
    return -EEXIST;
  }

  r = t.lookup_cached_header(get_coll_for_coll(), make_ghobject_for_coll(cid),
			     &header, false);
  if (r < 0) {
    dout(2) << __func__ << ": " << cid << " DNE" << dendl;
    return 0;
  }

  vector<hoid_t> objects;
  hoid_t next, current;
  int move_size = 0;
  while (1) {
    r = backend->list_objects(cid, current, get_ideal_list_max(),
			      &objects, &next);

    dout(20) << __func__ << cid << "objects size: " << objects.size()
	     << dendl;

    if (objects.empty())
      break;

    for (vector<hoid_t>::iterator i = objects.begin();
	i != objects.end(); ++i) {
      if (_collection_move_rename(cid, *i, ncid, *i, t) < 0) {
	return -1;
      }
      move_size++;
    }

    objects.clear();
    current = next;
  }

  t.rename_buffer(*header, get_coll_for_coll(), make_ghobject_for_coll(ncid));

  dout(10) << __func__ << " origin cid " << cid << " new cid " << ncid
	   << dendl;
  return 0;
}

int KeyValueStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << " " << dendl;

  vector<hoid_t> objs;
  hoid_t next;
  backend->list_objects(get_coll_for_coll(), hoid_t(), 0, &objs,
			&next);
  for (vector<hoid_t>::const_iterator iter = objs.begin();
       iter != objs.end(); ++iter) {
    ls.push_back(coll_t(iter->oid.name));
  }

  return 0;
}

CollectionHandle KeyValueStore::open_collection(const coll_t& c)
{
  return new ceph::os::Collection(this, c);
}

int KeyValueStore::close_collection(CollectionHandle ch)
{
  delete ch;
  return 0;
}

bool KeyValueStore::collection_exists(const coll_t& c)
{
  dout(10) << __func__ << " " << dendl;

  StripObjectMap::StripObjectHeader header;
  int r = backend->lookup_strip_header(get_coll_for_coll(),
				       make_ghobject_for_coll(c), header);
  if (r < 0) {
    return false;
  }
  return true;
}

bool KeyValueStore::collection_empty(CollectionHandle ch)
{
  const coll_t& cid = ch->get_cid();

  dout(10) << __func__ << " " << dendl;

  vector<hoid_t> oids;
  backend->list_objects(cid, hoid_t(), 1, &oids, 0);
  return oids.empty();
}

int KeyValueStore::collection_list_range(CollectionHandle ch,
					 hoid_t start, hoid_t end,
					 vector<hoid_t>* ls)
{
  bool done = false;
  hoid_t next = start;
  const coll_t& cid = ch->get_cid();

  while (!done) {
    vector<hoid_t> next_objects;
    int r = backend->list_objects(cid, next, get_ideal_list_max(),
				  &next_objects, &next);
    if (r < 0)
      return r;

    ls->insert(ls->end(), next_objects.begin(), next_objects.end());

    // special case for empty collection
    if (ls->empty()) {
      break;
    }

    while (!ls->empty() && ls->back() >= end) {
      ls->pop_back();
      done = true;
    }

    if (next >= end) {
      done = true;
    }
  }

  return 0;
}

int KeyValueStore::collection_list_partial(CollectionHandle ch,
					   hoid_t start,
					   int min, int max,
					   vector<hoid_t> *ls,
					   hoid_t *next)
{
  const coll_t& cid = ch->get_cid();

  dout(10) << __func__ << " " << cid << " start:" << start << dendl;

  if (min < 0 || max < 0)
      return -EINVAL;

  return backend->list_objects(cid, start, max, ls, next);
}

int KeyValueStore::collection_list_partial2(CollectionHandle ch,
					    int min, int max,
					    vector<hoid_t>* vs,
					    CLPCursor& cursor)
{
  /* TODO: implement */
  return 0;
}

int KeyValueStore::collection_list(CollectionHandle ch,
				   vector<hoid_t>& ls)
{
  return collection_list_partial(ch, hoid_t(), 0, 0, &ls, 0);
}

int KeyValueStore::collection_version_current(CollectionHandle ch,
					      uint32_t *version)
{
  *version = COLLECTION_VERSION;
  if (*version == target_version)
    return 1;
  else
    return 0;
}

// omap

int KeyValueStore::omap_get(CollectionHandle ch, ObjectHandle oh,
			    bufferlist *bl, map<string, bufferlist> *out)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& hoid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeader header;

  int r = backend->lookup_strip_header(cid, hoid, header);
  if (r < 0) {
    dout(10) << __func__ << " lookup_strip_header failed: r =" << r << dendl;
    return r;
  }

  r = backend->get_with_header(header, OBJECT_OMAP, out);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " err r =" << r << dendl;
    return r;
  }

  set<string> keys;
  map<string, bufferlist> got;

  keys.insert(OBJECT_OMAP_HEADER_KEY);
  r = backend->get_values_with_header(header, OBJECT_OMAP_HEADER, keys, &got);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " err r =" << r << dendl;
    return r;
  }

  if (!got.empty()) {
    assert(got.size() == 1);
    bl->swap(got.begin()->second);
  }

  return 0;
}

int KeyValueStore::omap_get_header(CollectionHandle ch,
				   ObjectHandle oh,
				   bufferlist *bl, bool allow_eio)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& hoid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  set<string> keys;
  map<string, bufferlist> got;

  keys.insert(OBJECT_OMAP_HEADER_KEY);
  int r = backend->get_values(cid, hoid, OBJECT_OMAP_HEADER, keys, &got);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " err r =" << r << dendl;
    return r;
  }

  if (!got.empty()) {
    assert(got.size() == 1);
    bl->swap(got.begin()->second);
  }

  return 0;
}

int KeyValueStore::omap_get_keys(CollectionHandle ch,
				 ObjectHandle oh, set<string> *keys)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& hoid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  int r = backend->get_keys(cid, hoid, OBJECT_OMAP, keys);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

int KeyValueStore::omap_get_values(CollectionHandle ch,
				   ObjectHandle oh,
				   const set<string>& keys,
				   map<string, bufferlist> *out)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& hoid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  int r = backend->get_values(cid, hoid, OBJECT_OMAP, keys, out);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

int KeyValueStore::omap_check_keys(CollectionHandle ch,
				   ObjectHandle oh,
				   const set<string>& keys,
				   set<string> *out)
{
  const coll_t& cid = ch->get_cid();
  const hoid_t& hoid = oh->get_oid();

  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  int r = backend->check_keys(cid, hoid, OBJECT_OMAP, keys, out);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

ObjectMap::ObjectMapIterator KeyValueStore::get_omap_iterator(
  CollectionHandle ch, ObjectHandle oh)
{
  dout(15) << __func__ << " " << ch->get_cid() << "/" <<
    oh->get_oid() << dendl;
  return backend->get_iterator(ch->get_cid(), oh->get_oid(),
			       OBJECT_OMAP);
}

int KeyValueStore::_omap_clear(const coll_t& cid, const hoid_t& hoid,
			       BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  set<string> keys;
  r = backend->get_keys_with_header(*header, OBJECT_OMAP, &keys);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get omap_keys r = " << r << dendl;
    return r;
  }

  r = t.remove_buffer_keys(*header, OBJECT_OMAP, keys);
  if (r < 0) {
    dout(10) << __func__ << " could not remove keys r = " << r << dendl;
    return r;
  }

  keys.clear();
  keys.insert(OBJECT_OMAP_HEADER_KEY);
  r = t.remove_buffer_keys(*header, OBJECT_OMAP_HEADER, keys);
  if (r < 0) {
    dout(10) << __func__ << " could not remove keys r = " << r << dendl;
    return r;
  }

  t.clear_buffer_keys(*header, OBJECT_OMAP_HEADER);

  dout(10) << __func__ << " " << cid << "/" << hoid << " r = " << r << dendl;
  return 0;
}

int KeyValueStore::_omap_setkeys(const coll_t& cid, const hoid_t& hoid,
				 map<string, bufferlist>& aset,
				 BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  t.set_buffer_keys(*header, OBJECT_OMAP, aset);

  return 0;
}

int KeyValueStore::_omap_rmkeys(const coll_t& cid, const hoid_t& hoid,
				const set<string>& keys,
				BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  r = t.remove_buffer_keys(*header, OBJECT_OMAP, keys);

  dout(10) << __func__ << " " << cid << "/" << hoid << " r = " << r << dendl;
  return r;
}

int KeyValueStore::_omap_rmkeyrange(const coll_t& cid,
				    const hoid_t& hoid,
				    const string& first,
				    const string& last,
				    BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << " [" << first << ","
	   << last << "]" << dendl;

  set<string> keys;
  {
    ObjectMap::ObjectMapIterator iter =
      backend->get_iterator(cid, hoid, OBJECT_OMAP);
    if (!iter)
      return -ENOENT;

    for (iter->lower_bound(first); iter->valid() && iter->key() < last;
	 iter->next()) {
      keys.insert(iter->key());
    }
  }
  return _omap_rmkeys(cid, hoid, keys, t);
}

int KeyValueStore::_omap_setheader(const coll_t& cid,
				   const hoid_t& hoid,
				   const bufferlist& bl,
				   BufferTransaction& t)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  map<string, bufferlist> sets;
  StripObjectMap::StripObjectHeader *header;

  int r = t.lookup_cached_header(cid, hoid, &header, false);
  if (r < 0) {
    dout(10) << __func__ << " " << cid << "/" << hoid << " "
	     << " failed to get header: r = " << r << dendl;
    return r;
  }

  sets[OBJECT_OMAP_HEADER_KEY] = bl;
  t.set_buffer_keys(*header, OBJECT_OMAP_HEADER, sets);
  return 0;
}

const char** KeyValueStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "keyvaluestore_queue_max_ops",
    "keyvaluestore_queue_max_bytes",
    NULL
  };
  return KEYS;
}

void KeyValueStore::handle_conf_change(const struct md_config_t *conf,
				       const std::set <std::string>& changed)
{
  if (changed.count("keyvaluestore_queue_max_ops") ||
      changed.count("keyvaluestore_queue_max_bytes")) {
    m_keyvaluestore_queue_max_ops = conf->keyvaluestore_queue_max_ops;
    m_keyvaluestore_queue_max_bytes = conf->keyvaluestore_queue_max_bytes;
  }
}

void KeyValueStore::dump_transactions(list<Transaction*>& ls,
				      uint64_t seq)
{
}
