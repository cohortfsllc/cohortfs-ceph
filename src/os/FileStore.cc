// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <cassert>
#include <unistd.h>
#include <stdlib.h>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <errno.h>
#include <dirent.h>
#include <sys/ioctl.h>

#if defined(__linux__)
#include <linux/fs.h>
#endif

#include <iostream>
#include <map>

#include "include/compat.h"
#include "include/linux_fiemap.h"

#include "common/xattr.h"
#include "chain_xattr.h"

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN


#include <fstream>
#include <sstream>

#include "FileStore.h"
#include "Factory.h"
#include "GenericFileStoreBackend.h"
#include "BtrfsFileStoreBackend.h"
#include "XfsFileStoreBackend.h"
#include "ZFSFileStoreBackend.h"
#include "common/BackTrace.h"
#include "include/types.h"
#include "FileJournal.h"

#include "osd/osd_types.h"
#include "include/color.h"
#include "include/buffer.h"

#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/sync_filesystem.h"
#include "common/fd.h"
#include "DBObjectMap.h"
#include "LevelDBStore.h"

#include "common/ceph_crypto.h"
using ceph::crypto::SHA1;

#include "common/config.h"

const uint32_t FileStore::target_version = 3;

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "filestore(" << basedir << ") "

/* Factory method */
ObjectStore* FileStore_factory(CephContext* cct,
			       const std::string& data,
			       const std::string& journal)
{
  return new FileStore(cct, data, journal);
}

/* DLL machinery */
extern "C" {
  void* objectstore_dllinit()
  {
    return reinterpret_cast<void*>(FileStore_factory);
  }
} /* extern "C" */

#define COMMIT_SNAP_ITEM "snap_%lld"
#define CLUSTER_SNAP_ITEM "clustersnap_%s"

#define REPLAY_GUARD_XATTR "user.cephos.seq"
#define GLOBAL_REPLAY_GUARD_XATTR "user.cephos.gseq"

// XATTR_SPILL_OUT_NAME as a xattr is used to maintain that indicates whether
// xattrs spill over into DBObjectMap, if XATTR_SPILL_OUT_NAME exists in file
// xattrs and the value is "no", it indicates no xattrs in DBObjectMap
#define XATTR_SPILL_OUT_NAME "user.cephos.spill_out"
#define XATTR_NO_SPILL_OUT "0"
#define XATTR_SPILL_OUT "1"

//Initial features in new superblock.
static CompatSet get_fs_initial_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

//Features are added here that this FileStore supports.
static CompatSet get_fs_supported_compat_set() {
  CompatSet compat =  get_fs_initial_compat_set();
  return compat;
}

int FileStore::peek_journal_fsid(boost::uuids::uuid* fsid)
{
  // make sure we don't try to use aio or direct_io (and get annoying
  // error messages from failing to do so); performance implications
  // should be irrelevant for this use
  FileJournal j(cct, this, *fsid, 0, 0, journalpath.c_str(), false,
		false);
  return j.peek_fsid(*fsid);
}

int FileStore::get_cdir(const coll_t& cid, char* s, int len)
{
  const string& cid_str(cid.to_str());
  return snprintf(s, len, "%s/current/%s", basedir.c_str(), cid_str.c_str());
}

int FileStore::lfn_unlink(FSCollection* fc, FSObject* o,
			  const SequencerPosition& spos,
			  bool force_clear_omap)
{

  int r = 0;
  const hoid_t &oid = o->get_oid();
  {
    struct stat st;
    r = fc->index.stat(oid, &st);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }

    if (st.st_nlink == 1)
      force_clear_omap = true;

    if (force_clear_omap) {
      dout(20) << __func__ << ": clearing omap on " << oid
	       << " in cid " << fc->get_cid() << dendl;
      r = object_map->clear(oid, &spos);
      if (r < 0 && r != -ENOENT) {
	assert(!m_filestore_fail_eio || r != -EIO);
	return r;
      }
      if (cct->_conf->filestore_debug_inject_read_err) {
	debug_obj_on_delete(oid);
      }
      flusher.clear_object(o); // should be only non-cache ref
    } else {
      /* Ensure that replay of this op doesn't result in the object_map
       * going away.
       */
      if (!backend->can_checkpoint())
	object_map->sync(&oid, &spos);
    }
  }
  return fc->index.unlink(oid);
}

FileStore::FileStore(CephContext* cct, const std::string& base,
                     const std::string& jdev, const char* name,
                     bool do_update) :
  JournalingObjectStore(cct, base),
  basedir(base), journalpath(jdev),
  blk_size(0),
  fsid_fd(-1), op_fd(-1),
  basedir_fd(-1), current_fd(-1),
  generic_backend(NULL), backend(NULL),
  ondisk_finisher(cct),
  force_sync(false), sync_epoch(0),
  stop(false), sync_thread(this),
  trace_endpoint("0.0.0.0", 0, NULL),
  flusher(this),
  op_finisher(cct),
  m_filestore_commit_timeout(cct->_conf->filestore_commit_timeout),
  m_filestore_journal_parallel(cct->_conf->filestore_journal_parallel),
  m_filestore_journal_trailing(cct->_conf->filestore_journal_trailing),
  m_filestore_journal_writeahead(cct->_conf->filestore_journal_writeahead),
  m_filestore_fiemap_threshold(cct->_conf->filestore_fiemap_threshold),
  m_filestore_max_sync_interval(
    cct->_conf->filestore_max_sync_interval),
  m_filestore_min_sync_interval(
    cct->_conf->filestore_min_sync_interval),
  m_filestore_fail_eio(cct->_conf->filestore_fail_eio),
  m_filestore_replica_fadvise(cct->_conf->filestore_replica_fadvise),
  do_update(do_update),
  m_journal_dio(cct->_conf->journal_dio),
  m_journal_aio(cct->_conf->journal_aio),
  m_journal_force_aio(cct->_conf->journal_force_aio),
  m_filestore_queue_max_ops(cct->_conf->filestore_queue_max_ops),
  m_filestore_queue_max_bytes(cct->_conf->filestore_queue_max_bytes),
  m_filestore_queue_committing_max_ops(cct->_conf->filestore_queue_committing_max_ops),
  m_filestore_queue_committing_max_bytes(cct->_conf->filestore_queue_committing_max_bytes),
  m_filestore_do_dump(false),
  m_filestore_dump_fmt(true),
  m_filestore_sloppy_crc(cct->_conf->filestore_sloppy_crc),
  m_filestore_sloppy_crc_block_size(cct->_conf->filestore_sloppy_crc_block_size),
  m_filestore_max_alloc_hint_size(cct->_conf->filestore_max_alloc_hint_size),
  m_fs_type(fs_types::FS_TYPE_NONE),
  m_filestore_max_inline_xattr_size(0),
  m_filestore_max_inline_xattrs(0)
{
  trace_endpoint.copy_name("FileStore (" + basedir + ")");

  m_filestore_kill_at = cct->_conf->filestore_kill_at;

  ostringstream oss;
  oss << basedir << "/current";
  current_fn = oss.str();

  ostringstream sss;
  sss << basedir << "/current/commit_op_seq";
  current_op_seq_fn = sss.str();

  ostringstream omss;
  omss << basedir << "/current/omap";
  omap_dir = omss.str();

  cct->_conf->add_observer(this);

  generic_backend = new GenericFileStoreBackend(cct, this);
  backend = generic_backend;

  superblock.compat_features = get_fs_initial_compat_set();
}

FileStore::~FileStore()
{
  cct->_conf->remove_observer(this);

  delete generic_backend;
}

static void get_attrname(const char* name, char* buf, int len)
{
  snprintf(buf, len, "user.ceph.%s", name);
}

bool parse_attrname(char** name)
{
  if (strncmp(*name, "user.ceph.", 10) == 0) {
    *name += 10;
    return true;
  }
  return false;
}

int FileStore::statfs(struct statfs* buf)
{
  if (::statfs(basedir.c_str(), buf) < 0) {
    int r = -errno;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int FileStore::open_journal()
{
  if (journalpath.length()) {
    dout(10) << "open_journal at " << journalpath << dendl;
    journal = new FileJournal(cct, this, fsid, &finisher, &sync_cond,
			      journalpath.c_str(), m_journal_dio,
			      m_journal_aio, m_journal_force_aio);
  }
  return 0;
}

int FileStore::dump_journal(ostream& out)
{
  int r;

  if (!journalpath.length())
    return -EINVAL;

  FileJournal* journal = new FileJournal(cct, this, fsid, &finisher,
					 &sync_cond,
					 journalpath.c_str(),
					 m_journal_dio);
  r = journal->dump(out);
  delete journal;
  return r;
}

int FileStore::mkfs()
{
  int ret = 0;
  char fsid_fn[PATH_MAX];
  boost::uuids::uuid old_fsid;

  dout(1) << "mkfs in " << basedir << dendl;
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  if (basedir_fd < 0) {
    ret = -errno;
    derr << "mkfs failed to open base dir " << basedir << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  // open+lock fsid
  snprintf(fsid_fn, sizeof(fsid_fn), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fsid_fn, O_RDWR|O_CREAT, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "mkfs: failed to open " << fsid_fn << ": " << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
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
      fsid = old_fsid;
      dout(1) << "mkfs using provided fsid " << fsid << dendl;
    }

    string fsid_str(to_string(fsid));
    fsid_str += "\n";
    ret = ::ftruncate(fsid_fd, 0);
    if (ret < 0) {
      ret = -errno;
      derr << "mkfs: failed to truncate fsid: "
	   << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    ret = safe_write(fsid_fd, fsid_str.c_str(), fsid_str.size());
    if (ret < 0) {
      derr << "mkfs: failed to write fsid: "
	   << cpp_strerror(ret) << dendl;
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
      derr << "mkfs on-disk fsid " << old_fsid << " != provided " << fsid
	   << dendl;
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

  ret = write_superblock();
  if (ret < 0) {
    derr << "mkfs: write_superblock() failed: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  struct statfs basefs;
  ret = ::fstatfs(basedir_fd, &basefs);
  if (ret < 0) {
    ret = -errno;
    derr << "mkfs cannot fstatfs basedir "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  if (basefs.f_type == BTRFS_SUPER_MAGIC) {
#if defined(__linux__)
    backend = new BtrfsFileStoreBackend(cct, this);
#endif
  } else if (basefs.f_type == XFS_SUPER_MAGIC) {
#ifdef HAVE_LIBXFS
    backend = new XfsFileStoreBackend(cct, this);
#endif
  } else if (basefs.f_type == ZFS_SUPER_MAGIC) {
#ifdef HAVE_LIBZFS
    backend = new ZFSFileStoreBackend(cct, this);
#endif
  }

  ret = backend->create_current();
  if (ret < 0) {
    derr << "mkfs: failed to create current/ " << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  // write initial op_seq
  {
    uint64_t initial_seq = 0;
    int fd = read_op_seq(&initial_seq);
    if (fd < 0) {
      derr << "mkfs: failed to create " << current_op_seq_fn << ": "
	   << cpp_strerror(fd) << dendl;
      goto close_fsid_fd;
    }
    if (initial_seq == 0) {
      int err = write_op_seq(fd, 1);
      if (err < 0) {
	VOID_TEMP_FAILURE_RETRY(::close(fd));
	derr << "mkfs: failed to write to " << current_op_seq_fn << ": "
	     << cpp_strerror(err) << dendl;
	goto close_fsid_fd;
      }

      if (backend->can_checkpoint()) {
	// create snap_1 too
	current_fd = ::open(current_fn.c_str(), O_RDONLY);
	assert(current_fd >= 0);
	char s[NAME_MAX];
	snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, 1ull);
	ret = backend->create_checkpoint(s, NULL);
	VOID_TEMP_FAILURE_RETRY(::close(current_fd));
	if (ret < 0 && ret != -EEXIST) {
	  VOID_TEMP_FAILURE_RETRY(::close(fd));
	  derr << "mkfs: failed to create snap_1: " << cpp_strerror(ret) << dendl;
	  goto close_fsid_fd;
	}
      }
    }
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }

  {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB* db;
    leveldb::Status status = leveldb::DB::Open(options, omap_dir, &db);
    if (status.ok()) {
      delete db;
      dout(1) << "leveldb db exists/created" << dendl;
    } else {
      derr << "mkfs failed to create leveldb: " << status.ToString() << dendl;
      ret = -1;
      goto close_fsid_fd;
    }
  }

  // journal?
  ret = mkjournal();
  if (ret)
    goto close_fsid_fd;

  dout(1) << "mkfs done in " << basedir << dendl;
  ret = 0;

 close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
 close_basedir_fd:
  VOID_TEMP_FAILURE_RETRY(::close(basedir_fd));
  if (backend != generic_backend) {
    delete backend;
    backend = generic_backend;
  }
  return ret;
}

int FileStore::mkjournal()
{
  // read fsid
  int ret;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
  int fd = ::open(fn, O_RDONLY, 0644);
  if (fd < 0) {
    int err = errno;
    derr << "FileStore::mkjournal: open error: " << cpp_strerror(err) << dendl;
    return -err;
  }
  ret = read_fsid(fd, &fsid);
  if (ret < 0) {
    derr << "FileStore::mkjournal: read error: " << cpp_strerror(ret) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));

  ret = 0;

  open_journal();
  if (journal) {
    ret = journal->check();
    if (ret < 0) {
      ret = journal->create();
      if (ret)
	derr << "mkjournal error creating journal on " << journalpath
		<< ": " << cpp_strerror(ret) << dendl;
      else
	dout(0) << "mkjournal created journal on " << journalpath << dendl;
    }
    delete journal;
    journal = 0;
  }
  return ret;
}

int FileStore::read_fsid(int fd, boost::uuids::uuid* id)
{
  boost::uuids::string_generator parse;
  char fsid_str[40];
  memset(fsid_str, 0, sizeof(fsid_str));
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

int FileStore::lock_fsid()
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
    dout(0) << "lock_fsid failed to lock " << basedir << "/fsid, is another ceph-osd still running? "
	    << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool FileStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << " journal " << journalpath << dendl;
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

int FileStore::_detect_fs()
{
  struct statfs st;
  int r = ::fstatfs(basedir_fd, &st);
  if (r < 0)
    return -errno;

  blk_size = st.f_bsize;

  m_fs_type = fs_types::FS_TYPE_OTHER;
  if (st.f_type == BTRFS_SUPER_MAGIC) {
#if defined(__linux__)
    dout(0) << "mount detected btrfs" << dendl;
    backend = new BtrfsFileStoreBackend(cct, this);
    m_fs_type = fs_types::FS_TYPE_BTRFS;
#endif
  } else if (st.f_type == XFS_SUPER_MAGIC) {
#ifdef HAVE_LIBXFS
    dout(0) << "mount detected xfs (libxfs)" << dendl;
    backend = new XfsFileStoreBackend(this);
#else
    dout(0) << "mount detected xfs" << dendl;
#endif
    m_fs_type = fs_types::FS_TYPE_XFS;

    // wbthrottle is constructed with fs(WBThrottle::XFS)
    if (m_filestore_replica_fadvise) {
      dout(1) << " disabling 'filestore replica fadvise' due to known issues with fadvise(DONTNEED) on xfs" << dendl;
      cct->_conf->set_val("filestore_replica_fadvise", "false");
      cct->_conf->apply_changes(NULL);
      assert(m_filestore_replica_fadvise == false);
    }
  } else if (st.f_type == ZFS_SUPER_MAGIC) {
#ifdef HAVE_LIBZFS
    dout(0) << "mount detected zfs (libzfs)" << dendl;
    backend = new ZFSFileStoreBackend(this);
    m_fs_type = FS_TYPE_ZFS;
#endif
  }

  set_xattr_limits_via_conf();

  r = backend->detect_features();
  if (r < 0) {
    derr << "_detect_fs: detect_features error: " << cpp_strerror(r) << dendl;
    return r;
  }

  // test xattrs
  char fn[PATH_MAX];
  int x = rand();
  int y = x+1;
  snprintf(fn, sizeof(fn), "%s/xattr_test", basedir.c_str());
  int tmpfd = ::open(fn, O_CREAT|O_WRONLY|O_TRUNC, 0700);
  if (tmpfd < 0) {
    int ret = -errno;
    derr << "_detect_fs unable to create " << fn << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  int ret = chain_fsetxattr(tmpfd, "user.test", &x, sizeof(x));
  if (ret >= 0)
    ret = chain_fgetxattr(tmpfd, "user.test", &y, sizeof(y));
  if ((ret < 0) || (x != y)) {
    derr << "Extended attributes don't appear to work. ";
    if (ret)
      *_dout << "Got error " + cpp_strerror(ret) + ". ";
    *_dout << "If you are using ext3 or ext4, be sure to mount the underlying "
	   << "file system with the 'user_xattr' option." << dendl;
    ::unlink(fn);
    VOID_TEMP_FAILURE_RETRY(::close(tmpfd));
    return -ENOTSUP;
  }

  char buf[1000];
  memset(buf, 0, sizeof(buf)); // shut up valgrind
  chain_fsetxattr(tmpfd, "user.test", &buf, sizeof(buf));
  chain_fsetxattr(tmpfd, "user.test2", &buf, sizeof(buf));
  chain_fsetxattr(tmpfd, "user.test3", &buf, sizeof(buf));
  chain_fsetxattr(tmpfd, "user.test4", &buf, sizeof(buf));
  ret = chain_fsetxattr(tmpfd, "user.test5", &buf, sizeof(buf));
  if (ret == -ENOSPC) {
    dout(0) << "limited size xattrs" << dendl;
  }
  chain_fremovexattr(tmpfd, "user.test");
  chain_fremovexattr(tmpfd, "user.test2");
  chain_fremovexattr(tmpfd, "user.test3");
  chain_fremovexattr(tmpfd, "user.test4");
  chain_fremovexattr(tmpfd, "user.test5");

  ::unlink(fn);
  VOID_TEMP_FAILURE_RETRY(::close(tmpfd));

  return 0;
}

int FileStore::_sanity_check_fs()
{
  // sanity check(s)

  if (((int)m_filestore_journal_writeahead +
      (int)m_filestore_journal_parallel +
      (int)m_filestore_journal_trailing) > 1) {
    dout(0) << "mount ERROR: more than one of filestore journal {writeahead,parallel,trailing} enabled" << dendl;
    cerr << TEXT_RED
	 << " ** WARNING: more than one of 'filestore journal {writeahead,parallel,trailing}'\n"
	 << "		  is enabled in ceph.conf.  You must choose a single journal mode."
	 << TEXT_NORMAL << std::endl;
    return -EINVAL;
  }

  if (!backend->can_checkpoint()) {
    if (!journal || !m_filestore_journal_writeahead) {
      dout(0) << "mount WARNING: no btrfs, and no journal in writeahead mode; data may be lost" << dendl;
      cerr << TEXT_RED
	   << " ** WARNING: no btrfs AND (no journal OR journal not in writeahead mode)\n"
	   << "		    For non-btrfs volumes, a writeahead journal is required to\n"
	   << "		    maintain on-disk consistency in the event of a crash.  Your conf\n"
	   << "		    should include something like:\n"
	   << "	       osd journal = /path/to/journal_device_or_file\n"
	   << "	       filestore journal writeahead = true\n"
	   << TEXT_NORMAL;
    }
  }

  if (!journal) {
    dout(0) << "mount WARNING: no journal" << dendl;
    cerr << TEXT_YELLOW
	 << " ** WARNING: No osd journal is configured: write latency may be high.\n"
	 << "		  If you will not be using an osd journal, write latency may be\n"
	 << "		  relatively high.  It can be reduced somewhat by lowering\n"
	 << "		  filestore_max_sync_interval, but lower values mean lower write\n"
	 << "		  throughput, especially with spinning disks.\n"
	 << TEXT_NORMAL;
  }

  return 0;
}

int FileStore::write_superblock()
{
  bufferlist bl;
  ::encode(superblock, bl);
  return safe_write_file(basedir.c_str(), "superblock",
      bl.c_str(), bl.length());
}

int FileStore::read_superblock()
{
  bufferptr bp(PATH_MAX);
  int ret = safe_read_file(basedir.c_str(), "superblock",
      bp.c_str(), bp.length());
  if (ret < 0) {
    if (ret == -ENOENT) {
      // If the file doesn't exist write initial CompatSet
      return write_superblock();
    }
    return ret;
  }

  bufferlist bl;
  bl.push_back(bp);
  bufferlist::iterator i = bl.begin();
  ::decode(superblock, i);
  return 0;
}

int FileStore::update_version_stamp()
{
  return write_version_stamp();
}

int FileStore::version_stamp_is_valid(uint32_t* version)
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

int FileStore::write_version_stamp()
{
  bufferlist bl;
  ::encode(target_version, bl);

  return safe_write_file(basedir.c_str(), "store_version",
      bl.c_str(), bl.length());
}

int FileStore::read_op_seq(uint64_t* seq)
{
  int op_fd = ::open(current_op_seq_fn.c_str(), O_CREAT|O_RDWR, 0644);
  if (op_fd < 0) {
    int r = -errno;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  char s[40];
  memset(s, 0, sizeof(s));
  int ret = safe_read(op_fd, s, sizeof(s) - 1);
  if (ret < 0) {
    derr << "error reading " << current_op_seq_fn << ": " << cpp_strerror(ret) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(op_fd));
    assert(!m_filestore_fail_eio || ret != -EIO);
    return ret;
  }
  *seq = atoll(s);
  return op_fd;
}

int FileStore::write_op_seq(int fd, uint64_t seq)
{
  char s[30];
  snprintf(s, sizeof(s), "%" PRId64 "\n", seq);
  int ret = TEMP_FAILURE_RETRY(::pwrite(fd, s, strlen(s), 0));
  if (ret < 0) {
    ret = -errno;
    assert(!m_filestore_fail_eio || ret != -EIO);
  }
  return ret;
}

int FileStore::mount()
{
  int ret;
  char buf[PATH_MAX];
  uint64_t initial_op_seq;
  set<string> cluster_snaps;
  CompatSet supported_compat_set = get_fs_supported_compat_set();

  dout(5) << "basedir " << basedir << " journal " << journalpath << dendl;

  // make sure global base dir exists
  if (::access(basedir.c_str(), R_OK | W_OK)) {
    ret = -errno;
    derr << "FileStore::mount: unable to access basedir '" << basedir << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  // get fsid
  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_RDWR, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "FileStore::mount: error opening '" << buf << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  ret = read_fsid(fsid_fd, &fsid);
  if (ret < 0) {
    derr << "FileStore::mount: error reading fsid_fd: " << cpp_strerror(ret)
	 << dendl;
    goto close_fsid_fd;
  }

  if (lock_fsid() < 0) {
    derr << "FileStore::mount: lock_fsid failed" << dendl;
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  dout(10) << "mount fsid is " << fsid << dendl;


  uint32_t version_stamp;
  ret = version_stamp_is_valid(&version_stamp);
  if (ret < 0) {
    derr << "FileStore::mount : error in version_stamp_is_valid: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  } else if (ret == 0) {
    if (do_update) {
      derr << "FileStore::mount : stale version stamp detected: "
	   << version_stamp
	   << ". Proceeding, do_update "
	   << "is set, performing disk format upgrade."
	   << dendl;
    } else {
      ret = -EINVAL;
      derr << "FileStore::mount : stale version stamp " << version_stamp
	   << ". Please run the FileStore update script before starting the "
	   << "OSD, or set filestore_update_to to " << target_version
	   << dendl;
      goto close_fsid_fd;
    }
  }

  ret = read_superblock();
  if (ret < 0) {
    ret = -EINVAL;
    goto close_fsid_fd;
  }

  // Check if this FileStore supports all the necessary features to mount
  if (supported_compat_set.compare(superblock.compat_features) == -1) {
    derr << "FileStore::mount : Incompatible features set "
	   << superblock.compat_features << dendl;
    ret = -EINVAL;
    goto close_fsid_fd;
  }

  // open some dir handles
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  if (basedir_fd < 0) {
    ret = -errno;
    derr << "FileStore::mount: failed to open " << basedir << ": "
	 << cpp_strerror(ret) << dendl;
    basedir_fd = -1;
    goto close_fsid_fd;
  }

  // test for btrfs, xattrs, etc.
  ret = _detect_fs();
  if (ret < 0) {
    derr << "FileStore::mount : error in _detect_fs: "
	 << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  {
    list<string> ls;
    ret = backend->list_checkpoints(ls);
    if (ret < 0) {
      derr << "FileStore::mount : error in _list_snaps: "<< cpp_strerror(ret) << dendl;
      goto close_basedir_fd;
    }

    long long unsigned c, prev = 0;
    char clustersnap[NAME_MAX];
    for (list<string>::iterator it = ls.begin(); it != ls.end(); ++it) {
      if (sscanf(it->c_str(), COMMIT_SNAP_ITEM, &c) == 1) {
	assert(c > prev);
	prev = c;
	snaps.push_back(c);
      } else if (sscanf(it->c_str(), CLUSTER_SNAP_ITEM, clustersnap) == 1)
	cluster_snaps.insert(*it);
    }
  }

  if (m_osd_rollback_to_cluster_snap.length() &&
      cluster_snaps.count(m_osd_rollback_to_cluster_snap) == 0) {
    derr << "rollback to cluster snapshot '" << m_osd_rollback_to_cluster_snap << "': not found" << dendl;
    ret = -ENOENT;
    goto close_basedir_fd;
  }

  char nosnapfn[200];
  snprintf(nosnapfn, sizeof(nosnapfn), "%s/nosnap", current_fn.c_str());

  if (backend->can_checkpoint()) {
    if (snaps.empty()) {
      dout(0) << "mount WARNING: no consistent snaps found, store may be in inconsistent state" << dendl;
    } else {
      char s[NAME_MAX];
      uint64_t curr_seq = 0;

      if (m_osd_rollback_to_cluster_snap.length()) {
	derr << TEXT_RED
	     << " ** NOTE: rolling back to cluster snapshot " << m_osd_rollback_to_cluster_snap << " **"
	     << TEXT_NORMAL
	     << dendl;
	assert(cluster_snaps.count(m_osd_rollback_to_cluster_snap));
	snprintf(s, sizeof(s), CLUSTER_SNAP_ITEM, m_osd_rollback_to_cluster_snap.c_str());
      } else {
	{
	  int fd = read_op_seq(&curr_seq);
	  if (fd >= 0) {
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
	  }
	}
	if (curr_seq)
	  dout(10) << " current/ seq was " << curr_seq << dendl;
	else
	  dout(10) << " current/ missing entirely (unusual, but okay)" << dendl;

	uint64_t cp = snaps.back();
	dout(10) << " most recent snap from " << snaps << " is " << cp << dendl;

	// if current/ is marked as non-snapshotted, refuse to roll
	// back (without clear direction) to avoid throwing out new
	// data.
	struct stat st;
	if (::stat(nosnapfn, &st) == 0) {
	  if (!m_osd_use_stale_snap) {
	    derr << "ERROR: " << nosnapfn << " exists, not rolling back to avoid losing new data" << dendl;
	    derr << "Force rollback to old snapshotted version with 'osd use stale snap = true'" << dendl;
	    derr << "config option for --osd-use-stale-snap startup argument." << dendl;
	    ret = -ENOTSUP;
	    goto close_basedir_fd;
	  }
	  derr << "WARNING: user forced start with data sequence mismatch: current was " << curr_seq
	       << ", newest snap is " << cp << dendl;
	  cerr << TEXT_YELLOW
	       << " ** WARNING: forcing the use of stale snapshot data **"
	       << TEXT_NORMAL << std::endl;
	}

	dout(10) << "mount rolling back to consistent snap " << cp << dendl;
	snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, (long long unsigned)cp);
      }

      // drop current?
      ret = backend->rollback_to(s);
      if (ret) {
	derr << "FileStore::mount: error rolling back to " << s << ": "
	     << cpp_strerror(ret) << dendl;
	goto close_basedir_fd;
      }
    }
  }
  initial_op_seq = 0;

  current_fd = ::open(current_fn.c_str(), O_RDONLY);
  if (current_fd < 0) {
    ret = -errno;
    derr << "FileStore::mount: error opening: " << current_fn << ": " << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  assert(current_fd >= 0);

  op_fd = read_op_seq(&initial_op_seq);
  if (op_fd < 0) {
    derr << "FileStore::mount: read_op_seq failed" << dendl;
    goto close_current_fd;
  }

  dout(5) << "mount op_seq is " << initial_op_seq << dendl;
  if (initial_op_seq == 0) {
    derr << "mount initial op seq is 0; something is wrong" << dendl;
    ret = -EINVAL;
    goto close_current_fd;
  }

  if (!backend->can_checkpoint()) {
    // mark current/ as non-snapshotted so that we don't rollback away
    // from it.
    int r = ::creat(nosnapfn, 0644);
    if (r < 0) {
      derr << "FileStore::mount: failed to create current/nosnap" << dendl;
      goto close_current_fd;
    }
    VOID_TEMP_FAILURE_RETRY(::close(r));
  } else {
    // clear nosnap marker, if present.
    ::unlink(nosnapfn);
  }

  {
    LevelDBStore* omap_store = new LevelDBStore(cct, omap_dir);

    omap_store->init();
    if (cct->_conf->osd_leveldb_write_buffer_size)
      omap_store->options.write_buffer_size = cct->_conf->osd_leveldb_write_buffer_size;
    if (cct->_conf->osd_leveldb_cache_size)
      omap_store->options.cache_size = cct->_conf->osd_leveldb_cache_size;
    if (cct->_conf->osd_leveldb_block_size)
      omap_store->options.block_size = cct->_conf->osd_leveldb_block_size;
    if (cct->_conf->osd_leveldb_bloom_size)
      omap_store->options.bloom_size = cct->_conf->osd_leveldb_bloom_size;
    if (cct->_conf->osd_leveldb_compression)
      omap_store->options.compression_enabled = cct->_conf->osd_leveldb_compression;
    if (cct->_conf->osd_leveldb_paranoid)
      omap_store->options.paranoid_checks = cct->_conf->osd_leveldb_paranoid;
    if (cct->_conf->osd_leveldb_max_open_files)
      omap_store->options.max_open_files = cct->_conf->osd_leveldb_max_open_files;
    if (cct->_conf->osd_leveldb_log.length())
      omap_store->options.log_file = cct->_conf->osd_leveldb_log;

    stringstream err;
    if (omap_store->create_and_open(err)) {
      delete omap_store;
      derr << "Error initializing leveldb: " << err.str() << dendl;
      ret = -1;
      goto close_current_fd;
    }

    if (cct->_conf->osd_compact_leveldb_on_mount) {
      derr << "Compacting store..." << dendl;
      omap_store->compact();
      derr << "...finished compacting store" << dendl;
    }

    DBObjectMap* dbomap = new DBObjectMap(cct, omap_store);
    ret = dbomap->init(do_update);
    if (ret < 0) {
      delete dbomap;
      derr << "Error initializing DBObjectMap: " << ret << dendl;
      goto close_current_fd;
    }
    stringstream err2;

    if (cct->_conf->filestore_debug_omap_check && !dbomap->check(err2)) {
      derr << err2.str() << dendl;;
      delete dbomap;
      ret = -EINVAL;
      goto close_current_fd;
    }
    object_map.reset(dbomap);
  }

  // journal
  open_journal();

  // select journal mode?
  if (journal) {
    if (!m_filestore_journal_writeahead &&
	!m_filestore_journal_parallel &&
	!m_filestore_journal_trailing) {
      if (!backend->can_checkpoint()) {
	m_filestore_journal_writeahead = true;
	dout(0) << "mount: enabling WRITEAHEAD journal mode: checkpoint is not enabled" << dendl;
      } else {
	m_filestore_journal_parallel = true;
	dout(0) << "mount: enabling PARALLEL journal mode: fs, checkpoint is enabled" << dendl;
      }
    } else {
      if (m_filestore_journal_writeahead)
	dout(0) << "mount: WRITEAHEAD journal mode explicitly enabled in conf" << dendl;
      if (m_filestore_journal_parallel)
	dout(0) << "mount: PARALLEL journal mode explicitly enabled in conf" << dendl;
      if (m_filestore_journal_trailing)
	dout(0) << "mount: TRAILING journal mode explicitly enabled in conf" << dendl;
    }
    if (m_filestore_journal_writeahead)
      journal->set_wait_on_full(true);
  }

  ret = _sanity_check_fs();
  if (ret) {
    derr << "FileStore::mount: _sanity_check_fs failed with error "
	 << ret << dendl;
    goto close_current_fd;
  }

  // XXX formerly, we enumerated collections (list_collections)
  // and called index->cleanup on each
  flusher.start();
  sync_thread.create();

  ret = journal_replay(initial_op_seq);
  if (ret < 0) {
    derr << "mount failed to open journal " << journalpath << ": "
	 << cpp_strerror(ret) << dendl;
    if (ret == -ENOTTY) {
      derr << "maybe journal is not pointing to a block device and its size "
	   << "wasn't configured?" << dendl;
    }

    // stop sync thread
    unique_lock l(lock);
    stop = true;
    sync_cond.notify_all();
    l.unlock();
    sync_thread.join();
    flusher.stop();

    goto close_current_fd;
  }

  {
    stringstream err2;
    if (cct->_conf->filestore_debug_omap_check && !object_map->check(err2)) {
      derr << err2.str() << dendl;;
      ret = -EINVAL;
      goto close_current_fd;
    }
  }

  journal_start();

  op_finisher.start();
  ondisk_finisher.start();

  // all okay.
  return 0;

close_current_fd:
  VOID_TEMP_FAILURE_RETRY(::close(current_fd));
  current_fd = -1;
close_basedir_fd:
  VOID_TEMP_FAILURE_RETRY(::close(basedir_fd));
  basedir_fd = -1;
close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
done:
  assert(!m_filestore_fail_eio || ret != -EIO);
  return ret;
}

int FileStore::umount()
{
  dout(5) << "umount " << basedir << dendl;


  start_sync();

  unique_lock l(lock);
  stop = true;
  sync_cond.notify_all();
  l.unlock();
  sync_thread.join();
  flusher.stop();

  journal_stop();

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
  if (basedir_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(basedir_fd));
    basedir_fd = -1;
  }

  if (backend != generic_backend) {
    delete backend;
    backend = generic_backend;
  }

  object_map.reset();

  // nothing
  return 0;
}


int FileStore::get_max_object_name_length()
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


int FileStore::queue_transactions(list<Transaction*> &tls, OpRequestRef osd_op)
{
  Context* onreadable;
  Context* ondisk;
  Context* onreadable_sync;
  Transaction::collect_contexts(tls, &onreadable, &ondisk, &onreadable_sync);
  if (cct->_conf->filestore_blackhole) {
    dout(0) << "queue_transactions filestore_blackhole = TRUE, dropping transaction" << dendl;
    delete ondisk;
    delete onreadable;
    delete onreadable_sync;
    return 0;
  }

  ZTracer::Trace trace("op", &trace_endpoint, osd_op ? &osd_op->trace : NULL);

  int r = 0;
  if (journal && journal->is_writeable() && !m_filestore_journal_trailing) {
    journal->throttle();
    const uint64_t op = ++submit_op_seq;
    trace.keyval("opnum", op);

    if (m_filestore_do_dump)
      dump_transactions(tls, op);

    if (m_filestore_journal_parallel) {
      dout(5) << "queue_transactions (parallel) " << op << " " << tls << dendl;
      trace.keyval("journal mode", "parallel");

      // start async journaling, which completes ondisk
      _op_journal_transactions(tls, op, ondisk, osd_op, trace);

      // run the transactions
      r = do_transactions(tls, op, trace);
    } else if (m_filestore_journal_writeahead) {
      dout(5) << "queue_transactions (writeahead) " << op << " " << tls << dendl;
      trace.keyval("journal mode", "writeahead");

      // start journaling and block until it completes
      trace.event("writeahead journal started");
      {
        std::mutex mutex;
        std::condition_variable cond;
        bool done = false;
        _op_journal_transactions(tls, op, new C_SafeCond(&mutex, &cond, &done),
                                 osd_op, trace);
        std::unique_lock<std::mutex> lock(mutex);
        while (!done)
          cond.wait(lock);
      }
      trace.event("writeahead journal finished");

      // do the transactions and queue the completion
      r = do_transactions(tls, op, trace);
      if (r < 0) {
        delete ondisk;
      } else if (ondisk) {
        dout(10) << " queueing ondisk " << ondisk << dendl;
        ondisk_finisher.queue(ondisk);
      }
    } else {
      assert(0);
    }
  } else {
    const uint64_t op = ++submit_op_seq;
    dout(5) << "queue_transactions (trailing journal) " << op << " " << tls << dendl;
    trace.keyval("journal mode", "trailing");

    if (m_filestore_do_dump)
      dump_transactions(tls, op);

    r = do_transactions(tls, op, trace);
    if (r >= 0) {
      _op_journal_transactions(tls, op, ondisk, osd_op, trace);
    } else {
      delete ondisk;
    }
  }

  if (onreadable_sync)
    onreadable_sync->complete(r);
  if (onreadable)
    op_finisher.queue(onreadable, r);

  return r;
}

int FileStore::do_transactions(list<Transaction*> &tls, uint64_t op,
                               ZTracer::Trace &trace)
{
  int r = 0;

  trace.event("op_apply_start");
  apply_manager.op_apply_start(op);
  trace.event("do_transactions");

  int trans_num = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p, trans_num++) {
    r = do_transaction(**p, op, trans_num);
    if (r < 0)
      break;
  }

  trace.event("op_apply_finish");
  apply_manager.op_apply_finish(op);

  return r;
}

void FileStore::_set_global_replay_guard(FSCollection* fc,
					 const SequencerPosition& spos)
{
  if (backend->can_checkpoint())
    return;

  // sync all previous operations on this sequencer
  sync_filesystem(basedir_fd);

  _inject_failure();

  // then record that we did it
  bufferlist v;
  ::encode(spos, v);
  int fd = fc->index.get_rootfd();
  int r = chain_fsetxattr(fd, GLOBAL_REPLAY_GUARD_XATTR, v.c_str(), v.length());
  if (r < 0) {
    derr << __func__ << ": fsetxattr " << GLOBAL_REPLAY_GUARD_XATTR
	 << " got " << cpp_strerror(r) << dendl;
    assert(0 == "fsetxattr failed");
  }

  // and make sure our xattr is durable.
  ::fsync(fd);

  _inject_failure();

  dout(10) << __func__ << ": " << spos << " done" << dendl;
}

int FileStore::_check_global_replay_guard(FSCollection* fc,
					  const SequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;

  char buf[100];
  int fd = fc->index.get_rootfd();
  int r = chain_fgetxattr(fd, GLOBAL_REPLAY_GUARD_XATTR, buf, sizeof(buf));
  if (r < 0) {
    dout(20) << __func__ << " no xattr" << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return 1;  // no xattr
  }
  bufferlist bl;
  bl.append(buf, r);

  SequencerPosition opos;
  bufferlist::iterator p = bl.begin();
  ::decode(opos, p);

  return spos >= opos ? 1 : -1;
}

void FileStore::_set_replay_guard(FSCollection* fc,
				  const SequencerPosition& spos,
				  bool in_progress=false)
{
  _set_replay_guard(fc->index.get_rootfd(), spos, 0, in_progress);
}


void FileStore::_set_replay_guard(int fd,
				  const SequencerPosition& spos,
				  const hoid_t* hoid,
				  bool in_progress)
{
  if (backend->can_checkpoint())
    return;

  dout(10) << "_set_replay_guard " << spos << (in_progress ? " START" : "")
	   << dendl;

  _inject_failure();

  // first make sure the previous operation commits
  ::fsync(fd);

  // sync object_map too.  even if this object has a header or keys,
  // it have had them in the past and then removed them, so always
  // sync.
  object_map->sync(hoid, &spos);

  _inject_failure();

  // then record that we did it
  bufferlist v(40);
  ::encode(spos, v);
  ::encode(in_progress, v);
  int r = chain_fsetxattr(fd, REPLAY_GUARD_XATTR, v.c_str(), v.length());
  if (r < 0) {
    derr << "fsetxattr " << REPLAY_GUARD_XATTR << " got " << cpp_strerror(r)
	 << dendl;
    assert(0 == "fsetxattr failed");
  }

  // and make sure our xattr is durable.
  ::fsync(fd);

  _inject_failure();

  dout(10) << "_set_replay_guard " << spos << " done" << dendl;
}

void FileStore::_close_replay_guard(FSCollection* fc,
				    const SequencerPosition& spos)
{
  _close_replay_guard(fc->index.get_rootfd(), spos);
}

void FileStore::_close_replay_guard(int fd, const SequencerPosition& spos)
{
  if (backend->can_checkpoint())
    return;

  dout(10) << "_close_replay_guard " << spos << dendl;

  _inject_failure();

  // then record that we are done with this operation
  bufferlist v(40);
  ::encode(spos, v);
  bool in_progress = false;
  ::encode(in_progress, v);
  int r = chain_fsetxattr(fd, REPLAY_GUARD_XATTR, v.c_str(), v.length());
  if (r < 0) {
    derr << "fsetxattr " << REPLAY_GUARD_XATTR << " got " << cpp_strerror(r)
	 << dendl;
    assert(0 == "fsetxattr failed");
  }

  // and make sure our xattr is durable.
  ::fsync(fd);

  _inject_failure();

  dout(10) << "_close_replay_guard " << spos << " done" << dendl;
}

struct replay_guard_buf
{
  char buf[100];
  bool fail_eio;
  replay_guard_buf(bool fail_eio) : fail_eio(fail_eio) {}
};

static inline int
_check_global_replay_guard_inl(CephContext *cct,
                               FileStore::FSCollection* fc,
			       const SequencerPosition& spos,
			       replay_guard_buf& sb,
			       const string& basedir) {
  int r = chain_fgetxattr(fc->index.get_rootfd(), GLOBAL_REPLAY_GUARD_XATTR,
			  sb.buf, sizeof(sb.buf));
  if (r < 0) {
    dout(20) << __func__ << " no xattr" << dendl;
    assert(!sb.fail_eio || r != -EIO);
    return 1;  // no xattr
  }
  bufferlist bl;
  bl.append(sb.buf, r);

  SequencerPosition opos;
  bufferlist::iterator p = bl.begin();
  ::decode(opos, p);

  return spos >= opos ? 1 : -1;
} /* _check_global_replay_guard_inl */

static inline int _check_replay_guard_inl(CephContext *cct,
                                          const int fd,
					  const SequencerPosition& spos,
					  replay_guard_buf& sb,
					  const string& basedir) {
  int r = chain_fgetxattr(fd, REPLAY_GUARD_XATTR, sb.buf, sizeof(sb.buf));
  if (r < 0) {
    dout(20) << "_check_replay_guard no xattr" << dendl;
    assert(!sb.fail_eio || r != -EIO);
    return 1;  // no xattr
  }
  bufferlist bl;
  bl.append(sb.buf, r);

  SequencerPosition opos;
  bufferlist::iterator p = bl.begin();
  ::decode(opos, p);
  bool in_progress = false;
  if (!p.end())	  // older journals don't have this
    ::decode(in_progress, p);
  if (opos > spos) {
    dout(10) << "_check_replay_guard object has " << opos
	     << " > current pos " << spos <<
      ", now or in future, SKIPPING REPLAY" << dendl;
    return -1;
  } else if (opos == spos) {
    if (in_progress) {
      dout(10) << "_check_replay_guard object has " << opos
	       << " == current pos " << spos
	       << ", in_progress=true, CONDITIONAL REPLAY" << dendl;
      return 0;
    } else {
      dout(10) << "_check_replay_guard object has " << opos
	       << " == current pos " << spos
	       << ", in_progress=false, SKIPPING REPLAY" << dendl;
      return -1;
    }
  } else {
    dout(10) << "_check_replay_guard object has " << opos
	     << " < current pos " << spos
	     << ", in past, will replay" << dendl;
    return 1;
  }
} /* _check_replay_guard_inl */

int FileStore::_check_replay_guard(FSCollection* fc,
				   FSObject* fo,
				   const SequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;

  replay_guard_buf sb(m_filestore_fail_eio);
  int r = _check_global_replay_guard_inl(cct, fc, spos, sb, basedir);
  if (r < 0)
    return r;

  r = _check_replay_guard_inl(cct, fo->fd, spos, sb, basedir);
  return r;
}

int FileStore::_check_replay_guard(FSCollection* fc,
				   const SequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;
  int ret = _check_replay_guard(fc->index.get_rootfd(), spos);
  return ret;
}

int FileStore::_check_replay_guard(int fd, const SequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;

  char buf[100];
  int r = chain_fgetxattr(fd, REPLAY_GUARD_XATTR, buf, sizeof(buf));
  if (r < 0) {
    dout(20) << "_check_replay_guard no xattr" << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return 1;  // no xattr
  }
  bufferlist bl;
  bl.append(buf, r);

  SequencerPosition opos;
  bufferlist::iterator p = bl.begin();
  ::decode(opos, p);
  bool in_progress = false;
  if (!p.end())	  // older journals don't have this
    ::decode(in_progress, p);
  if (opos > spos) {
    dout(10) << "_check_replay_guard object has " << opos
	     << " > current pos " << spos <<
      ", now or in future, SKIPPING REPLAY" << dendl;
    return -1;
  } else if (opos == spos) {
    if (in_progress) {
      dout(10) << "_check_replay_guard object has " << opos
	       << " == current pos " << spos
	       << ", in_progress=true, CONDITIONAL REPLAY" << dendl;
      return 0;
    } else {
      dout(10) << "_check_replay_guard object has " << opos
	       << " == current pos " << spos
	       << ", in_progress=false, SKIPPING REPLAY" << dendl;
      return -1;
    }
  } else {
    dout(10) << "_check_replay_guard object has " << opos
	     << " < current pos " << spos
	     << ", in past, will replay" << dendl;
    return 1;
  }
}

unsigned FileStore::do_transaction(Transaction& t, uint64_t op_seq,
                                   int trans_num)
{
  dout(10) << "_do_transaction on " << &t << dendl;

  SequencerPosition spos(op_seq, trans_num, 0);

  for (Transaction::op_iterator i = t.begin(); i != t.end(); ++i) {
    int r = 0;

    _inject_failure();

    FSCollection* fc = nullptr;
    FSObject* fo, *fo2;

    switch (i->op) {
    case Transaction::OP_NOP:
      break;

    case Transaction::OP_TOUCH:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _touch(fc, fo);
	}
      }
      break;

    case Transaction::OP_WRITE:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _write(fc, fo, i->off, i->len, i->data, t.get_replica());
	}
      }
      break;
    case Transaction::OP_ZERO:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _zero(fc, fo, i->off, i->len);
	}
      }
      break;

    case Transaction::OP_TRIMCACHE:
      // deprecated, no-op
      break;

    case Transaction::OP_TRUNCATE:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _truncate(fc, fo, i->off);
	}
      }
      break;

    case Transaction::OP_REMOVE:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _remove(fc, fo, spos);
	}
      }
      break;

    case Transaction::OP_SETATTR:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0) {
	    bufferlist &bl = i->data;
	    map<string, bufferptr> to_set;
	    to_set[i->name] = bufferptr(bl.c_str(), bl.length());
	    r = _setattrs(fc, fo, to_set, spos);
	    if (r == -ENOSPC)
	      dout(0) << " ENOSPC on setxattr on " << fc->get_cid() << "/"
		      << fo->get_oid() << " name " << i->name << " size "
		      << bl.length() << dendl;
	  }
	}
      }
      break;

    case Transaction::OP_SETATTRS:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _setattrs(fc, fo, i->xattrs, spos);
	  if (r == -ENOSPC)
	    dout(0) << " ENOSPC on setxattrs on " << fc->get_cid() << "/"
		    << fo->get_oid() << dendl;
	}
      }
      break;

    case Transaction::OP_RMATTR:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _rmattr(fc, fo, i->name.c_str(), spos);
	}
      }
      break;

    case Transaction::OP_RMATTRS:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _rmattrs(fc, fo, spos);
	}
      }
      break;

    case Transaction::OP_CLONE:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  fo2 = get_slot_object(t, fc, i->o2_ix, spos, true /* create */);
	  if (fo2) {
	    r = _clone(fc, fo, fo2, spos);
	  }
	}
      }
      break;

    case Transaction::OP_CLONERANGE:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  fo2 = get_slot_object(t, fc, i->o2_ix, spos, true /* create */);
	  if (fo2) {
	    r = _clone_range(fc, fo, fo2, i->off, i->len, i->off, spos);
	  }
	}
      }
      break;

    case Transaction::OP_CLONERANGE2:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  fo2 = get_slot_object(t, fc, i->o2_ix, spos, true /* create */);
	  if (fo2) {
	    r = _clone_range(fc, fo, fo2, i->off, i->len, i->off2, spos);
	  }
	}
      }
      break;

    case Transaction::OP_MKCOLL:
      // XXX fix replay guard
      r = _create_collection(std::get<1>(t.c_slot(i->c1_ix)), spos);
      if (!r) {
	(void) get_slot_collection(t, i->c1_ix);
      }
      break;

    case Transaction::OP_RMCOLL:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	if (_check_replay_guard(fc, spos) > 0)
	  r = _destroy_collection(fc);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	if (_check_replay_guard(fc, spos) > 0)
	  r = _collection_setattr(fc, i->name.c_str(),
				  i->data.c_str(), i->data.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	if (_check_replay_guard(fc, spos) > 0)
	  r = _collection_rmattr(fc, i->name.c_str());
      }
      break;

    case Transaction::OP_STARTSYNC:
      _start_sync();
      break;

    case Transaction::OP_OMAP_CLEAR:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* !create */);
	if (fo) {
	  r = _omap_clear(fc, fo, spos);
	}
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* create */);
	if (fo) {
	  r = _omap_setkeys(fc, fo, i->attrs, spos);
	}
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* !create */);
	if (fo) {
	  r = _omap_rmkeys(fc, fo, i->keys, spos);
	}
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* !create */);
	if (fo) {
	  r = _omap_rmkeyrange(fc, fo, i->name, i->name2, spos);
	}
      }	
      break;
    case Transaction::OP_OMAP_SETHEADER:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, true /* !create */);
	if (fo) {
	  r = _omap_setheader(fc, fo, i->data, spos);
	}
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      r = -ENOENT;
      fc = get_slot_collection(t, i->c1_ix);
      if (fc) {
	fo = get_slot_object(t, fc, i->o1_ix, spos, false /* !create */);
	if (fo) {
	  if (_check_replay_guard(fc, fo, spos) > 0)
	    r = _set_alloc_hint(fc, fo, i->value1, i->value2);
	}
      }
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

      if (replaying && !backend->can_checkpoint()) {
	if (r == -EEXIST && i->op == Transaction::OP_MKCOLL) {
	  dout(10) << "tolerating EEXIST during journal replay since "
	    "checkpoint is not enabled" << dendl;
	  ok = true;
	}
	if (r == -EEXIST && i->op == Transaction::OP_COLL_ADD) {
	  dout(10) << "tolerating EEXIST during journal replay since "
	    "checkpoint is not enabled" << dendl;
	  ok = true;
	}
	if (r == -ERANGE) {
	  dout(10) << "tolerating ERANGE on replay" << dendl;
	  ok = true;
	}
	if (r == -ENOENT) {
	  dout(10) << "tolerating ENOENT on replay" << dendl;
	  ok = true;
	}
      }

      if (!ok) {
	const char* msg = "unexpected error code";

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
	t.dump(&f);
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

  _inject_failure();

  return 0;  // FIXME count errors
}

  /*********************************************/



// --------------------
// objects

bool FileStore::exists(CollectionHandle ch, const hoid_t& oid)
{
  FSCollection* fc = static_cast<FSCollection*>(ch);
  return fc->index.lookup(oid) == 0;
}

FileStore::FSObject* FileStore::get_object(FSCollection* fc,
					   const hoid_t& oid,
					   const SequencerPosition& spos,
					   bool create)
{
  int fd;
  FSObject* oh = nullptr;

  /* XXX redundant, hoid_t has hk */
  std::tuple<uint64_t, const hoid_t&> k(oid.hk, oid);
  ceph::os::Object::ObjCache::Latch lat;

  if (fc->flags & ceph::os::Collection::FLAG_CLOSED) /* atomicity? */
    return oh;

retry:
  oh =
    static_cast<FSObject*>(fc->obj_cache.find_latch(
				oid.hk, oid,
				lat,
				ceph::os::Object::ObjCache::FLAG_LOCK));
  /* LATCHED */
  if (oh) {
    /* need initial ref from LRU (fast path) */
    if (! obj_lru.ref(oh, cohort::lru::FLAG_INITIAL)) {
      lat.lock->unlock();
      goto retry; /* !LATCHED */
    }
    /* LATCHED */
  } else {
    /* allocate and insert "new" Object */

    int r = fc->index.open(oid, false, &fd);
    if ((r < 0) &&
	create &&
	_check_global_replay_guard(fc, spos)) {
      r = fc->index.open(oid, true, &fd);
    }
    if (r != 0)
      goto out; /* !LATCHED */

    FSObject::FSObjectFactory prototype(fc, oid, fd);
    oh = static_cast<FSObject*>(
      obj_lru.insert(&prototype,
		     cohort::lru::Edge::MRU,
		     cohort::lru::FLAG_INITIAL));
    if (oh) {
      fc->obj_cache.insert_latched(oh, lat,
				   ceph::os::Object::ObjCache::FLAG_UNLOCK);
      goto out; /* !LATCHED */
    } else {
      lat.lock->unlock();
      goto retry; /* !LATCHED */
    }
  }
  lat.lock->unlock(); /* !LATCHED */
 out:
  return oh;
}

ObjectHandle FileStore::get_object(CollectionHandle ch,
				   const hoid_t& oid)
{
  static SequencerPosition dummy_spos;
  return get_object(static_cast<FSCollection*>(ch), oid, dummy_spos,
		    false);
}

ObjectHandle FileStore::get_object(CollectionHandle ch,
				   const hoid_t& oid,
				   bool create)
{
  static SequencerPosition dummy_spos;
  return get_object(static_cast<FSCollection*>(ch), oid, dummy_spos,
		    create);
}

void FileStore::put_object(FSObject* fo)
{
  obj_lru.unref(fo, cohort::lru::FLAG_NONE);
}

void FileStore::put_object(ObjectHandle oh)
{
  return put_object(static_cast<FSObject*>(oh));
}

int FileStore::stat(
  CollectionHandle ch, ObjectHandle oh, struct stat* st,
  bool allow_eio)
{
  FSObject* fo = static_cast<FSObject*>(oh);
  int r = ::fstat(fo->fd, st);
  if (r < 0) {
    r = -errno;
    dout(10) << "stat " << ch->get_cid() << "/" << fo->get_oid()
	     << " = " << r << dendl;
  } else {
    dout(10) << "stat " << ch->get_cid() << "/" << fo->get_oid()
	     << " = " << r
	     << " (size " << st->st_size << ")" << dendl;
  }
  if (cct->_conf->filestore_debug_inject_read_err &&
      debug_mdata_eio(fo->get_oid())) {
    return -EIO;
  } else {
    return r;
  }
}

int FileStore::read(
  CollectionHandle ch,
  ObjectHandle oh,
  uint64_t offset,
  size_t len,
  bufferlist& bl,
  bool allow_eio)
{
  int got;

  dout(15) << "read " << ch->get_cid() << "/" << oh->get_oid() << " "
	   << offset << "~" << len << dendl;

  FSObject* fo = static_cast<FSObject*>(oh);

  if (len == 0)
    return 0;

  if (len == CEPH_READ_ENTIRE) {
    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    int r = ::fstat(fo->fd, &st);
    assert(r == 0);
    len = st.st_size;
  }

  bufferptr bptr;
  try {
    bptr = bufferptr(len);	// prealloc space for entire read
  } catch (std::bad_alloc& e ) {
    derr << "FileStore::read(" << ch->get_cid() << "/"
	 << fo->get_oid() << ") cannot allocate "
	 << len << " bytes" << dendl;
    return -ENOMEM;
  }
  got = safe_pread(fo->fd, bptr.c_str(), len, offset);
  if (got < 0) {
    dout(10) << "FileStore::read(" << ch->get_cid() << "/"
	     << fo->get_oid() << ") pread error: "
	     << cpp_strerror(got)
	     << dendl;
    assert(allow_eio || !m_filestore_fail_eio || got != -EIO);
    return got;
  }
  bptr.set_length(got);	  // properly size the buffer
  bl.push_back(bptr);	// put it in the target bufferlist

  if (m_filestore_sloppy_crc &&
      (!replaying || backend->can_checkpoint())) {
    ostringstream ss;
    int errors = backend->_crc_verify_read(fo->fd, offset, got, bl,
					   &ss);
    if (errors > 0) {
      dout(0) << "FileStore::read " << ch->get_cid() << "/"
	      << fo->get_oid()
	      << " " << offset << "~" << got << " ... BAD CRC:\n"
	      << ss.str()
	      << dendl;
      assert(0 == "bad crc on read");
    }
  }

  dout(10) << "FileStore::read " << ch->get_cid() << "/"
	   << fo->get_oid() << " " << offset << "~" << got << "/"
	   << len << dendl;
  if (cct->_conf->filestore_debug_inject_read_err &&
      debug_data_eio(fo->get_oid())) {
    return -EIO;
  } else {
    return got;
  }
}

int FileStore::fiemap(CollectionHandle ch, ObjectHandle oh,
		      uint64_t offset, size_t len, bufferlist& bl)
{
  if (!backend->has_fiemap() ||
      len <= (size_t)m_filestore_fiemap_threshold) {
    map<uint64_t, uint64_t> m;
    m[offset] = len;
    ::encode(m, bl);
    return 0;
  }

  FSObject* fo = static_cast<FSObject*>(oh);
  map<uint64_t, uint64_t> exomap;
  struct fiemap* fiemap = NULL;

  dout(15) << "fiemap " << ch->get_cid() << "/" << fo->get_oid() << " "
	   << offset << "~" << len << dendl;

  uint64_t i;
  struct fiemap_extent* next,* extent;

  int r = backend->do_fiemap(fo->fd, offset, len, &fiemap);
  if (r < 0)
    goto done;

  if (fiemap->fm_mapped_extents == 0) {
    free(fiemap);
    goto done;
  }

  extent = &fiemap->fm_extents[0];

  /* start where we were asked to start */
  if (extent->fe_logical < offset) {
    extent->fe_length -= offset - extent->fe_logical;
    extent->fe_logical = offset;
  }

  i = 0;
  while (i < fiemap->fm_mapped_extents) {
    next = extent + 1;

    dout(10) << "FileStore::fiemap() fm_mapped_extents="
	     << fiemap->fm_mapped_extents
	     << " fe_logical=" << extent->fe_logical << " fe_length="
	     << extent->fe_length << dendl;

    /* try to merge extents */
    while ((i < fiemap->fm_mapped_extents - 1) &&
	   (extent->fe_logical + extent->fe_length == next->fe_logical)) {
      next->fe_length += extent->fe_length;
      next->fe_logical = extent->fe_logical;
      extent = next;
      next = extent + 1;
      i++;
    }

    if (extent->fe_logical + extent->fe_length > offset + len)
      extent->fe_length = offset + len - extent->fe_logical;
    exomap[extent->fe_logical] = extent->fe_length;
    i++;
    extent++;
  }
  free(fiemap);

done:
  if (r >= 0) {
    ::encode(exomap, bl);
  }

  dout(10) << "fiemap " << ch->get_cid() << "/" << fo->get_oid() << " "
	   << offset << "~" << len << " = " << r << " num_extents="
	   << exomap.size() << " " << exomap << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int FileStore::_remove(FSCollection* fc, FSObject *fo,
		       const SequencerPosition& spos)
{
  dout(15) << "remove " << fc->get_cid() << "/" << fo->get_oid() << dendl;
  int r = lfn_unlink(fc, fo, spos);
  dout(10) << "remove " << fc->get_cid() << "/" << fo->get_oid()
      << " = " << r << dendl;
  return r;
}

int FileStore::_truncate(FSCollection* fc, FSObject* fo,
			 uint64_t size)
{
  dout(15) << "truncate " << fc->get_cid() << "/" << fo->get_oid()
	   << " size " << size << dendl;

  int r = ::ftruncate(fo->fd, size);
  if (r < 0)
    r = -errno;
  if (r >= 0 && m_filestore_sloppy_crc) {
    int rc = backend->_crc_update_truncate(fo->fd, size);
    assert(rc >= 0);
  }
  assert(!m_filestore_fail_eio || r != -EIO);

  dout(10) << "truncate " << fc->get_cid() << "/" << fo->get_oid()
	   << " size " << size << " = " << r << dendl;
  return r;
}

int FileStore::_touch(FSCollection* fc, FSObject* fo)
{
  dout(15) << "touch " << fc->get_cid() << "/" << fo->get_oid() << dendl;
  return 0;
}

int FileStore::_write(FSCollection* fc, FSObject* fo,
		     uint64_t offset, size_t len,
		     const bufferlist& bl, bool replica)
{

  dout(15) << "write " << fc->get_cid() << "/" << fo->get_oid() << " "
	   << offset << "~" << len << dendl;
  int r = 0;
  int fd = fo->fd;

  //  pwritev up to 256 segments at a time (allocate w/alloca), until done
  int iov_ix, n_iov = std::min(256UL, bl.buffers().size());
  struct iovec *iov = static_cast<struct iovec*>(::alloca(n_iov));

  auto pb = bl.buffers().begin();
  while (pb != bl.buffers().end()) {
    int ilen = 0;
    for (iov_ix = 0; (iov_ix < n_iov) && (pb != bl.buffers().end());
	 ++iov_ix, ++pb) {
      iov[iov_ix].iov_base = (void*) pb->c_str();
      iov[iov_ix].iov_len = pb->length();
      ilen += pb->length();
    }
    auto nwritten = TEMP_FAILURE_RETRY(::pwritev(fd, iov, iov_ix, offset));
    if (nwritten < 0) {
      r = -EIO;
      goto out;
    }
    offset += ilen; // XXX do short pwritevs happen?
  }

  // pretend it all worked
  if (r == 0)
    r = bl.length();

  if (r >= 0 && m_filestore_sloppy_crc) {
    int rc = backend->_crc_update_write(fo->fd, offset, len, bl);
    assert(rc >= 0);
  }

  // flush?
  if (!replaying &&
      cct->_conf->filestore_wbthrottle_enable)
    flusher.queue_wb(fo, offset, len, replica);

 out:
  dout(10) << "write " << fc->get_cid() << "/" << fo->get_oid() << " "
	   << offset << "~" << len << " = " << r << dendl;
  return r;
}

int FileStore::_zero(FSCollection* fc, FSObject* fo,
		     uint64_t offset, size_t len)
{
  dout(15) << "zero " << fc->get_cid() << "/" << fo->get_oid() << " "
	   << offset << "~" << len << dendl;
  int ret = 0;

#ifdef CEPH_HAVE_FALLOCATE
# if !defined(DARWIN) && !defined(__FreeBSD__)
  // first try to punch a hole.
  ret = fallocate(fo->fd, FALLOC_FL_PUNCH_HOLE, offset, len);
  if (ret < 0)
    ret = -errno;

  if (ret >= 0 && m_filestore_sloppy_crc) {
    int rc = backend->_crc_update_zero(fo->fd, offset, len);
    assert(rc >= 0);
  }

  if (ret == 0)
    goto out;  // yay!
  if (ret != -EOPNOTSUPP)
    goto out;  // some other error
# endif
#endif

  // lame, kernel is old and doesn't support it.
  // write zeros.. yuck!
  dout(20) << "zero FALLOC_FL_PUNCH_HOLE not supported, falling back to "
    "writing zeros" << dendl;
  {
    bufferptr bp(len);
    bp.zero();
    bufferlist bl;
    bl.push_back(bp);
    ret = _write(fc, fo, offset, len, bl);
  }

 out:
  dout(20) << "zero " << fc->get_cid() << "/" << fo->get_oid() << " "
	   << offset << "~" << len << " = " << ret << dendl;
  return ret;
}

int FileStore::_clone(FSCollection* fc,
		      FSObject* fo  /* old */,
		      FSObject* fo2 /* new */,
		      const SequencerPosition& spos)
{
  dout(15) << "clone " << fc->get_cid() << "/" << fo->get_oid()
	   << " -> " << fc->get_cid() << "/" << fo2->get_oid()
	   << dendl;

  if (_check_replay_guard(fc, fo2, spos) < 0)
    return 0;

  int r;
  {
    r = ::ftruncate(fo2->fd, 0);
    if (r < 0) {
      goto out;
    }
    struct stat st;
    ::fstat(fo->fd, &st);
    r = _do_clone_range(fo->fd, fo2->fd, 0, st.st_size, 0);
    if (r < 0) {
      r = -errno;
      goto out;
    }
    dout(20) << "objectmap clone" << dendl;
    r = object_map->clone(fo->get_oid(), fo2->get_oid(), &spos);
    if (r < 0 && r != -ENOENT)
      goto out;
  }

  {
    map<string, bufferptr> aset;
    r = _fgetattrs(fo->fd, aset, false);
    if (r < 0)
      goto out;

    r = _fsetattrs(fo2->fd, aset);
    if (r < 0)
      goto out;
  }

  // clone is non-idempotent; record our work.
  _set_replay_guard(fo2->fd, spos, &fo2->get_oid() /* !out */);

 out:
  dout(10) << "clone " << fc->get_cid() << "/" << fo->get_oid()
	   << " -> " << fc->get_cid() << "/" << fo2->get_oid()
	   << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int FileStore::_do_clone_range(int from, int to, uint64_t srcoff, uint64_t len,
			       uint64_t dstoff)
{
  dout(20) << "_do_clone_range copy " << srcoff << "~" << len << " to "
	   << dstoff << dendl;
  return backend->clone_range(from, to, srcoff, len, dstoff);
}

int FileStore::_do_copy_range(int from, int to, uint64_t srcoff, uint64_t len,
			      uint64_t dstoff)
{
  dout(20) << "_do_copy_range " << srcoff << "~" << len << " to " << dstoff
	   << dendl;
  int r = 0;
  int64_t actual;

  actual = ::lseek64(from, srcoff, SEEK_SET);
  if (actual != (int64_t)srcoff) {
    r = errno;
    derr << "lseek64 to " << srcoff << " got " << cpp_strerror(r) << dendl;
    return r;
  }
  actual = ::lseek64(to, dstoff, SEEK_SET);
  if (actual != (int64_t)dstoff) {
    r = errno;
    derr << "lseek64 to " << dstoff << " got " << cpp_strerror(r) << dendl;
    return r;
  }

  loff_t pos = srcoff;
  loff_t end = srcoff + len;
  int buflen = 4096*32;
  char buf[buflen];
  while (pos < end) {
    int l = MIN(end-pos, buflen);
    r = ::read(from, buf, l);
    dout(25) << "  read from " << pos << "~" << l << " got " << r << dendl;
    if (r < 0) {
      if (errno == EINTR) {
	continue;
      } else {
	r = -errno;
	derr << "FileStore::_do_copy_range: read error at " << pos << "~" << len
	     << ", " << cpp_strerror(r) << dendl;
	break;
      }
    }
    if (r == 0) {
      // hrm, bad source range, wtf.
      r = -ERANGE;
      derr << "FileStore::_do_copy_range got short read result at " << pos
	      << " of fd " << from << " len " << len << dendl;
      break;
    }
    int op = 0;
    while (op < r) {
      int r2 = safe_write(to, buf+op, r-op);
      dout(25) << " write to " << to << " len " << (r-op)
	       << " got " << r2 << dendl;
      if (r2 < 0) {
	r = r2;
	derr << "FileStore::_do_copy_range: write error at " << pos << "~"
	     << r-op << ", " << cpp_strerror(r) << dendl;

	break;
      }
      op += (r-op);
    }
    if (r < 0)
      break;
    pos += r;
  }
  if (r >= 0 && m_filestore_sloppy_crc) {
    int rc = backend->_crc_update_clone_range(from, to, srcoff, len, dstoff);
    assert(rc >= 0);
  }
  dout(20) << "_do_copy_range " << srcoff << "~" << len << " to " << dstoff << " = " << r << dendl;
  return r;
}

int FileStore::_clone_range(FSCollection* fc,
			    FSObject* fo  /* old */,
			    FSObject* fo2 /* new */,
			    uint64_t srcoff, uint64_t len,
			    uint64_t dstoff,
			    const SequencerPosition& spos)
{
  dout(15) << "clone_range " << fc->get_cid() << "/" << fo->get_oid()
	   << " -> " << fc->get_cid() << "/" << fo2->get_oid()
	   << " " << srcoff << "~" << len << " to " << dstoff << dendl;

  if (_check_replay_guard(fc, fo2, spos) < 0)
    return 0;

  int r = _do_clone_range(fo->fd, fo2->fd, srcoff, len, dstoff);

  // clone is non-idempotent; record our work.
  _set_replay_guard(fo2->fd, spos, &fo2->get_oid() /* !out */);

  dout(10) << "clone_range " << fc->get_cid() << "/" << fo->get_oid()
	   << " -> " << fc->get_cid() << "/" << fo2->get_oid() << " "
	   << srcoff << "~" << len << " to " << dstoff << " = "
	   << r << dendl;
  return r;
}

class SyncEntryTimeout {
public:
  SyncEntryTimeout(CephContext* cct, ceph::timespan commit_timeo)
    : cct(cct), m_commit_timeo(commit_timeo)
  {
  }

  void operator()() {
    BackTrace *bt = new BackTrace(1);
    generic_dout(-1) << "FileStore: sync_entry timed out after "
		     << m_commit_timeo << " seconds.\n";
    bt->print(*_dout);
    *_dout << dendl;
    delete bt;
    abort();
  }
private:
  CephContext* cct;
  ceph::timespan m_commit_timeo;
};

void FileStore::sync_entry()
{
  unique_lock l(lock);
  while (!stop) {
    ceph::timespan max_interval(m_filestore_max_sync_interval);
    ceph::timespan min_interval(m_filestore_min_sync_interval);

    auto startwait = ceph::mono_clock::now();
    if (!force_sync) {
      dout(20) << "sync_entry waiting for max_interval " << max_interval << dendl;
      sync_cond.wait_for(l, max_interval);
    } else {
      dout(20) << "sync_entry not waiting, force_sync set" << dendl;
    }

    if (force_sync) {
      dout(20) << "sync_entry force_sync set" << dendl;
      force_sync = false;
    } else {
      // wait for at least the min interval
      ceph::timespan woke = ceph::mono_clock::now() - startwait;
      dout(20) << "sync_entry woke after " << woke << dendl;
      if (woke < min_interval) {
	ceph::timespan t = min_interval - woke;
	dout(20) << "sync_entry waiting for another " << t
		 << " to reach min interval " << min_interval << dendl;
	sync_cond.wait_for(l, t);
      }
    }

    Context::List fin;
  again:
    fin.swap(sync_waiters);
    l.unlock();

    if (apply_manager.commit_start()) {
      auto start = ceph::mono_clock::now();
      uint64_t cp = apply_manager.get_committing_seq();

      uint64_t sync_entry_timeo =
	timer.add_event(m_filestore_commit_timeout,
			SyncEntryTimeout(cct, m_filestore_commit_timeout));

      // make flusher stop flushing previously queued stuff
      sync_epoch++;

      dout(15) << "sync_entry committing " << cp << " sync_epoch "
	       << sync_epoch << dendl;
      stringstream errstream;
      if (cct->_conf->filestore_debug_omap_check &&
	  !object_map->check(errstream)) {
	derr << errstream.str() << dendl;
	assert(0);
      }

      if (backend->can_checkpoint()) {
	int err = write_op_seq(op_fd, cp);
	if (err < 0) {
	  derr << "Error during write_op_seq: " << cpp_strerror(err) << dendl;
	  assert(0 == "error during write_op_seq");
	}

	char s[NAME_MAX];
	snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, (long long unsigned)cp);
	uint64_t cid = 0;
	err = backend->create_checkpoint(s, &cid);
	if (err < 0) {
	    int err = errno;
	    derr << "snap create '" << s << "' got error " << err << dendl;
	    assert(err == 0);
	}

	snaps.push_back(cp);
	apply_manager.commit_started();

	if (cid > 0) {
	  dout(20) << " waiting for checkpoint " << cid << " to complete"
		   << dendl;
	  err = backend->sync_checkpoint(cid);
	  if (err < 0) {
	    derr << "ioctl WAIT_SYNC got " << cpp_strerror(err) << dendl;
	    assert(0 == "wait_sync got error");
	  }
	  dout(20) << " done waiting for checkpoint" << cid << " to complete"
		   << dendl;
	}
      } else
      {
	apply_manager.commit_started();

	int err = backend->syncfs();
	if (err < 0) {
	  derr << "syncfs got " << cpp_strerror(err) << dendl;
	  assert(0 == "syncfs returned error");
	}

	err = write_op_seq(op_fd, cp);
	if (err < 0) {
	  derr << "Error during write_op_seq: " << cpp_strerror(err) << dendl;
	  assert(0 == "error during write_op_seq");
	}
	err = ::fsync(op_fd);
	if (err < 0) {
	  derr << "Error during fsync of op_seq: " << cpp_strerror(err)
	       << dendl;
	  assert(0 == "error during fsync of op_seq");
	}
      }

      auto done = ceph::mono_clock::now();
      ceph::timespan lat = done - start;
      ceph::timespan dur = done - startwait;
      dout(10) << "sync_entry commit took " << lat << ", interval was "
	       << dur << dendl;

      apply_manager.commit_finish();
      flusher.clear();

      // remove old snaps?
      if (backend->can_checkpoint()) {
	char s[NAME_MAX];
	while (snaps.size() > 2) {
	  snprintf(s, sizeof(s), COMMIT_SNAP_ITEM,
		   (long long unsigned)snaps.front());
	  snaps.pop_front();
	  dout(10) << "removing snap '" << s << "'" << dendl;
	  int r = backend->destroy_checkpoint(s);
	  if (r) {
	    int err = errno;
	    derr << "unable to destroy snap '" << s << "' got "
		 << cpp_strerror(err) << dendl;
	  }
	}
      }

      dout(15) << "sync_entry committed to op_seq " << cp << dendl;

      timer.cancel_event(sync_entry_timeo);
    }

    l.lock();
    finish_contexts(fin, 0);
    fin.clear();
    if (!sync_waiters.empty()) {
      dout(10) << "sync_entry more waiters, committing again" << dendl;
      goto again;
    }
    if (journal && journal->should_commit_now()) {
      dout(10) << "sync_entry journal says we should commit again "
	"(probably is/was full)" << dendl;
      goto again;
    }
  }
  stop = false;
  l.unlock();
}

void FileStore::_start_sync()
{
  if (!journal) {  // don't do a big sync if the journal is on
    dout(10) << "start_sync" << dendl;
    sync_cond.notify_all();
  } else {
    dout(10) << "start_sync - NOOP (journal is on)" << dendl;
  }
}

void FileStore::start_sync()
{
  lock_guard l(lock);
  force_sync = true;
  sync_cond.notify_all();
}

void FileStore::start_sync(Context* onsafe)
{
  lock_guard l(lock);
  sync_waiters.push_back(*onsafe);
  sync_cond.notify_all();
  dout(10) << "start_sync" << dendl;
}

void FileStore::sync()
{
  std::mutex lock;
  std::condition_variable cond;
  bool done;
  C_SafeCond* fin = new C_SafeCond(&lock, &cond, &done);

  start_sync(fin);

  unique_lock l(lock);
  while (!done) {
    dout(10) << "sync waiting" << dendl;
    cond.wait(l);
  }
  l.unlock();
  dout(10) << "sync done" << dendl;
}

void FileStore::_flush_op_queue()
{
  dout(10) << "_flush_op_queue waiting for apply finisher" << dendl;
  op_finisher.wait_for_empty();
}

/*
 * flush - make every queued write readable
 */
void FileStore::flush()
{
  dout(10) << "flush" << dendl;

  if (cct->_conf->filestore_blackhole) {
    // wait forever
    std::mutex lock;
    std::condition_variable cond;
    unique_lock l(lock);
    while (true)
      cond.wait(l);
    assert(0);
  }

  if (m_filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    dout(10) << "flush draining ondisk finisher" << dendl;
    ondisk_finisher.wait_for_empty();
  }

  _flush_op_queue();
  dout(10) << "flush complete" << dendl;
}

/*
 * sync_and_flush - make every queued write readable AND committed to disk
 */
void FileStore::sync_and_flush()
{
  dout(10) << "sync_and_flush" << dendl;

  if (m_filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    _flush_op_queue();
  } else {
    // includes m_filestore_journal_parallel
    _flush_op_queue();
    sync();
  }
  dout(10) << "sync_and_flush done" << dendl;
}

int FileStore::snapshot(const string& name)
{
  dout(10) << "snapshot " << name << dendl;
  sync_and_flush();

  if (!backend->can_checkpoint()) {
    dout(0) << "snapshot " << name << " failed, not supported" << dendl;
    return -EOPNOTSUPP;
  }

  char s[NAME_MAX];
  snprintf(s, sizeof(s), CLUSTER_SNAP_ITEM, name.c_str());

  int r = backend->create_checkpoint(s, NULL);
  if (r) {
    r = -errno;
    derr << "snapshot " << name << " failed: " << cpp_strerror(r) << dendl;
  }

  return r;
}

// -------------------------------
// attributes

int FileStore::_fgetattr(int fd, const char* name, bufferptr& bp)
{
  char val[100];
  int l = chain_fgetxattr(fd, name, val, sizeof(val));
  if (l >= 0) {
    bp = buffer::create(l);
    memcpy(bp.c_str(), val, l);
  } else if (l == -ERANGE) {
    l = chain_fgetxattr(fd, name, 0, 0);
    if (l > 0) {
      bp = buffer::create(l);
      l = chain_fgetxattr(fd, name, bp.c_str(), l);
    }
  }
  assert(!m_filestore_fail_eio || l != -EIO);
  return l;
}

int FileStore::_fgetattrs(int fd, map<string,bufferptr>& aset, bool user_only)
{
  // get attr list
  char names1[100];
  int len = chain_flistxattr(fd, names1, sizeof(names1)-1);
  char* names2 = 0;
  char* name = 0;
  if (len == -ERANGE) {
    len = chain_flistxattr(fd, 0, 0);
    if (len < 0) {
      assert(!m_filestore_fail_eio || len != -EIO);
      return len;
    }
    dout(10) << " -ERANGE, len is " << len << dendl;
    names2 = new char[len+1];
    len = chain_flistxattr(fd, names2, len);
    dout(10) << " -ERANGE, got " << len << dendl;
    if (len < 0) {
      assert(!m_filestore_fail_eio || len != -EIO);
      return len;
    }
    name = names2;
  } else if (len < 0) {
    assert(!m_filestore_fail_eio || len != -EIO);
    return len;
  } else {
    name = names1;
  }
  name[len] = 0;

  char* end = name + len;
  while (name < end) {
    char* attrname = name;
    if (parse_attrname(&name)) {
      char* set_name = name;
      bool can_get = true;
      if (user_only) {
	if (*set_name =='_')
	  set_name++;
	else
	  can_get = false;
      }
      if (*set_name && can_get) {
	dout(20) << "fgetattrs " << fd << " getting '" << name << "'" << dendl;
	int r = _fgetattr(fd, attrname, aset[set_name]);
	if (r < 0)
	  return r;
      }
    }
    name += strlen(name) + 1;
  }

  delete[] names2;
  return 0;
}

int FileStore::_fsetattrs(int fd, map<string, bufferptr>& aset)
{
  for (map<string, bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[CHAIN_XATTR_MAX_NAME_LEN];
    get_attrname(p->first.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);
    const char* val;
    if (p->second.length())
      val = p->second.c_str();
    else
      val = "";
    // ??? Why do we skip setting all the other attrs if one fails?
    int r = chain_fsetxattr(fd, n, val, p->second.length());
    if (r < 0) {
      derr << "FileStore::_setattrs: chain_setxattr returned " << r << dendl;
      return r;
    }
  }
  return 0;
}

// debug EIO injection
void FileStore::inject_data_error(const hoid_t& oid) {
  lock_guard l(read_error_lock);
  dout(10) << __func__ << ": init error on " << oid << dendl;
  data_error_set.insert(oid);
}
void FileStore::inject_mdata_error(const hoid_t& oid) {
  lock_guard l(read_error_lock);
  dout(10) << __func__ << ": init error on " << oid << dendl;
  mdata_error_set.insert(oid);
}
void FileStore::debug_obj_on_delete(const hoid_t& oid) {
  lock_guard l(read_error_lock);
  dout(10) << __func__ << ": clear error on " << oid << dendl;
  data_error_set.erase(oid);
  mdata_error_set.erase(oid);
}
bool FileStore::debug_data_eio(const hoid_t& oid) {
  lock_guard l(read_error_lock);
  if (data_error_set.count(oid)) {
    dout(10) << __func__ << ": inject error on " << oid << dendl;
    return true;
  } else {
    return false;
  }
}
bool FileStore::debug_mdata_eio(const hoid_t& oid) {
  lock_guard l(read_error_lock);
  if (mdata_error_set.count(oid)) {
    dout(10) << __func__ << ": inject error on " << oid << dendl;
    return true;
  } else {
    return false;
  }
}


// objects

int FileStore::getattr(CollectionHandle ch, ObjectHandle oh,
		       const char* name, bufferptr& bp)
{
  dout(15) << "getattr " << ch->get_cid() << "/" << oh->get_oid()
	   << " '" << name << "'" << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  FSObject* fo = static_cast<FSObject*>(oh);

  char n[CHAIN_XATTR_MAX_NAME_LEN];
  get_attrname(name, n, CHAIN_XATTR_MAX_NAME_LEN);
  int r = _fgetattr(fo->fd, n, bp);
  if (r == -ENODATA) {
    map<string, bufferlist> got;
    set<string> to_get;
    to_get.insert(string(name));
    r = object_map->get_xattrs(oh->get_oid(), to_get, &got);
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
    r = bp.length();
  }
 out:
  dout(10) << "getattr " << fc->get_cid() << "/" << oh->get_oid() << " '"
	   << name << "' = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  if (cct->_conf->filestore_debug_inject_read_err &&
      debug_mdata_eio(oh->get_oid())) {
    return -EIO;
  } else {
    return r;
  }
}

int FileStore::getattrs(CollectionHandle ch, ObjectHandle oh,
			map<string,bufferptr>& aset, bool user_only)
{
  set<string> omap_attrs;
  map<string, bufferlist> omap_aset;

  FSObject* fo = static_cast<FSObject*>(oh);
  
  dout(15) << "getattrs " << ch->get_cid() << "/" << oh->get_oid()
	   << dendl;

  bool spill_out = true;
  char buf[2];

  int r = chain_fgetxattr(fo->fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r >= 0 && !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT)))
    spill_out = false;

  r = _fgetattrs(fo->fd, aset, user_only);
  if (r < 0) {
    goto out;
  }

  if (!spill_out) {
    dout(10) << __func__ << " no xattr exists in object_map r = " << r << dendl;
    goto out;
  }

  r = object_map->get_all_xattrs(oh->get_oid(), &omap_attrs);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
    goto out;
  }

  r = object_map->get_xattrs(oh->get_oid(), omap_attrs, &omap_aset);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
    goto out;
  }
  if (r == -ENOENT)
    r = 0;
  assert(omap_attrs.size() == omap_aset.size());
  for (map<string, bufferlist>::iterator i = omap_aset.begin();
	 i != omap_aset.end();
	 ++i) {
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
    aset.insert(
      make_pair(key, bufferptr(i->second.c_str(), i->second.length())));
  }
 out:
  dout(10) << "getattrs " << ch->get_cid() << "/" << oh->get_oid() << " = "
	   << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);

  if (cct->_conf->filestore_debug_inject_read_err &&
      debug_mdata_eio(oh->get_oid())) {
    return -EIO;
  } else {
    return r;
  }
}

int FileStore::_setattrs(FSCollection* fc, FSObject* fo,
			 map<string,bufferptr>& aset,
			 const SequencerPosition& spos)
{
  map<string, bufferlist> omap_set;
  set<string> omap_remove;
  map<string, bufferptr> inline_set;
  map<string, bufferptr> inline_to_set;
  int spill_out = -1;
  char buf[2];
  
  int r = chain_fgetxattr(fo->fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if ((r >= 0) &&
      !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT)))
    spill_out = 0;
  else
    spill_out = 1;

  r = _fgetattrs(fo->fd, inline_set, false);
  assert(!m_filestore_fail_eio || r != -EIO);
  dout(15) << "setattrs " << fc->get_cid() << "/" << fo->get_oid()
	   << dendl;
  r = 0;

  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[CHAIN_XATTR_MAX_NAME_LEN];
    get_attrname(p->first.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);

    if (p->second.length() > m_filestore_max_inline_xattr_size) {
	if (inline_set.count(p->first)) {
	  inline_set.erase(p->first);
	  r = chain_fremovexattr(fo->fd, n);
	  if (r < 0)
	    goto out;
	}
	omap_set[p->first].push_back(p->second);
	continue;
    }

    if (!inline_set.count(p->first) &&
	(inline_set.size() >= m_filestore_max_inline_xattrs)) {
	if (inline_set.count(p->first)) {
	  inline_set.erase(p->first);
	  r = chain_fremovexattr(fo->fd, n);
	  if (r < 0)
	    goto out;
	}
	omap_set[p->first].push_back(p->second);
	continue;
    }
    omap_remove.insert(p->first);
    inline_set.insert(*p);

    inline_to_set.insert(*p);
  }

  if ((spill_out != 1) && !omap_set.empty()) {
    chain_fsetxattr(fo->fd, XATTR_SPILL_OUT_NAME, XATTR_SPILL_OUT,
		    sizeof(XATTR_SPILL_OUT));
  }

  r = _fsetattrs(fo->fd, inline_to_set);
  if (r < 0)
    goto out;

  if (spill_out && !omap_remove.empty()) {
    r = object_map->remove_xattrs(fo->get_oid(), omap_remove, &spos);
    if ((r < 0) && (r != -ENOENT)) {
      dout(10) << __func__ << " could not remove_xattrs r = " << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out;
    } else {
      r = 0; // don't confuse the debug output
    }
  }

  if (!omap_set.empty()) {
    r = object_map->set_xattrs(fo->get_oid(), omap_set, &spos);
    if (r < 0) {
      dout(10) << __func__ << " could not set_xattrs r = " << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out;
    }
  }

 out:
  dout(10) << "setattrs " << fc->get_cid() << "/" << fo->get_oid() << " = "
	   << r << dendl;
  return r;
}

int FileStore::_rmattr(FSCollection* fc, FSObject* fo,
		       const char* name, const SequencerPosition& spos)
{
  dout(15) << "rmattr " << fc->get_cid() << "/" << fo->get_oid()
	   << " '" << name << "'" << dendl;

  bool spill_out = true;
  bufferptr bp;
  char buf[2];
  
  int r = chain_fgetxattr(fo->fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if ((r >= 0) &&
      !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
    spill_out = false;
  }

  char n[CHAIN_XATTR_MAX_NAME_LEN];
  get_attrname(name, n, CHAIN_XATTR_MAX_NAME_LEN);
  r = chain_fremovexattr(fo->fd, n);
  if (r == -ENODATA && spill_out) {
    set<string> to_remove;
    to_remove.insert(string(name));
    r = object_map->remove_xattrs(fo->get_oid(), to_remove, &spos);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not remove_xattrs index r = "
	       << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out;
    }
  }

out:
  dout(10) << "rmattr " << fc->get_cid() << "/" << fo->get_oid()
	   << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::_rmattrs(FSCollection* fc, FSObject* fo,
			const SequencerPosition& spos)
{
  dout(15) << "rmattrs " << fc->get_cid() << "/" << fo->get_oid()
	   << dendl;

  map<string,bufferptr> aset;
  set<string> omap_attrs;
  bool spill_out = true;
  char buf[2];

  int r = chain_fgetxattr(fo->fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if ((r >= 0) &&
      !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
    spill_out = false;
  }

  r = _fgetattrs(fo->fd, aset, false);
  if (r >= 0) {
    for (map<string,bufferptr>::iterator p = aset.begin(); p != aset.end();
	 ++p) {
      char n[CHAIN_XATTR_MAX_NAME_LEN];
      get_attrname(p->first.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);
      r = chain_fremovexattr(fo->fd, n);
      if (r < 0)
	break;
    }
  }

  if (!spill_out) {
    dout(10) << __func__ << " no xattr exists in object_map r = " << r << dendl;
    goto out;
  }

  r = object_map->get_all_xattrs(fo->get_oid(), &omap_attrs);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    goto out;
  }
  r = object_map->remove_xattrs(fo->get_oid(), omap_attrs, &spos);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not remove omap_attrs r = " << r << dendl;
    goto out;
  }
  if (r == -ENOENT)
    r = 0;

  chain_fsetxattr(fo->fd, XATTR_SPILL_OUT_NAME, XATTR_NO_SPILL_OUT,
		  sizeof(XATTR_NO_SPILL_OUT));

 out:
  dout(10) << "rmattrs " << fc->get_cid() << "/" << fo->get_oid()
	   << " = " << r << dendl;
  return r;
}


// collections

int FileStore::collection_getattr(CollectionHandle ch, const char* name,
				  void* value, size_t size)
{
  /* XXXX dirfd?-- and this could apply to replay_guard, &c */
  char fn[PATH_MAX];
  get_cdir(ch->get_cid(), fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "' len " << size
	   << dendl;
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  r = chain_fgetxattr(fd, n, value, size);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int FileStore::collection_getattr(CollectionHandle ch, const char* name,
				  bufferlist& bl)
{
  char fn[PATH_MAX];
  get_cdir(ch->get_cid(), fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "'" << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  buffer::ptr bp;
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  r = _fgetattr(fd, n, bp);
  bl.push_back(bp);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_getattr " << fn << " '" << name << "' = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int FileStore::collection_getattrs(CollectionHandle ch,
				   map<string,bufferptr>& aset)
{
  /* XXX dirfd? */
  char fn[PATH_MAX];
  get_cdir(ch->get_cid(), fn, sizeof(fn));
  dout(10) << "collection_getattrs " << fn << dendl;
  int r = 0;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  r = _fgetattrs(fd, aset, true);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_getattrs " << fn << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int FileStore::_collection_setattr(FSCollection* fc,
				   const char* name, const void* value,
				   size_t size)
{
  /* XXX dirdf? */
  char fn[PATH_MAX];
  get_cdir(fc->get_cid(), fn, sizeof(fn));
  dout(10) << "collection_setattr " << fn << " '" << name << "' len "
	   << size << dendl;
  char n[PATH_MAX];
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  get_attrname(name, n, PATH_MAX);
  r = chain_fsetxattr(fd, n, value, size);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_setattr " << fn << " '" << name << "' len "
	   << size << " = " << r << dendl;
  return r;
}

int FileStore::_collection_rmattr(FSCollection* fc,
				  const char* name)
{
  /* dirfd? */
  char fn[PATH_MAX];
  get_cdir(fc->get_cid(), fn, sizeof(fn));
  dout(15) << "collection_rmattr " << fn << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  r = chain_fremovexattr(fd, n);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_rmattr " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_collection_setattrs(FSCollection* fc,
				    map<string,bufferptr>& aset)
{
  /* dirfd? */
  char fn[PATH_MAX];
  get_cdir(fc->get_cid(), fn, sizeof(fn));
  dout(15) << "collection_setattrs " << fn << dendl;
  int r = 0;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[PATH_MAX];
    get_attrname(p->first.c_str(), n, PATH_MAX);
    r = chain_fsetxattr(fd, n, p->second.c_str(), p->second.length());
    if (r < 0)
      break;
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_setattrs " << fn << " = " << r << dendl;
  return r;
}

// --------------------------
// collections

int FileStore::collection_version_current(CollectionHandle ch,
					  uint32_t* version)
{
  *version = 0; /* XXX this was the FORMAT of Index */
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int FileStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << "list_collections" << dendl;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/current", basedir.c_str());

  int r = 0;
  DIR* dir = ::opendir(fn);
  if (!dir) {
    r = -errno;
    derr << "tried opening directory " << fn << ": " << cpp_strerror(-r)
	 << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }

  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
  struct dirent* de;
  while ((r = ::readdir_r(dir, (struct dirent* )&buf, &de)) == 0) {
    if (!de)
      break;
    if (de->d_type == DT_UNKNOWN) {
      // d_type not supported (non-ext[234], btrfs), must stat
      struct stat sb;
      char filename[PATH_MAX];
      snprintf(filename, sizeof(filename), "%s/%s", fn, de->d_name);

      r = ::stat(filename, &sb);
      if (r < 0) {
	r = -errno;
	derr << "stat on " << filename << ": " << cpp_strerror(-r) << dendl;
	assert(!m_filestore_fail_eio || r != -EIO);
	break;
      }
      if (!S_ISDIR(sb.st_mode)) {
	continue;
      }
    } else if (de->d_type != DT_DIR) {
      continue;
    }
    if (strcmp(de->d_name, "omap") == 0) {
      continue;
    }
    if (de->d_name[0] == '.' &&
	(de->d_name[1] == '\0' ||
	 (de->d_name[1] == '.' &&
	  de->d_name[2] == '\0')))
      continue;
    ls.push_back(coll_t(de->d_name));
  }

  if (r > 0) {
    derr << "trying readdir_r " << fn << ": " << cpp_strerror(r) << dendl;
    r = -r;
  }

  ::closedir(dir);
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

CollectionHandle FileStore::open_collection(const coll_t& cid)
{
  string cpath = basedir.c_str();
  cpath += "/current/";
  cpath += cid.to_str();

  FSCollection *fc = new FSCollection(this, cid);
  int r = fc->index.mount(cpath);
  if (r == 0)
    return fc;

  derr << "FileStore::open_collection(" << cid << ") failed to mount "
       << cpath << ": " << cpp_strerror(-r) << dendl;
  delete fc;
  return NULL;
} /* open_collection */

int FileStore::close_collection(CollectionHandle ch)
{
  FSCollection* fc = static_cast<FSCollection*>(ch);
  fc->flags |= ceph::os::Collection::FLAG_CLOSED;

  class ObjUnref
  {
    FileStore* fs;
  public:
    ObjUnref(FileStore *_fs) : fs(_fs) {}

    void operator()(ceph::os::Object* o) const {
      fs->obj_lru.unref(o, cohort::lru::FLAG_NONE);
    }
  };

  /* force cache drain, forces objects to evict */
  fc->obj_cache.drain(ObjUnref(this),
		      ceph::os::Object::ObjCache::FLAG_LOCK);

  /* return initial ref */
  fc->put();

  return 0;
} /* close_collection */

int FileStore::collection_stat(const coll_t& c, struct stat* st)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_stat " << fn << dendl;
  int r = ::stat(fn, st);
  if (r < 0)
    r = -errno;
  dout(10) << "collection_stat " << fn << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

bool FileStore::collection_exists(const coll_t& c)
{
  struct stat st;
  return collection_stat(c, &st) == 0;
}

bool FileStore::collection_empty(CollectionHandle ch)
{
  dout(15) << "collection_empty " << ch->get_cid() << dendl;

  struct stat st;
  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = ::fstat(fc->index.get_rootfd(), &st);
  if (r < 0)
    return 0;
  return ((st.st_nlink - 2) > 0);
}

int FileStore::collection_list_range(CollectionHandle ch,
				     hoid_t start, hoid_t end,
				     vector<hoid_t>* ls)
{
  dout(10) << "collection_list_range: " << ch->get_cid() << dendl;
  
  // TODO: implement
  ls->clear();
  
  return 0;
}

int FileStore::collection_list_partial(CollectionHandle ch,
				       hoid_t start, int min, int max,
				       vector<hoid_t>* ls, hoid_t* next)
{
  dout(10) << "collection_list_partial: " << ch->get_cid() << dendl;

  // TODO: implement
  ls->clear();

  return 0;
}

int FileStore::collection_list_partial2(CollectionHandle ch,
					int min,
					int max,
					vector<hoid_t>* vs,
					CLPCursor& cursor)
{
  // TODO:  implement
  return 0;
}

int FileStore::collection_list(CollectionHandle ch, vector<hoid_t>& ls)
{
  dout(10) << "collection_list: " << ch->get_cid() << dendl;

  // TODO: implement
  ls.clear();

  return 0;
}

int FileStore::omap_get(CollectionHandle ch, ObjectHandle oh,
			bufferlist* header,
			map<string, bufferlist>* out)
{
  const hoid_t &oid = oh->get_oid();
  dout(15) << __func__ << " " << ch->get_cid() << "/" << oid << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->get(oid, header, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int FileStore::omap_get_header(
  CollectionHandle ch,
  ObjectHandle oh,
  bufferlist* bl,
  bool allow_eio)
{
  const hoid_t &oid = oh->get_oid();
  dout(15) << __func__ << " " << ch->get_cid() << "/" << oid << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->get_header(oid, bl);
  if (r < 0 && r != -ENOENT) {
    assert(allow_eio || !m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int FileStore::omap_get_keys(CollectionHandle ch, ObjectHandle oh,
			     set<string>* keys)
{
  const hoid_t &oid = oh->get_oid();
  dout(15) << __func__ << " " << ch->get_cid() << "/" << oid << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->get_keys(oid, keys);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int FileStore::omap_get_values(
  CollectionHandle ch,
  ObjectHandle oh,
  const set<string>& keys,
  map<string, bufferlist>* out)
{
  const hoid_t &oid = oh->get_oid();
  dout(15) << __func__ << " " << ch->get_cid() << "/" << oid << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->get_values(oid, keys, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int FileStore::omap_check_keys(
  CollectionHandle ch,
  ObjectHandle oh,
  const set<string>& keys,
  set<string>* out)
{
  const hoid_t &oid = oh->get_oid();
  dout(15) << __func__ << " " << ch->get_cid() << "/" << oid << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->check_keys(oid, keys, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

ObjectMap::ObjectMapIterator
FileStore::get_omap_iterator(
  CollectionHandle ch,
  ObjectHandle oh)
{
  const hoid_t &oid = oh->get_oid();
  dout(15) << __func__ << " " << ch->get_cid() << "/" << oid << dendl;

  FSCollection* fc = static_cast<FSCollection*>(ch);
  int r = fc->index.lookup(oid);
  if (r < 0)
    return ObjectMap::ObjectMapIterator();
  return object_map->get_iterator(oid);
}

int FileStore::_create_collection(
  const coll_t& c,
  const SequencerPosition& spos)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "create_collection " << fn << dendl;
  int r = ::mkdir(fn, 0755);
  if (r < 0)
    r = -errno;
  if (r == -EEXIST && replaying)
    r = 0;
  dout(10) << "create_collection " << fn << " = " << r << dendl;
  if (r < 0)
    return r;

  // initialize an index at the given directory
  cohort::FragTreeIndex index(cct, cct->_conf->fragtreeindex_initial_split);
  r = index.init(fn); // XXX: mount() instead if replaying=true?
  if (r < 0) {
    derr << "FileStore::_create_collection(" << c << ") failed: "
	 << cpp_strerror(-r) << dendl;
    return r;
  } 
  // XXX: _set_replay_guard(dirfd, spos);
  return 0;
}

// DEPRECATED -- remove with _split_collection_create
int FileStore::_create_collection(const coll_t& c)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "create_collection " << fn << dendl;
  int r = ::mkdir(fn, 0755);
  if (r < 0)
    r = -errno;
  dout(10) << "create_collection " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_destroy_collection(FSCollection* fc)
{
  char fn[PATH_MAX];
  get_cdir(fc->get_cid(), fn, sizeof(fn));
  int r = fc->index.destroy(fn);
  dout(10) << "_destroy_collection " << fn << " = " << r << dendl;
  return r;
}

void FileStore::_inject_failure()
{
  if (m_filestore_kill_at) {
    int final = --m_filestore_kill_at;
    dout(5) << "_inject_failure " << (final+1) << " -> " << final << dendl;
    if (final == 0) {
      derr << "_inject_failure KILLING" << dendl;
      cct->_log->flush();
      _exit(1);
    }
  }
}

int FileStore::_omap_clear(
  FSCollection* fc,
  FSObject* fo,
  const SequencerPosition& spos) {
  const hoid_t &oid = fo->get_oid();
  dout(15) << __func__ << " " << fc->get_cid() << "/" << oid << dendl;

  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->clear_keys_header(oid, &spos);
  if (r < 0 && r != -ENOENT)
    return r;
  return 0;
}

int FileStore::_omap_setkeys(
  FSCollection* fc,
  FSObject* fo,
  const map<string, bufferlist>& aset,
  const SequencerPosition& spos) {
  const hoid_t &oid = fo->get_oid();
  dout(15) << __func__ << " " << fc->get_cid() << "/" << oid << dendl;

  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  return object_map->set_keys(oid, aset, &spos);
}

int FileStore::_omap_rmkeys(
  FSCollection* fc,
  FSObject* fo,
  const set<string>& keys,
  const SequencerPosition& spos) {
  const hoid_t &oid = fo->get_oid();
  dout(15) << __func__ << " " << fc->get_cid() << "/" << oid << dendl;

  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  r = object_map->rm_keys(oid, keys, &spos);
  if (r < 0 && r != -ENOENT)
    return r;
  return 0;
}

int FileStore::_omap_rmkeyrange(
  FSCollection* fc,
  FSObject* fo,
  const string& first, const string& last,
  const SequencerPosition& spos) {

  dout(15) << __func__ << " " << fc->get_cid() << "/" << fo->get_oid()
	   << " [" << first << "," << last << "]" << dendl;

  set<string> keys;
  {
    /* XXX check get_omap_iterator signature */
    ObjectMap::ObjectMapIterator iter =
      get_omap_iterator(fc, fo);
    if (!iter)
      return -ENOENT;
    for (iter->lower_bound(first); iter->valid() && iter->key() < last;
	 iter->next()) {
      keys.insert(iter->key());
    }
  }
  return _omap_rmkeys(fc, fo, keys, spos);
}

int FileStore::_omap_setheader(FSCollection* fc,
			       FSObject* fo,
			       const bufferlist& bl,
			       const SequencerPosition& spos)
{
  const hoid_t &oid = fo->get_oid();
  dout(15) << __func__ << " " << fc->get_cid() << "/" << oid << dendl;

  int r = fc->index.lookup(oid);
  if (r < 0)
    return r;
  return object_map->set_header(oid, bl, &spos);
}

int FileStore::_set_alloc_hint(
  FSCollection* fc,
  FSObject* fo,
  uint64_t expected_object_size,
  uint64_t expected_write_size)
{

  dout(15) << "set_alloc_hint " << fc->get_cid() << "/" << fo->get_oid()
	   << " object_size " << expected_object_size << " write_size "
	   << expected_write_size << dendl;

  // TODO: a more elaborate hint calculation
  uint64_t hint = MIN(expected_write_size, m_filestore_max_alloc_hint_size);
  int r = backend->set_alloc_hint(fo->fd, hint);

  dout(10) << "set_alloc_hint " << fc->get_cid() << "/" << fo->get_oid()
	   << " object_size " << expected_object_size << " write_size "
	   << expected_write_size << " = " << r << dendl;

  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

const char** FileStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "filestore_min_sync_interval",
    "filestore_max_sync_interval",
    "filestore_queue_max_ops",
    "filestore_queue_max_bytes",
    "filestore_queue_committing_max_ops",
    "filestore_queue_committing_max_bytes",
    "filestore_commit_timeout",
    "filestore_dump_file",
    "filestore_kill_at",
    "filestore_fail_eio",
    "filestore_replica_fadvise",
    "filestore_sloppy_crc",
    "filestore_sloppy_crc_block_size",
    "filestore_max_alloc_hint_size",
    NULL
  };
  return KEYS;
}

void FileStore::handle_conf_change(const struct md_config_t* conf,
			  const std::set <std::string>& changed)
{
  if (changed.count("filestore_max_inline_xattr_size") ||
      changed.count("filestore_max_inline_xattr_size_xfs") ||
      changed.count("filestore_max_inline_xattr_size_btrfs") ||
      changed.count("filestore_max_inline_xattr_size_other") ||
      changed.count("filestore_max_inline_xattrs") ||
      changed.count("filestore_max_inline_xattrs_xfs") ||
      changed.count("filestore_max_inline_xattrs_btrfs") ||
      changed.count("filestore_max_inline_xattrs_other")) {
    lock_guard l(lock);
    set_xattr_limits_via_conf();
  }
  if (changed.count("filestore_min_sync_interval") ||
      changed.count("filestore_max_sync_interval") ||
      changed.count("filestore_queue_max_ops") ||
      changed.count("filestore_queue_max_bytes") ||
      changed.count("filestore_queue_committing_max_ops") ||
      changed.count("filestore_queue_committing_max_bytes") ||
      changed.count("filestore_kill_at") ||
      changed.count("filestore_fail_eio") ||
      changed.count("filestore_sloppy_crc") ||
      changed.count("filestore_sloppy_crc_block_size") ||
      changed.count("filestore_max_alloc_hint_size") ||
      changed.count("filestore_replica_fadvise")) {
    lock_guard l(lock);
    m_filestore_min_sync_interval
      = conf->filestore_min_sync_interval;
    m_filestore_max_sync_interval
      = conf->filestore_max_sync_interval;
    m_filestore_queue_max_ops = conf->filestore_queue_max_ops;
    m_filestore_queue_max_bytes = conf->filestore_queue_max_bytes;
    m_filestore_queue_committing_max_ops
      = conf->filestore_queue_committing_max_ops;
    m_filestore_queue_committing_max_bytes
      = conf->filestore_queue_committing_max_bytes;
    m_filestore_kill_at = conf->filestore_kill_at;
    m_filestore_fail_eio = conf->filestore_fail_eio;
    m_filestore_replica_fadvise = conf->filestore_replica_fadvise;
    m_filestore_sloppy_crc = conf->filestore_sloppy_crc;
    m_filestore_sloppy_crc_block_size = conf->filestore_sloppy_crc_block_size;
    m_filestore_max_alloc_hint_size = conf->filestore_max_alloc_hint_size;
  }
  if (changed.count("filestore_commit_timeout")) {
    m_filestore_commit_timeout = conf->filestore_commit_timeout;
  }
  if (changed.count("filestore_dump_file")) {
    if (conf->filestore_dump_file.length() &&
	conf->filestore_dump_file != "-") {
      dump_start(conf->filestore_dump_file);
    } else {
      dump_stop();
    }
  }
}

void FileStore::dump_start(const std::string& file)
{
  dout(10) << "dump_start " << file << dendl;
  if (m_filestore_do_dump) {
    dump_stop();
  }
  m_filestore_dump_fmt.reset();
  m_filestore_dump_fmt.open_array_section("dump");
  m_filestore_dump.open(file.c_str());
  m_filestore_do_dump = true;
}

void FileStore::dump_stop()
{
  dout(10) << "dump_stop" << dendl;
  m_filestore_do_dump = false;
  if (m_filestore_dump.is_open()) {
    m_filestore_dump_fmt.close_section();
    m_filestore_dump_fmt.flush(m_filestore_dump);
    m_filestore_dump.flush();
    m_filestore_dump.close();
  }
}

void FileStore::dump_transactions(list<Transaction*>& ls,
				  uint64_t seq)
{
  m_filestore_dump_fmt.open_array_section("transactions");
  unsigned trans_num = 0;
  for (list<Transaction*>::iterator i = ls.begin(); i != ls.end(); ++i, ++trans_num) {
    m_filestore_dump_fmt.open_object_section("transaction");
    m_filestore_dump_fmt.dump_unsigned("seq", seq);
    m_filestore_dump_fmt.dump_unsigned("trans_num", trans_num);
    (*i)->dump(&m_filestore_dump_fmt);
    m_filestore_dump_fmt.close_section();
  }
  m_filestore_dump_fmt.close_section();
  m_filestore_dump_fmt.flush(m_filestore_dump);
  m_filestore_dump.flush();
}

void FileStore::set_xattr_limits_via_conf()
{
  uint32_t fs_xattr_size;
  uint32_t fs_xattrs;

  assert(m_fs_type != fs_types::FS_TYPE_NONE);

  switch(m_fs_type) {
    case fs_types::FS_TYPE_XFS:
      fs_xattr_size = cct->_conf->filestore_max_inline_xattr_size_xfs;
      fs_xattrs = cct->_conf->filestore_max_inline_xattrs_xfs;
      break;
    case fs_types::FS_TYPE_BTRFS:
      fs_xattr_size = cct->_conf->filestore_max_inline_xattr_size_btrfs;
      fs_xattrs = cct->_conf->filestore_max_inline_xattrs_btrfs;
      break;
    case fs_types::FS_TYPE_ZFS:
    case fs_types::FS_TYPE_OTHER:
      fs_xattr_size = cct->_conf->filestore_max_inline_xattr_size_other;
      fs_xattrs = cct->_conf->filestore_max_inline_xattrs_other;
      break;
    default:
      assert(!"Unknown fs type");
  }

  //Use override value if set
  if (cct->_conf->filestore_max_inline_xattr_size)
    m_filestore_max_inline_xattr_size = cct->_conf->filestore_max_inline_xattr_size;
  else
    m_filestore_max_inline_xattr_size = fs_xattr_size;

  //Use override value if set
  if (cct->_conf->filestore_max_inline_xattrs)
    m_filestore_max_inline_xattrs = cct->_conf->filestore_max_inline_xattrs;
  else
    m_filestore_max_inline_xattrs = fs_xattrs;
}

// -- FSSuperblock --

void FSSuperblock::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  compat_features.encode(bl);
  ENCODE_FINISH(bl);
}

void FSSuperblock::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  compat_features.decode(bl);
  DECODE_FINISH(bl);
}

void FSSuperblock::dump(Formatter* f) const
{
  f->open_object_section("compat");
  compat_features.dump(f);
  f->close_section();
}

void FSSuperblock::generate_test_instances(list<FSSuperblock*>& o)
{
  FSSuperblock z;
  o.push_back(new FSSuperblock(z));
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  z.compat_features = CompatSet(feature_compat, feature_ro_compat,
				feature_incompat);
  o.push_back(new FSSuperblock(z));
}

const char** FileStore::FSFlush::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "filestore_wbthrottle_btrfs_bytes_start_flusher",
    "filestore_wbthrottle_btrfs_bytes_hard_limit",
    "filestore_wbthrottle_btrfs_ios_start_flusher",
    "filestore_wbthrottle_btrfs_ios_hard_limit",
    "filestore_wbthrottle_btrfs_inodes_start_flusher",
    "filestore_wbthrottle_btrfs_inodes_hard_limit",
    "filestore_wbthrottle_xfs_bytes_start_flusher",
    "filestore_wbthrottle_xfs_bytes_hard_limit",
    "filestore_wbthrottle_xfs_ios_start_flusher",
    "filestore_wbthrottle_xfs_ios_hard_limit",
    "filestore_wbthrottle_xfs_inodes_start_flusher",
    "filestore_wbthrottle_xfs_inodes_hard_limit",
    NULL
  };
  return KEYS;
}

void FileStore::FSFlush::handle_conf_change(const md_config_t *conf,
					    const std::set<std::string> &keys)

{
  unique_sp waitq_sp(waitq.lock);
  switch(fs->m_fs_type) {
  case fs_types::FS_TYPE_BTRFS:
    size_limits.first = conf->filestore_wbthrottle_btrfs_bytes_start_flusher;
    size_limits.second = conf->filestore_wbthrottle_btrfs_bytes_hard_limit;
    io_limits.first = conf->filestore_wbthrottle_btrfs_ios_start_flusher;
    io_limits.second = conf->filestore_wbthrottle_btrfs_ios_hard_limit;
    fd_limits.first = conf->filestore_wbthrottle_btrfs_inodes_start_flusher;
    fd_limits.second = conf->filestore_wbthrottle_btrfs_inodes_hard_limit;
    break;
  case fs_types::FS_TYPE_XFS:
    size_limits.first = conf->filestore_wbthrottle_xfs_bytes_start_flusher;
    size_limits.second = conf->filestore_wbthrottle_xfs_bytes_hard_limit;
    io_limits.first = conf->filestore_wbthrottle_xfs_ios_start_flusher;
    io_limits.second = conf->filestore_wbthrottle_xfs_ios_hard_limit;
    fd_limits.first = conf->filestore_wbthrottle_xfs_inodes_start_flusher;
    fd_limits.second = conf->filestore_wbthrottle_xfs_inodes_hard_limit;
    break;
  default:
    break;
  }
}

void FileStore::FSFlush::queue_wb(FSObject* o,
				  uint64_t off,
				  uint64_t len,
				  bool nocache )
{
  FSObject::PendingWB pwb(o, len, 1, nocache);
  unique_sp waitq_sp(waitq.lock);
  if (should_block(len)) {
    cohort::WaitQueue<FSObject::PendingWB, cohort::SpinLock>::Entry wqe(pwb);
    waitq_sp.unlock();
    waitq.wait_on(wqe);
  } else
    queue_finish(pwb);
} /* queue_wb */

void FileStore::FSFlush::queue_finish(FSObject::PendingWB& pwb) {
  FSObject* o = pwb.o;
  /* o can be null (thread entry waiting on itself) */
  if (o) {
    if (! o->pwb.ios) {
      fs->ref(o); /* ref+ */
      fl_queue.push_front(*o); /* enqueue at fl_queue MRU */
    }
    cur_size += pwb.size;
    cur_ios += pwb.ios;
    o->pwb.add(pwb);
  }
} /* queue_finish */

void FileStore::FSFlush::cond_signal_waiters(unique_sp& waitq_sp)
{
  assert(waitq_sp.owns_lock());
  auto i = waitq.queue.begin();
  while (i != waitq.queue.end()) {
    auto &wqe = *i++;
    FSObject::PendingWB& pwb = wqe.get();
    if (should_wake(pwb)) {
      queue_finish(pwb);
      waitq.dequeue(wqe, WaitQueue::FLAG_LOCKED | WaitQueue::FLAG_SIGNAL);
    }
  }
} /* cond_signal_waiters */

void FileStore::FSFlush::clear_object(FSObject* o)
{
  unique_sp waitq_sp(waitq.lock);
  if (o->pwb.ios) {
    FSObject::FlushQueue::iterator it =
      FSObject::FlushQueue::s_iterator_to(*o);
    fl_queue.erase(it);
    (void) o->pwb.sub(cur_ios, cur_size);
    fs->unref(o);
  }
  cond_signal_waiters(waitq_sp);
} /* clear_object */

void FileStore::FSFlush::clear() {
  unique_sp waitq_sp(waitq.lock);
  while (fl_queue.size()) {
    FSObject& o = fl_queue.back();
    fl_queue.pop_back();
    (void) o.pwb.sub(cur_ios, cur_size);
    fs->unref(&o);
  }
  cond_signal_waiters(waitq_sp);
} /* clear */

void* FileStore::FSFlush::entry()
{
  unique_sp waitq_sp(waitq.lock);
  FSObject::PendingWB pwb(nullptr, 0, 0, false); /* thread fake pwb */

  while (!stopping) {
    while (!stopping &&
	   cur_ios < io_limits.first &&
	   fl_queue.size() < fd_limits.first &&
	   cur_size < size_limits.first) {
      /* queue ourselves on fake wb */
      cohort::WaitQueue<FSObject::PendingWB, cohort::SpinLock>::Entry wqe(pwb);
      waitq_sp.unlock();
      waitq.wait_on(wqe, 100 /* ms */);
      waitq_sp.lock();
      if (wqe.q_hook.is_linked())
        waitq.dequeue(wqe, WaitQueue::FLAG_LOCKED);
    }
    if (stopping)
      return nullptr;
    /* do it */
    assert(!fl_queue.empty());
    FSObject& o = fl_queue.back();
    fl_queue.pop_back();
    bool nocache = o.pwb.sub(cur_ios, cur_size);
    waitq_sp.unlock();
#ifdef HAVE_FDATASYNC
    ::fdatasync(o.fd);
#else
    ::fsync(o.fd);
#endif
#ifdef HAVE_POSIX_FADVISE
    if (nocache) {
      int fa_r = posix_fadvise(o.fd, 0, 0, POSIX_FADV_DONTNEED);
      assert(fa_r == 0);
    }
#endif
    waitq_sp.lock();
    fs->unref(&o);
    cond_signal_waiters(waitq_sp);
  } /* (!stopping) */
  /* Thread exit */
  return nullptr;
} /* entry */

