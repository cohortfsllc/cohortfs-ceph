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


#ifndef CEPH_FILESTORE_H
#define CEPH_FILESTORE_H

#include <atomic>
#include <cassert>
#include <map>
#include <deque>
#include <boost/scoped_ptr.hpp>
#include <fstream>
#include <unordered_map>
#include "include/types.h"
#include "CollectionIndex.h"

#include "ObjectStore.h"
#include "JournalingObjectStore.h"

#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/Mutex.h"
#include "common/zipkin_trace.h"

#include "IndexManager.h"
#include "ObjectMap.h"
#include "SequencerPosition.h"
#include "FDCache.h"
#include "WBThrottle.h"


// from include/linux/falloc.h:
#ifndef FALLOC_FL_PUNCH_HOLE
# define FALLOC_FL_PUNCH_HOLE 0x2
#endif

#if defined(__linux__)
# ifndef BTRFS_SUPER_MAGIC
static const __SWORD_TYPE BTRFS_SUPER_MAGIC(0x9123683E);
# endif
# ifndef XFS_SUPER_MAGIC
static const __SWORD_TYPE XFS_SUPER_MAGIC(0x58465342);
# endif
#endif

#ifndef ZFS_SUPER_MAGIC
static const __SWORD_TYPE ZFS_SUPER_MAGIC(0x2fc12fc1);
#endif

enum fs_types {
  FS_TYPE_NONE = 0,
  FS_TYPE_XFS,
  FS_TYPE_BTRFS,
  FS_TYPE_ZFS,
  FS_TYPE_OTHER
};

class FileStoreBackend;

class FSSuperblock {
public:
  CompatSet compat_features;

  FSSuperblock() { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<FSSuperblock*>& o);
};
WRITE_CLASS_ENCODER(FSSuperblock)

inline ostream& operator<<(ostream& out, const FSSuperblock& sb)
{
  return out << "sb(" << sb.compat_features << ")";
}

class FileStore : public JournalingObjectStore,
		  public md_config_obs_t
{
  static const uint32_t target_version;
public:
  uint32_t get_target_version() {
    return target_version;
  }

  int peek_journal_fsid(boost::uuids::uuid *fsid);

  class FSCollection : public ObjectStore::Collection
  {
  public:
    int fd; // collection's dirfd
    // and object fdcache
    Mutex fdcache_lock;
    FDCache fdcache;

    FSCollection(const coll_t& cid, int fd)
      : ObjectStore::Collection(cid), fd(fd), fdcache(g_ceph_context) {
      // TODO:  do something
    }
    friend class FileStore;
  };

  class FSObject : public ObjectStore::Object
  {
  public:
    FSObject(const hobject_t& oid, const FDRef& _fd)
      : ObjectStore::Object(oid) {
      fd = _fd;
    }
    FDRef fd;
  };

  inline FSCollection* get_slot_collection(Transaction& t, uint16_t c_ix) {
    using std::get;
    col_slot_t& c_slot = t.c_slot(c_ix);
    FSCollection* fc = static_cast<FSCollection*>(get<0>(c_slot));
    if (fc)
      return fc;
    fc = static_cast<FSCollection*>(open_collection(get<1>(c_slot)));
    if (fc) {
      // update slot for queued Ops to find
      get<0>(c_slot) = fc;
      // then mark it for release when t is cleaned up
      get<2>(c_slot) |= ObjectStore::Transaction::FLAG_REF;
      t.os = this;
    }
    return fc;
  } /* get_slot_collection */

  inline FSObject* get_slot_object(Transaction& t, FSCollection* fc,
				   uint16_t o_ix, const SequencerPosition& spos,
				   bool create) {
    using std::get;
    obj_slot_t& o_slot = t.o_slot(o_ix);
    FSObject* fo = static_cast<FSObject*>(get<0>(o_slot));
    if (fo)
      return fo;
    if (create) {
      fo = static_cast<FSObject*>(get_object(fc, get<1>(o_slot), spos, create));
      if (fo) {
	// update slot for queued Ops to find
	get<0>(o_slot) = fo;
	// then mark it for release when t is cleaned up
	get<2>(o_slot) |= ObjectStore::Transaction::FLAG_REF;
        t.os = this;
      }
    }
    return fo;
  } /* get_slot_object */

private:
  string basedir, journalpath;
  std::string current_fn;
  std::string current_op_seq_fn;
  std::string omap_dir;
  boost::uuids::uuid fsid;

  size_t blk_size;	      ///< fs block size

  int fsid_fd, op_fd, basedir_fd, current_fd;

  FileStoreBackend *generic_backend;
  FileStoreBackend *backend;

  deque<uint64_t> snaps;

  // Indexed Collections
  IndexManager index_manager;
  int get_index(const coll_t &c, Index *index);
  int init_index(const coll_t &c);

  // ObjectMap
  boost::scoped_ptr<ObjectMap> object_map;

  Finisher ondisk_finisher;

  // helper fns
  int get_cdir(const coll_t &cid, char *s, int len);

  /// read a uuid from fd
  int read_fsid(int fd, boost::uuids::uuid *uuid);

  /// lock fsid_fd
  int lock_fsid();

  // sync thread
  Mutex lock;
  bool force_sync;
  Cond sync_cond;
  uint64_t sync_epoch;

  Mutex sync_entry_timeo_lock;
  SafeTimer timer;

  list<Context*> sync_waiters;
  bool stop;
  void sync_entry();
  struct SyncThread : public Thread {
    FileStore *fs;
    SyncThread(FileStore *f) : fs(f) {}
    void *entry() {
      fs->sync_entry();
      return 0;
    }
  } sync_thread;

  ZTracer::Endpoint trace_endpoint;

  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    list<Transaction*> tls;
    Context *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    OpRequestRef osd_op;
    ZTracer::Trace trace;
  };
  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    list<uint64_t> jq;
    Cond cond;
  public:
    Sequencer *parent;
    FileStore *store;
    Mutex apply_lock;  // for apply mutual exclusion

    void queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void dequeue_journal() {
      Mutex::Locker l(qlock);
      jq.pop_front();
      cond.Signal();
    }
    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
      o->trace.keyval("queue depth", q.size());
    }
    Op *peek_queue() {
      assert(apply_lock.is_locked());
      return q.front();
    }
    Op *dequeue() {
      assert(apply_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.Signal();
      return o;
    }
    void flush() {
      Mutex::Locker l(qlock);

      while (store->cct->_conf->filestore_blackhole)
	cond.Wait(qlock);  // wait forever

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
	seq = q.back()->op;
      if (!jq.empty() && jq.back() > seq)
	seq = jq.back();

      if (seq) {
	// everything prior to our watermark to drain through either/both queues
	while ((!q.empty() && q.front()->op <= seq) ||
	       (!jq.empty() && jq.front() <= seq))
	  cond.Wait(qlock);
      }
    }

    OpSequencer(FileStore *_store)
      : parent(0), store(_store) {}
    ~OpSequencer() {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  };

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  WBThrottle wbthrottle;
  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  Cond op_throttle_cond;
  Mutex op_throttle_lock;
  Finisher op_finisher;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    FileStore *store;
    OpWQ(FileStore *fs, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("FileStore::OpWQ", timeout, suicide_timeout, tp), store(fs) {}

    bool _enqueue(OpSequencer *osr) {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) {
      assert(0);
    }
    bool _empty() {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() {
      if (store->op_queue.empty())
	return NULL;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) {
      store->_do_op(osr, handle);
    }
    void _process_finish(OpSequencer *osr) {
      store->_finish_op(osr);
    }
    void _clear() {
      assert(store->op_queue.empty());
    }
  } op_wq;

  void _do_op(OpSequencer *o, ThreadPool::TPHandle &handle);
  void _finish_op(OpSequencer *o);
  Op *build_op(list<Transaction*>& tls,
	       Context *onreadable, Context *onreadable_sync,
	       OpRequestRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o, ThreadPool::TPHandle *handle = NULL);
  void op_queue_release_throttle(Op *o);
  void _journaled_ahead(OpSequencer *osr, Op *o, Context *ondisk);
  friend struct C_JournaledAhead;
  int write_version_stamp();

  int open_journal();

public:
  /* XXX lfn_ methods likely to move, and be renamed (since the LFN
   * concept is gone) */  
  int lfn_find(FSCollection* fc, const hobject_t& oid);
  int lfn_stat(FSCollection* fc, const hobject_t& oid,
	       struct stat *st);
  int lfn_open(FSCollection* fc, const hobject_t& oid, bool create,
	       FDRef *outfd);
  void lfn_close(FDRef fd);
  int lfn_link(FSCollection* fc, FSCollection* newfc,
	       const hobject_t& o, const hobject_t& newoid);
  int lfn_unlink(FSCollection* fc, const hobject_t& o,
		 const SequencerPosition &spos, bool force_clear_omap=false);

public:
  FileStore(CephContext *_cct, const std::string &base, const std::string &jdev, const char *internal_name = "filestore", bool update_to=false);
  ~FileStore();

  int _detect_fs();
  int _sanity_check_fs();

  bool test_mount_in_use();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  int read_op_seq(uint64_t *seq);
  int write_op_seq(int, uint64_t seq);
  int mount();
  int umount();
  int get_max_object_name_length();
  int mkfs();
  int mkjournal();

  int statfs(struct statfs *buf);

  int _do_transactions(
    list<Transaction*> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle);
  int do_transactions(list<Transaction*> &tls, uint64_t op_seq) {
    return _do_transactions(tls, op_seq, 0);
  }
  unsigned _do_transaction(
    Transaction& t, uint64_t op_seq, int trans_num,
    ThreadPool::TPHandle *handle);

  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
			 OpRequestRef op = OpRequestRef(),
			 ThreadPool::TPHandle *handle = NULL);

private:
  /**
   * set replay guard xattr on given file
   *
   * This will ensure that we will not replay this (or any previous) operation
   * against this particular inode/object.
   *
   * @param fd open file descriptor for the file/object
   * @param spos sequencer position of the last operation we should not replay
   */
  void _set_replay_guard(int fd,
			 const SequencerPosition& spos,
			 const hobject_t *oid=0,
			 bool in_progress=false);
  void _set_replay_guard(FSCollection* fc,
			 const SequencerPosition& spos,
			 bool in_progress);
  void _set_global_replay_guard(FSCollection* fc,
				const SequencerPosition &spos);

  /// close a replay guard opened with in_progress=true
  void _close_replay_guard(int fd, const SequencerPosition& spos);
  void _close_replay_guard(FSCollection* fc,
			   const SequencerPosition& spos);

  /**
   * check replay guard xattr on given file
   *
   * Check the current position against any marker on the file that
   * indicates which operations have already been applied.  If the
   * current or a newer operation has been marked as applied, we
   * should not replay the current operation again.
   *
   * If we are not replaying the journal, we already return true.  It
   * is only on replay that we might return false, indicated that the
   * operation should not be performed (again).
   *
   * @param fd open fd on the file/object in question
   * @param spos sequencerposition for an operation we could apply/replay
   * @return 1 if we can apply (maybe replay) this operation, -1 if spos has already been applied, 0 if it was in progress
   */
  int _check_replay_guard(int fd, const SequencerPosition& spos);
  int _check_replay_guard(FSCollection* fc,
			  const SequencerPosition& spos);
  int _check_replay_guard(FSCollection* fc,
			  FSObject* fo,
			  const SequencerPosition& pos);
  int _check_global_replay_guard(FSCollection* fc,
				 const SequencerPosition& spos);

public:

  // ------------------
  // objects
  int pick_object_revision_lt(hobject_t& oid) {
    return 0;
  }

  bool exists(CollectionHandle ch, const hobject_t& oid);

  ObjectHandle get_object(CollectionHandle ch, const hobject_t& oid);
  void put_object(ObjectHandle oh);

  FSObject* get_object(FSCollection* fc, const hobject_t& oid,
		       const SequencerPosition& spos, bool create);
  void put_object(FSObject* fo);

  int stat(
    CollectionHandle ch,
    ObjectHandle oh,
    struct stat *st,
    bool allow_eio = false);

  int read(
    CollectionHandle ch,
    ObjectHandle oh,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(CollectionHandle ch, ObjectHandle oh,
	     uint64_t offset, size_t len, bufferlist& bl);

  int _touch(FSCollection* fc, FSObject* fo);
  int _write(FSCollection* fc, FSObject* fo,
	     uint64_t offset, size_t len,
	     const bufferlist& bl, bool replica = false);
  int _zero(FSCollection* fc, FSObject* fo,
	    uint64_t offset, size_t len);
  int _truncate(FSCollection* fc, FSObject* fo,
		uint64_t size);
  int _clone(FSCollection* fc, FSObject* fo,
	     FSObject* fo2, const SequencerPosition& spos);
  int _clone_range(FSCollection* fc, FSObject* fo,
		   FSObject* fo2, uint64_t srcoff,
		   uint64_t len, uint64_t dstoff,
		   const SequencerPosition& spos);
  int _do_clone_range(int from, int to, uint64_t srcoff, uint64_t len,
		      uint64_t dstoff);
  int _do_copy_range(int from, int to, uint64_t srcoff, uint64_t len,
		     uint64_t dstoff);
  int _remove(FSCollection* fc, const hobject_t& oid,
	      const SequencerPosition &spos);

  int _fgetattr(int fd, const char *name, bufferptr& bp);
  int _fgetattrs(int fd, map<string,bufferptr>& aset, bool user_only);
  int _fsetattrs(int fd, map<string, bufferptr> &aset);

  void _start_sync();

  void start_sync();
  void start_sync(Context *onsafe);
  void sync();
  void _flush_op_queue();
  void flush();
  void sync_and_flush();

  int dump_journal(ostream& out);

  void set_fsid(const boost::uuids::uuid& u) {
    fsid = u;
  }
  boost::uuids::uuid get_fsid() { return fsid; }

  // DEBUG read error injection, an object is removed from both on delete()
  Mutex read_error_lock;
  set<hobject_t> data_error_set; // read() will return -EIO
  set<hobject_t> mdata_error_set; // getattr(),stat() will return -EIO
  void inject_data_error(const hobject_t &oid);
  void inject_mdata_error(const hobject_t &oid);
  void debug_obj_on_delete(const hobject_t &oid);
  bool debug_data_eio(const hobject_t &oid);
  bool debug_mdata_eio(const hobject_t &oid);

  int snapshot(const string& name);

  // attrs
  int getattr(CollectionHandle ch, ObjectHandle oh,
	      const char *name, bufferptr &bp);
  int getattrs(CollectionHandle ch, ObjectHandle oh,
	       map<string,bufferptr>& aset, bool user_only = false);

  int _setattrs(FSCollection* fc, FSObject* fo,
		map<string,bufferptr>& aset, const SequencerPosition &spos);
  int _rmattr(FSCollection* fc, FSObject* fo,
	      const char *name, const SequencerPosition &spos);
  int _rmattrs(FSCollection* fc, FSObject* fo,
	       const SequencerPosition &spos);

  int collection_getattr(CollectionHandle ch, const char *name,
			 void *value, size_t size);
  int collection_getattr(CollectionHandle ch, const char *name,
			 bufferlist& bl);
  int collection_getattrs(CollectionHandle ch,
			  map<string,bufferptr> &aset);
  int _collection_setattr(FSCollection* fc, const char *name,
			  const void *value, size_t size);
  int _collection_rmattr(FSCollection* fc, const char *name);
  int _collection_setattrs(FSCollection* fc,
			   map<string,bufferptr> &aset);

  // collections
  using ObjectStore::CollectionHandle;

  int list_collections(vector<coll_t>& ls);
  CollectionHandle open_collection(const coll_t& c);
  int close_collection(CollectionHandle chandle);
  int collection_version_current(CollectionHandle ch, uint32_t *version);
  int collection_stat(const coll_t &c, struct stat *st);
  bool collection_exists(const coll_t &c);
  bool collection_empty(CollectionHandle ch);
  int collection_list(CollectionHandle ch, vector<hobject_t>& oid);
  int collection_list_partial(CollectionHandle ch, hobject_t start,
			      int min, int max, vector<hobject_t> *ls,
			      hobject_t *next);
  int collection_list_range(CollectionHandle ch, hobject_t start,
			    hobject_t end, vector<hobject_t> *ls);

  // omap (see ObjectStore.h for documentation)
  int omap_get(CollectionHandle ch, ObjectHandle oh,
	       bufferlist *header, map<string, bufferlist> *out);
  int omap_get_header(
    CollectionHandle ch,
    ObjectHandle oh,
    bufferlist *out,
    bool allow_eio = false);
  int omap_get_keys(CollectionHandle ch, ObjectHandle oh,
		    set<string> *keys);
  int omap_get_values(CollectionHandle ch, ObjectHandle oh,
		      const set<string> &keys, map<string, bufferlist> *out);
  int omap_check_keys(CollectionHandle ch, ObjectHandle oh,
		      const set<string> &keys, set<string> *out);
  ObjectMap::ObjectMapIterator get_omap_iterator(CollectionHandle ch,
						 ObjectHandle oh);

  int _create_collection(const coll_t &c);
  int _create_collection(const coll_t &c, const SequencerPosition &spos);
  int _destroy_collection(FSCollection* fc);
  int _set_alloc_hint(FSCollection* fc, FSObject* fo,
		      uint64_t expected_object_size,
		      uint64_t expected_write_size);

  void dump_start(const std::string& file);
  void dump_stop();
  void dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq,
			 OpSequencer *osr);

private:
  void _inject_failure();

  // omap
  int _omap_clear(FSCollection* fc, FSObject* fo,
		  const SequencerPosition &spos);
  int _omap_setkeys(FSCollection* fc, FSObject* fo,
		    const map<string, bufferlist> &aset,
		    const SequencerPosition &spos);
  int _omap_rmkeys(FSCollection* fc, FSObject* fo,
		   const set<string> &keys, const SequencerPosition &spos);
  int _omap_rmkeyrange(FSCollection* fc, FSObject* fo,
		       const string& first, const string& last,
		       const SequencerPosition &spos);
  int _omap_setheader(FSCollection* fc, FSObject* fo,
		      const bufferlist &bl, const SequencerPosition &spos);

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed);
  float m_filestore_commit_timeout;
  bool m_filestore_journal_parallel;
  bool m_filestore_journal_trailing;
  bool m_filestore_journal_writeahead;
  int m_filestore_fiemap_threshold;
  double m_filestore_max_sync_interval;
  double m_filestore_min_sync_interval;
  bool m_filestore_fail_eio;
  bool m_filestore_replica_fadvise;
  int do_update;
  bool m_journal_dio, m_journal_aio, m_journal_force_aio;
  std::string m_osd_rollback_to_cluster_snap;
  bool m_osd_use_stale_snap;
  int m_filestore_queue_max_ops;
  int m_filestore_queue_max_bytes;
  int m_filestore_queue_committing_max_ops;
  int m_filestore_queue_committing_max_bytes;
  bool m_filestore_do_dump;
  std::ofstream m_filestore_dump;
  JSONFormatter m_filestore_dump_fmt;
  std::atomic<int> m_filestore_kill_at;
  bool m_filestore_sloppy_crc;
  int m_filestore_sloppy_crc_block_size;
  uint64_t m_filestore_max_alloc_hint_size;
  enum fs_types m_fs_type;

  //Determined xattr handling based on fs type
  void set_xattr_limits_via_conf();
  uint32_t m_filestore_max_inline_xattr_size;
  uint32_t m_filestore_max_inline_xattrs;

  FSSuperblock superblock;

  /**
   * write_superblock()
   *
   * Write superblock to persisent storage
   *
   * return value: 0 on success, otherwise negative errno
   */
  int write_superblock();

  /**
   * read_superblock()
   *
   * Fill in FileStore::superblock by reading persistent storage
   *
   * return value: 0 on success, otherwise negative errno
   */
  int read_superblock();

  friend class FileStoreBackend;
};

ostream& operator<<(ostream& out, const FileStore::OpSequencer& s);

struct fiemap;

class FileStoreBackend {
private:
  FileStore *filestore;
protected:
  int get_basedir_fd() {
    return filestore->basedir_fd;
  }
  int get_current_fd() {
    return filestore->current_fd;
  }
  int get_op_fd() {
    return filestore->op_fd;
  }
  size_t get_blksize() {
    return filestore->blk_size;
  }
  const string& get_basedir_path() {
    return filestore->basedir;
  }
  const string& get_current_path() {
    return filestore->current_fn;
  }
  int _copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) {
    return filestore->_do_copy_range(from, to, srcoff, len, dstoff);
  }
  int get_crc_block_size() {
    return filestore->m_filestore_sloppy_crc_block_size;
  }
public:
  FileStoreBackend(FileStore *fs) : filestore(fs) {}
  virtual ~FileStoreBackend() {};
  virtual int detect_features() = 0;
  virtual int create_current() = 0;
  virtual bool can_checkpoint() = 0;
  virtual int list_checkpoints(list<string>& ls) = 0;
  virtual int create_checkpoint(const string& name, uint64_t *cid) = 0;
  virtual int sync_checkpoint(uint64_t id) = 0;
  virtual int rollback_to(const string& name) = 0;
  virtual int destroy_checkpoint(const string& name) = 0;
  virtual int syncfs() = 0;
  virtual bool has_fiemap() = 0;
  virtual int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap) = 0;
  virtual int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) = 0;
  virtual int set_alloc_hint(int fd, uint64_t hint) = 0;

  // hooks for (sloppy) crc tracking
  virtual int _crc_update_write(int fd, loff_t off, size_t len, const bufferlist& bl) = 0;
  virtual int _crc_update_truncate(int fd, loff_t off) = 0;
  virtual int _crc_update_zero(int fd, loff_t off, size_t len) = 0;
  virtual int _crc_update_clone_range(int srcfd, int destfd,
				      loff_t srcoff, size_t len, loff_t dstoff) = 0;
  virtual int _crc_verify_read(int fd, loff_t off, size_t len, const bufferlist& bl,
			       ostream *out) = 0;
};

#endif
