// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OBJECTCACHER_H
#define CEPH_OBJECTCACHER_H

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"
#include "include/xlist.h"

#include "common/Cond.h"
#include "common/Thread.h"

#include "Objecter.h"
#include "Filer.h"

class CephContext;
class WritebackHandler;
class PerfCounters;

enum {
  l_objectcacher_first = 25000,

  l_objectcacher_cache_ops_hit, // ops we satisfy completely from cache
  l_objectcacher_cache_ops_miss, // ops we don't satisfy completely from cache

  l_objectcacher_cache_bytes_hit, // bytes read directly from cache
  l_objectcacher_cache_bytes_miss, // bytes we couldn't read directly from cache

  l_objectcacher_data_read, // total bytes read out
  l_objectcacher_data_written, // bytes written to cache
  l_objectcacher_data_flushed, // bytes flushed to WritebackHandler
  l_objectcacher_overwritten_in_flush, // bytes overwritten while flushing is in progress

  l_objectcacher_write_ops_blocked, // total write ops we delayed due to dirty limits
  l_objectcacher_write_bytes_blocked, // total number of write bytes we delayed due to dirty limits
  l_objectcacher_write_time_blocked, // total time in seconds spent blocking a write due to dirty limits

  l_objectcacher_last,
};

class ObjectCacher {
  PerfCounters *perfcounter;
 public:
  CephContext *cct;
  class Object;
  class ObjectSet;

  typedef void (*flush_set_callback_t) (void *p, ObjectSet *oset);

  // read scatter/gather  
  struct OSDRead {
    vector<ObjectExtent> extents;
    snapid_t snap;
    map<object_t, bufferlist*> read_data;  // bits of data as they come back
    bufferlist *bl;
    int flags;
    OSDRead(snapid_t s, bufferlist *b, int f) : snap(s), bl(b), flags(f) {}
  };

  OSDRead *prepare_read(snapid_t snap, bufferlist *b, int f) {
    return new OSDRead(snap, b, f);
  }
  
  // write scatter/gather  
  struct OSDWrite {
    vector<ObjectExtent> extents;
    SnapContext snapc;
    bufferlist bl;
    utime_t mtime;
    int flags;
    OSDWrite(const SnapContext& sc, bufferlist& b, utime_t mt, int f) : snapc(sc), bl(b), mtime(mt), flags(f) {}
  };

  OSDWrite *prepare_write(const SnapContext& sc, bufferlist &b, utime_t mt, int f) { 
    return new OSDWrite(sc, b, mt, f); 
  }



  // ******* BufferHead *********
  class BufferHead : public LRUObject {
  public:
    // states
    static const int STATE_MISSING = 0;
    static const int STATE_CLEAN = 1;
    static const int STATE_DIRTY = 2;
    static const int STATE_RX = 3;
    static const int STATE_TX = 4;
    static const int STATE_ERROR = 5; // a read error occurred

  private:
    // my fields
    int state;
    int ref;
    struct {
      loff_t start, length;   // bh extent in object
    } ex;
        
  public:
    Object *ob;
    bufferlist  bl;
    tid_t last_write_tid;  // version of bh (if non-zero)
    utime_t last_write;
    SnapContext snapc;
    int error; // holds return value for failed reads
    
    map< loff_t, list<Context*> > waitfor_read;
    
    // cons
    BufferHead(Object *o) : 
      state(STATE_MISSING),
      ref(0),
      ob(o),
      last_write_tid(0),
      error(0) {}
  
    // extent
    loff_t start() const { return ex.start; }
    void set_start(loff_t s) { ex.start = s; }
    loff_t length() const { return ex.length; }
    void set_length(loff_t l) { ex.length = l; }
    loff_t end() const { return ex.start + ex.length; }
    loff_t last() const { return end() - 1; }

    // states
    void set_state(int s) {
      if (s == STATE_RX || s == STATE_TX) get();
      if (state == STATE_RX || state == STATE_TX) put();
      state = s;
    }
    int get_state() const { return state; }
    
    bool is_missing() { return state == STATE_MISSING; }
    bool is_dirty() { return state == STATE_DIRTY; }
    bool is_clean() { return state == STATE_CLEAN; }
    bool is_tx() { return state == STATE_TX; }
    bool is_rx() { return state == STATE_RX; }
    bool is_error() { return state == STATE_ERROR; }
    
    // reference counting
    int get() {
      assert(ref >= 0);
      if (ref == 0) lru_pin();
      return ++ref;
    }
    int put() {
      assert(ref > 0);
      if (ref == 1) lru_unpin();
      --ref;
      return ref;
    }
  };

  // ******* Object *********
  class Object {
  private:
    // ObjectCacher::Object fields
    ObjectCacher *oc;
    sobject_t oid;
    friend class ObjectSet;

  public:
    ObjectSet *oset;
    xlist<Object*>::item set_item;
    object_locator_t oloc;
    

  public:
    map<loff_t, BufferHead*>     data;

    tid_t last_write_tid;  // version of bh (if non-zero)
    tid_t last_commit_tid; // last update commited.

    int dirty_or_tx;

    map< tid_t, list<Context*> > waitfor_commit;
    list<Context*> waitfor_rd;
    list<Context*> waitfor_wr;

    // lock
    static const int LOCK_NONE = 0;
    static const int LOCK_WRLOCKING = 1;
    static const int LOCK_WRLOCK = 2;
    static const int LOCK_WRUNLOCKING = 3;
    static const int LOCK_RDLOCKING = 4;
    static const int LOCK_RDLOCK = 5;
    static const int LOCK_RDUNLOCKING = 6;
    static const int LOCK_UPGRADING = 7;    // rd -> wr
    static const int LOCK_DOWNGRADING = 8;  // wr -> rd
    int lock_state;
    int wrlock_ref;  // how many ppl want or are using a WRITE lock
    int rdlock_ref;  // how many ppl want or are using a READ lock

  public:
    Object(const Object& other);
    const Object& operator=(const Object& other);

    Object(ObjectCacher *_oc, sobject_t o, ObjectSet *os, object_locator_t& l) : 
      oc(_oc),
      oid(o), oset(os), set_item(this), oloc(l),
      last_write_tid(0), last_commit_tid(0),
      dirty_or_tx(0),
      lock_state(LOCK_NONE), wrlock_ref(0), rdlock_ref(0) {
      // add to set
      os->objects.push_back(&set_item);
    }
    ~Object() {
      assert(data.empty());
      assert(dirty_or_tx == 0);
      set_item.remove_myself();
    }

    sobject_t get_soid() { return oid; }
    object_t get_oid() { return oid.oid; }
    snapid_t get_snap() { return oid.snap; }
    ObjectSet *get_object_set() { return oset; }
    
    object_locator_t& get_oloc() { return oloc; }
    void set_object_locator(object_locator_t& l) { oloc = l; }

    bool can_close() {
      return data.empty() && lock_state == LOCK_NONE &&
        waitfor_commit.empty() &&
        waitfor_rd.empty() && waitfor_wr.empty() &&
	dirty_or_tx == 0;
    }

    /**
     * find first buffer that includes or follows an offset
     *
     * @param offset object byte offset
     * @return iterator pointing to buffer, or data.end()
     */
    map<loff_t,BufferHead*>::iterator data_lower_bound(loff_t offset) {
      map<loff_t,BufferHead*>::iterator p = data.lower_bound(offset);
      if (p != data.begin() &&
	  (p == data.end() || p->first > offset)) {
	p--;     // might overlap!
	if (p->first + p->second->length() <= offset)
	  p++;   // doesn't overlap.
      }
      return p;
    }

    // bh
    // add to my map
    void add_bh(BufferHead *bh) {
      assert(data.count(bh->start()) == 0);
      data[bh->start()] = bh;
    }
    void remove_bh(BufferHead *bh) {
      assert(data.count(bh->start()));
      data.erase(bh->start());
    }

    bool is_empty() { return data.empty(); }

    // mid-level
    BufferHead *split(BufferHead *bh, loff_t off);
    void merge_left(BufferHead *left, BufferHead *right);
    void try_merge_bh(BufferHead *bh);

    bool is_cached(loff_t off, loff_t len);
    int map_read(OSDRead *rd,
                 map<loff_t, BufferHead*>& hits,
                 map<loff_t, BufferHead*>& missing,
                 map<loff_t, BufferHead*>& rx,
		 map<loff_t, BufferHead*>& errors);
    BufferHead *map_write(OSDWrite *wr);
    
    void truncate(loff_t s);
    void discard(loff_t off, loff_t len);
  }; // class Object
  

  struct ObjectSet {
    void *parent;

    inodeno_t ino;
    uint64_t truncate_seq, truncate_size;

    int64_t poolid;
    xlist<Object*> objects;

    int dirty_or_tx;

    ObjectSet(void *p, int64_t _poolid, inodeno_t i)
      : parent(p), ino(i), truncate_seq(0),
	truncate_size(0), poolid(_poolid), dirty_or_tx(0) {}
  };


  // ******* ObjectCacher *********
  // ObjectCacher fields
 private:
  WritebackHandler& writeback_handler;

  string name;
  Mutex& lock;
  
  int64_t max_dirty, target_dirty, max_size;
  utime_t max_dirty_age;

  flush_set_callback_t flush_set_callback;
  void *flush_set_callback_arg;

  vector<hash_map<sobject_t, Object*> > objects; // indexed by pool_id

  set<BufferHead*>    dirty_bh;
  LRU   lru_dirty, lru_rest;

  Cond flusher_cond;
  bool flusher_stop;
  void flusher_entry();
  class FlusherThread : public Thread {
    ObjectCacher *oc;
  public:
    FlusherThread(ObjectCacher *o) : oc(o) {}
    void *entry() {
      oc->flusher_entry();
      return 0;
    }
  } flusher_thread;
  

  // objects
  Object *get_object_maybe(sobject_t oid, object_locator_t &l) {
    // have it?
    if (((uint32_t)l.pool < objects.size()) &&
        (objects[l.pool].count(oid)))
      return objects[l.pool][oid];
    return NULL;
  }

  Object *get_object(sobject_t oid, ObjectSet *oset,
                     object_locator_t &l);
  void close_object(Object *ob);

  // bh stats
  Cond  stat_cond;

  loff_t stat_clean;
  loff_t stat_dirty;
  loff_t stat_rx;
  loff_t stat_tx;
  loff_t stat_missing;
  loff_t stat_error;
  loff_t stat_dirty_waiting;   // bytes that writers are waiting on to write

  void verify_stats() const;

  void bh_stat_add(BufferHead *bh);
  void bh_stat_sub(BufferHead *bh);
  loff_t get_stat_tx() { return stat_tx; }
  loff_t get_stat_rx() { return stat_rx; }
  loff_t get_stat_dirty() { return stat_dirty; }
  loff_t get_stat_dirty_waiting() { return stat_dirty_waiting; }
  loff_t get_stat_clean() { return stat_clean; }

  void touch_bh(BufferHead *bh) {
    if (bh->is_dirty())
      lru_dirty.lru_touch(bh);
    else
      lru_rest.lru_touch(bh);
  }

  // bh states
  void bh_set_state(BufferHead *bh, int s);
  void copy_bh_state(BufferHead *bh1, BufferHead *bh2) { 
    bh_set_state(bh2, bh1->get_state());
  }
  
  void mark_missing(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_MISSING); };
  void mark_clean(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_CLEAN); };
  void mark_rx(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_RX); };
  void mark_tx(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_TX); };
  void mark_error(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_ERROR); };
  void mark_dirty(BufferHead *bh) { 
    bh_set_state(bh, BufferHead::STATE_DIRTY); 
    lru_dirty.lru_touch(bh);
    //bh->set_dirty_stamp(ceph_clock_now(g_ceph_context));
  };

  void bh_add(Object *ob, BufferHead *bh);
  void bh_remove(Object *ob, BufferHead *bh);

  // io
  void bh_read(BufferHead *bh);
  void bh_write(BufferHead *bh);

  void trim(loff_t max=-1);
  void flush(loff_t amount=0);

  /**
   * flush a range of buffers
   *
   * Flush any buffers that intersect the specified extent.  If len==0,
   * flush *all* buffers for the object.
   *
   * @param o object
   * @param off start offset
   * @param len extent length, or 0 for entire object
   * @return true if object was already clean/flushed.
   */
  bool flush(Object *o, loff_t off, loff_t len);
  loff_t release(Object *o);
  void purge(Object *o);

  void rdlock(Object *o);
  void rdunlock(Object *o);
  void wrlock(Object *o);
  void wrunlock(Object *o);

  int _readx(OSDRead *rd, ObjectSet *oset, Context *onfinish,
	     bool external_call);

 public:
  void bh_read_finish(int64_t poolid, sobject_t oid, loff_t offset,
		      uint64_t length, bufferlist &bl, int r);
  void bh_write_commit(int64_t poolid, sobject_t oid, loff_t offset,
		       uint64_t length, tid_t t, int r);
  void lock_ack(int64_t poolid, list<sobject_t>& oids, tid_t tid);

  class C_ReadFinish : public Context {
    ObjectCacher *oc;
    int64_t poolid;
    sobject_t oid;
    loff_t start;
    uint64_t length;
  public:
    bufferlist bl;
    C_ReadFinish(ObjectCacher *c, int _poolid, sobject_t o, loff_t s, uint64_t l) :
      oc(c), poolid(_poolid), oid(o), start(s), length(l) {}
    void finish(int r) {
      oc->bh_read_finish(poolid, oid, start, length, bl, r);
    }
  };

  class C_WriteCommit : public Context {
    ObjectCacher *oc;
    int64_t poolid;
    sobject_t oid;
    loff_t start;
    uint64_t length;
  public:
    tid_t tid;
    C_WriteCommit(ObjectCacher *c, int64_t _poolid, sobject_t o, loff_t s, uint64_t l) :
      oc(c), poolid(_poolid), oid(o), start(s), length(l) {}
    void finish(int r) {
      oc->bh_write_commit(poolid, oid, start, length, tid, r);
    }
  };

  class C_LockAck : public Context {
    ObjectCacher *oc;
  public:
    int64_t poolid;
    list<sobject_t> oids;
    tid_t tid;
    C_LockAck(ObjectCacher *c, int64_t _poolid, sobject_t o) : oc(c), poolid(_poolid) {
      oids.push_back(o);
    }
    void finish(int r) {
      oc->lock_ack(poolid, oids, tid);
    }
  };

  void perf_start();
  void perf_stop();



  ObjectCacher(CephContext *cct_, string name, WritebackHandler& wb, Mutex& l,
	       flush_set_callback_t flush_callback,
	       void *flush_callback_arg,
	       uint64_t max_size, uint64_t max_dirty, uint64_t target_dirty, double max_age);
  ~ObjectCacher();

  void start() {
    flusher_thread.create();
  }
  void stop() {
    assert(flusher_thread.is_started());
    lock.Lock();  // hmm.. watch out for deadlock!
    flusher_stop = true;
    flusher_cond.Signal();
    lock.Unlock();
    flusher_thread.join();
  }


  class C_RetryRead : public Context {
    ObjectCacher *oc;
    OSDRead *rd;
    ObjectSet *oset;
    Context *onfinish;
  public:
    C_RetryRead(ObjectCacher *_oc, OSDRead *r, ObjectSet *os, Context *c) : oc(_oc), rd(r), oset(os), onfinish(c) {}
    void finish(int r) {
      if (r < 0) {
	if (onfinish)
	  onfinish->complete(r);
	return;
      }
      int ret = oc->_readx(rd, oset, onfinish, false);
      if (ret != 0 && onfinish) {
        onfinish->complete(ret);
      }
    }
  };



  // non-blocking.  async.

  /**
   * @note total read size must be <= INT_MAX, since
   * the return value is total bytes read
   */
  int readx(OSDRead *rd, ObjectSet *oset, Context *onfinish);
  int writex(OSDWrite *wr, ObjectSet *oset, Mutex& wait_on_lock);
  bool is_cached(ObjectSet *oset, vector<ObjectExtent>& extents, snapid_t snapid);

private:
  // write blocking
  int _wait_for_write(OSDWrite *wr, uint64_t len, ObjectSet *oset, Mutex& lock);
  
public:
  bool set_is_cached(ObjectSet *oset);
  bool set_is_dirty_or_committing(ObjectSet *oset);

  bool flush_set(ObjectSet *oset, Context *onfinish=0);
  bool flush_set(ObjectSet *oset, vector<ObjectExtent>& ex, Context *onfinish=0);
  void flush_all(Context *onfinish=0);

  bool commit_set(ObjectSet *oset, Context *oncommit);

  void purge_set(ObjectSet *oset);

  loff_t release_set(ObjectSet *oset);  // returns # of bytes not released (ie non-clean)
  uint64_t release_all();

  void discard_set(ObjectSet *oset, vector<ObjectExtent>& ex);

  // cache sizes
  void set_max_dirty(int64_t v) {
    max_dirty = v;
  }
  void set_target_dirty(int64_t v) {
    target_dirty = v;
  }
  void set_max_size(int64_t v) {
    max_size = v;
  }
  void set_max_dirty_age(double a) {
    max_dirty_age.set_from_double(a);
  }

  // file functions

  /*** async+caching (non-blocking) file interface ***/
  int file_is_cached(ObjectSet *oset, ceph_file_layout *layout, snapid_t snapid,
		     loff_t offset, uint64_t len) {
    vector<ObjectExtent> extents;
    Filer::file_to_extents(cct, oset->ino, layout, offset, len, extents);
    return is_cached(oset, extents, snapid);
  }

  int file_read(ObjectSet *oset, ceph_file_layout *layout, snapid_t snapid,
                loff_t offset, uint64_t len, 
                bufferlist *bl,
		int flags,
                Context *onfinish) {
    OSDRead *rd = prepare_read(snapid, bl, flags);
    Filer::file_to_extents(cct, oset->ino, layout, offset, len, rd->extents);
    return readx(rd, oset, onfinish);
  }

  int file_write(ObjectSet *oset, ceph_file_layout *layout, const SnapContext& snapc,
                 loff_t offset, uint64_t len, 
                 bufferlist& bl, utime_t mtime, int flags,
		 Mutex& wait_on_lock) {
    OSDWrite *wr = prepare_write(snapc, bl, mtime, flags);
    Filer::file_to_extents(cct, oset->ino, layout, offset, len, wr->extents);
    return writex(wr, oset, wait_on_lock);
  }
};


inline ostream& operator<<(ostream& out, ObjectCacher::BufferHead &bh)
{
  out << "bh["
      << bh.start() << "~" << bh.length()
      << " " << bh.ob
      << " (" << bh.bl.length() << ")"
      << " v " << bh.last_write_tid;
  if (bh.is_tx()) out << " tx";
  if (bh.is_rx()) out << " rx";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) out << " clean";
  if (bh.is_missing()) out << " missing";
  if (bh.bl.length() > 0) out << " firstbyte=" << (int)bh.bl[0];
  if (bh.error) out << " error=" << bh.error;
  out << "]";
  return out;
}

inline ostream& operator<<(ostream& out, ObjectCacher::ObjectSet &os)
{
  return out << "objectset[" << os.ino
	     << " ts " << os.truncate_seq << "/" << os.truncate_size
	     << " objects " << os.objects.size()
	     << " dirty_or_tx " << os.dirty_or_tx
	     << "]";
}

inline ostream& operator<<(ostream& out, ObjectCacher::Object &ob)
{
  out << "object["
      << ob.get_soid() << " oset " << ob.oset << dec
      << " wr " << ob.last_write_tid << "/" << ob.last_commit_tid;

  switch (ob.lock_state) {
  case ObjectCacher::Object::LOCK_WRLOCKING: out << " wrlocking"; break;
  case ObjectCacher::Object::LOCK_WRLOCK: out << " wrlock"; break;
  case ObjectCacher::Object::LOCK_WRUNLOCKING: out << " wrunlocking"; break;
  case ObjectCacher::Object::LOCK_RDLOCKING: out << " rdlocking"; break;
  case ObjectCacher::Object::LOCK_RDLOCK: out << " rdlock"; break;
  case ObjectCacher::Object::LOCK_RDUNLOCKING: out << " rdunlocking"; break;
  }

  out << "]";
  return out;
}

#endif
