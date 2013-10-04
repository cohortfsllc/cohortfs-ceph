#ifndef CEPH_CLIENT_INODECACHE_H
#define CEPH_CLIENT_INODECACHE_H

#include "osdc/ObjectCacher.h"
#include "common/Finisher.h"
#include "mds/mdstypes.h"

class Client;
class Inode;
class Mutex;

typedef void (*ino_cache_callback_t)(void *handle, vinodeno_t ino,
                                     int64_t off, int64_t len);

class InodeCache {
 private:
  CephContext *cct;
  Client *client;
  Mutex &lock; // client_lock
  ObjectCacher *objectcacher;

  ino_cache_callback_t invalidate_cb;
  void *invalidate_cb_handle;
  Finisher async_invalidator;

 public:
  InodeCache(Client *client, Mutex &lock, ObjectCacher *objectcacher);

  void register_callback(ino_cache_callback_t cb, void *handle);
  void shutdown();

  void invalidate(Inode *in, int64_t off, int64_t len, bool keep_caps);
  void release(Inode *in);
  bool flush(Inode *in);
  void flush_range(Inode *in, int64_t offset, uint64_t size);
  void flush_set_callback(ObjectCacher::ObjectSet *oset);

 private:
  void _schedule(Inode *in, int64_t off, int64_t len, bool keep_caps);
  void _invalidate(Inode *in, bool keep_caps);
  void _async_invalidate(Inode *in, int64_t off, int64_t len, bool keep_caps);
  void _flush_range(Inode *in, int64_t off, uint64_t size);
  void _flushed(Inode *in);

  friend class C_CacheInvalidate;
};

#endif
