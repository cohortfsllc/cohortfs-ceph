#include "InodeCache.h"
#include "Inode.h"
#include "Client.h"
#include "SnapRealm.h"

#include "osdc/ObjectCacher.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.cache "

InodeCache::InodeCache(Client *client, Mutex &lock, ObjectCacher *objectcacher)
  : cct(client->cct),
    client(client),
    lock(lock),
    objectcacher(objectcacher),
    invalidate_cb(NULL),
    invalidate_cb_handle(NULL),
    async_invalidator(client->cct)
{
}

void InodeCache::register_callback(ino_cache_callback_t cb, void *handle)
{
  ldout(cct, 10) << "register_callback " << cb << dendl;
  invalidate_cb = cb;
  invalidate_cb_handle = handle;
  async_invalidator.start();
}

void InodeCache::shutdown()
{
  if (invalidate_cb) {
    ldout(cct, 10) << "shutdown stopping invalidator finisher" << dendl;
    async_invalidator.wait_for_empty();
    async_invalidator.stop();
  }
}

class C_CacheInvalidate : public Context {
 private:
  InodeCache *cache;
  Inode *inode;
  int64_t offset, length;
  bool keep_caps;
 public:
  C_CacheInvalidate(InodeCache *c, Inode *in, int64_t off, int64_t len, bool keep)
      : cache(c), inode(in), offset(off), length(len), keep_caps(keep) {
    inode->get();
  }
  void finish(int r) {
    cache->_async_invalidate(inode, offset, length, keep_caps);
  }
};

void InodeCache::_async_invalidate(Inode *in, int64_t off, int64_t len,
                                   bool keep_caps)
{
  invalidate_cb(invalidate_cb_handle, in->vino(), off, len);

  lock.Lock();
  if (!keep_caps)
    client->put_cap_ref(in, CEPH_CAP_FILE_CACHE);
  client->put_inode(in);
  lock.Unlock();
}

void InodeCache::_schedule(Inode *in, int64_t off, int64_t len, bool keep_caps)
{
  if (invalidate_cb)
    // we queue the invalidate, which calls the callback and decrements the ref
    async_invalidator.queue(new C_CacheInvalidate(this, in, off, len, keep_caps));
  else if (!keep_caps)
    // if not set, we just decrement the cap ref here
    in->put_cap_ref(CEPH_CAP_FILE_CACHE);
}

void InodeCache::_invalidate(Inode *in, bool keep_caps)
{
  ldout(cct, 10) << "_invalidate " << *in << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc)
    objectcacher->release_set(&in->oset);

  _schedule(in, 0, 0, keep_caps);
}

void InodeCache::invalidate(Inode *in, int64_t off, int64_t len, bool keep_caps)
{
  ldout(cct, 10) << "invalidate " << *in << " " << off << "~" << len << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc) {
    vector<ObjectExtent> ls;
    Striper::file_to_extents(cct, in->ino, &in->layout, off, len, ls);
    objectcacher->discard_set(&in->oset, ls);
  }

  _schedule(in, off, len, keep_caps);
}

void InodeCache::release(Inode *in)
{
  if (in->cap_refs[CEPH_CAP_FILE_CACHE])
    _invalidate(in, false);
}


class C_PutInode : public Context {
  Client *client;
  Inode *in;
 public:
  C_PutInode(Client *c, Inode *i) : client(c), in(i) {
    in->get();
  }
  void finish(int) {
    client->put_inode(in);
  }
};

bool InodeCache::flush(Inode *in)
{
  ldout(cct, 10) << "flush " << *in << dendl;

  if (!in->oset.dirty_or_tx) {
    ldout(cct, 10) << " nothing to flush" << dendl;
    return true;
  }

  Context *onfinish = new C_PutInode(client, in);
  bool safe = objectcacher->flush_set(&in->oset, onfinish);
  if (safe)
    onfinish->complete(0);
  return safe;
}

void InodeCache::flush_range(Inode *in, int64_t offset, uint64_t size)
{
  assert(lock.is_locked());
  if (!in->oset.dirty_or_tx) {
    ldout(cct, 10) << " nothing to flush" << dendl;
    return;
  }

  Mutex flock("InodeCache::_flush_range flock");
  Cond cond;
  bool safe = false;
  Context *onflush = new C_SafeCond(&flock, &cond, &safe);
  safe = objectcacher->file_flush(&in->oset, &in->layout,
                                  in->snaprealm->get_snap_context(),
                                  offset, size, onflush);
  if (safe)
    return;

  // wait for flush
  lock.Unlock();
  flock.Lock();
  while (!safe)
    cond.Wait(flock);
  flock.Unlock();
  lock.Lock();
}

void InodeCache::flush_set_callback(ObjectCacher::ObjectSet *oset)
{
  // will be called via dispatch() -> objecter -> ...
  assert(lock.is_locked());
  Inode *in = (Inode*)oset->parent;
  assert(in);
  _flushed(in);
}

void InodeCache::_flushed(Inode *in)
{
  ldout(cct, 10) << "_flushed " << *in << dendl;

  // release clean pages too, if we dont hold RDCACHE reference
  if (in->cap_refs[CEPH_CAP_FILE_CACHE] == 0)
    _invalidate(in, true);

  client->put_cap_ref(in, CEPH_CAP_FILE_BUFFER);
}

