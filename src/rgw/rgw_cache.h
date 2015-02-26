// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include <cassert>
#include "rgw_rados.h"
#include <string>
#include <map>
#include "include/types.h"
#include "include/utime.h"
#include "common/RWLock.h"

enum {
  UPDATE_OBJ,
  REMOVE_OBJ,
};

#define CACHE_FLAG_DATA		  0x01
#define CACHE_FLAG_XATTRS	  0x02
#define CACHE_FLAG_META		  0x04
#define CACHE_FLAG_MODIFY_XATTRS  0x08
#define CACHE_FLAG_OBJV		  0x10

#define mydout(v) lsubdout(T::cct, rgw, v)

struct ObjectMetaInfo {
  uint64_t size;
  time_t mtime;

  ObjectMetaInfo() : size(0), mtime(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(size, bl);
    utime_t t(mtime, 0);
    ::encode(t, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(size, bl);
    utime_t t;
    ::decode(t, bl);
    mtime = t.sec();
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectMetaInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectMetaInfo)

struct ObjectCacheInfo {
  int status;
  uint32_t flags;
  uint64_t epoch;
  bufferlist data;
  map<string, bufferlist> xattrs;
  map<string, bufferlist> rm_xattrs;
  ObjectMetaInfo meta;
  obj_version version;

  ObjectCacheInfo() : status(0), flags(0), epoch(0), version() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    ::encode(status, bl);
    ::encode(flags, bl);
    ::encode(data, bl);
    ::encode(xattrs, bl);
    ::encode(meta, bl);
    ::encode(rm_xattrs, bl);
    ::encode(epoch, bl);
    ::encode(version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    ::decode(status, bl);
    ::decode(flags, bl);
    ::decode(data, bl);
    ::decode(xattrs, bl);
    ::decode(meta, bl);
    if (struct_v >= 2)
      ::decode(rm_xattrs, bl);
    if (struct_v >= 4)
      ::decode(epoch, bl);
    if (struct_v >= 5)
      ::decode(version, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectCacheInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectCacheInfo)

struct RGWCacheNotifyInfo {
  uint32_t op;
  rgw_obj oid;
  ObjectCacheInfo obj_info;
  off_t ofs;
  string ns;

  RGWCacheNotifyInfo() : op(0), ofs(0) {}

  void encode(bufferlist& obl) const {
    ENCODE_START(2, 2, obl);
    ::encode(op, obl);
    ::encode(oid, obl);
    ::encode(obj_info, obl);
    ::encode(ofs, obl);
    ::encode(ns, obl);
    ENCODE_FINISH(obl);
  }
  void decode(bufferlist::iterator& ibl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, ibl);
    ::decode(op, ibl);
    ::decode(oid, ibl);
    ::decode(obj_info, ibl);
    ::decode(ofs, ibl);
    ::decode(ns, ibl);
    DECODE_FINISH(ibl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<RGWCacheNotifyInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWCacheNotifyInfo)

struct ObjectCacheEntry {
  ObjectCacheInfo info;
  std::list<string>::iterator lru_iter;
  uint64_t lru_promotion_ts;

  ObjectCacheEntry() : lru_promotion_ts(0) {}
};

class ObjectCache {
  std::map<string, ObjectCacheEntry> cache_map;
  std::list<string> lru;
  unsigned long lru_size;
  unsigned long lru_counter;
  unsigned long lru_window;
  RWLock lock;
  CephContext *cct;

  void touch_lru(string& name, ObjectCacheEntry& entry, std::list<string>::iterator& lru_iter);
  void remove_lru(string& name, std::list<string>::iterator& lru_iter);
public:
  ObjectCache() : lru_size(0), lru_counter(0), lru_window(0), cct(NULL) { }
  int get(std::string& name, ObjectCacheInfo& bl, uint32_t mask);
  void put(std::string& name, ObjectCacheInfo& bl);
  void remove(std::string& name);
  void set_ctx(CephContext *_cct) {
    cct = _cct;
    lru_window = cct->_conf->rgw_cache_lru_size / 2;
  }
};

template <class T>
class RGWCache	: public T
{
  ObjectCache cache;

  int list_objects_raw_init(rgw_bucket& bucket, RGWAccessHandle *handle) {
    return T::list_objects_raw_init(bucket, handle);
  }
  int list_objects_raw_next(RGWObjEnt& oid, RGWAccessHandle *handle) {
    return T::list_objects_raw_next(oid, handle);
  }

  string normal_name(rgw_bucket& bucket, std::string& oid_t) {
    string& bucket_name = bucket.name;
    char buf[bucket_name.size() + 1 + oid_t.size() + 1];
    const char *bucket_str = bucket_name.c_str();
    const char *oid_str = oid_t.c_str();
    sprintf(buf, "%s+%s", bucket_str, oid_str);
    return string(buf);
  }

  void normalize_bucket_and_obj(rgw_bucket& src_bucket, string& src_obj, rgw_bucket& dst_bucket, string& dst_obj);
  string normal_name(rgw_obj& oid) {
    return normal_name(oid.bucket, oid.object);
  }

  int init_rados() {
    int ret;
    cache.set_ctx(T::cct);
    ret = T::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }

  bool need_watch_notify() {
    return true;
  }

  int distribute_cache(const string& normal_name, rgw_obj& oid, ObjectCacheInfo& obj_info, int op);
  int watch_cb(int opcode, uint64_t ver, bufferlist& bl);
public:
  RGWCache() {}

  int set_attr(void *ctx, rgw_obj& oid, const char *name, bufferlist& bl, RGWObjVersionTracker *objv_tracker);
  int set_attrs(void *ctx, rgw_obj& oid,
		map<string, bufferlist>& attrs,
		map<string, bufferlist>* rmattrs,
		RGWObjVersionTracker *objv_tracker);
  int put_obj_meta_impl(void *ctx, rgw_obj& oid, uint64_t size, time_t *mtime,
		   map<std::string, bufferlist>& attrs, RGWObjCategory category, int flags,
		   map<std::string, bufferlist>* rmattrs, const bufferlist *data,
		   RGWObjManifest *manifest, const string *ptag, list<string> *remove_objs,
		   bool modify_version, RGWObjVersionTracker *objv_tracker, time_t set_mtime,
		   const string& owner);
  int put_obj_data(void *ctx, rgw_obj& oid, const char *data,
	      off_t ofs, size_t len, bool exclusive);

  int get_obj(void *ctx, RGWObjVersionTracker *objv_tracker, void **handle, rgw_obj& oid, bufferlist& bl, off_t ofs, off_t end);

  int obj_stat(void *ctx, rgw_obj& oid, uint64_t *psize, time_t *pmtime, uint64_t *epoch, map<string, bufferlist> *attrs,
	       bufferlist *first_chunk, RGWObjVersionTracker *objv_tracker);

  int delete_obj_impl(void *ctx, const string& bucket_owner, rgw_obj& oid, RGWObjVersionTracker *objv_tracker);
};

template <class T>
void RGWCache<T>::normalize_bucket_and_obj(rgw_bucket& src_bucket, string& src_obj, rgw_bucket& dst_bucket, string& dst_obj)
{
  if (src_obj.size()) {
    dst_bucket = src_bucket;
    dst_obj = src_obj;
  } else {
    dst_bucket = T::zone.domain_root;
    dst_obj = src_bucket.name;
  }
}

template <class T>
int RGWCache<T>::delete_obj_impl(void *ctx, const string& bucket_owner, rgw_obj& oid, RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  if (bucket.name[0] != '.')
    return T::delete_obj_impl(ctx, bucket_owner, oid, objv_tracker);

  string name = normal_name(oid);
  cache.remove(name);

  ObjectCacheInfo info;
  distribute_cache(name, oid, info, REMOVE_OBJ);

  return T::delete_obj_impl(ctx, bucket_owner, oid, objv_tracker);
}

template <class T>
int RGWCache<T>::get_obj(void *ctx, RGWObjVersionTracker *objv_tracker, void **handle, rgw_obj& oid, bufferlist& obl, off_t ofs, off_t end)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  if (bucket.name[0] != '.' || ofs != 0)
    return T::get_obj(ctx, objv_tracker, handle, oid, obl, ofs, end);

  string name = normal_name(oid.bucket, oid_t);

  ObjectCacheInfo info;

  uint32_t flags = CACHE_FLAG_DATA;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;

  if (cache.get(name, info, flags) == 0) {
    if (info.status < 0)
      return info.status;

    bufferlist& bl = info.data;

    bufferlist::iterator i = bl.begin();

    obl.clear();

    i.copy_all(obl);
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    return bl.length();
  }
  int r = T::get_obj(ctx, objv_tracker, handle, oid, obl, ofs, end);
  if (r < 0) {
    if (r == -ENOENT) { // only update ENOENT, we'd rather retry other errors
      info.status = r;
      cache.put(name, info);
    }
    return r;
  }

  if (obl.length() == end + 1) {
    /* in this case, most likely object contains more data, we can't cache it */
    return r;
  }

  bufferptr p(r);
  bufferlist& bl = info.data;
  bl.clear();
  bufferlist::iterator o = obl.begin();
  o.copy_all(bl);
  info.status = 0;
  info.flags = flags;
  if (objv_tracker) {
    info.version = objv_tracker->read_version;
  }
  cache.put(name, info);
  return r;
}

template <class T>
int RGWCache<T>::set_attr(void *ctx, rgw_obj& oid, const char *attr_name, bufferlist& bl, RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  ObjectCacheInfo info;
  bool cacheable = false;
  if (bucket.name[0] == '.') {
    cacheable = true;
    info.xattrs[attr_name] = bl;
    info.status = 0;
    info.flags = CACHE_FLAG_MODIFY_XATTRS;
    if (objv_tracker) {
      info.version = objv_tracker->write_version;
      info.flags |= CACHE_FLAG_OBJV;
    }
  }
  int ret = T::set_attr(ctx, oid, attr_name, bl, objv_tracker);
  if (cacheable) {
    string name = normal_name(bucket, oid_t);
    if (ret >= 0) {
      cache.put(name, info);
      int r = distribute_cache(name, oid, info, UPDATE_OBJ);
      if (r < 0)
	mydout(0) << "ERROR: failed to distribute cache for " << oid << dendl;
    } else {
     cache.remove(name);
    }
  }

  return ret;
}

template <class T>
int RGWCache<T>::set_attrs(void *ctx, rgw_obj& oid,
			   map<string, bufferlist>& attrs,
			   map<string, bufferlist>* rmattrs,
			   RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  ObjectCacheInfo info;
  bool cacheable = false;
  if (bucket.name[0] == '.') {
    cacheable = true;
    info.xattrs = attrs;
    if (rmattrs)
      info.rm_xattrs = *rmattrs;
    info.status = 0;
    info.flags = CACHE_FLAG_MODIFY_XATTRS;
    if (objv_tracker) {
      info.version = objv_tracker->write_version;
      info.flags |= CACHE_FLAG_OBJV;
    }
  }
  int ret = T::set_attrs(ctx, oid, attrs, rmattrs, objv_tracker);
  if (cacheable) {
    string name = normal_name(bucket, oid_t);
    if (ret >= 0) {
      cache.put(name, info);
      int r = distribute_cache(name, oid, info, UPDATE_OBJ);
      if (r < 0)
	mydout(0) << "ERROR: failed to distribute cache for " << oid << dendl;
    } else {
     cache.remove(name);
    }
  }

  return ret;
}

template <class T>
int RGWCache<T>::put_obj_meta_impl(void *ctx, rgw_obj& oid, uint64_t size, time_t *mtime,
			      map<std::string, bufferlist>& attrs, RGWObjCategory category, int flags,
			      map<std::string, bufferlist>* rmattrs, const bufferlist *data,
			      RGWObjManifest *manifest, const string *ptag, list<string> *remove_objs,
			      bool modify_version, RGWObjVersionTracker *objv_tracker, time_t set_mtime,
			      const string& owner)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  ObjectCacheInfo info;
  bool cacheable = false;
  if (bucket.name[0] == '.') {
    cacheable = true;
    info.xattrs = attrs;
    info.status = 0;
    info.flags = CACHE_FLAG_XATTRS;
    if (data) {
      info.data = *data;
      info.flags |= CACHE_FLAG_DATA;
    }
    if (objv_tracker) {
      info.version = objv_tracker->write_version;
      info.flags |= CACHE_FLAG_OBJV;
    }
  }
  int ret = T::put_obj_meta_impl(ctx, oid, size, mtime, attrs, category, flags, rmattrs, data, manifest, ptag, remove_objs,
				 modify_version, objv_tracker, set_mtime, owner);
  if (cacheable) {
    string name = normal_name(bucket, oid_t);
    if (ret >= 0) {
      cache.put(name, info);
      int r = distribute_cache(name, oid, info, UPDATE_OBJ);
      if (r < 0)
	mydout(0) << "ERROR: failed to distribute cache for " << oid << dendl;
    } else {
     cache.remove(name);
    }
  }

  return ret;
}

template <class T>
int RGWCache<T>::put_obj_data(void *ctx, rgw_obj& oid, const char *data,
	      off_t ofs, size_t len, bool exclusive)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  ObjectCacheInfo info;
  bool cacheable = false;
  if ((bucket.name[0] == '.') && ((ofs == 0) || (ofs == -1))) {
    cacheable = true;
    bufferptr p(len);
    memcpy(p.c_str(), data, len);
    bufferlist& bl = info.data;
    bl.append(p);
    info.meta.size = bl.length();
    info.status = 0;
    info.flags = CACHE_FLAG_DATA;
  }
  int ret = T::put_obj_data(ctx, oid, data, ofs, len, exclusive);
  if (cacheable) {
    string name = normal_name(bucket, oid_t);
    if (ret >= 0) {
      cache.put(name, info);
      int r = distribute_cache(name, oid, info, UPDATE_OBJ);
      if (r < 0)
	mydout(0) << "ERROR: failed to distribute cache for " << oid << dendl;
    } else {
     cache.remove(name);
    }
  }

  return ret;
}

template <class T>
int RGWCache<T>::obj_stat(void *ctx, rgw_obj& oid, uint64_t *psize, time_t *pmtime,
			  uint64_t *pepoch, map<string, bufferlist> *attrs,
			  bufferlist *first_chunk, RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(oid.bucket, oid.object, bucket, oid_t);
  if (bucket.name[0] != '.')
    return T::obj_stat(ctx, oid, psize, pmtime, pepoch, attrs, first_chunk, objv_tracker);

  string name = normal_name(bucket, oid_t);

  uint64_t size;
  time_t mtime;
  uint64_t epoch;

  ObjectCacheInfo info;
  uint32_t flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  int r = cache.get(name, info, flags);
  if (r == 0) {
    if (info.status < 0)
      return info.status;

    size = info.meta.size;
    mtime = info.meta.mtime;
    epoch = info.epoch;
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    goto done;
  }
  r = T::obj_stat(ctx, oid, &size, &mtime, &epoch, &info.xattrs, first_chunk, objv_tracker);
  if (r < 0) {
    if (r == -ENOENT) {
      info.status = r;
      cache.put(name, info);
    }
    return r;
  }
  info.status = 0;
  info.epoch = epoch;
  info.meta.mtime = mtime;
  info.meta.size = size;
  info.flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker) {
    info.flags |= CACHE_FLAG_OBJV;
    info.version = objv_tracker->read_version;
  }
  cache.put(name, info);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (pepoch)
    *pepoch = epoch;
  if (attrs)
    *attrs = info.xattrs;
  return 0;
}

template <class T>
int RGWCache<T>::distribute_cache(const string& normal_name, rgw_obj& oid, ObjectCacheInfo& obj_info, int op)
{
  RGWCacheNotifyInfo info;

  info.op = op;

  info.obj_info = obj_info;
  info.oid = obj;
  bufferlist bl;
  ::encode(info, bl);
  int ret = T::distribute(normal_name, bl);
  return ret;
}

template <class T>
int RGWCache<T>::watch_cb(int opcode, uint64_t ver, bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(info, iter);
  } catch (buffer::end_of_buffer& err) {
    mydout(0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    mydout(0) << "ERROR: buffer::error" << dendl;
    return -EIO;
  }

  rgw_bucket bucket;
  string oid_t;
  normalize_bucket_and_obj(info.oid.bucket, info.oid.object, bucket, oid_t);
  string name = normal_name(bucket, oid_t);

  switch (info.op) {
  case UPDATE_OBJ:
    cache.put(name, info.obj_info);
    break;
  case REMOVE_OBJ:
    cache.remove(name);
    break;
  default:
    mydout(0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

#endif
