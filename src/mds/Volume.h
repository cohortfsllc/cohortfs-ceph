/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_VOLUME_H
#define COHORT_MDS_VOLUME_H

#include <memory>

#include <boost/uuid/uuid.hpp>

#include "common/mcas_skiplist.h"
#include "mds_types.h"

struct md_config_t;

namespace cohort {
namespace mds {

class Cache;
class Storage;

class Volume : public mcas::skiplist_object {
 private:
  boost::uuids::uuid uuid;
  std::mutex mutex;

 public:
  std::unique_ptr<Cache> cache;

  // search template
  Volume(const boost::uuids::uuid &uuid);

  // move constructor for cache inserts
  Volume(Volume &&o);

  ~Volume();

  const boost::uuids::uuid& get_uuid() const { return uuid; }

  int mkfs(const mcas::gc_global &gc, const mcas::gc_guard &guard,
           const mcas::obj_cache &inode_cache,
           Storage *storage, const md_config_t *conf);

  static int cmp(const void *lhs, const void *rhs)
  {
    const Volume *l = static_cast<const Volume*>(lhs);
    const Volume *r = static_cast<const Volume*>(rhs);
    if (l->uuid == r->uuid)
      return 0;
    if (l->uuid > r->uuid)
      return 1;
    return -1;
  }
};
typedef boost::intrusive_ptr<Volume> VolumeRef;
inline void intrusive_ptr_add_ref(Volume *p) { p->get(); }
inline void intrusive_ptr_release(Volume *p) { p->put(); }

class VolumeTable {
 private:
  const mcas::gc_global &gc;
  mcas::skiplist<Volume> skiplist;
 public:
  VolumeTable(const mcas::gc_global &gc, const mcas::obj_cache &cache)
    : gc(gc),
      skiplist(gc, cache, Volume::cmp)
  {}

  VolumeRef get(const mcas::gc_guard &guard, const boost::uuids::uuid &uuid)
  {
    return skiplist.get(guard, Volume(uuid));
  }

  VolumeRef get_or_create(const mcas::gc_guard &guard,
                          const boost::uuids::uuid &uuid)
  {
    return skiplist.get_or_create(guard, Volume(uuid));
  }

  void destroy(const mcas::gc_guard &guard, VolumeRef &&volume)
  {
    skiplist.destroy(guard, std::move(volume));
  }
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_VOLUME_H
