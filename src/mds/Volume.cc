/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "Volume.h"
#include "Cache.h"
#include "Storage.h"
#include "Inode.h"

#include "common/config.h"

namespace cohort {
namespace mds {

Volume::Volume(const boost::uuids::uuid &uuid)
  : uuid(uuid)
{
}

Volume::Volume(Volume &&o)
{
  std::swap(uuid, o.uuid);
}

Volume::~Volume()
{
}

int Volume::mkfs(const mcas::gc_global &gc, const mcas::obj_cache &inode_cache,
                 Storage *storage, const md_config_t *conf)
{
  std::lock_guard<std::mutex> lock(mutex);

  if (cache)
    return -EINVAL;

  // create the storage and cache
  std::unique_ptr<Cache> c(new Cache(this, gc, inode_cache, storage,
                                     conf->mds_cache_highwater,
                                     conf->mds_cache_lowwater));

  // create the root directory inode
  const identity who = {0, 0, 0};
  auto root = c->create(who, S_IFDIR);
  assert(root);

  std::swap(c, cache);
  return 0;
}

} // namespace mds
} // namespace cohort
