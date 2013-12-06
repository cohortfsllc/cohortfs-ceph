#include "common/debug.h"
#include "SnapRealmMap.h"
#include "ClientSnapRealm.h"

#define dout_subsys ceph_subsys_client

SnapRealm* SnapRealmMap::get(inodeno_t ino)
{
  pair<realm_hashmap::iterator, bool> result = realms.insert(
      pair<inodeno_t, SnapRealm*>(ino, NULL));
  if (result.second)
    result.first->second = new SnapRealm(this, ino);

  SnapRealm *realm = result.first->second;
  const int nref = realm->get_num_refs();
  ldout(cct, 20) << "snap_realm get " << ino << " " << realm << " "
      << nref << " -> " << (nref + 1) << dendl;
  realm->get();
  return realm;
}

SnapRealm* SnapRealmMap::find(inodeno_t ino) const
{
  realm_hashmap::const_iterator r = realms.find(ino);
  if (r == realms.end()) {
    ldout(cct, 20) << "snap_realm find " << ino << " fail" << dendl;
    return NULL;
  }
  SnapRealm *realm = r->second;
  const int nref = realm->get_num_refs();
  ldout(cct, 20) << "snap_realm find " << ino << " " << realm << " "
      << nref << " -> " << (nref + 1) << dendl;
  realm->get();
  return realm;
}

void SnapRealmMap::remove(inodeno_t ino)
{
  realm_hashmap::iterator r = realms.find(ino);
  assert(r != realms.end());
  ldout(cct, 20) << "snap_realm remove " << ino << " " << r->second << dendl;
  realms.erase(r);
}

