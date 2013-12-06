#ifndef CEPH_CLIENT_SNAPREALMMAP_H
#define CEPH_CLIENT_SNAPREALMMAP_H

#include "include/types.h"

class SnapRealm;

class SnapRealmMap {
 private:
  CephContext *cct;
  typedef hash_map<inodeno_t, SnapRealm*> realm_hashmap;
  realm_hashmap realms;

 public:
  SnapRealmMap(CephContext *cct) : cct(cct) {}

  // find the given snap realm, or return NULL
  SnapRealm* find(inodeno_t ino) const;

  // find or create the given snap realm
  SnapRealm* get(inodeno_t ino);

  // remove the entry for the given snap realm
  void remove(inodeno_t ino);
};

#endif
