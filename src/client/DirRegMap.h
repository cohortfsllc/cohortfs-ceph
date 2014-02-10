// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLIENT_DIRREGMAP_H
#define CEPH_CLIENT_DIRREGMAP_H

#include <map>
#include <vector>

#include "common/Mutex.h"

#include "mds/mdstypes.h"


class Finisher;
class MDSRegMap;

// DirRegMap allows libcephfs clients to register for callbacks on
// changes to the dentry placement algorithm of a directory
class DirRegMap {
 private:
  CephContext *cct;
  MDSRegMap *mdsregs;
  Mutex mtx;
  vinodeno_t vino;

  // map of callback registrations
  struct registration {
    void *place;
    void *recall;
    void *user;
    Finisher *async; // send callbacks in the thread from MDSRegMap
  };
  typedef std::map<uint32_t, registration> reg_map;
  reg_map regs;

  // cached dentry placement information
  vector<int> stripe_auth;
  uint64_t hash_seed;

  // wait for the finisher to complete
  void cleanup(registration &reg);

  // schedule a callback on updated placement
  void update(registration &reg);

 public:
  DirRegMap(CephContext *cct, MDSRegMap *mdsregs, vinodeno_t vino);
  ~DirRegMap();

  // add/remove callback registrations
  bool add_registration(uint32_t regid, void *place, void *recall, void *user);
  void remove_registration(uint32_t regid);

  // recall a registration; called by MDSRegMap
  void recall_registration(uint32_t regid);

  void update(const vector<int> &stripes, uint64_t seed = 0);

  void close();
};

#endif
