// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLIENT_MDSREGMAP_H
#define CEPH_CLIENT_MDSREGMAP_H

#include <map>
#include <set>
#include <vector>

#include <tr1/memory>

#include "common/Finisher.h"
#include "common/Mutex.h"

#include "mds/mdstypes.h"


class DirRegMap;
class MDSMap;
// use shared ptrs so we can remove mds info while callbacks are outstanding
struct ceph_mds_info_t;
typedef std::tr1::shared_ptr<ceph_mds_info_t> mds_info_ptr;

// MDSRegMap allows libcephfs clients to register for callbacks on
// changes to the MDSMap involving MDS status and addresses
class MDSRegMap {
 private:
  CephContext *cct;
  Mutex mtx;

  typedef std::set<DirRegMap*> dir_reg_set;

  // map of callback registrations
  struct registration {
    void *add;
    void *remove;
    void *place;
    void *user;
    Finisher *async; // send callbacks in a separate thread
    vector<bool> known; // up mds' for which the client got callbacks
    ceph_seq_t placement_seq; // last placement_seq sent to client
    dir_reg_set dirs; // directory registrations
  };
  typedef std::map<uint32_t, registration> reg_map;
  reg_map regs;
  uint32_t next_regid;

  // cached inode placement information
  mds_inode_placement_t placement;
  ceph_seq_t placement_seq;

  // cache of device info
  vector<mds_info_ptr> devices;

  // wait for the finisher to complete
  void cleanup(uint32_t regid, registration &reg);

  // schedule callbacks for any added/removed devices
  void update(registration &reg);

 public:
  MDSRegMap(CephContext *cct);
  ~MDSRegMap();

  // add/remove callback registrations
  uint32_t add_registration(void *add, void *remove, void *place, void *user);
  void remove_registration(uint32_t regid);

  // add/remove directory registrations for the associated mdsmap registration
  Finisher* add_dir_registration(uint32_t regid, DirRegMap *dirregs);
  void remove_dir_registration(uint32_t regid, DirRegMap *dirregs);

  // called with MDSMap each time the epoch changes
  void update(const MDSMap *mdsmap);

  // wait for all registrations to be cleaned up and removed
  void shutdown();
};

#endif
