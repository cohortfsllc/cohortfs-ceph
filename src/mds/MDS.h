/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_H
#define COHORT_MDS_H

#include "msg/Dispatcher.h"
#include "common/Timer.h"
#include "common/mcas_skiplist.h"

#include "MDSMap.h"
#include "mds_types.h"

#define CEPH_MDS_PROTOCOL 23 /* cluster internal */

class MonClient;
class Objecter;

namespace cohort {
namespace mds {

class Cache;
class Storage;

class MDSVol {
 public:
  void release() {}
};

class MDS : public Dispatcher {
 public:
  const int whoami;
 private:
  const mcas::gc_global gc;
  Messenger *messenger;
  MonClient *monc;
  Objecter *objecter;
  MDSMap mdsmap;
  cohort::Timer<ceph::mono_clock> beacon_timer;
  version_t beacon_last_seq;
  int last_state, state, want_state;
  ceph_tid_t last_tid;

  std::unique_ptr<Cache> cache;
  std::unique_ptr<Storage> storage;

  void beacon_send();

  int get_state() const { return state; }
  int get_want_state() const { return want_state; }
  void request_state(int s);

  ceph_tid_t issue_tid() { return ++last_tid; }

 public:
  MDS(int whoami, Messenger *m, MonClient *mc);
  ~MDS();

  int get_nodeid() const { return whoami; }
  const MDSMap& get_mds_map() const { return mdsmap; }

  int init();
  int mkfs();
  void shutdown();
  void handle_signal(int signum);

  // for libmds
  int create(_inodeno_t parent, const char *name,
             const identity &who, int type);
  int unlink(_inodeno_t parent, const char *name);
  int lookup(_inodeno_t parent, const char *name, _inodeno_t *ino);
  int readdir(_inodeno_t dir, uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user);
  int getattr(_inodeno_t ino, int mask, ObjAttr &attr);
  int setattr(_inodeno_t ino, int mask, const ObjAttr &attr);

  // void handle_mds_beacon(MMDSBeacon *m);
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_H
