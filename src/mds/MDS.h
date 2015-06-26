/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_H
#define COHORT_MDS_H

#include "msg/Dispatcher.h"
#include "common/Timer.h"

#include "MDSMap.h"
#include "mds_types.h"
#include "Volume.h"

#define CEPH_MDS_PROTOCOL 23 /* cluster internal */

class MonClient;
namespace rados { class Objecter; }

namespace cohort {
namespace mds {

class Storage;

class MDS : public Dispatcher {
 public:
  const int whoami;
 private:
  const mcas::gc_global gc;
  const mcas::obj_cache volume_cache;
  const mcas::obj_cache storage_cache;
  const mcas::obj_cache inode_cache;

  Messenger *messenger;
  MonClient *monc;
  rados::Objecter *objecter;
  MDSMap mdsmap;
  std::unique_ptr<cohort::Timer<ceph::mono_clock>> beacon_timer;
  version_t beacon_last_seq;
  int last_state, state, want_state;
  ceph_tid_t last_tid;

  std::unique_ptr<Storage> storage; // inode storage
  VolumeTable volumes;

  void beacon_send();

  int get_state() const { return state; }
  int get_want_state() const { return want_state; }
  void request_state(int s);

  ceph_tid_t issue_tid() { return ++last_tid; }

  VolumeRef get_volume(const mcas::gc_guard &guard, libmds_volume_t volume);

 public:
  MDS(int whoami, Messenger *m, MonClient *mc);
  ~MDS();

  int get_nodeid() const { return whoami; }
  const MDSMap& get_mds_map() const { return mdsmap; }

  int init();
  void shutdown();
  void handle_signal(int signum);

  // for libmds
  int create(const libmds_fileid_t *parent, const char *name,
             const identity &who, int type);
  int link(const libmds_fileid_t *parent, const char *name, libmds_ino_t ino);
  int rename(const libmds_fileid_t *src_parent, const char *src_name,
             const libmds_fileid_t *dst_parent, const char *dst_name);
  int unlink(const libmds_fileid_t *parent, const char *name);
  int lookup(const libmds_fileid_t *parent, const char *name,
             libmds_ino_t *ino);
  int readdir(const libmds_fileid_t *dir, uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user);
  int getattr(const libmds_fileid_t *file, int mask, ObjAttr &attr);
  int setattr(const libmds_fileid_t *file, int mask, const ObjAttr &attr);

  // void handle_mds_beacon(MMDSBeacon *m);
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_H
