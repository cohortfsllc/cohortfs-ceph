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
  const mcas::obj_cache inode_cache;
  const mcas::obj_cache inode_storage_cache;
  const mcas::obj_cache dir_cache;
  const mcas::obj_cache dir_storage_cache;
  const mcas::obj_cache dentry_cache;

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

  VolumeRef get_volume(const mcas::gc_guard &guard, volume_t volume);

  uint32_t pick_dir_stripe(const std::string &name) const;

 public:
  MDS(int whoami, Messenger *m, MonClient *mc);
  ~MDS();

  int get_nodeid() const { return whoami; }
  const MDSMap& get_mds_map() const { return mdsmap; }

  int init();
  void shutdown();
  void handle_signal(int signum);

  // for libmds
  int get_root(volume_t volume, ino_t *ino);
  int create(const fileid_t &parent, const std::string &name,
             int mode, const identity_t &who, ino_t *ino, ObjAttr &attr);
  int link(const fileid_t &parent, const std::string &name,
           ino_t ino, ObjAttr &attr);
  int rename(const fileid_t &src_parent, const std::string &src_name,
             const fileid_t &dst_parent, const std::string &dst_name,
             const identity_t &who);
  int unlink(const fileid_t &parent, const std::string &name,
             const identity_t &who);
  int lookup(const fileid_t &parent, const std::string &name, ino_t *ino);
  int readdir(const fileid_t &dir, uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user);
  int getattr(const fileid_t &file, ObjAttr &attr);
  int setattr(const fileid_t &file, int mask, const ObjAttr &attr);

  // void handle_mds_beacon(MMDSBeacon *m);
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_H
