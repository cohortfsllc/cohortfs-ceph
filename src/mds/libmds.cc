// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>

#include "mon/MonClient.h"

#if defined(HAVE_XIO)
#include "msg/XioMessenger.h"
#include "msg/FastStrategy.h"
#endif

#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "include/color.h"

#include "MDSMap.h"
#include "MDS.h"
#include "MessageFactory.h"
#include "ceph_mds.h"

#define dout_subsys ceph_subsys_mds

namespace
{
// Maintain a map to prevent multiple MDSs with the same name
// TODO: allow same name with different cluster name
std::mutex mds_lock;
typedef std::map<int, libmds*> mdsmap;
mdsmap mdslist;

int context_create(int id, char const *config, char const *cluster,
                   CephContext **cctp)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_MDS);
  if (id >= 0) {
    char name[12];
    snprintf(name, sizeof name, "%d", id);
    iparams.name.set_id(name);
  }
  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_DAEMON, 0);
  std::deque<std::string> parse_errors;
  int r = cct->_conf->parse_config_files(config, &parse_errors, &cerr, 0);
  if (r != 0) {
    derr << "failed to parse configuration " << config << dendl;
    return r;
  }
  cct->_conf->parse_env();
  cct->_conf->apply_changes(NULL);
  cct->init();
  *cctp = cct;
  return 0;
}
}

namespace cohort {
namespace mds {

class LibMDS : public libmds {
 public:
  CephContext *cct;
 private:

  MessageFactory *factory;
  MDS *mds;

public:
  LibMDS(int whoami);
  ~LibMDS();

  int init(const libmds_init_args *args);

  // libmds interface
  void join();
  void shutdown();
  void signal(int signum);

  int create(inodenum_t parent, const char *name);
  int mkdir(inodenum_t parent, const char *name);
  int unlink(inodenum_t parent, const char *name);
  int lookup(inodenum_t parent, const char *name, inodenum_t *ino);

  int getattr(inodenum_t ino, struct stat *st);
  int setattr(inodenum_t ino, const struct stat *st);
};


LibMDS::LibMDS(int whoami)
  : libmds(whoami),
    cct(nullptr),
    factory(nullptr),
    mds(nullptr)
{
}

LibMDS::~LibMDS()
{
  delete mds;
  delete factory;
}

int LibMDS::init(const struct libmds_init_args *args)
{
  // create the CephContext and parse the configuration
  int r = context_create(args->id, args->config, args->cluster, &cct);
  if (r != 0)
    return r;

  MonClient *monc = new MonClient(cct);
  factory = new MDSMessageFactory(cct, &monc->factory);

  const entity_name_t me(entity_name_t::MDS(whoami));
  const pid_t pid = getpid();

  // create messengers
  Messenger *msgr;
#if defined(HAVE_XIO)
  if (cct->_conf->cluster_rdma) {
    XioMessenger *xmsgr = new XioMessenger(cct, me, "xio mds", pid, factory,
                                           2, new FastStrategy());
    xmsgr->set_port_shift(111);
    msgr = xmsgr;
  }
  else
#endif
  {
    msgr = Messenger::create(cct, me, "mds", pid, factory);
  }
  int features = CEPH_FEATURE_OSDREPLYMUX;
  msgr->set_default_policy(Messenger::Policy::lossy_client(0, features));
  msgr->set_cluster_protocol(CEPH_MDS_PROTOCOL);

  common_init_finish(cct, 0);

  // monitor client
  r = monc->build_initial_monmap();
  if (r < 0)
    return r;

  // create mds
  mds = new MDS(args->id, msgr, monc);
  return mds->init();
}

void LibMDS::join()
{
}

void LibMDS::shutdown()
{
  mds->shutdown();
}

void LibMDS::signal(int signum)
{
  mds->handle_signal(signum);
}

int LibMDS::create(inodenum_t parent, const char *name)
{
  const identity who = {0, 0, 0}; // XXX
  return mds->create(parent, name, who, S_IFREG);
}

int LibMDS::mkdir(inodenum_t parent, const char *name)
{
  const identity who = {0, 0, 0}; // XXX
  return mds->create(parent, name, who, S_IFDIR);
}

int LibMDS::unlink(inodenum_t parent, const char *name)
{
  return mds->unlink(parent, name);
}

int LibMDS::lookup(inodenum_t parent, const char *name, inodenum_t *ino)
{
  return mds->lookup(parent, name, ino);
}

int LibMDS::getattr(inodenum_t ino, struct stat *st)
{
  const int mask = ATTR_SIZE | ATTR_MODE |
      ATTR_GROUP | ATTR_OWNER |
      ATTR_ATIME | ATTR_MTIME | ATTR_CTIME |
      ATTR_NLINKS | ATTR_TYPE | ATTR_RAWDEV;

  ObjAttr attr;
  int r = mds->getattr(ino, mask, attr);
  if (r == 0) {
    st->st_mode = attr.mode | attr.type;
    st->st_nlink = attr.nlinks;
    st->st_uid = attr.user;
    st->st_gid = attr.group;
    st->st_rdev = attr.rawdev;
    st->st_size = attr.filesize;
    st->st_atime = ceph::real_clock::to_time_t(attr.atime);
    st->st_mtime = ceph::real_clock::to_time_t(attr.mtime);
    st->st_ctime = ceph::real_clock::to_time_t(attr.ctime);
  }
  return r;
}

int LibMDS::setattr(inodenum_t ino, const struct stat *st)
{
  const int mask = ATTR_MODE | ATTR_GROUP | ATTR_OWNER |
      ATTR_ATIME | ATTR_MTIME | ATTR_CTIME |
      ATTR_NLINKS | ATTR_TYPE | ATTR_RAWDEV;

  ObjAttr attr;
  attr.mode = st->st_mode & ~S_IFMT;
  attr.type = st->st_mode & S_IFMT;
  attr.nlinks = st->st_nlink;
  attr.user = st->st_uid;
  attr.group = st->st_gid;
  attr.rawdev = st->st_rdev;
  attr.filesize = st->st_size;
  attr.atime = ceph::real_clock::from_time_t(st->st_atime);
  attr.mtime = ceph::real_clock::from_time_t(st->st_mtime);
  attr.ctime = ceph::real_clock::from_time_t(st->st_ctime);

  return mds->setattr(ino, mask, attr);
}

} // namespace mds
} // namespace cohort


// C interface

struct libmds* libmds_init(const struct libmds_init_args *args)
{
  if (args == nullptr)
    return nullptr;

  cohort::mds::LibMDS *mds;
  {
    // protect access to the map of mdslist
    std::lock_guard<std::mutex> lock(mds_lock);

    // existing mds with this name?
    std::pair<mdsmap::iterator, bool> result =
      mdslist.insert(mdsmap::value_type(args->id, nullptr));
    if (!result.second) {
      return nullptr;
    }

    result.first->second = mds = new cohort::mds::LibMDS(args->id);
  }

  try {
    if (mds->init(args) == 0)
      return mds;
  } catch (std::exception &e) {
  }

  // remove from the map of mdslist
  std::unique_lock<std::mutex> ol(mds_lock);
  mdslist.erase(args->id);
  ol.unlock();

  delete mds;
  return nullptr;
}

void libmds_join(struct libmds *mds)
{
  try {
    mds->join();
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_join caught exception " << e.what() << dendl;
  }
}

void libmds_shutdown(struct libmds *mds)
{
  try {
    mds->shutdown();
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_shutdown caught exception " << e.what() << dendl;
  }
}

void libmds_cleanup(struct libmds *mds)
{
  // assert(!running)
  const int id = mds->whoami;
  // delete LibMDS because base destructor is protected
  delete static_cast<cohort::mds::LibMDS*>(mds);

  // remove from the map of mdslist
  std::lock_guard<std::mutex> lock(mds_lock);
  mdslist.erase(id);
}

void libmds_signal(int signum)
{
  // signal all mdslist under list lock
  std::lock_guard<std::mutex> lock(mds_lock);
  for (auto mds : mdslist) {
    try {
      mds.second->signal(signum);
    } catch (std::exception &e) {
      CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds.second)->cct;
      lderr(cct) << "libmds_signal caught exception " << e.what() << dendl;
    }
  }
}

int libmds_create(struct libmds *mds, inodenum_t parent, const char *name)
{
  try {
    return mds->create(parent, name);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_create caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_mkdir(struct libmds *mds, inodenum_t parent, const char *name)
{
  try {
    return mds->mkdir(parent, name);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_mkdir caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_unlink(struct libmds *mds, inodenum_t parent, const char *name)
{
  try {
    return mds->unlink(parent, name);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_unlink caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_lookup(struct libmds *mds, inodenum_t parent, const char *name,
                  inodenum_t *ino)
{
  try {
    return mds->lookup(parent, name, ino);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_lookup caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_getattr(struct libmds *mds, inodenum_t ino, struct stat *st)
{
  try {
    return mds->getattr(ino, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_getattr caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_setattr(struct libmds *mds, inodenum_t ino, const struct stat *st)
{
  try {
    return mds->setattr(ino, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_setattr caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}
