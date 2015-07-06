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
                   int argc, const char **argv, CephContext **cctp)
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
  // parse command line arguments
  if (argc) {
    assert(argv);
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    cct->_conf->parse_argv(args);
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

  int get_root(libmds_volume_t volume, libmds_ino_t *ino);
  int create(const libmds_fileid_t *parent, const char *name,
             int mode, const libmds_identity_t *who,
             libmds_ino_t *ino, struct stat *st);
  int mkdir(const libmds_fileid_t *parent, const char *name,
            int mode, const libmds_identity_t *who,
            libmds_ino_t *ino, struct stat *st);
  int link(const libmds_fileid_t *parent, const char *name,
           libmds_ino_t ino, struct stat *st);
  int symlink(const libmds_fileid_t *parent, const char *name,
              const char *target, const libmds_identity_t *who,
              libmds_ino_t *ino, struct stat *st);
  int readlink(const libmds_fileid_t *parent, char *buf, int buf_len);
  int rename(const libmds_fileid_t *parent1, const char *name1,
             const libmds_fileid_t *parent2, const char *name2,
             const libmds_identity_t *who);
  int unlink(const libmds_fileid_t *parent, const char *name,
             const libmds_identity_t *who);
  int lookup(const libmds_fileid_t *parent, const char *name,
             libmds_ino_t *ino);
  int readdir(const libmds_fileid_t *dir, uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user);

  int getattr(const libmds_fileid_t *file, struct stat *st);
  int setattr(const libmds_fileid_t *file, int mask, const struct stat *st);

  int open(const libmds_fileid_t *file, int flags,
           struct libmds_open_state **state);
  int close(struct libmds_open_state *state);
  ssize_t read(struct libmds_open_state *state, size_t offset,
               char *buf, size_t buf_len);
  ssize_t write(struct libmds_open_state *state, size_t offset,
                const char *buf, size_t buf_len);
  int commit(struct libmds_open_state *state, uint64_t offset, size_t len);
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
  int r = context_create(args->id, args->config, args->cluster,
                         args->argc, args->argv, &cct);
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

int LibMDS::get_root(libmds_volume_t volume, libmds_ino_t *ino)
{
  return mds->get_root(volume, ino);
}

int LibMDS::create(const libmds_fileid_t *parent, const char *name,
                   int mode, const libmds_identity_t *who,
                   libmds_ino_t *ino, struct stat *st)
{
  assert(!S_ISDIR(mode));
  return mds->create(*parent, name, mode, *who, ino, st);
}

int LibMDS::mkdir(const libmds_fileid_t *parent, const char *name,
                  int mode, const libmds_identity_t *who,
                  libmds_ino_t *ino, struct stat *st)
{
  assert(S_ISDIR(mode));
  return mds->create(*parent, name, mode, *who, ino, st);
}

int LibMDS::link(const libmds_fileid_t *parent, const char *name,
                 libmds_ino_t ino, struct stat *st)
{
  ObjAttr attr;
  int r = mds->link(*parent, name, ino, attr);
  if (r == 0) {
    st->st_mode = attr.mode;
    st->st_uid = attr.user;
    st->st_gid = attr.group;
    st->st_size = attr.filesize;
    st->st_atime = ceph::real_clock::to_time_t(attr.atime);
    st->st_mtime = ceph::real_clock::to_time_t(attr.mtime);
    st->st_ctime = ceph::real_clock::to_time_t(attr.ctime);
    st->st_nlink = attr.nlinks;
    st->st_rdev = attr.rawdev;
  }
  return r;
}

int LibMDS::symlink(const libmds_fileid_t *parent, const char *name,
                    const char *target, const libmds_identity_t *who,
                    libmds_ino_t *ino, struct stat *st)
{
  return -ENOTSUP;
}

int LibMDS::readlink(const libmds_fileid_t *parent, char *buf, int buf_len)
{
  return -ENOTSUP;
}

int LibMDS::rename(const libmds_fileid_t *src_parent, const char *src_name,
                   const libmds_fileid_t *dst_parent, const char *dst_name,
                   const libmds_identity_t *who)
{
  return mds->rename(*src_parent, src_name, *dst_parent, dst_name, *who);
}

int LibMDS::unlink(const libmds_fileid_t *parent, const char *name,
                   const libmds_identity_t *who)
{
  return mds->unlink(*parent, name, *who);
}

int LibMDS::lookup(const libmds_fileid_t *parent, const char *name,
                   libmds_ino_t *ino)
{
  return mds->lookup(*parent, name, ino);
}

int LibMDS::readdir(const libmds_fileid_t *dir, uint64_t pos, uint64_t gen,
                    libmds_readdir_fn cb, void *user)
{
  return mds->readdir(*dir, pos, gen, cb, user);
}

int LibMDS::getattr(const libmds_fileid_t *file, struct stat *st)
{
  ObjAttr attr;
  int r = mds->getattr(*file, attr);
  if (r == 0) {
    st->st_mode = attr.mode;
    st->st_uid = attr.user;
    st->st_gid = attr.group;
    st->st_size = attr.filesize;
    st->st_atime = ceph::real_clock::to_time_t(attr.atime);
    st->st_mtime = ceph::real_clock::to_time_t(attr.mtime);
    st->st_ctime = ceph::real_clock::to_time_t(attr.ctime);
    st->st_nlink = attr.nlinks;
    st->st_rdev = attr.rawdev;
  }
  return r;
}

int LibMDS::setattr(const libmds_fileid_t *file, int mask,
                    const struct stat *st)
{
  ObjAttr attr;
  attr.mode = st->st_mode;
  attr.user = st->st_uid;
  attr.group = st->st_gid;
  attr.filesize = st->st_size;
  attr.atime = ceph::real_clock::from_time_t(st->st_atime);
  attr.mtime = ceph::real_clock::from_time_t(st->st_mtime);
  attr.ctime = ceph::real_clock::from_time_t(st->st_ctime);

  return mds->setattr(*file, mask, attr);
}

int LibMDS::open(const libmds_fileid_t *file, int flags,
                 struct libmds_open_state **state)
{
  return -ENOTSUP;
}

int LibMDS::close(struct libmds_open_state *state)
{
  return -ENOTSUP;
}

ssize_t LibMDS::read(struct libmds_open_state *state, size_t offset,
                     char *buf, size_t buf_len)
{
  return -ENOTSUP;
}

ssize_t LibMDS::write(struct libmds_open_state *state, size_t offset,
                      const char *buf, size_t buf_len)
{
  return -ENOTSUP;
}

int LibMDS::commit(struct libmds_open_state *state, uint64_t offset, size_t len)
{
  return -ENOTSUP;
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
    if (!result.second)
      return nullptr;

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

int libmds_get_root(struct libmds *mds, libmds_volume_t volume,
                    libmds_ino_t *ino)
{
  try {
    return mds->get_root(volume, ino);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_get_root caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_create(struct libmds *mds, const libmds_fileid_t *parent,
                  const char *name, int mode, const libmds_identity_t *who,
                  libmds_ino_t *ino, struct stat *st)
{
  try {
    return mds->create(parent, name, mode, who, ino, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_create caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_mkdir(struct libmds *mds, const libmds_fileid_t *parent,
                 const char *name, int mode, const libmds_identity_t *who,
                 libmds_ino_t *ino, struct stat *st)
{
  try {
    return mds->mkdir(parent, name, mode, who, ino, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_mkdir caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_link(struct libmds *mds, const libmds_fileid_t *parent,
                const char *name, libmds_ino_t ino, struct stat *st)
{
  try {
    return mds->link(parent, name, ino, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_link caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_symlink(struct libmds *mds, const libmds_fileid_t *parent,
                   const char *name, const char *target,
                   const libmds_identity_t *who, libmds_ino_t *ino,
                   struct stat *st)
{
  try {
    return mds->symlink(parent, name, target, who, ino, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_symlink caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_readlink(struct libmds *mds, const libmds_fileid_t *parent,
                    char *buf, int buf_len)
{
  try {
    return mds->readlink(parent, buf, buf_len);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_readlink caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_rename(struct libmds *mds,
                  const libmds_fileid_t *src_parent, const char *src_name,
                  const libmds_fileid_t *dst_parent, const char *dst_name,
                  const libmds_identity_t *who)
{
  try {
    return mds->rename(src_parent, src_name, dst_parent, dst_name, who);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_rename caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_unlink(struct libmds *mds, const libmds_fileid_t *parent,
                  const char *name, const libmds_identity_t *who)
{
  try {
    return mds->unlink(parent, name, who);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_unlink caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_lookup(struct libmds *mds, const libmds_fileid_t *parent,
                  const char *name, libmds_ino_t *ino)
{
  try {
    return mds->lookup(parent, name, ino);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_lookup caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_readdir(struct libmds *mds, const libmds_fileid_t *dir,
                   uint64_t pos, uint64_t gen,
                   libmds_readdir_fn cb, void *user)
{
  try {
    return mds->readdir(dir, pos, gen, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_readdir caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_getattr(struct libmds *mds, const libmds_fileid_t *file,
                   struct stat *st)
{
  try {
    return mds->getattr(file, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_getattr caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_setattr(struct libmds *mds, const libmds_fileid_t *file,
                   int mask, const struct stat *st)
{
  try {
    return mds->setattr(file, mask, st);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_setattr caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_open(struct libmds *mds, const libmds_fileid_t *file,
                int flags, struct libmds_open_state **state)
{
  try {
    return mds->open(file, flags, state);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_open caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_close(struct libmds *mds, struct libmds_open_state *state)
{
  try {
    return mds->close(state);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_close caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

ssize_t libmds_read(struct libmds *mds, struct libmds_open_state *state,
                    size_t offset, char *buf, size_t buf_len)
{
  try {
    return mds->read(state, offset, buf, buf_len);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_read caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

ssize_t libmds_write(struct libmds *mds, struct libmds_open_state *state,
                     size_t offset, const char *buf, size_t buf_len)
{
  try {
    return mds->write(state, offset, buf, buf_len);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_write caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libmds_commit(struct libmds *mds, struct libmds_open_state *state,
                  uint64_t offset, size_t len)
{
  try {
    return mds->commit(state, offset, len);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<cohort::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_commit caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}
