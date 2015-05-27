// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>

#include "mon/MonClient.h"

#include "msg/Dispatcher.h"
#if defined(HAVE_XIO)
#include "msg/XioMessenger.h"
#include "msg/FastStrategy.h"
#endif

#include "common/Finisher.h"
#include "mds/MDSMap.h"
#include "mds/MDSimpl.h"
#include "mds/MessageFactory.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "include/color.h"

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

namespace ceph
{
namespace mds
{

class LibMDS : public libmds {
 public:
  CephContext *cct;
 private:
  libmds_callbacks *callbacks;
  void *user;
  Finisher *finisher; // thread to send callbacks to user

  MessageFactory *factory;
  MDSimpl *mds;

public:
  LibMDS(int whoami);
  ~LibMDS();

  int init(const libmds_init_args *args);

  // libmds interface
  void join();
  void shutdown();
  void signal(int signum);
};


LibMDS::LibMDS(int whoami)
  : libmds(whoami),
    cct(nullptr),
    callbacks(nullptr),
    user(nullptr),
    finisher(nullptr),
    factory(nullptr),
    mds(nullptr)
{
}

LibMDS::~LibMDS()
{
  delete mds;
  delete factory;
  if (finisher) {
    finisher->stop();
    delete finisher;
  }
}

int LibMDS::init(const struct libmds_init_args *args)
{
  callbacks = args->callbacks;
  user = args->user;

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
  mds = new MDSimpl(cct->_conf->name.get_id().c_str(), msgr, monc);
  return mds->init();
}

struct C_StateCb : public ::Context {
  typedef void (*callback_fn)(struct libmds *mds, void *user);
  callback_fn cb;
  libmds *mds;
  void *user;
  C_StateCb(callback_fn cb, libmds *mds, void *user)
    : cb(cb), mds(mds), user(user) {}
  void finish(int r) {
    cb(mds, user);
  }
};

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

} // namespace mds
} // namespace ceph


// C interface

struct libmds* libmds_init(const struct libmds_init_args *args)
{
  if (args == nullptr)
    return nullptr;

  ceph::mds::LibMDS *mds;
  {
    // protect access to the map of mdslist
    std::lock_guard<std::mutex> lock(mds_lock);

    // existing mds with this name?
    std::pair<mdsmap::iterator, bool> result =
      mdslist.insert(mdsmap::value_type(args->id, nullptr));
    if (!result.second) {
      return nullptr;
    }

    result.first->second = mds = new ceph::mds::LibMDS(args->id);
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
    CephContext *cct = static_cast<ceph::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_join caught exception " << e.what() << dendl;
  }
}

void libmds_shutdown(struct libmds *mds)
{
  try {
    mds->shutdown();
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::mds::LibMDS*>(mds)->cct;
    lderr(cct) << "libmds_shutdown caught exception " << e.what() << dendl;
  }
}

void libmds_cleanup(struct libmds *mds)
{
  // assert(!running)
  const int id = mds->whoami;
  // delete LibMDS because base destructor is protected
  delete static_cast<ceph::mds::LibMDS*>(mds);

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
      CephContext *cct = static_cast<ceph::mds::LibMDS*>(mds.second)->cct;
      lderr(cct) << "libmds_signal caught exception " << e.what() << dendl;
    }
  }
}
