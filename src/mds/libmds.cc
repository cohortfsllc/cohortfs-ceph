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
typedef std::unique_lock<std::mutex> unique_mds_lock;
typedef std::lock_guard<std::mutex> mds_lock_guard;
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

  MonClient *monc;
  MessageFactory *factory;
  MDSimpl *mds;

  struct _mdsmap {
    std::mutex mtx;
    std::condition_variable cond;
    typedef std::unique_lock<std::mutex> unique_lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    int state;
    epoch_t epoch;
    bool shutdown;
  } mdsmap;

  // MDSStateObserver
  void on_mds_state(int state, epoch_t epoch);

  // Objecter
  bool wait_for_active(epoch_t *epoch);

  void init_dispatcher(MDSimpl *mds);

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
    monc(nullptr),
    factory(nullptr),
    mds(nullptr)
{
  mdsmap.state = 0;
  mdsmap.epoch = 0;
  mdsmap.shutdown = false;
}

LibMDS::~LibMDS()
{
  delete mds;
  delete factory;
  delete monc;
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

  monc = new MonClient(cct);
  factory = new MDSMessageFactory(cct, &monc->factory);

  const entity_name_t me(entity_name_t::MDS(whoami));
  const pid_t pid = getpid();

  // create and bind messengers
  Messenger *simple_msgr = Messenger::create(cct,
					     entity_name_t::MDS(-1), "mds",
					     pid, factory);
  simple_msgr->set_cluster_protocol(CEPH_MDS_PROTOCOL);
#if defined(HAVE_XIO)
  XioMessenger *xmsgr = new XioMessenger(
    cct,
    entity_name_t::MDS(-1),
    "xio mds",
    0 /* nonce */,
    factory,
    2 /* portals */,
    new FastStrategy() /* dispatch strategy */);

  xmsgr->set_cluster_protocol(CEPH_MDS_PROTOCOL);
  xmsgr->set_port_shift(111);;
#endif
  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_DIRLAYOUTHASH |
    CEPH_FEATURE_MDS_INLINE_DATA |
    CEPH_FEATURE_MSG_AUTH;
  uint64_t required =
    CEPH_FEATURE_OSDREPLYMUX;

  simple_msgr->set_default_policy(Messenger::Policy::lossy_client(supported, required));
  simple_msgr->set_policy(entity_name_t::TYPE_MON,
			Messenger::Policy::lossy_client(supported,
							CEPH_FEATURE_UID));
  simple_msgr->set_policy(entity_name_t::TYPE_MDS,
			  Messenger::Policy::lossless_peer(supported,
							 CEPH_FEATURE_UID));
  simple_msgr->set_policy(entity_name_t::TYPE_CLIENT,
			  Messenger::Policy::stateful_server(supported, 0));
  r = simple_msgr->bind(cct->_conf->public_addr);
  if (r < 0)
    exit(1);

#if defined(HAVE_XIO)
  xmsgr->set_default_policy(Messenger::Policy::lossy_client(supported, required));
  xmsgr->set_policy(entity_name_t::TYPE_MON,
		    Messenger::Policy::lossy_client(supported,
						    CEPH_FEATURE_UID));
  xmsgr->set_policy(entity_name_t::TYPE_MDS,
		    Messenger::Policy::lossless_peer(supported,
						     CEPH_FEATURE_UID));
  xmsgr->set_policy(entity_name_t::TYPE_CLIENT,
		    Messenger::Policy::stateful_server(supported, 0));

  r = xmsgr->bind(simple_msgr->get_myaddr());
  if (r < 0)
    exit(1);
#endif

  common_init_finish(cct, 0);

  // monitor client
  r = monc->build_initial_monmap();
  if (r < 0)
    return r;

  simple_msgr->start();
#if defined(HAVE_XIO)
  xmsgr->start();
#endif

  // create mds
  Messenger *cluster_msgr = (cct->_conf->cluster_rdma) ?
#if defined(HAVE_XIO)
    xmsgr
#else
    simple_msgr
#endif
    : simple_msgr;

  mds = new MDSimpl(cct->_conf->name.get_id().c_str(), cluster_msgr, monc);

  r = mds->init();	// setup dispatcher?
  return 0;
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

void LibMDS::on_mds_state(int state, epoch_t epoch)
{
  ldout(cct, 1) << "on_mds_state " << state << " epoch " << epoch << dendl;

  _mdsmap::lock_guard lock(mdsmap.mtx);
  if (mdsmap.state != state) {
    mdsmap.state = state;
    mdsmap.cond.notify_all();

    if (state == MDSMap::STATE_ACTIVE) {
      if (callbacks && callbacks->mds_active)
	finisher->queue(new C_StateCb(callbacks->mds_active, this, user));
    } else if (state == MDSMap::STATE_STOPPING) {
      // make mds_shutdown calback only if we haven't called libmds_shutdown()
      if (!mdsmap.shutdown && callbacks && callbacks->mds_shutdown)
	finisher->queue(new C_StateCb(callbacks->mds_shutdown, this, user));
      mds->shutdown();

#if 0
      ms_client->shutdown();
      ms_server->shutdown();
#endif
    }
  }
  mdsmap.epoch = epoch;
}

bool LibMDS::wait_for_active(epoch_t *epoch)
{
  _mdsmap::unique_lock l(mdsmap.mtx);
  while (mdsmap.state != MDSMap::STATE_ACTIVE
      && mdsmap.state != MDSMap::STATE_STOPPING)
    mdsmap.cond.wait(l);

  *epoch = mdsmap.epoch;
  return mdsmap.state != MDSMap::STATE_STOPPING;
}

void LibMDS::join()
{
  // wait on messengers
#if 0
  ms_client->wait();
  ms_server->wait();
#endif
}

void LibMDS::shutdown()
{
  _mdsmap::unique_lock l(mdsmap.mtx);
  mdsmap.shutdown = true;
  l.unlock();

#if 0
  mds->shutdown();
#endif
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
    mds_lock_guard lock(mds_lock);

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
  unique_mds_lock ol(mds_lock);
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
  unique_mds_lock ol(mds_lock);
  mdslist.erase(id);
  ol.unlock();
}

void libmds_signal(int signum)
{
  // signal all mdslist under list lock
  mds_lock_guard lock(mds_lock);

  for (auto mds : mdslist) {
    try {
      mds.second->signal(signum);
    } catch (std::exception &e) {
      CephContext *cct = static_cast<ceph::mds::LibMDS*>(mds.second)->cct;
      lderr(cct) << "libmds_signal caught exception " << e.what() << dendl;
    }
  }
}
