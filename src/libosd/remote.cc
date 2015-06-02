// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>
#include "include/ceph_time.h"
#include "ceph_osd_remote.h"

#include "Context.h"
#include "Dispatcher.h"
#include "Objecter.h"

#include "mon/MonClient.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#ifdef HAVE_XIO
#include "msg/XioMessenger.h"
#include "msg/FastStrategy.h"
#endif

#include "common/common_init.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix (*_dout << "libosd_remote ")

namespace ceph
{
namespace osd
{

class LibOSDRemote : public libosd_remote, private Objecter,
		     private ::Dispatcher {
 public:
  CephContext*& cct;
 private:
  struct Factory : public MessageFactory {
    MessageFactory *parent;
    Message* create(int type);
  };
  Factory factory;
  std::unique_ptr<Messenger> ms;
  std::unique_ptr<MonClient> monc;

  std::mutex mtx;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  std::condition_variable cond; // signaled on updates to map.epoch
  OSDMap map;

  bool shutdown;
  int waiters;
  std::condition_variable shutdown_cond; // signaled on waiters->0

  // Objecter
  bool wait_for_active(epoch_t *epoch);

  void init_dispatcher(const entity_inst_t &inst);

  // Dispatcher for MOSDMap
  bool ms_dispatch(Message *m);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			 bool force_new);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}

  void handle_osd_map(MOSDMap *m);

 public:
  LibOSDRemote(int whoami);
  ~LibOSDRemote();

  int init(const libosd_remote_args *args);

  // libosd_remote interface
  int get_volume(const char *name, uint8_t id[16]);

  // read/write/truncate satisfied by Objecter
  int read(const char *object, const uint8_t volume[16],
	   uint64_t offset, uint64_t length, char *data,
	   int flags, libosd_io_completion_fn cb, void *user) {
    return Objecter::read(object, volume, offset, length,
			  data, flags, cb, user);
  }
  int write(const char *object, const uint8_t volume[16],
	    uint64_t offset, uint64_t length, char *data,
	    int flags, libosd_io_completion_fn cb, void *user) {
    return Objecter::write(object, volume, offset, length,
			   data, flags, cb, user);
  }
  int truncate(const char *object, const uint8_t volume[16], uint64_t offset,
	       int flags, libosd_io_completion_fn cb, void *user) {
    return Objecter::truncate(object, volume, offset, flags, cb, user);
  }
};

Message* LibOSDRemote::Factory::create(int type)
{
  switch (type) {
  case CEPH_MSG_OSD_OPREPLY:  return new MOSDOpReply;
  case CEPH_MSG_OSD_MAP:      return new MOSDMap;
  default: return parent ? parent->create(type) : nullptr;
  }
}

LibOSDRemote::LibOSDRemote(int whoami)
  : libosd_remote(whoami),
    ::Dispatcher(nullptr),
    cct(::Dispatcher::cct),
    shutdown(0),
    waiters(0)
{
}

LibOSDRemote::~LibOSDRemote()
{
  // allow any threads waiting on a map to see 'shutdown'
  unique_lock lock(mtx);
  shutdown = true;
  while (waiters)
    shutdown_cond.wait(lock);

  if (monc)
    monc->shutdown();
  if (ms) {
    ms->shutdown();
    ms->wait();
  }
}

int LibOSDRemote::init(const struct libosd_remote_args *args)
{
  // create the CephContext and parse the configuration
  int r = ceph::osd::context_create(args->id, args->config, args->cluster,
                                    args->argc, args->argv, &cct);
  if (r != 0)
    return r;

  common_init_finish(cct, 0);

  // monitor client
  monc.reset(new MonClient(cct));
  r = monc->build_initial_monmap();
  if (r < 0) {
    lderr(cct) << "failed to build initial monmap: "
		   << cpp_strerror(-r) << dendl;
    return r;
  }
  factory.parent = &monc->factory;

  // create messenger for the monitor and osd
#ifdef HAVE_XIO
  if (cct->_conf->client_rdma) {
    XioMessenger *xmsgr = new XioMessenger(cct, entity_name_t::CLIENT(-1),
					   "libosd remote", getpid(),
					   &factory, 1, new FastStrategy());
    xmsgr->set_port_shift(111);
    ms.reset(xmsgr);
  } else
    ms.reset(Messenger::create(cct, entity_name_t::CLIENT(-1),
			       "libosd remote", getpid(), &factory));
#else
  ms.reset(Messenger::create(cct, entity_name_t::CLIENT(-1),
			     "libosd remote", getpid(), &factory));
#endif
  ms->start();

  monc->set_messenger(ms.get());
  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);

  r = monc->init();
  if (r < 0) {
    lderr(cct) << "failed to initialize monclient: "
		   << cpp_strerror(-r) << dendl;
    return r;
  }

  r = monc->authenticate(cct->_conf->client_mount_timeout);
  if (r < 0) {
    lderr(cct) << "failed to authenticate monclient: "
		   << cpp_strerror(-r) << dendl;
    return r;
  }
  ms->set_myname(entity_name_t::CLIENT(monc->get_global_id()));

  // request an OSDMap
  ms->add_dispatcher_tail(this);
  monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  ldout(cct, 1) << "waiting for osd map" << dendl;

  // wait for an OSDMap that shows osd.whoami is up
  unique_lock lock(mtx);
  ++waiters;
  while (!map.is_up(whoami) && !shutdown)
    cond.wait(lock);
  if (--waiters == 0)
    shutdown_cond.notify_all();

  return shutdown ? -1 : 0;
}

void LibOSDRemote::init_dispatcher(const entity_inst_t &inst)
{
  assert(!dispatcher);

  // create a connection to the osd
  ConnectionRef conn = ms->get_connection(inst);

  // create a dispatcher for osd replies
  dispatcher.reset(new osd::Dispatcher(cct, ms.get(), conn));

  ms->add_dispatcher_head(dispatcher.get());
}

bool LibOSDRemote::wait_for_active(epoch_t *epoch)
{
  unique_lock lock(mtx);
  ++waiters;
  while (!map.is_up(whoami) && !shutdown)
    cond.wait(lock);

  *epoch = map.get_epoch();

  if (--waiters == 0)
    shutdown_cond.notify_all();
  return !shutdown;
}

void LibOSDRemote::handle_osd_map(MOSDMap *m)
{
  ldout(cct, 3) << "handle_osd_map " << *m << dendl;

  lock_guard lock(mtx);

  if (m->get_last() <= map.get_epoch()) {
    // already have everything
    ldout(cct, 3) << "ignoring epochs ["
		      << m->get_first() << "," << m->get_last()
		      << "] <= " << map.get_epoch() << dendl;
  } else if (map.get_epoch() == 0) {
    // need a full map
    auto f = m->maps.find(m->get_last());
    if (f == m->maps.end()) {
      ldout(cct, 3) << "requesting a full map" << dendl;
      if (monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME))
	monc->renew_subs();
    } else {
      ldout(cct, 3) << "decoding full epoch " << m->get_last() << dendl;
      map.decode(f->second);
    }
  } else {
    // apply incremental maps
    for (epoch_t e = map.get_epoch() + 1; e <= m->get_last(); e++) {
      // apply the incremental if we have epoch-1
      if (map.get_epoch() == e-1) {
	auto i = m->incremental_maps.find(e);
	if (i != m->incremental_maps.end()) {
	  ldout(cct, 3) << "applying incremental epoch " << e << dendl;
	  OSDMap::Incremental inc(i->second);
	  map.apply_incremental(inc);
	  continue;
	}
      }

      // decode the full map if we have it
      auto f = m->maps.find(e);
      if (f != m->maps.end()) {
	ldout(cct, 3) << "decoding full epoch " << e << dendl;
	map.decode(f->second);
	continue;
      }

      if (e <= m->get_oldest()) {
	ldout(cct, 3) << "missing epoch " << e
			  << ", jumping to oldest epoch " << m->get_oldest()
			  << dendl;
	e = m->get_oldest() - 1;
	continue;
      }

      ldout(cct, 3) << "requesting missing epoch " << e << dendl;
      if (monc->sub_want("osdmap", e, CEPH_SUBSCRIBE_ONETIME))
	monc->renew_subs();
      break;
    }
  }

  m->put();
  monc->sub_got("osdmap", map.get_epoch());
  cond.notify_all();

  if (map.get_epoch() > 0 && !dispatcher) {
    if (!map.is_up(whoami)) {
      ldout(cct, 3) << "osd." << whoami << " is not up at epoch "
			<< map.get_epoch() << dendl;
      // wait for next epoch
      if (monc->sub_want("osdmap", map.get_epoch()+1, CEPH_SUBSCRIBE_ONETIME))
	monc->renew_subs();
      return;
    }

    init_dispatcher(map.get_inst(whoami));
  }
}

bool LibOSDRemote::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
				     bool force_new)
{
  ldout(cct, 3) << "ms_get_authorizer" << dendl;
  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

bool LibOSDRemote::ms_dispatch(Message *m)
{
  // only handle MOSDMap
  if (m->get_type() == CEPH_MSG_OSD_MAP) {
    handle_osd_map(static_cast<MOSDMap*>(m));
    return true;
  }
  return false;
}

int LibOSDRemote::get_volume(const char *name, uint8_t id[16])
{
  // wait for osdmap (doesn't matter whether our osd is up yet)
  unique_lock lock(mtx);
  while (map.get_epoch() == 0 && !shutdown)
    cond.wait(lock);

  if (shutdown)
    return -ENODEV;

  try {
    Volume volume(map.lookup_volume(name));
    memcpy(id, &volume.id, sizeof(volume.id));
    return 0;
  } catch (std::system_error& e) {
    return -ENOENT;
  }
}

} // namespace osd
} // namespace ceph


// C interface

struct libosd_remote* libosd_remote_init(const struct libosd_remote_args *args)
{
  if (args == nullptr)
    return nullptr;

  ceph::osd::LibOSDRemote *osd = new ceph::osd::LibOSDRemote(args->id);
  try {
    if (osd->init(args) == 0)
      return osd;
  } catch (std::exception &e) {
    //derr << "libosd_remote_init caught exception " << e.what() << dendl;
  }

  delete osd;
  return nullptr;
}

void libosd_remote_cleanup(struct libosd_remote *osd)
{
  // assert(!running)
  // delete LibOSDRemote because base destructor is protected
  delete static_cast<ceph::osd::LibOSDRemote*>(osd);
}

int libosd_remote_get_volume(struct libosd_remote *osd, const char *name,
			     uint8_t id[16])
{
  try {
    return osd->get_volume(name, id);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSDRemote*>(osd)->cct;
    lderr(cct) << "libosd_remote_get_volume caught exception "
	       << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_remote_read(struct libosd_remote *osd, const char *object,
		       const uint8_t volume[16], uint64_t offset,
		       uint64_t length, char *data, int flags,
		       libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->read(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSDRemote*>(osd)->cct;
    lderr(cct) << "libosd_remote_read caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_remote_write(struct libosd_remote *osd, const char *object,
			const uint8_t volume[16], uint64_t offset,
			uint64_t length, char *data, int flags,
			libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->write(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSDRemote*>(osd)->cct;
    lderr(cct) << "libosd_remote_write caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_remote_truncate(struct libosd_remote *osd, const char *object,
			   const uint8_t volume[16], uint64_t offset,
			   int flags, libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->truncate(object, volume, offset, flags, cb, user);
  } catch (std::exception &e) {
    CephContext *cct = static_cast<ceph::osd::LibOSDRemote*>(osd)->cct;
    lderr(cct) << "libosd_remote_truncate caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}
