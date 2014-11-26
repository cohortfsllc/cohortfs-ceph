// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ceph_osd_proxy.h"

#include "Context.h"
#include "Dispatcher.h"
#include "Objecter.h"

#include "mon/MonClient.h"
#include "messages/MOSDMap.h"

#ifdef HAVE_XIO
#include "msg/XioMessenger.h"
#include "msg/FastStrategy.h"
#endif

#include "common/common_init.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix (*_dout << "libosd_proxy ")

namespace ceph
{
namespace osd
{

class LibOSDProxy : public libosd_proxy, private Objecter, private ::Dispatcher
{
 private:
  Context ctx;
  std::unique_ptr<Messenger> ms;
  std::unique_ptr<MonClient> monc;

  Mutex mtx;
  Cond cond; // signaled on updates to map.epoch
  OSDMap map;

  bool shutdown;
  int waiters;
  Cond shutdown_cond; // signaled on waiters->0

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
  LibOSDProxy(int whoami);
  ~LibOSDProxy();

  int init(const libosd_proxy_args *args);

  // libosd_proxy interface
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

LibOSDProxy::LibOSDProxy(int whoami)
  : libosd_proxy(whoami),
    ::Dispatcher(nullptr),
    shutdown(0),
    waiters(0)
{
}

LibOSDProxy::~LibOSDProxy()
{
  // allow any threads waiting on a map to see 'shutdown'
  Mutex::Locker lock(mtx);
  shutdown = true;
  while (waiters)
    shutdown_cond.Wait(mtx);

  if (monc)
    monc->shutdown();
  if (ms) {
    ms->shutdown();
    ms->wait();
  }
}

int LibOSDProxy::init(const struct libosd_proxy_args *args)
{
  // create the CephContext and parse the configuration
  int r = ctx.create(args->id, args->config, args->cluster);
  if (r != 0)
    return r;

  // fix Dispatcher's cct
  ::Dispatcher::cct = ctx.cct;

  common_init_finish(ctx.cct, 0);

  // create messenger for the monitor and osd
#ifdef HAVE_XIO
  if (ctx.conf->client_rdma) {
    XioMessenger *xmsgr = new XioMessenger(ctx.cct, entity_name_t::CLIENT(-1),
                                           "libosd proxy", getpid(), 1,
                                           new FastStrategy());
    xmsgr->set_port_shift(111);
    ms.reset(xmsgr);
  } else
    ms.reset(Messenger::create(ctx.cct, entity_name_t::CLIENT(-1),
                               "libosd proxy", getpid()));
#else
  ms.reset(Messenger::create(ctx.cct, entity_name_t::CLIENT(-1),
                             "libosd proxy", getpid()));
#endif
  ms->start();

  // monitor client
  monc.reset(new MonClient(ctx.cct));
  r = monc->build_initial_monmap();
  if (r < 0) {
    derr << "failed to build initial monmap: " << cpp_strerror(-r) << dendl;
    return r;
  }

  monc->set_messenger(ms.get());
  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);

  ldout(ctx.cct, 0) << "init calling monclient.init" << dendl;
  r = monc->init();
  if (r < 0) {
    derr << "failed to initialize monclient: " << cpp_strerror(-r) << dendl;
    return r;
  }

  ldout(ctx.cct, 0) << "init calling monclient.authenticate" << dendl;
  r = monc->authenticate(ctx.conf->client_mount_timeout);
  if (r < 0) {
    derr << "failed to authenticate monclient: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ms->set_myname(entity_name_t::CLIENT(monc->get_global_id()));

  // request an OSDMap
  ms->add_dispatcher_tail(this);
  monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  // wait for an OSDMap that shows osd.whoami is up
  Mutex::Locker lock(mtx);
  ++waiters;
  while (!map.is_up(whoami) && !shutdown)
    cond.Wait(mtx);
  if (--waiters == 0)
    shutdown_cond.Signal();

  return shutdown ? -1 : 0;
}

void LibOSDProxy::init_dispatcher(const entity_inst_t &inst)
{
  assert(!dispatcher);

  // create a connection to the osd
  ConnectionRef conn = ms->get_connection(inst);

  // create a dispatcher for osd replies
  dispatcher.reset(new osd::Dispatcher(ctx.cct, ms.get(), conn));

  ms->add_dispatcher_head(dispatcher.get());
}

bool LibOSDProxy::wait_for_active(epoch_t *epoch)
{
  Mutex::Locker lock(mtx);
  ++waiters;
  while (!map.is_up(whoami) && !shutdown)
    cond.Wait(mtx);

  *epoch = map.get_epoch();

  if (--waiters == 0)
    shutdown_cond.Signal();
  return !shutdown;
}

void LibOSDProxy::handle_osd_map(MOSDMap *m)
{
  ldout(ctx.cct, 3) << "handle_osd_map " << *m << dendl;

  Mutex::Locker lock(mtx);

  if (m->get_last() <= map.get_epoch()) {
    // already have everything
    ldout(ctx.cct, 3) << "ignoring epochs ["
        << m->get_first() << "," << m->get_last()
        << "] <= " << map.get_epoch() << dendl;
  } else if (map.get_epoch() == 0) {
    // need a full map
    auto f = m->maps.find(m->get_last());
    if (f == m->maps.end()) {
      ldout(ctx.cct, 3) << "requesting a full map" << dendl;
      if (monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME))
        monc->renew_subs();
    } else {
      ldout(ctx.cct, 3) << "decoding full epoch " << m->get_last() << dendl;
      map.decode(f->second);
    }
  } else {
    // apply incremental maps
    for (epoch_t e = map.get_epoch() + 1; e <= m->get_last(); e++) {
      // apply the incremental if we have epoch-1
      if (map.get_epoch() == e-1) {
        auto i = m->incremental_maps.find(e);
        if (i != m->incremental_maps.end()) {
          ldout(ctx.cct, 3) << "applying incremental epoch " << e << dendl;
          OSDMap::Incremental inc(i->second);
          map.apply_incremental(inc);
          continue;
        }
      }

      // decode the full map if we have it
      auto f = m->maps.find(e);
      if (f != m->maps.end()) {
        ldout(ctx.cct, 3) << "decoding full epoch " << e << dendl;
        map.decode(f->second);
        continue;
      }

      if (e <= m->get_oldest()) {
        ldout(ctx.cct, 3) << "missing epoch " << e
            << ", jumping to oldest epoch " << m->get_oldest() << dendl;
        e = m->get_oldest() - 1;
        continue;
      }

      ldout(ctx.cct, 3) << "requesting missing epoch " << e << dendl;
      if (monc->sub_want("osdmap", e, CEPH_SUBSCRIBE_ONETIME))
        monc->renew_subs();
      break;
    }
  }

  m->put();
  monc->sub_got("osdmap", map.get_epoch());
  cond.Signal();

  if (map.get_epoch() > 0 && !dispatcher) {
    if (!map.is_up(whoami)) {
      ldout(ctx.cct, 3) << "osd." << whoami << " is not up at epoch "
          << map.get_epoch() << dendl;
      // wait for next epoch
      if (monc->sub_want("osdmap", map.get_epoch()+1, CEPH_SUBSCRIBE_ONETIME))
        monc->renew_subs();
      return;
    }

    init_dispatcher(map.get_inst(whoami));
  }
}

bool LibOSDProxy::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                                    bool force_new)
{
  ldout(ctx.cct, 3) << "ms_get_authorizer" << dendl;
  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

bool LibOSDProxy::ms_dispatch(Message *m)
{
  // only handle MOSDMap
  if (m->get_type() == CEPH_MSG_OSD_MAP) {
    handle_osd_map(static_cast<MOSDMap*>(m));
    return true;
  }
  return false;
}

int LibOSDProxy::get_volume(const char *name, uint8_t id[16])
{
  // wait for osdmap
  Mutex::Locker lock(mtx);
  while (map.get_epoch() == 0 && !shutdown)
    cond.Wait(mtx);

  if (shutdown)
    return -ENODEV;

  VolumeRef volume;
  if (!map.find_by_name(name, volume))
    return -ENOENT;

  memcpy(id, &volume->id, sizeof(volume->id));
  return 0;
}

} // namespace osd
} // namespace ceph


// C interface

struct libosd_proxy* libosd_proxy_init(const struct libosd_proxy_args *args)
{
  if (args == nullptr)
    return nullptr;

  ceph::osd::LibOSDProxy *osd = new ceph::osd::LibOSDProxy(args->id);
  try {
    if (osd->init(args) == 0)
      return osd;
  } catch (std::exception &e) {
    derr << "libosd_proxy_init caught exception " << e.what() << dendl;
  }

  delete osd;
  return nullptr;
}

void libosd_proxy_cleanup(struct libosd_proxy *osd)
{
  // assert(!running)
  // delete LibOSDProxy because base destructor is protected
  delete static_cast<ceph::osd::LibOSDProxy*>(osd);
}

int libosd_proxy_get_volume(struct libosd_proxy *osd, const char *name,
                            uint8_t id[16])
{
  try {
    return osd->get_volume(name, id);
  } catch (std::exception &e) {
    derr << "libosd_proxy_get_volume caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_proxy_read(struct libosd_proxy *osd, const char *object,
                      const uint8_t volume[16], uint64_t offset,
                      uint64_t length, char *data, int flags,
                      libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->read(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    derr << "libosd_proxy_read caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_proxy_write(struct libosd_proxy *osd, const char *object,
                       const uint8_t volume[16], uint64_t offset,
                       uint64_t length, char *data, int flags,
                       libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->write(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    derr << "libosd_proxy_write caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_proxy_truncate(struct libosd_proxy *osd, const char *object,
                          const uint8_t volume[16], uint64_t offset,
                          int flags, libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->truncate(object, volume, offset, flags, cb, user);
  } catch (std::exception &e) {
    derr << "libosd_proxy_truncate caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}
