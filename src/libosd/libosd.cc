// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ceph_osd.h"

#include "Context.h"
#include "Dispatcher.h"
#include "Messengers.h"
#include "Objecter.h"

#include "os/ObjectStore.h"
#include "osd/OSD.h"
#include "mon/MonClient.h"

#include "msg/DirectMessenger.h"
#include "msg/FastStrategy.h"

#include "common/Finisher.h"
#include "common/common_init.h"
#include "include/color.h"

#define dout_subsys ceph_subsys_osd

namespace
{
// Maintain a map to prevent multiple OSDs with the same name
// TODO: allow same name with different cluster name
Mutex osd_lock;

typedef std::map<int, libosd*> osdmap;
osdmap osds;
}

namespace ceph
{
namespace osd
{

class LibOSD : private Objecter, public libosd, private OSDStateObserver {
  Context ctx;
  libosd_callbacks *callbacks;
  void *user;
  Finisher *finisher; // thread to send callbacks to user

  MonClient *monc;
  ObjectStore *store;
  OSD *osd;
  OSDMessengers *ms;
  DirectMessenger *ms_client, *ms_server; // DirectMessenger pair

  struct {
    Mutex mtx;
    Cond cond;
    int state;
    epoch_t epoch;
    bool shutdown;
  } osdmap;

  // OSDStateObserver
  void on_osd_state(int state, epoch_t epoch);

  // Objecter
  bool wait_for_active(epoch_t *epoch);

  void init_dispatcher(OSD *osd);

public:
  LibOSD(int whoami);
  ~LibOSD();

  int init(const libosd_init_args *args);

  // libosd interface
  void join();
  void shutdown();
  void signal(int signum);

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


LibOSD::LibOSD(int whoami)
  : libosd(whoami),
    callbacks(nullptr),
    user(nullptr),
    finisher(nullptr),
    monc(nullptr),
    store(nullptr),
    osd(nullptr),
    ms(nullptr),
    ms_client(nullptr),
    ms_server(nullptr)
{
  osdmap.state = 0;
  osdmap.epoch = 0;
  osdmap.shutdown = false;
}

LibOSD::~LibOSD()
{
  delete osd;
  delete monc;
  delete ms_server;
  delete ms_client;
  delete ms;
  if (finisher) {
    finisher->stop();
    delete finisher;
  }
}

int LibOSD::init(const struct libosd_init_args *args)
{
  callbacks = args->callbacks;
  user = args->user;

  // create the CephContext and parse the configuration
  int r = ctx.create(args->id, args->config, args->cluster);
  if (r != 0)
    return r;

  const entity_name_t me(entity_name_t::OSD(whoami));
  const pid_t pid = getpid();

  // create and bind messengers
  ms = new OSDMessengers();
  r = ms->create(ctx.cct, ctx.conf, me, pid);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: messenger creation failed: "
         << cpp_strerror(-r) << TEXT_NORMAL << dendl;
    return r;
  }

  r = ms->bind(ctx.cct, ctx.conf);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: bind failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }

  // the store
  ObjectStore *store = ObjectStore::create(ctx.cct,
      ctx.conf->osd_objectstore,
      ctx.conf->osd_data,
      ctx.conf->osd_journal);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  common_init_finish(ctx.cct, 0);

  // monitor client
  monc = new MonClient(ctx.cct);
  r = monc->build_initial_monmap();
  if (r < 0)
    return r;

  // create osd
  osd = new OSD(ctx.cct, store, whoami,
      ms->cluster, ms->client, ms->client_xio,
      ms->client_hb, ms->front_hb, ms->back_hb,
      ms->objecter, ms->objecter_xio,
      monc, ctx.conf->osd_data, ctx.conf->osd_journal);

  // set up the dispatcher
  init_dispatcher(osd);

  // initialize osd
  r = osd->pre_init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }

  // start callback finisher thread
  finisher = new Finisher(ctx.cct);
  finisher->start();

  // register for state change notifications
  osd->add_state_observer(this);

  // start messengers
  ms->start();
  ms_client->start();
  ms_server->start();

  // start osd
  r = osd->init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }
  return 0;
}

void LibOSD::init_dispatcher(OSD *osd)
{
  const entity_name_t name(entity_name_t::CLIENT(whoami));

  // construct and attach the direct messenger pair
  ms_client = new DirectMessenger(ctx.cct, name, "direct osd client",
                                  0, new FastStrategy());
  ms_server = new DirectMessenger(ctx.cct, name, "direct osd server",
                                  0, new FastStrategy());

  ms_client->set_direct_peer(ms_server);
  ms_server->set_direct_peer(ms_client);

  ms_server->add_dispatcher_head(osd);

  // create a client connection
  ConnectionRef conn = ms_client->get_connection(ms_server->get_myinst());

  // attach a session; we bypass OSD::ms_verify_authorizer, which
  // normally takes care of this
  OSD::Session *s = new OSD::Session;
  s->con = conn;
  s->entity_name.set_name(ms_client->get_myname());
  s->auid = CEPH_AUTH_UID_DEFAULT;
  conn->set_priv(s);

  // allocate the dispatcher
  dispatcher.reset(new Dispatcher(ctx.cct, ms_client, conn));

  ms_client->add_dispatcher_head(dispatcher.get());
}

struct C_StateCb : public ::Context {
  typedef void (*callback_fn)(struct libosd *osd, void *user);
  callback_fn cb;
  libosd *osd;
  void *user;
  C_StateCb(callback_fn cb, libosd *osd, void *user)
    : cb(cb), osd(osd), user(user) {}
  void finish(int r) {
    cb(osd, user);
  }
};

void LibOSD::on_osd_state(int state, epoch_t epoch)
{
  dout(1) << "on_osd_state " << state << " epoch " << epoch << dendl;

  Mutex::Locker lock(osdmap.mtx);
  if (osdmap.state != state) {
    osdmap.state = state;
    osdmap.cond.Signal();

    if (state == OSD::STATE_ACTIVE) {
      if (callbacks && callbacks->osd_active)
	finisher->queue(new C_StateCb(callbacks->osd_active, this, user));
    } else if (state == OSD::STATE_STOPPING) {
      // make osd_shutdown calback only if we haven't called libosd_shutdown()
      if (!osdmap.shutdown && callbacks && callbacks->osd_shutdown)
	finisher->queue(new C_StateCb(callbacks->osd_shutdown, this, user));
      dispatcher->shutdown();

      ms_client->shutdown();
      ms_server->shutdown();
    }
  }
  osdmap.epoch = epoch;
}

bool LibOSD::wait_for_active(epoch_t *epoch)
{
  Mutex::Locker lock(osdmap.mtx);
  while (osdmap.state != OSD::STATE_ACTIVE
      && osdmap.state != OSD::STATE_STOPPING)
    osdmap.cond.Wait(osdmap.mtx);

  *epoch = osdmap.epoch;
  return osdmap.state != OSD::STATE_STOPPING;
}

void LibOSD::join()
{
  // wait on messengers
  ms->wait();
  ms_client->wait();
  ms_server->wait();
}

void LibOSD::shutdown()
{
  osdmap.mtx.Lock();
  osdmap.shutdown = true;
  osdmap.mtx.Unlock();

  osd->shutdown();
}

void LibOSD::signal(int signum)
{
  osd->handle_signal(signum);
}

int LibOSD::get_volume(const char *name, uint8_t id[16])
{
  // wait for osdmap
  epoch_t epoch;
  if (!wait_for_active(&epoch))
    return -ENODEV;

  OSDMapRef osdmap = osd->service.get_osdmap();
  VolumeRef volume;
  if (!osdmap->find_by_name(name, volume))
    return -ENOENT;

  memcpy(id, &volume->id, sizeof(volume->id));
  return 0;
}

} // namespace osd
} // namespace ceph


// C interface

struct libosd* libosd_init(const struct libosd_init_args *args)
{
  if (args == nullptr)
    return nullptr;

  ceph::osd::LibOSD *osd;
  {
    // protect access to the map of osds
    Mutex::Locker lock(osd_lock);

    // existing osd with this name?
    std::pair<osdmap::iterator, bool> result =
      osds.insert(osdmap::value_type(args->id, nullptr));
    if (!result.second) {
      derr << "libosd_init found existing osd." << args->id << dendl;
      return nullptr;
    }

    result.first->second = osd = new ceph::osd::LibOSD(args->id);
  }

  try {
    if (osd->init(args) == 0)
      return osd;
  } catch (std::exception &e) {
    derr << "libosd_init caught exception " << e.what() << dendl;
  }

  // remove from the map of osds
  osd_lock.Lock();
  osds.erase(args->id);
  osd_lock.Unlock();

  delete osd;
  return nullptr;
}

void libosd_join(struct libosd *osd)
{
  try {
    osd->join();
  } catch (std::exception &e) {
    derr << "libosd_join caught exception " << e.what() << dendl;
  }
}

void libosd_shutdown(struct libosd *osd)
{
  try {
    osd->shutdown();
  } catch (std::exception &e) {
    derr << "libosd_shutdown caught exception " << e.what() << dendl;
  }
}

void libosd_cleanup(struct libosd *osd)
{
  // assert(!running)
  const int id = osd->whoami;
  // delete LibOSD because base destructor is protected
  delete static_cast<ceph::osd::LibOSD*>(osd);

  // remove from the map of osds
  osd_lock.Lock();
  osds.erase(id);
  osd_lock.Unlock();
}

void libosd_signal(int signum)
{
  // signal all osds under list lock
  Mutex::Locker lock(osd_lock);

  for (auto osd : osds) {
    try {
      osd.second->signal(signum);
    } catch (std::exception &e) {
      derr << "libosd_signal caught exception " << e.what() << dendl;
    }
  }
}

int libosd_get_volume(struct libosd *osd, const char *name,
		      uint8_t id[16])
{
  try {
    return osd->get_volume(name, id);
  } catch (std::exception &e) {
    derr << "libosd_get_volume caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_read(struct libosd *osd, const char *object,
		const uint8_t volume[16], uint64_t offset, uint64_t length,
		char *data, int flags, libosd_io_completion_fn cb,
		void *user)
{
  try {
    return osd->read(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    derr << "libosd_read caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_write(struct libosd *osd, const char *object, const uint8_t volume[16],
		 uint64_t offset, uint64_t length, char *data, int flags,
		 libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->write(object, volume, offset, length, data, flags, cb, user);
  } catch (std::exception &e) {
    derr << "libosd_write caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_truncate(struct libosd *osd, const char *object,
		    const uint8_t volume[16], uint64_t offset,
		    int flags, libosd_io_completion_fn cb, void *user)
{
  try {
    return osd->truncate(object, volume, offset, flags, cb, user);
  } catch (std::exception &e) {
    derr << "libosd_truncate caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}
