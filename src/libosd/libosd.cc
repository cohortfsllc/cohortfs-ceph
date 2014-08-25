// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <mutex> // once_flag, call_once

#include "ceph_osd.h"
#include "acconfig.h"

#include "os/ObjectStore.h"
#include "osd/OSD.h"
#include "mon/MonClient.h"

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/common_init.h"
#include "include/msgr.h"
#include "common/debug.h"
#include "include/color.h"

#include "Messengers.h"

#define dout_subsys ceph_subsys_osd

namespace global
{
// global init
std::once_flag init_flag;

static void init()
{
  std::vector<const char*> args;
  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON, 0);
  common_init_finish(g_ceph_context);
}

// osd map
Mutex osd_lock;

typedef std::map<int, libosd*> osdmap;
osdmap osds;
}

struct LibOSD : public libosd {
private:
  CephContext *cct;
  md_config_t *conf;

  MonClient *monc;
  ObjectStore *store;
  OSD *osd;
  OSDMessengers ms;

public:
  LibOSD(int whoami);
  ~LibOSD();

  int init();
  void cleanup();

  // libosd interface
  void signal(int signum);
};

LibOSD::LibOSD(int whoami)
  : libosd(whoami),
    cct(g_ceph_context),
    conf(g_conf),
    monc(NULL),
    store(NULL),
    osd(NULL)
{
}

LibOSD::~LibOSD()
{
  delete osd;
  delete monc;
  cct->put();
}

int LibOSD::init()
{
  const entity_name_t me(entity_name_t::OSD(whoami));
  const pid_t pid = getpid();

  // create and bind messengers
  int r = ms.create(cct, conf, me, pid);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: messenger creation failed: "
         << cpp_strerror(-r) << TEXT_NORMAL << dendl;
    return r;
  }

  r = ms.bind(cct, conf);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: bind failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }

  // Set up crypto, daemonize, etc.
  global_init_daemonize(cct, 0);

  // the store
  ObjectStore *store = ObjectStore::create(cct,
      conf->osd_objectstore,
      conf->osd_data,
      conf->osd_journal);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  monc = new MonClient(cct);
  r = monc->build_initial_monmap();
  if (r < 0)
    return r;

  // create osd
  osd = new OSD(cct, store, whoami,
      ms.cluster, ms.client, ms.client_xio,
      ms.client_hb, ms.front_hb, ms.back_hb,
      ms.objecter, ms.objecter_xio,
      monc, conf->osd_data, conf->osd_journal);

  r = osd->pre_init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }

  // start messengers
  ms.start();

  // start osd
  r = osd->init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }
  return 0;
}

void LibOSD::cleanup()
{
  // close/wait on messengers
  ms.cleanup();
}

void LibOSD::signal(int signum)
{
  osd->handle_signal(signum);
}


// C interface

struct libosd* libosd_init(int name)
{
  // call global_init() on first entry
  std::call_once(global::init_flag, global::init);

  LibOSD *osd;
  {
    using namespace global;
    Mutex::Locker lock(osd_lock);

    // existing osd with this name?
    std::pair<osdmap::iterator, bool> result =
      osds.insert(osdmap::value_type(name, NULL));
    if (!result.second)
      return NULL;

    result.first->second = osd = new LibOSD(name);
  }

  if (osd->init() == 0)
    return osd;

  delete osd;
  return NULL;
}

void libosd_cleanup(struct libosd *osd)
{
  static_cast<LibOSD*>(osd)->cleanup();

  global::osd_lock.Lock();
  global::osds.erase(osd->whoami);
  global::osd_lock.Unlock();

  delete osd;
}

void libosd_signal(int signum)
{
  // signal all osds under list lock
  Mutex::Locker lock(global::osd_lock);

  for (auto osd : global::osds)
    osd.second->signal(signum);
}
