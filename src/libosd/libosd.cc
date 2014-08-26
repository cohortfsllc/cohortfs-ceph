// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <mutex> // once_flag, call_once

#include "ceph_osd.h"
#include "acconfig.h"

#include "os/ObjectStore.h"
#include "osd/OSD.h"
#include "mon/MonClient.h"

#include "common/common_init.h"
#include "common/ceph_argparse.h"
//#include "global/global_context.h"
#include "include/msgr.h"
#include "common/debug.h"
#include "include/color.h"

#include "Messengers.h"

#define dout_subsys ceph_subsys_osd

namespace global
{
// Maintain a map to prevent multiple OSDs with the same name
// TODO: allow same name with different cluster name
Mutex osd_lock;

typedef std::map<int, libosd*> osdmap;
osdmap osds;

// Maintain a set of ceph contexts so we can make sure that
// g_ceph_context and g_conf are always valid, if available
class ContextSet {
  Mutex mtx;
  std::set<CephContext*> contexts;
public:
  void insert(CephContext *cct) {
    Mutex::Locker lock(mtx);
    contexts.insert(cct);
    // initialize g_ceph_context
    if (g_ceph_context == NULL) {
      g_ceph_context = cct;
      g_conf = cct->_conf;
    }
  }
  void erase(CephContext *cct) {
    Mutex::Locker lock(mtx);
    contexts.erase(cct);
    // replace g_ceph_context
    if (g_ceph_context == cct) {
      g_ceph_context = contexts.empty() ? NULL : *contexts.begin();
      g_conf = g_ceph_context ? g_ceph_context->_conf : NULL;
    }
  }
};
ContextSet contexts;
}

struct LibOSD : public libosd {
private:
  CephContext *cct;
  md_config_t *conf;

  MonClient *monc;
  ObjectStore *store;
  OSD *osd;
  OSDMessengers ms;

  int create_context(const libosd_init_args *args);

public:
  LibOSD(int whoami);
  ~LibOSD();

  int init(const libosd_init_args *args);

  // libosd interface
  int run();
  void shutdown();
  void signal(int signum);
};


LibOSD::LibOSD(int whoami)
  : libosd(whoami),
    cct(NULL),
    conf(NULL),
    monc(NULL),
    store(NULL),
    osd(NULL)
{
}

LibOSD::~LibOSD()
{
  delete osd;
  delete monc;
  if (cct) {
    cct->put();
    global::contexts.erase(cct);
  }
}

int LibOSD::create_context(const struct libosd_init_args *args)
{
  CephInitParameters params(CEPH_ENTITY_TYPE_OSD);
  char id[12];
  sprintf(id, "%d", args->id);
  params.name.set_id(id);

  cct = common_preinit(params, CODE_ENVIRONMENT_DAEMON, 0);
  global::contexts.insert(cct);

  conf = cct->_conf;

  if (args->cluster && args->cluster[0])
    conf->cluster.assign(args->cluster);

  // parse configuration
  if (args->config && args->config[0]) {
    std::deque<std::string> parse_errors;
    int r = conf->parse_config_files(args->config, &parse_errors, &cerr, 0);
    if (r != 0) {
      derr << "libosd_init failed to parse configuration "
	<< args->config << dendl;
      return r;
    }
    conf->apply_changes(NULL);
    complain_about_parse_errors(cct, &parse_errors);
  }

  cct->init();

  return 0;
}

int LibOSD::init(const struct libosd_init_args *args)
{
  // create the CephContext and parse the configuration
  int r = create_context(args);
  if (r != 0)
    return r;

  const entity_name_t me(entity_name_t::OSD(whoami));
  const pid_t pid = getpid();

  // create and bind messengers
  r = ms.create(cct, conf, me, pid);
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

  // the store
  ObjectStore *store = ObjectStore::create(cct,
      conf->osd_objectstore,
      conf->osd_data,
      conf->osd_journal);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  // monitor client
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
  return 0;
}

int LibOSD::run()
{
  // start messengers
  ms.start();

  // start osd
  int r = osd->init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }

  // wait on messengers
  ms.wait();
  return 0;
}

void LibOSD::shutdown()
{
  osd->shutdown();
}

void LibOSD::signal(int signum)
{
  osd->handle_signal(signum);
}


// C interface

struct libosd* libosd_init(const struct libosd_init_args *args)
{
  LibOSD *osd;
  {
    using namespace global;
    // protect access to the map of osds
    Mutex::Locker lock(osd_lock);

    // existing osd with this name?
    std::pair<osdmap::iterator, bool> result =
      osds.insert(osdmap::value_type(args->id, NULL));
    if (!result.second) {
      derr << "libosd_init found existing osd." << args->id << dendl;
      return NULL;
    }

    result.first->second = osd = new LibOSD(args->id);
  }

  try {
    if (osd->init(args) == 0)
      return osd;
  } catch (std::exception &e) {
    derr << "libosd_init caught exception " << e.what() << dendl;
  }

  // remove from the map of osds
  global::osd_lock.Lock();
  global::osds.erase(args->id);
  global::osd_lock.Unlock();

  delete osd;
  return NULL;
}

int libosd_run(struct libosd *osd)
{
  try {
    return osd->run();
  } catch (std::exception &e) {
    derr << "libosd_run caught exception " << e.what() << dendl;
    return -EFAULT;
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
  delete osd;

  // remove from the map of osds
  global::osd_lock.Lock();
  global::osds.erase(id);
  global::osd_lock.Unlock();
}

void libosd_signal(int signum)
{
  // signal all osds under list lock
  Mutex::Locker lock(global::osd_lock);

  for (auto osd : global::osds) {
    try {
      osd.second->signal(signum);
    } catch (std::exception &e) {
      derr << "libosd_signal caught exception " << e.what() << dendl;
    }
  }
}
