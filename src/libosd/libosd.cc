// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ceph_osd.h"
#include "Messengers.h"

#include "os/ObjectStore.h"
#include "osd/OSD.h"
#include "mon/MonClient.h"

#include "Dispatcher.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "include/msgr.h"
#include "include/color.h"

#include "include/uuid.h"

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

class LibOSD : public libosd, private OSDStateObserver {
  libosd_callbacks *callbacks;

  CephContext *cct;
  md_config_t *conf;

  MonClient *monc;
  ObjectStore *store;
  OSD *osd;
  OSDMessengers *ms;
  LibOSDDispatcher *dispatcher;

  struct {
    Mutex mtx;
    Cond cond;
    int state;
    epoch_t epoch;
  } osdmap;

  // OSDStateObserver
  void on_osd_state(int state, epoch_t epoch);

  bool wait_for_epoch(epoch_t min);
  bool wait_for_active(epoch_t *epoch);

  int create_context(const libosd_init_args *args);

public:
  LibOSD(int whoami);
  ~LibOSD();

  int init(const libosd_init_args *args);

  // libosd interface
  void join();
  void shutdown();
  void signal(int signum);

  int get_volume(const char *name, uuid_t uuid);
  int read(const char *object, const uuid_t volume,
	   uint64_t offset, uint64_t length, char *data,
	   void *user, uint64_t *id);
  int write(const char *object, const uuid_t volume,
	    uint64_t offset, uint64_t length, char *data,
	    void *user, uint64_t *id);
};


LibOSD::LibOSD(int whoami)
  : libosd(whoami),
    callbacks(NULL),
    cct(NULL),
    conf(NULL),
    monc(NULL),
    store(NULL),
    osd(NULL),
    ms(NULL),
    dispatcher(NULL)
{
}

LibOSD::~LibOSD()
{
  delete osd;
  delete monc;
  delete ms;
  delete dispatcher;
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
  callbacks = args->callbacks;

  // create the CephContext and parse the configuration
  int r = create_context(args);
  if (r != 0)
    return r;

  const entity_name_t me(entity_name_t::OSD(whoami));
  const pid_t pid = getpid();

  // create and bind messengers
  ms = new OSDMessengers();
  r = ms->create(cct, conf, me, pid);
  if (r != 0) {
    derr << TEXT_RED << " ** ERROR: messenger creation failed: "
         << cpp_strerror(-r) << TEXT_NORMAL << dendl;
    return r;
  }

  r = ms->bind(cct, conf);
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

  common_init_finish(cct, 0);

  // monitor client
  monc = new MonClient(cct);
  r = monc->build_initial_monmap();
  if (r < 0)
    return r;

  // create osd
  osd = new OSD(cct, store, whoami,
      ms->cluster, ms->client, ms->client_xio,
      ms->client_hb, ms->front_hb, ms->back_hb,
      ms->objecter, ms->objecter_xio,
      monc, conf->osd_data, conf->osd_journal);

  // set up direct messengers
  dispatcher = new LibOSDDispatcher(cct, osd);

  // initialize osd
  r = osd->pre_init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }

  // register for state change notifications
  osd->add_state_observer(this);

  // start messengers
  ms->start();

  // start osd
  r = osd->init();
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
    return r;
  }
  return 0;
}

void LibOSD::on_osd_state(int state, epoch_t epoch)
{
  dout(1) << "on_osd_state " << state << " epoch " << epoch << dendl;

  Mutex::Locker lock(osdmap.mtx);
  if (osdmap.state != state) {
    osdmap.state = state;
    osdmap.cond.Signal();

    if (state == OSD::STATE_STOPPING)
      dispatcher->shutdown();
  }
  osdmap.epoch = epoch;
}

bool LibOSD::wait_for_epoch(epoch_t min)
{
  Mutex::Locker lock(osdmap.mtx);
  while (osdmap.epoch < min
      && osdmap.state != OSD::STATE_STOPPING)
    osdmap.cond.Wait(osdmap.mtx);

  return osdmap.state != OSD::STATE_STOPPING;
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
  dispatcher->wait();
}

void LibOSD::shutdown()
{
  osd->shutdown();
}

void LibOSD::signal(int signum)
{
  osd->handle_signal(signum);
}

int LibOSD::get_volume(const char *name, uuid_t uuid)
{
  // wait for osdmap
  epoch_t epoch;
  if (!wait_for_active(&epoch))
    return -ENODEV;

  OSDMapRef osdmap = osd->service.get_osdmap();
  VolumeRef volume;
  if (!osdmap->find_by_name(name, volume))
    return -ENOENT;

  memcpy(uuid, volume->uuid.uuid, sizeof(uuid_t));
  return 0;
}

// Dispatcher callback to fire the read completion
class OnReadReply : public LibOSDDispatcher::OnReply {
  char *data;
  uint64_t length;
  io_completion_fn completion;
  void *user;
public:
  OnReadReply(char *data, uint64_t length, io_completion_fn completion,
	      void *user)
    : data(data), length(length), completion(completion), user(user) {}

  void on_reply(ceph_tid_t tid, Message *reply) {
    assert(reply->get_type() == CEPH_MSG_OSD_OPREPLY);
    MOSDOpReply *m = static_cast<MOSDOpReply*>(reply);

    vector<OSDOp> ops;
    m->claim_ops(ops);
    m->put();

    assert(ops.size() == 1);
    OSDOp &op = *ops.begin();
    assert(op.op.op == CEPH_OSD_OP_READ);

    if (op.rval != 0) {
      length = 0;
    } else {
      assert(length >= op.op.payload_len);
      length = op.op.payload_len;

      assert(length == op.outdata.length());
      op.outdata.copy(0, length, data);
    }

    completion(tid, op.rval, length, user);
  }

  void on_failure(ceph_tid_t tid, int r) {
    completion(tid, r, 0, user);
  }
};

int LibOSD::read(const char *object, const uuid_t volume,
		 uint64_t offset, uint64_t length, char *data,
		 void *user, uint64_t *id)
{
  const int client = 0;
  const long tid = 0;
  hobject_t oid = object_t(object);
  uuid_d vol(volume);
  epoch_t epoch = 0;
  const int flags = 0;

  if (!callbacks || !callbacks->read_completion)
    return -EINVAL;

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd read op
  MOSDOp *m = new MOSDOp(client, tid, oid, vol, epoch, flags);
  m->read(offset, length);

  // create reply callback
  OnReadReply *onreply = new OnReadReply(data, length,
					 callbacks->read_completion, user);

  // send request over direct messenger
  *id = dispatcher->send_request(m, onreply);
  return 0;
}

// Dispatcher callback to fire the write completion
class OnWriteReply : public LibOSDDispatcher::OnReply {
  io_completion_fn completion;
  void *user;
public:
  OnWriteReply(io_completion_fn completion, void *user)
    : completion(completion), user(user) {}

  void on_reply(ceph_tid_t tid, Message *reply) {
    assert(reply->get_type() == CEPH_MSG_OSD_OPREPLY);
    MOSDOpReply *m = static_cast<MOSDOpReply*>(reply);

    vector<OSDOp> ops;
    m->claim_ops(ops);
    m->put();

    assert(ops.size() == 1);
    OSDOp &op = *ops.begin();
    assert(op.op.op == CEPH_OSD_OP_WRITE);

    uint64_t length = op.rval ? 0 : op.op.extent.length;
    completion(tid, op.rval, length, user);
  }

  virtual void on_failure(ceph_tid_t tid, int r) {
    completion(tid, r, 0, user);
  }
};

int LibOSD::write(const char *object, const uuid_t volume,
		  uint64_t offset, uint64_t length, char *data,
		  void *user, uint64_t *id)
{
  const int client = 0;
  const long tid = 0;
  hobject_t oid = object_t(object);
  uuid_d vol(volume);
  epoch_t epoch = 0;
  const int flags = CEPH_OSD_FLAG_ONDISK; // ONACK

  if (!callbacks || !callbacks->write_completion)
    return -EINVAL;

  if (!wait_for_active(&epoch))
    return -ENODEV;

  bufferlist bl;
  bl.append(buffer::create_static(length, data));

  // set up osd write op
  MOSDOp *m = new MOSDOp(client, tid, oid, vol, epoch, flags);
  m->write(offset, length, bl);

  // create reply callback
  OnWriteReply *onreply = new OnWriteReply(callbacks->write_completion, user);

  // send request over direct messenger
  *id = dispatcher->send_request(m, onreply);
  return 0;
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
  delete static_cast<LibOSD*>(osd);

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

int libosd_get_volume(struct libosd *osd, const char *name, uuid_t uuid)
{
  try {
    return osd->get_volume(name, uuid);
  } catch (std::exception &e) {
    derr << "libosd_get_volume caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_read(struct libosd *osd, const char *object, const uuid_t volume,
		uint64_t offset, uint64_t length, char *data,
		void *user, uint64_t *id)
{
  try {
    return osd->read(object, volume, offset, length, data, id);
  } catch (std::exception &e) {
    derr << "libosd_read caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}

int libosd_write(struct libosd *osd, const char *object, const uuid_t volume,
		 uint64_t offset, uint64_t length, char *data,
		 void *user, uint64_t *id)
{
  try {
    return osd->write(object, volume, offset, length, data, id);
  } catch (std::exception &e) {
    derr << "libosd_write caught exception " << e.what() << dendl;
    return -EFAULT;
  }
}
