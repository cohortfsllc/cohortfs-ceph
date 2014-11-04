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
#include "common/Finisher.h"
#include "include/msgr.h"
#include "include/color.h"

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
  void *user;
  Finisher *finisher; // thread to send callbacks to user

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
    bool shutdown;
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

  int get_volume(const char *name, uint8_t id[16]);
  int read(const char *object, const uint8_t volume[16],
	   uint64_t offset, uint64_t length, char *data,
	   int flags, libosd_io_completion_fn cb, void *user);
  int write(const char *object, const uint8_t volume[16],
	    uint64_t offset, uint64_t length, char *data,
	    int flags, libosd_io_completion_fn cb, void *user);
  int truncate(const char *object, const uint8_t volume[16], uint64_t offset,
	       int flags, libosd_io_completion_fn cb, void *user);
};


LibOSD::LibOSD(int whoami)
  : libosd(whoami),
    callbacks(NULL),
    user(NULL),
    finisher(NULL),
    cct(NULL),
    conf(NULL),
    monc(NULL),
    store(NULL),
    osd(NULL),
    ms(NULL),
    dispatcher(NULL)
{
    osdmap.state = 0;
    osdmap.epoch = 0;
    osdmap.shutdown = false;
}

LibOSD::~LibOSD()
{
  delete osd;
  delete monc;
  delete ms;
  delete dispatcher;
  if (finisher) {
    finisher->stop();
    delete finisher;
  }
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
  std::deque<std::string> parse_errors;
  int r = conf->parse_config_files(args->config, &parse_errors, &cerr, 0);
  if (r != 0) {
    derr << "libosd_init failed to parse configuration "
      << args->config << dendl;
    return r;
  }
  conf->apply_changes(NULL);
  complain_about_parse_errors(cct, &parse_errors);

  cct->init();

  return 0;
}

int LibOSD::init(const struct libosd_init_args *args)
{
  callbacks = args->callbacks;
  user = args->user;

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

  // start callback finisher thread
  finisher = new Finisher(cct);
  finisher->start();

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

struct C_StateCb : public Context {
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
    }
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

class SyncCompletion {
private:
  Mutex mutex;
  Cond cond;
  bool done;
  int result;
  int length;

  void signal(int r, int len) {
    Mutex::Locker lock(mutex);
    done = true;
    result = r;
    length = len;
    cond.Signal();
  }

public:
  SyncCompletion() : done(false) {}

  int wait() {
    Mutex::Locker lock(mutex);
    while (!done)
      cond.Wait(mutex);
    return result != 0 ? result : length;
  }

  // libosd_io_completion_fn to signal the condition variable
  static void callback(int result, uint64_t length, int flags, void *user) {
    SyncCompletion *sync = static_cast<SyncCompletion*>(user);
    sync->signal(result, length);
  }
};

// Dispatcher callback to fire the read completion
class OnReadReply : public LibOSDDispatcher::OnReply {
  char *data;
  uint64_t length;
  libosd_io_completion_fn cb;
  void *user;
public:
  OnReadReply(char *data, uint64_t length,
	      libosd_io_completion_fn cb, void *user)
    : data(data), length(length), cb(cb), user(user) {}

  void on_reply(Message *reply) {
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
      assert(length >= op.outdata.length());
      length = op.outdata.length();
      op.outdata.copy(0, length, data);
    }

    cb(op.rval, length, 0, user);
  }

  void on_failure(int r) {
    cb(r, 0, 0, user);
  }
};

int LibOSD::read(const char *object, const uint8_t volume[16],
		 uint64_t offset, uint64_t length, char *data,
		 int flags, libosd_io_completion_fn cb, void *user)
{
  const int client = 0;
  const long tid = 0;
  hobject_t oid = object_t(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  std::unique_ptr<SyncCompletion> sync;
  memcpy(&vol, volume, sizeof(vol));

  if (!cb) {
    // set up a synchronous completion
    cb = SyncCompletion::callback;
    sync.reset(new SyncCompletion());
    user = sync.get();
  }

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd read op
  MOSDOp *m = new MOSDOp(client, tid, oid, vol, epoch, 0);
  m->read(offset, length);

  // create reply callback
  OnReadReply *onreply = new OnReadReply(data, length, cb, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);

  return sync ? sync->wait() : 0;
}

// Dispatcher callback to fire the write completion
class OnWriteReply : public LibOSDDispatcher::OnReply {
  libosd_io_completion_fn cb;
  int flags;
  void *user;
public:
  OnWriteReply(libosd_io_completion_fn cb, int flags, void *user)
    : cb(cb), flags(flags), user(user) {}

  bool is_last_reply(Message *reply) {
    assert(reply->get_type() == CEPH_MSG_OSD_OPREPLY);
    MOSDOpReply *m = static_cast<MOSDOpReply*>(reply);
    return (flags & LIBOSD_WRITE_CB_STABLE) == 0 || m->is_ondisk();
  }

  void on_reply(Message *reply) {
    assert(reply->get_type() == CEPH_MSG_OSD_OPREPLY);
    MOSDOpReply *m = static_cast<MOSDOpReply*>(reply);

    const int flag = m->is_ondisk() ? LIBOSD_WRITE_CB_STABLE :
      LIBOSD_WRITE_CB_UNSTABLE;

    vector<OSDOp> ops;
    m->claim_ops(ops);
    m->put();

    assert(ops.size() == 1);
    OSDOp &op = *ops.begin();
    assert(op.op.op == CEPH_OSD_OP_WRITE ||
           op.op.op == CEPH_OSD_OP_TRUNCATE);

    uint64_t length = op.rval ? 0 : op.op.extent.length;
    cb(op.rval, length, flag, user);
  }

  void on_failure(int r) {
    cb(r, 0, 0, user);
  }
};

#define WRITE_CB_FLAGS (LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE)

int LibOSD::write(const char *object, const uint8_t volume[16],
		  uint64_t offset, uint64_t length, char *data,
		  int flags, libosd_io_completion_fn cb, void *user)
{
  const int client = 0;
  const long tid = 0;
  hobject_t oid = object_t(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  std::unique_ptr<SyncCompletion> sync;

  mempcpy(&vol, volume, sizeof(vol));

  if (!cb) {
    // when synchronous, flags must specify exactly one of UNSTABLE or STABLE
    if ((flags & WRITE_CB_FLAGS) == 0 ||
	(flags & WRITE_CB_FLAGS) == WRITE_CB_FLAGS)
      return -EINVAL;

    // set up a synchronous completion
    cb = SyncCompletion::callback;
    sync.reset(new SyncCompletion());
    user = sync.get();
  } else {
    // when asynchronous, flags must specify one or more of UNSTABLE or STABLE
    if ((flags & WRITE_CB_FLAGS) == 0)
      return -EINVAL;
  }

  if (!wait_for_active(&epoch))
    return -ENODEV;

  bufferlist bl;
  bl.append(buffer::create_static(length, data));

  // set up osd write op
  MOSDOp *m = new MOSDOp(client, tid, oid, vol, epoch, 0);
  m->write(offset, length, bl);

  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback
  OnWriteReply *onreply = NULL;
  if (m->wants_ack() || m->wants_ondisk())
    onreply = new OnWriteReply(cb, flags, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);

  return sync ? sync->wait() : 0;
}

int LibOSD::truncate(const char *object, const uint8_t volume[16],
		     uint64_t offset, int flags,
		     libosd_io_completion_fn cb, void *user)
{
  const int client = 0;
  const long tid = 0;
  hobject_t oid = object_t(object);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  std::unique_ptr<SyncCompletion> sync;

  memcpy(&vol, volume, sizeof(vol));

  if (!cb) {
    // when synchronous, flags must specify exactly one of UNSTABLE or STABLE
    if ((flags & WRITE_CB_FLAGS) == 0 ||
	(flags & WRITE_CB_FLAGS) == WRITE_CB_FLAGS)
      return -EINVAL;

    // set up a synchronous completion
    cb = SyncCompletion::callback;
    sync.reset(new SyncCompletion());
    user = sync.get();
  } else if ((flags & WRITE_CB_FLAGS) == 0) {
    // when asynchronous, flags must specify one or more of UNSTABLE or STABLE
    return -EINVAL;
  }

  if (!wait_for_active(&epoch))
    return -ENODEV;

  // set up osd truncate op
  MOSDOp *m = new MOSDOp(client, tid, oid, vol, epoch, 0);
  m->truncate(offset);

  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback
  OnWriteReply *onreply = NULL;
  if (m->wants_ack() || m->wants_ondisk())
    onreply = new OnWriteReply(cb, flags, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);

  return sync ? sync->wait() : 0;
}


// C interface

struct libosd* libosd_init(const struct libosd_init_args *args)
{
  if (args == NULL)
    return NULL;

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
