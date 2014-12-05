// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ceph_osd.h"

#include "Objecter.h"
#include "Dispatcher.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#define dout_subsys ceph_subsys_osd

namespace ceph
{
namespace osd
{

// helper class to wait on a libosd_io_completion_fn
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
class OnReadReply : public Dispatcher::OnReply {
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

int Objecter::read(const char *object, const uint8_t volume[16],
                   uint64_t offset, uint64_t length, char *data,
                   int flags, libosd_io_completion_fn cb, void *user)
{
  const int client = 0;
  const long tid = 0;
  object_t poid = object_t(object);
  hobject_t oid = hobject_t(poid, DATA);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  std::unique_ptr<SyncCompletion> sync;
  memcpy(&vol, volume, sizeof(vol));

  if (!wait_for_active(&epoch))
    return -ENODEV;

  if (!cb) {
    // set up a synchronous completion
    cb = SyncCompletion::callback;
    sync.reset(new SyncCompletion());
    user = sync.get();
  }

  cout << "Objecter::read oid " << oid << " poid " << poid << " oname " << object << std::endl;

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
class OnWriteReply : public Dispatcher::OnReply {
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

int Objecter::write(const char *object, const uint8_t volume[16],
                    uint64_t offset, uint64_t length, char *data,
                    int flags, libosd_io_completion_fn cb, void *user)
{
  const int client = 0;
  const long tid = 0;
  object_t poid = object_t(object);
  hobject_t oid = hobject_t(poid, DATA);
  boost::uuids::uuid vol;
  epoch_t epoch = 0;
  std::unique_ptr<SyncCompletion> sync;

  cout << "Objecter::write oid " << oid << " poid " << poid << " oname " << object << std::endl;

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
  bl.append(ceph::buffer::create_static(length, data));

  // set up osd write op
  MOSDOp *m = new MOSDOp(client, tid, oid, vol, epoch, 0);
  m->write(offset, length, bl);

  if (flags & LIBOSD_WRITE_CB_UNSTABLE)
    m->set_want_ack(true);
  if (flags & LIBOSD_WRITE_CB_STABLE)
    m->set_want_ondisk(true);

  // create reply callback
  OnWriteReply *onreply = nullptr;
  if (m->wants_ack() || m->wants_ondisk())
    onreply = new OnWriteReply(cb, flags, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);

  return sync ? sync->wait() : 0;
}

int Objecter::truncate(const char *object, const uint8_t volume[16],
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
  OnWriteReply *onreply = nullptr;
  if (m->wants_ack() || m->wants_ondisk())
    onreply = new OnWriteReply(cb, flags, user);

  // send request over direct messenger
  dispatcher->send_request(m, onreply);

  return sync ? sync->wait() : 0;
}

} // namespace osd
} // namespace ceph
