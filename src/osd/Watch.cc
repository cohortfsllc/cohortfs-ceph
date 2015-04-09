// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "OSDVol.h"

#include "include/types.h"
#include "messages/MWatchNotify.h"

#include <map>

#include "OSD.h"
#include "Watch.h"

#include "common/config.h"

struct CancelableContext : public Context {
  virtual void cancel() = 0;
};

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

static ostream& _prefix(
  std::ostream* _dout,
  Notify *notify) {
  return *_dout << notify->gen_dbg_prefix();
}

Notify::Notify(
  ConnectionRef client,
  unsigned num_watchers,
  bufferlist &payload,
  ceph::timespan timeout,
  uint64_t cookie,
  uint64_t notify_id,
  uint64_t version,
  OSDService *osd)
  : client(client),
    in_progress_watchers(num_watchers),
    complete(false),
    discarded(false),
    payload(payload),
    timeout(timeout),
    cookie(cookie),
    notify_id(notify_id),
    version(version),
    osd(osd),
    cb(0) {}

NotifyRef Notify::makeNotifyRef(
  ConnectionRef client,
  unsigned num_watchers,
  bufferlist &payload,
  ceph::timespan timeout,
  uint64_t cookie,
  uint64_t notify_id,
  uint64_t version,
  OSDService *osd) {
  NotifyRef ret(
    new Notify(
      client, num_watchers,
      payload, timeout,
      cookie, notify_id,
      version, osd));
  ret->set_self(ret);
  return ret;
}

void Notify::do_timeout()
{
  unique_lock nl(lock);
  ldout(osd->osd->cct, 10) << "timeout" << dendl;
  cb = 0;
  if (is_discarded()) {
    nl.unlock();
    return;
  }

  in_progress_watchers = 0; // we give up TODO: we should return an error code
  maybe_complete_notify();
  assert(complete);
  set<WatchRef> _watchers;
  _watchers.swap(watchers);
  nl.unlock();

  for (auto& i : _watchers) {
    boost::intrusive_ptr<OSDVol> vol(i->get_vol());
    OSDVol::unique_lock vl(vol->lock);
    if (!i->is_discarded()) {
      i->cancel_notify(self.lock());
    }
    vl.unlock();
  }
}

void Notify::register_cb()
{
  OSD::lock_guard wl(osd->watch_lock);
  cb = osd->watch_timer.add_event(
    timeout,
    &Notify::do_timeout, this);
}

void Notify::unregister_cb()
{
  // Must be called with lock locked
  if (!cb)
    return;
  osd->watch_timer.cancel_event(cb);
  cb = 0;
}

void Notify::start_watcher(WatchRef watch)
{
  lock_guard l(lock);
  ldout(osd->osd->cct, 10) << "start_watcher" << dendl;
  watchers.insert(watch);
}

void Notify::complete_watcher(WatchRef watch)
{
  lock_guard l(lock);
  ldout(osd->osd->cct, 10) << "complete_watcher" << dendl;
  if (is_discarded())
    return;
  assert(in_progress_watchers > 0);
  watchers.erase(watch);
  --in_progress_watchers;
  maybe_complete_notify();
}

void Notify::maybe_complete_notify()
{
  ldout(osd->osd->cct, 10) << "maybe_complete_notify -- "
	   << in_progress_watchers
	   << " in progress watchers " << dendl;
  if (!in_progress_watchers) {
    MWatchNotify *reply(new MWatchNotify(cookie, version, notify_id,
					 WATCH_NOTIFY, payload));
    osd->send_message_osd_client(reply, client.get());
    unregister_cb();
    complete = true;
  }
}

void Notify::discard()
{
  lock_guard l(lock);
  discarded = true;
  unregister_cb();
  watchers.clear();
}

void Notify::init()
{
  lock_guard l(lock);
  register_cb();
  maybe_complete_notify();
  assert(in_progress_watchers == watchers.size());
}

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, watch.get())

static ostream& _prefix(
  std::ostream* _dout,
  Watch *watch) {
  return *_dout << watch->gen_dbg_prefix();
}

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

string Watch::gen_dbg_prefix() {
  stringstream ss;
  ss << vol->gen_prefix() << " -- Watch("
     << make_pair(cookie, entity) << ") ";
  return ss.str();
}

Watch::Watch(
  OSDVol *vol,
  OSDService *osd,
  ObjectContextRef obc,
  ceph::timespan timeout,
  uint64_t cookie,
  entity_name_t entity,
  const entity_addr_t &addr)
  : cb(0),
    osd(osd),
    vol(vol),
    obc(obc),
    timeout(timeout),
    cookie(cookie),
    addr(addr),
    entity(entity),
    discarded(false) {
  ldout(osd->osd->cct, 10) << "Watch()" << dendl;
}

Watch::~Watch() {
  ldout(osd->osd->cct, 10) << "~Watch" << dendl;
  // users must have called remove() or discard() prior to this point
  assert(!obc);
  assert(!conn);
}

bool Watch::connected() { return !!conn; }

void Watch::register_cb()
{
  OSD::lock_guard l(osd->watch_lock);
  ldout(osd->osd->cct, 15) << "registering callback, timeout: " << timeout
			   << dendl;
  cb = osd->watch_timer.add_event(
    timeout,
    &Watch::handle_watch_timeout, this);
}

void Watch::unregister_cb()
{
  ldout(osd->osd->cct, 15) << "unregister_cb" << dendl;
  if (!cb)
    return;
  ldout(osd->osd->cct, 15) << "actually registered, cancelling" << dendl;
  osd->watch_timer.cancel_event(cb); // harmless if not registered with timer
  cb = 0;
}

void Watch::connect(ConnectionRef con)
{
  ldout(osd->osd->cct, 10) << "connecting" << dendl;
  conn = con;
  OSD::Session* sessionref(static_cast<OSD::Session*>(con->get_priv()));
  sessionref->wstate.addWatch(self.lock());
  sessionref->put();
  for (map<uint64_t, NotifyRef>::iterator i = in_progress_notifies.begin();
       i != in_progress_notifies.end();
       ++i) {
    send_notify(i->second);
  }
  unregister_cb();
}

void Watch::disconnect()
{
  ldout(osd->osd->cct, 10) << "disconnect" << dendl;
  conn = ConnectionRef();
  register_cb();
}

void Watch::discard()
{
  ldout(osd->osd->cct, 10) << "discard" << dendl;
  for (map<uint64_t, NotifyRef>::iterator i = in_progress_notifies.begin();
       i != in_progress_notifies.end();
       ++i) {
    i->second->discard();
  }
  discard_state();
}

void Watch::discard_state()
{
  // vol->lock should be locked
  assert(!discarded);
  assert(obc);
  in_progress_notifies.clear();
  unregister_cb();
  discarded = true;
  if (conn) {
    OSD::Session* sessionref(static_cast<OSD::Session*>(conn->get_priv()));
    sessionref->wstate.removeWatch(self.lock());
    sessionref->put();
    conn = ConnectionRef();
  }
  obc = ObjectContextRef();
}

bool Watch::is_discarded()
{
  return discarded;
}

void Watch::remove()
{
  ldout(osd->osd->cct, 10) << "remove" << dendl;
  for (map<uint64_t, NotifyRef>::iterator i = in_progress_notifies.begin();
       i != in_progress_notifies.end();
       ++i) {
    i->second->complete_watcher(self.lock());
  }
  discard_state();
}

void Watch::start_notify(NotifyRef notif)
{
  ldout(osd->osd->cct, 10) << "start_notify " << notif->notify_id << dendl;
  assert(in_progress_notifies.find(notif->notify_id) ==
	 in_progress_notifies.end());
  in_progress_notifies[notif->notify_id] = notif;
  notif->start_watcher(self.lock());
  if (connected())
    send_notify(notif);
}

void Watch::cancel_notify(NotifyRef notif)
{
  ldout(osd->osd->cct, 10) << "cancel_notify " << notif->notify_id << dendl;
  in_progress_notifies.erase(notif->notify_id);
}

void Watch::send_notify(NotifyRef notif)
{
  ldout(osd->osd->cct, 10) << "send_notify" << dendl;
  MWatchNotify *notify_msg = new MWatchNotify(
    cookie, notif->version, notif->notify_id,
    WATCH_NOTIFY, notif->payload);
  osd->send_message_osd_client(notify_msg, conn.get());
}

void Watch::notify_ack(uint64_t notify_id)
{
  ldout(osd->osd->cct, 10) << "notify_ack" << dendl;
  map<uint64_t, NotifyRef>::iterator i = in_progress_notifies.find(notify_id);
  if (i != in_progress_notifies.end()) {
    i->second->complete_watcher(self.lock());
    in_progress_notifies.erase(i);
  }
}

WatchRef Watch::makeWatchRef(
  OSDVol *vol, OSDService *osd, ObjectContextRef obc,
  ceph::timespan timeout, uint64_t cookie, entity_name_t entity,
  const entity_addr_t& addr)
{
  WatchRef ret(new Watch(vol, osd, obc, timeout, cookie, entity, addr));
  ret->set_self(ret);
  return ret;
}

void Watch::handle_watch_timeout() {
  ldout(osd->osd->cct, 10) << "HandleWatchTimeout" << dendl;
  OSDVol::unique_lock vl(vol->lock);
  cb = 0;
  if (!is_discarded()) {
    vol->handle_watch_timeout(self.lock());
    vl.unlock();
  }
}

void WatchConState::addWatch(WatchRef watch)
{
  lock_guard l(lock);
  watches.insert(watch);
}

void WatchConState::removeWatch(WatchRef watch)
{
  lock_guard l(lock);
  watches.erase(watch);
}

void WatchConState::reset()
{
  set<WatchRef> _watches;
  {
    lock_guard l(lock);
    _watches.swap(watches);
  }
  for (auto& i : _watches) {
    boost::intrusive_ptr<OSDVol> vol(i->get_vol());
    OSDVol::unique_lock vl(vol->lock);
    if (!i->is_discarded()) {
      i->disconnect();
    }
    vl.unlock();
  }
}
