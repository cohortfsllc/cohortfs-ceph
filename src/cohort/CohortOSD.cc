// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "CohortOSD.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

bool CohortOSDService::test_ops_sub(ObjectStore *store,
				    std::string command,
				    std::string args,
				    ostream &ss)
{
  /* Maybe come back to this later, it's not really important. */
  return false;
}

void CohortOSDService::init_sub()
{
}

void CohortOSDService::shutdown_sub()
{
}

CohortOSD* CohortOSDService::cohortosd() const
{
  return static_cast<CohortOSD*>(osd);
}

CohortOSDService::CohortOSDService(CohortOSD *osd) :
  OSDService(osd)
{
}

void CohortOSD::handle_conf_change(const struct md_config_t *conf,
				   const std::set <std::string> &changed)
{
  inherited::handle_conf_change(conf, changed);
}

CohortOSD::CohortOSD(int id, Messenger *internal, Messenger *external,
		     Messenger *hb_client, Messenger *hb_front_server,
		     Messenger *hb_back_server, MonClient *mc,
		     const std::string &dev,
		     const std::string &jdev) :
  OSD(id, internal, external, hb_client, hb_front_server, hb_back_server,
      mc, dev, jdev),
  op_wq(this, g_conf->osd_op_thread_timeout, &op_tp)

{
}

int CohortOSD::init()
{
  return init_super();
}


int CohortOSD::shutdown()
{
  return shutdown_super();
}


bool CohortOSD::do_command_sub(Connection *con,
			       tid_t tid,
			       vector<string>& cmd,
			       bufferlist& data,
			       bufferlist& odata,
			       int& r,
			       ostringstream& ss)
{
  return false;
}

bool CohortOSD::do_command_debug_sub(vector<string>& cmd,
				     int& r,
				     ostringstream& ss)
{
  return true;
}

bool CohortOSD::op_must_wait_for_map(OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !have_same_or_newer_map(
      static_cast<MOSDOp*>(op->request)->get_osdmap_epoch());

  case MSG_OSD_SUBOP:
    return !have_same_or_newer_map(
      static_cast<MOSDSubOp*>(op->request)->osdmap_epoch);

  case MSG_OSD_SUBOPREPLY:
    return !have_same_or_newer_map(
      static_cast<MOSDSubOpReply*>(op->request)->osdmap_epoch);
  }
  assert(0);
  return false;
}

void CohortOSD::handle_op_sub(OpRequestRef op)
{
  MOSDOp *m = (MOSDOp*)op->request;

  uuid_d volid = m->get_volume();

  // get and lock *pg.
  OSDVolRef vol = get_volume(volid);

  if (!vol) {
    /* No such volume */
    if (m->get_volmap_epoch() > volmap->get_epoch()) {
      /* Should wait for new volmap */
      return;
    }
    service->reply_op_error(op, -ENXIO);
    return;
  }

  enqueue_op(vol, op);
}

bool CohortOSD::handle_sub_op_reply_sub(OpRequestRef op)
{
  MOSDSubOpReply *m = static_cast<MOSDSubOpReply*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOPREPLY);

  if (!require_osd_peer(op))
    return false;

  // must be a rep op.
  assert(m->get_source().is_osd());

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_osdmap_epoch(),
				 m->get_volmap_epoch())) {
    return false;
  }

  return true;
}

void CohortOSD::build_heartbeat_peers_list()
{
}

void CohortOSD::tick_sub(const utime_t& now)
{
    if (now - last_mon_report > g_conf->osd_mon_report_interval_min) {
      do_mon_report();
    }
}

void CohortOSD::do_mon_report_sub(const utime_t& now)
{
}

void CohortOSD::ms_handle_connect_sub(Connection *con)
{
}

void CohortOSD::ms_handle_reset_sub(OSD::Session* session)
{
}

void CohortOSD::advance_map_sub(ObjectStore::Transaction& t,
				C_Contexts *tfin)
{
  assert(osd_lock.is_locked());

  if (!up_epoch &&
      osdmap->is_up(whoami) &&
      osdmap->get_inst(whoami) == client_messenger->get_myinst()) {
    up_epoch = osdmap->get_epoch();
    if (!boot_epoch) {
      boot_epoch = osdmap->get_epoch();
    }
  }
}

void CohortOSD::consume_map_sub()
{
}


void CohortOSD::dispatch_op_sub(OpRequestRef op)
{
}

bool CohortOSD::_dispatch_sub(Message *m)
{
  return false;
}

bool CohortOSD::asok_command_sub(string command, string args, ostream& ss)
{
  return false;
}

void CohortOSD::check_replay_queue()
{
}

void CohortOSD::sched_scrub()
{
}

void CohortOSD::OpWQ::_enqueue(pair<OSDVolRef, OpRequestRef> item)
{
  unsigned priority = item.second->request->get_priority();
  unsigned cost = item.second->request->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict(
      item.second->request->get_source_inst(),
      priority, item);
  else
    pqueue.enqueue(item.second->request->get_source_inst(),
      priority, cost, item);
  osd->logger->set(l_osd_opq, pqueue.length());
}

void CohortOSD::OpWQ::_enqueue_front(pair<OSDVolRef, OpRequestRef> item)
{
  {
    Mutex::Locker l(qlock);
    if (vol_for_processing.count(item.first)) {
      vol_for_processing[(item.first)].push_front(item.second);
      item.second = vol_for_processing[item.first].back();
      vol_for_processing[item.first].pop_back();
    }
  }
  unsigned priority = item.second->request->get_priority();
  unsigned cost = item.second->request->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict_front(
      item.second->request->get_source_inst(),
      priority, item);
  else
    pqueue.enqueue_front(item.second->request->get_source_inst(),
      priority, cost, item);
  osd->logger->set(l_osd_opq, pqueue.length());
}

OSDVolRef CohortOSD::OpWQ::_dequeue()
{
  assert(!pqueue.empty());
  OSDVolRef vol;
  {
    Mutex::Locker l(qlock);
    pair<OSDVolRef, OpRequestRef> ret = pqueue.dequeue();
    vol = ret.first;
    vol_for_processing[vol].push_back(ret.second);
  }
  osd->logger->set(l_osd_opq, pqueue.length());
  return vol;
}

void CohortOSD::OpWQ::_process(OSDVolRef vol)
{
  vol->lock();
  OpRequestRef op;
  {
    Mutex::Locker l(qlock);
    if (!vol_for_processing.count(vol)) {
      vol->unlock();
      return;
    }
    assert(vol_for_processing[vol].size());
    op = vol_for_processing[vol].front();
    vol_for_processing[vol].pop_front();
    if (!(vol_for_processing[vol].size()))
      vol_for_processing.erase(vol);
  }
  osd->dequeue_op(vol, op);
  vol->unlock();
}

void CohortOSD::enqueue_op(OSDVolRef vol, OpRequestRef op)
{
  vol->queue_op(op);
}

void CohortOSD::dequeue_op(OSDVolRef vol, OpRequestRef op)
{
  op->mark_reached_vol();

  vol->do_request(op);
}
