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
  RWLock::RLocker l(map_lock);

  /* Possibly break out op queues per-volume. */

  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    return;
  }
  if (op_must_wait_for_map(op)) {
    waiting_for_map.push_back(op);
    return;
  }
  op_wq.queue(op);
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

void CohortOSD::OpWQ::_enqueue(OpRequestRef request)
{
  Message *message = request->request;
  unsigned priority = message->get_priority();
  unsigned cost = message->get_cost();

  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict(message->get_source_inst(), priority, request);
  else
    pqueue.enqueue(message->get_source_inst(), priority, cost, request);
  osd->logger->set(l_osd_opq, pqueue.length());
}

void CohortOSD::OpWQ::_enqueue_front(OpRequestRef request)
{
  Message *message = request->request;
  unsigned priority = message->get_priority();
  unsigned cost = message->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict_front(message->get_source_inst(),
				priority, request);
  else
    pqueue.enqueue_front(message->get_source_inst(),
			 priority, cost, request);
  osd->logger->set(l_osd_opq, pqueue.length());
}

OpRequestRef CohortOSD::OpWQ::_dequeue()
{
  assert(!pqueue.empty());
  Mutex::Locker l(qlock);
  OpRequestRef ret = pqueue.dequeue();
  osd->logger->set(l_osd_opq, pqueue.length());
  return ret;
}

bool CohortOSD::OpWQ::_empty()
{
  Mutex::Locker l(qlock);
  return pqueue.empty();
}

void CohortOSD::OpWQ::_process(void)
{
  OpRequestRef op;
  Mutex::Locker l(qlock);
  #warning Actually do things here.
}
