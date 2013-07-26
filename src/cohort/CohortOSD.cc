// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "CohortOSD.h"
#include "messages/MOSDSubOpReply.h"

bool CohortOSDService::test_ops_sub(ObjectStore *store,
				    std::string command,
				    std::string args,
				    ostream &ss)
{
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
      mc, dev, jdev)
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

void CohortOSD::handle_op_sub(OpRequestRef op)
{
//  MOSDOp *m = (MOSDOp*)op->request;
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
  if (!require_same_or_newer_map(op, m->get_map_epoch())) return false;

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
