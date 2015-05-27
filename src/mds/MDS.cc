#include "msg/Dispatcher.h"
#include "osdc/Objecter.h"

// #include "os/ObjectStore.h"
#include "mon/MonClient.h"

#include "msg/DirectMessenger.h"
#include "msg/FastStrategy.h"

#include "common/Finisher.h"
#include "mds/MDSMap.h"
#include "mds/MDSimpl.h"
#include "messages/MMDSBeacon.h"

#define dout_subsys ceph_subsys_mds

MDSimpl::MDSimpl(const std::string &n, Messenger *m, MonClient *mc) :
	Dispatcher(m->cct),
	name(n),
	messenger(m),
	monc(mc),
  objecter(new Objecter(cct, messenger, monc,
                        cct->_conf->rados_mon_op_timeout,
                        cct->_conf->rados_osd_op_timeout))
{
}

MDSimpl::~MDSimpl()
{
	delete objecter;
  delete monc;
  delete messenger;
}

void MDSimpl::shutdown()
{
  objecter->shutdown();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}

void MDSimpl::handle_signal(int signum)
{
	// XXX suicide
}

int MDSimpl::init()
{
  messenger->add_dispatcher_tail(objecter);
  messenger->start();

  monc->set_messenger(messenger);
  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  int r = monc->init();
  if (r) {
    lderr(cct) << "MonClient::init failed with " << r << dendl;
    shutdown();
    return r;
  }

  objecter->start();
  monc->renew_subs();

  beacon_start();
  return 0;
}

bool MDSimpl::ms_dispatch(Message *m)
{
    bool ret = true;
    if (ret) {
	m->put();
    }
    return ret;
}

bool MDSimpl::ms_handle_reset(Connection *con)
{
    // dout(5) << "ms_handle_reset on " << con->get_peer_addr() << dendl;
    switch(con->get_peer_type()) {
    case CEPH_ENTITY_TYPE_OSD:
	objecter->ms_handle_reset(con);
	break;
    case CEPH_ENTITY_TYPE_CLIENT:
	// XXX handle session here
	messenger->mark_down(con);
	break;
    }
    return false;
}

void MDSimpl::ms_handle_remote_reset(Connection *con)
{
    // dout(5) << "ms_handle_remote_reset on " << con->get_peer_addr() << dendl;
    switch(con->get_peer_type()) {
    case CEPH_ENTITY_TYPE_OSD:
	objecter->ms_handle_reset(con);
	break;
    case CEPH_ENTITY_TYPE_CLIENT:
	// XXX handle session here
	messenger->mark_down(con);
	break;
    }
}

#if 0
void MDSimpl::ms_handle_connect(Connection *con)
{
    // dout(5) << "ms_handle_connect on " << con->get_peer_addr() << dendl;
    objecter->ms_handle_connect(con);
}

void MDSimpl::ms_handle_accept(Connection *con)
{
    // XXX if existing session, send any queued messages
}
#endif

void MDSimpl::request_state(int s)
{
    // dout(3) << "request_state " << ceph_mds_state_name(s) << dendl;
    want_state = s;
    beacon_send();
}

void MDSimpl::beacon_start()
{
  beacon_timer.add_event(cct->_conf->mds_beacon_interval,
                         &MDSimpl::beacon_send, this);
}

void MDSimpl::beacon_send()
{
    ++beacon_last_seq;
	// dout(10) << "beacon_send " << ceph_mds_state_name(want_state)
	//	<< " seq " << beacon_last_seq
	//	<< " (currently " << ceph_mds_state_name(state) << ")"
	//	<< dendl;
    MMDSBeacon *beacon = new MMDSBeacon(monc->get_fsid(), monc->get_global_id(),
	name, mdsmap->get_epoch(),
	want_state, beacon_last_seq);
    monc->send_mon_message(beacon);

  beacon_timer.reschedule_me(cct->_conf->mds_beacon_interval);
}
