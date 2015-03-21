#include <boost/function.hpp>

#include "msg/Dispatcher.h"
#include "osdc/Objecter.h"

// #include "os/ObjectStore.h"
#include "mon/MonClient.h"

#include "msg/DirectMessenger.h"
#include "msg/FastStrategy.h"

#include "common/Finisher.h"
#include "mds/MDSMap.h"
#include "mds/MDS.h"
#include "messages/MMDSBeacon.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix

MDS::MDS(const std::string &n, Messenger *m, MonClient *mc) :
	Dispatcher(m->cct),
	name(n),
	messenger(m),
	monc(mc)
{
}

MDS::~MDS()
{
    if (objecter) {
	delete objecter; objecter = 0;
    }
    if (finisher) {
	finisher->stop();
	delete finisher;
	finisher = 0;
    }
}

bool MDS::shutdown()
{
    return false;
}

void MDS::handle_signal(int signum)
{
	// XXX suicide
}

int MDS::init(int wanted_state)
{
    return 0;
    whoami = -1;
    messenger->set_myname(entity_name_t::MDS(whoami));
    beacon_start();
// XXX sched tick
}

bool MDS::ms_dispatch(Message *m)
{
    bool ret = true;
    if (ret) {
	m->put();
    }
    return ret;
}

bool MDS::ms_handle_reset(Connection *con)
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

void MDS::ms_handle_remote_reset(Connection *con)
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
void MDS::ms_handle_connect(Connection *con)
{
    // dout(5) << "ms_handle_connect on " << con->get_peer_addr() << dendl;
    objecter->ms_handle_connect(con);
}

void MDS::ms_handle_accept(Connection *con)
{
    // XXX if existing session, send any queued messages
}
#endif

void MDS::request_state(int s)
{
    // dout(3) << "request_state " << ceph_mds_state_name(s) << dendl;
    want_state = s;
    beacon_send();
}

void MDS::beacon_start()
{
    beacon_send();
}

void MDS::beacon_send()
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
    // XXX schedule next sender
}

namespace ceph {
namespace mds {
int context_create(int id, char const *config, char const *cluster, CephContext **cct)
{
    return 0;
}
} // namespace mds
} // namespace ceph
