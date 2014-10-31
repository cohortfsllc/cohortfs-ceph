// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messengers.h"

#include "msg/Messenger.h"
#include "include/msgr.h"

#ifdef HAVE_XIO
#include "msg/XioMessenger.h"
#include "msg/QueueStrategy.h"
#endif

#include "osd/OSD.h"
#include "common/pick_address.h"
#include "common/debug.h"
#include "include/color.h"

#define dout_subsys ceph_subsys_osd


OSDMessengers::OSDMessengers()
  : cluster(NULL),
    client(NULL),
    client_xio(NULL),
    objecter(NULL),
    objecter_xio(NULL),
    client_hb(NULL),
    front_hb(NULL),
    back_hb(NULL),
    byte_throttler(NULL),
    msg_throttler(NULL)
{
}

OSDMessengers::~OSDMessengers()
{
  delete cluster;
  if (client != client_xio)
    delete client;
  delete client_xio;
  if (objecter != objecter_xio)
    delete objecter;
  delete objecter_xio;
  delete client_hb;
  delete front_hb;
  delete back_hb;
  delete byte_throttler;
  delete msg_throttler;
}

Messenger* create_messenger(CephContext *cct, const entity_name_t &me,
			    const char *name, pid_t pid)
{
  Messenger *ms = Messenger::create(cct, me, name, pid);
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  return ms;
}

#ifdef HAVE_XIO
Messenger* create_messenger_xio(CephContext *cct, const entity_name_t &me,
				const char *name, pid_t pid)
{
  const int nportals = 2;
  XioMessenger *ms = new XioMessenger(cct, me, name, pid, nportals,
				      new QueueStrategy(nportals));
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_port_shift(111);
  return ms;
}
#endif

int OSDMessengers::create(CephContext *cct, md_config_t *conf,
			  const entity_name_t &name, pid_t pid)
{
  // create messengers
#ifdef HAVE_XIO
  if (conf->cluster_rdma) {
    cluster = create_messenger_xio(cct, name, "cluster", pid);
    client_xio = create_messenger_xio(cct, name, "xio client", pid);
    objecter_xio = create_messenger_xio(cct, name, "xio objecter", pid);
    client_hb = create_messenger_xio(cct, name, "hbclient", pid);
    front_hb = create_messenger_xio(cct, name, "hb_front_server", pid);
    back_hb = create_messenger_xio(cct, name, "hb_back_server", pid);
  } else {
    cluster = create_messenger(cct, name, "cluster", pid);
    client = create_messenger(cct, name, "client", pid);
    objecter = create_messenger(cct, name, "objecter", pid);
    client_xio = create_messenger_xio(cct, name, "xio client", pid);
    objecter_xio = create_messenger_xio(cct, name, "xio objecter", pid);
    client_hb = create_messenger(cct, name, "hbclient", pid);
    front_hb = create_messenger(cct, name, "hb_front_server", pid);
    back_hb = create_messenger(cct, name, "hb_back_server", pid);
  }
#else // !HAVE_XIO
  cluster = create_messenger(cct, name, "cluster", pid);
  client = create_messenger(cct, name, "client", pid);
  objecter = create_messenger(cct, name, "objecter", pid);
  client_hb = create_messenger(cct, name, "hbclient", pid);
  front_hb = create_messenger(cct, name, "hb_front_server", pid);
  back_hb = create_messenger(cct, name, "hb_back_server", pid);
#endif // !HAVE_XIO

  // set up policies
  byte_throttler = new Throttle(cct, "osd_client_bytes",
			        conf->osd_client_message_size_cap);
  msg_throttler = new Throttle(cct, "osd_client_messages",
			       conf->osd_client_message_cap);

  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_MSG_AUTH;

  if (client) {
    client->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    client->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
	byte_throttler, msg_throttler);
    client->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported,
	  CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
    client->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));
  } else {
    client = client_xio;
  }

  if (client_xio) {
    client_xio->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    client_xio->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported,
	  CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
    client_xio->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));
  }

  if (objecter) {
    objecter->set_default_policy(
	Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
  } else {
    objecter = objecter_xio;
  }

  if (objecter_xio) {
    objecter_xio->set_default_policy(
	Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
  }

  cluster->set_default_policy(
      Messenger::Policy::stateless_server(0, 0));
  cluster->set_policy(entity_name_t::TYPE_MON,
      Messenger::Policy::lossy_client(0,0));
  cluster->set_policy(entity_name_t::TYPE_OSD,
      Messenger::Policy::lossless_peer(supported,
	CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
  cluster->set_policy(entity_name_t::TYPE_CLIENT,
      Messenger::Policy::stateless_server(0, 0));

  client_hb->set_policy(entity_name_t::TYPE_OSD,
			   Messenger::Policy::lossy_client(0, 0));
  front_hb->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::stateless_server(0, 0));
  back_hb->set_policy(entity_name_t::TYPE_OSD,
			 Messenger::Policy::stateless_server(0, 0));

  return 0;
}

int OSDMessengers::bind(CephContext *cct, md_config_t *conf)
{
  // bind messengers
  pick_addresses(cct, CEPH_PICK_ADDRESS_PUBLIC|CEPH_PICK_ADDRESS_CLUSTER);
  dout(10) << __FUNCTION__ << ": client " << conf->public_addr
    << ", cluster " << conf->cluster_addr << dendl;

  if (conf->public_addr.is_blank_ip() &&
      !conf->cluster_addr.is_blank_ip()) {
    derr << TEXT_YELLOW
      << " ** WARNING: specified cluster addr but not client addr; we **\n"
      << " **          recommend you specify neither or both.         **"
      << TEXT_NORMAL << dendl;
  }

  int r = cluster->bind(conf->cluster_addr);
  if (r < 0)
    return r;
  dout(10) << "bound cluster: " << cluster->get_myaddr() << dendl;

  entity_addr_t public_addr(conf->public_addr);
  if (client != client_xio) {
    r = client->bind(public_addr);
    if (r < 0)
      return r;
    dout(10) << "bound client: " << client->get_myaddr() << dendl;
    public_addr = client->get_myaddr();
  }
  if (client_xio) {
    r = client_xio->bind(public_addr);
    if (r < 0)
      return r;
    dout(10) << "bound client_xio: " << client_xio->get_myaddr() << dendl;
  }

  entity_addr_t objecter_addr(conf->public_addr);
  if (objecter != objecter_xio) {
    r = objecter->bind(objecter_addr);
    if (r < 0)
      return r;
    dout(10) << "bound objecter: " << objecter->get_myaddr() << dendl;
    objecter_addr = objecter->get_myaddr();
  }
  if (objecter_xio) {
    r = objecter_xio->bind(objecter_addr);
    if (r < 0)
      return r;
    dout(10) << "bound objecter_xio: " << objecter_xio->get_myaddr() << dendl;
  }

  // hb front should bind to same ip as public_addr
  entity_addr_t hb_front_addr(conf->public_addr);
  if (hb_front_addr.is_ip())
    hb_front_addr.set_port(0);
  r = front_hb->bind(hb_front_addr);
  if (r < 0)
    return r;
  dout(10) << "bound front_hb: " << front_hb->get_myaddr() << dendl;

  // hb back should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_back_addr(conf->osd_heartbeat_addr);
  if (hb_back_addr.is_blank_ip()) {
    hb_back_addr = conf->cluster_addr;
    if (hb_back_addr.is_ip())
      hb_back_addr.set_port(0);
  }
  r = back_hb->bind(hb_back_addr);
  if (r < 0)
    return r;
  dout(10) << "bound back_hb: " << back_hb->get_myaddr() << dendl;

  return 0;
}

void OSDMessengers::start()
{
  cluster->start();
  if (client != client_xio)
    client->start();
  if (client_xio)
    client_xio->start();
  if (objecter != objecter_xio)
    objecter->start();
  if (objecter_xio)
    objecter_xio->start();
  client_hb->start();
  front_hb->start();
  back_hb->start();
}

void OSDMessengers::wait()
{
  // XXX: assert(started);
  // close/wait on messengers
  cluster->wait();
  if (client != client_xio)
    client->wait();
  if (client_xio)
    client_xio->wait();
  if (objecter != objecter_xio)
    objecter->wait();
  if (objecter_xio)
    objecter_xio->wait();
  client_hb->wait();
  front_hb->wait();
  back_hb->wait();
}
