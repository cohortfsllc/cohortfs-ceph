// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <mutex> // once_flag, call_once

#include "libosd.h"
#include "acconfig.h"

#include "msg/Messenger.h"
#include "msg/QueueStrategy.h"

#ifdef HAVE_XIO
#include "msg/XioMessenger.h"
#endif

#include "os/ObjectStore.h"
#include "osd/OSD.h"
#include "mon/MonClient.h"

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/common_init.h"
#include "include/msgr.h"
#include "common/debug.h"
#include "common/pick_address.h"
#include "include/color.h"

#define dout_subsys ceph_subsys_osd

// global init
std::once_flag global_init_flag;

static void libosd_global_init()
{
  std::vector<const char*> args;
  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON, 0);
  common_init_finish(g_ceph_context);
}

struct LibOSD : public libosd {
private:
  const int whoami;
  MonClient *monc;
  ObjectStore *store;
  Messenger *ms_cluster;
  Messenger *ms_public;
  Messenger *ms_public_xio;
  Messenger *ms_objecter;
  Messenger *ms_objecter_xio;
  Messenger *ms_client_hb;
  Messenger *ms_front_hb;
  Messenger *ms_back_hb;
  std::unique_ptr<Throttle> client_byte_throttler;
  std::unique_ptr<Throttle> client_msg_throttler;

  int create_messengers();

public:
  LibOSD(int whoami);
  ~LibOSD();

  int init();
  void cleanup();
};

LibOSD::LibOSD(int whoami)
  : whoami(whoami),
    monc(NULL),
    store(NULL),
    ms_cluster(NULL),
    ms_public(NULL),
    ms_public_xio(NULL),
    ms_objecter(NULL),
    ms_objecter_xio(NULL),
    ms_client_hb(NULL),
    ms_front_hb(NULL),
    ms_back_hb(NULL)
{
}

LibOSD::~LibOSD()
{
  delete ms_cluster;
  if (ms_public != ms_public_xio)
    delete ms_public;
  delete ms_public_xio;
  if (ms_objecter != ms_objecter_xio)
    delete ms_objecter;
  delete ms_objecter_xio;
  delete ms_client_hb;
  delete ms_front_hb;
  delete ms_back_hb;
}

Messenger* create_messenger(const entity_name_t &me,
			    const char *name, pid_t pid)
{
  Messenger *ms = Messenger::create(g_ceph_context, me, name, pid);
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  return ms;
}

#ifdef HAVE_XIO
Messenger* create_messenger_xio(const entity_name_t &me,
				const char *name, pid_t pid)
{
  const int nportals = 2;
  XioMessenger *ms = new XioMessenger(g_ceph_context,
				      me, name, pid, nportals,
				      new QueueStrategy(nportals));
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_port_shift(111);
  return ms;
}
#endif

int LibOSD::create_messengers()
{
  const entity_name_t me(entity_name_t::OSD(whoami));
  const pid_t pid = getpid();
  int r = 0;

  // create messengers
#ifdef HAVE_XIO
  if (g_conf->cluster_rdma) {
    ms_cluster = create_messenger_xio(me, "cluster", pid);
    ms_public_xio = create_messenger_xio(me, "xio client", pid);
    ms_objecter_xio = create_messenger_xio(me, "xio objecter", pid);
    ms_client_hb = create_messenger_xio(me, "hbclient", pid);
    ms_front_hb = create_messenger_xio(me, "hb_front_server", pid);
    ms_back_hb = create_messenger_xio(me, "hb_back_server", pid);
  } else {
    ms_cluster = create_messenger(me, "cluster", pid);
    ms_public = create_messenger(me, "client", pid);
    ms_objecter = create_messenger(me, "ms_objecter", pid);
    ms_public_xio = create_messenger_xio(me, "xio client", pid);
    ms_objecter_xio = create_messenger_xio(me, "xio objecter", pid);
    ms_client_hb = create_messenger(me, "hbclient", pid);
    ms_front_hb = create_messenger(me, "hb_front_server", pid);
    ms_back_hb = create_messenger(me, "hb_back_server", pid);
  }
#else // !HAVE_XIO
  ms_cluster = create_messenger(me, "cluster", pid);
  ms_public = create_messenger(me, "client", pid);
  ms_objecter = create_messenger(me, "ms_objecter", pid);
  ms_client_hb = create_messenger(me, "hbclient", pid);
  ms_front_hb = create_messenger(me, "hb_front_server", pid);
  ms_back_hb = create_messenger(me, "hb_back_server", pid);
#endif // !HAVE_XIO

  // set up policies
  client_byte_throttler.reset(
      new Throttle(g_ceph_context, "osd_client_bytes",
		   g_conf->osd_client_message_size_cap));
  client_msg_throttler.reset(
      new Throttle(g_ceph_context, "osd_client_messages",
		   g_conf->osd_client_message_cap));

  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_MSG_AUTH;

  if (ms_public) {
    ms_public->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
	client_byte_throttler.get(), client_msg_throttler.get());
    ms_public->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported,
	  CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
    ms_public->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));
  } else {
    ms_public = ms_public_xio;
  }

  if (ms_public_xio) {
    ms_public_xio->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    ms_public_xio->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported,
	  CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
    ms_public_xio->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));
  }

  if (ms_objecter) {
    ms_objecter->set_default_policy(
	Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
  } else {
    ms_objecter = ms_objecter_xio;
  }

  if (ms_objecter_xio) {
    ms_objecter_xio->set_default_policy(
	Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
  }

  ms_cluster->set_default_policy(
      Messenger::Policy::stateless_server(0, 0));
  ms_cluster->set_policy(entity_name_t::TYPE_MON,
      Messenger::Policy::lossy_client(0,0));
  ms_cluster->set_policy(entity_name_t::TYPE_OSD,
      Messenger::Policy::lossless_peer(supported,
	CEPH_FEATURE_UID | CEPH_FEATURE_OSDENC));
  ms_cluster->set_policy(entity_name_t::TYPE_CLIENT,
      Messenger::Policy::stateless_server(0, 0));

  ms_client_hb->set_policy(entity_name_t::TYPE_OSD,
			   Messenger::Policy::lossy_client(0, 0));
  ms_front_hb->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::stateless_server(0, 0));
  ms_back_hb->set_policy(entity_name_t::TYPE_OSD,
			 Messenger::Policy::stateless_server(0, 0));

  // bind messengers
  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC
				|CEPH_PICK_ADDRESS_CLUSTER);
  dout(-1) << __FUNCTION__ << ": public " << g_conf->public_addr
    << ", cluster " << g_conf->cluster_addr << dendl;

  if (g_conf->public_addr.is_blank_ip() &&
      !g_conf->cluster_addr.is_blank_ip()) {
    derr << TEXT_YELLOW
      << " ** WARNING: specified cluster addr but not public addr; we **\n"
      << " **          recommend you specify neither or both.         **"
      << TEXT_NORMAL << dendl;
  }

  entity_addr_t public_addr(g_conf->public_addr);
  if (ms_public != ms_public_xio) {
    dout(-1) << "binding ms_public on " << public_addr << dendl;
    r = ms_public->bind(public_addr);
    if (r < 0)
      return r;
    public_addr = ms_public->get_myaddr();
  }
  if (ms_public_xio) {
    dout(-1) << "binding ms_public_xio on " << public_addr << dendl;
    r = ms_public_xio->bind(public_addr);
    if (r < 0)
      return r;
  }

  r = ms_cluster->bind(g_conf->cluster_addr);
  if (r < 0)
    return r;

  return r;
}

int LibOSD::init()
{
  // call global_init() on first entry
  std::call_once(global_init_flag, libosd_global_init);

  int r = create_messengers();
  return r;
}

void LibOSD::cleanup()
{
  // close/wait on messengers
}


// C interface
struct libosd* libosd_init(int name)
{
  LibOSD *osd = new LibOSD(name);
  if (osd->init() == 0)
    return osd;

  delete osd;
  return NULL;
}

void libosd_cleanup(struct libosd *osd)
{
  static_cast<LibOSD*>(osd)->cleanup();
  delete osd;
}

