// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "global/global_init.h"
#include "common/ceph_argparse.h"

#include "msg/DirectMessenger.h"
#include "msg/FastStrategy.h"
#include "messages/MDataPing.h"


int main(int argc, const char *argv[]) {
  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
      CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const entity_name_t entity1 = entity_name_t::GENERIC(1);
  const entity_name_t entity2 = entity_name_t::GENERIC(2);

  DirectMessenger *m1 = new DirectMessenger(g_ceph_context,
      entity1, "m1", 0, new FastStrategy());
  DirectMessenger *m2 = new DirectMessenger(g_ceph_context,
      entity2, "m2", 0, new FastStrategy());

  m1->set_direct_peer(m2);
  m2->set_direct_peer(m1);

  // condition variable to wait on ping reply
  Mutex mtx("test_direct_messenger");
  Cond cond;
  bool done;

  class ClientDispatcher : public Dispatcher {
    Context *c;
  public:
    ClientDispatcher(CephContext *cct, Context *c)
      : Dispatcher(cct), c(c) {}

    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}

    bool ms_dispatch(Message *m) {
      std::cout << "ClientDispatcher received " << *m << std::endl;
      c->complete(0);
      return true;
    }
  };
  m1->add_dispatcher_head(new ClientDispatcher(g_ceph_context,
	new C_SafeCond(&mtx, &cond, &done)));

  class ServerDispatcher : public Dispatcher {
  public:
    ServerDispatcher(CephContext *cct) : Dispatcher(cct) {}

    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}

    bool ms_dispatch(Message *m) {
      std::cout << "ServerDispatcher received " << *m
	<< ", sending reply" << std::endl;
      ConnectionRef c = m->get_connection();
      c->get_messenger()->send_message(new MDataPing(), c);
      return true;
    }
  };
  m2->add_dispatcher_head(new ServerDispatcher(g_ceph_context));

  // send message to m2
  m1->send_message(new MDataPing(), m2->get_myinst());

  // wait for response
  mtx.Lock();
  while (!done)
    cond.Wait(mtx);
  mtx.Unlock();

  std::cout << "Done" << std::endl;
  return 0;
}
