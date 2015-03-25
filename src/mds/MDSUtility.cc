// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 */

#include "mds/MDSUtility.h"
#include "mon/MonClient.h"

#define dout_subsys ceph_subsys_mds


Message* MDSUtility::Factory::create(int type)
{
  switch (type) {
  case CEPH_MSG_MDS_MAP:  return new MMDSMap(cct);
  default: return parent ? parent->create(type) : nullptr;
  }
}

MDSUtility::MDSUtility(CephContext *cct) :
  Dispatcher(cct),
  objecter(NULL),
  timer(lock),
  waiting_for_mds_map(NULL)
{
  factory.cct = cct;
  monc = new MonClient(cct);
  factory.parent = &monc->factory;
  messenger = Messenger::create(cct, entity_name_t::CLIENT(), "mds",
                                getpid(), &factory);
  mdsmap = new MDSMap(cct);
  objecter = new Objecter(cct, messenger, monc);
}


MDSUtility::~MDSUtility()
{
  delete objecter;
  delete monc;
  delete messenger;
  delete mdsmap;
  assert(waiting_for_mds_map == NULL);
}


int MDSUtility::init()
{
  // Initialize Messenger
  int r = messenger->bind(cct->_conf->public_addr);
  if (r < 0)
    return r;

  messenger->add_dispatcher_head(this);
  messenger->start();

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0)
    return -1;

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD |
		      CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify an MDS ID with a "
	 << "valid keyring?" << dendl;
    return r;
  }

  client_t whoami = monc->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  // Initialize Objecter and wait for OSD map
  objecter->set_client_incarnation(0);
  unique_lock l(lock);
  objecter->init();
  l.unlock();
  objecter->wait_for_osd_map();
  timer.init();

  // Prepare to receive MDS map and request it
  std::mutex init_lock;
  std::condition_variable cond;
  bool done = false;
  assert(!mdsmap->get_epoch());
  l.lock();
  waiting_for_mds_map = new C_SafeCond(&init_lock, &cond, &done, NULL);
  l.unlock();
  monc->sub_want("mdsmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  // Wait for MDS map
  dout(4) << "waiting for MDS map..." << dendl;
  unique_lock il(init_lock);
  while (!done)
    cond.wait(il);
  il.unlock();
  dout(4) << "Got MDS map " << mdsmap->get_epoch() << dendl;

  return 0;
}


void MDSUtility::shutdown()
{
  unique_lock l(lock);
  timer.shutdown(l);
  objecter->shutdown();
  l.unlock();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}


bool MDSUtility::ms_dispatch(Message *m)
{
  unique_lock l(lock);
  switch (m->get_type()) {
  case CEPH_MSG_MDS_MAP:
    handle_mds_map((MMDSMap*)m);
    return true;
    break;
  }
  return false;
}


void MDSUtility::handle_mds_map(MMDSMap* m)
{
  mdsmap->decode(m->get_encoded());
  if (waiting_for_mds_map) {
    waiting_for_mds_map->complete(0);
    waiting_for_mds_map = NULL;
  }
}


bool MDSUtility::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			 bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10s) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}
