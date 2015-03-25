// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "msg/msg_types.h"
#include "msg/XioMessenger.h"
#include "msg/FastStrategy.h"
#include "msg/QueueStrategy.h"
#include "msg/XioMsg.h"
#include "msg/MessageFactory.h"
#include "messages/MPing.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "perfglue/heap_profiler.h"
#include "common/address_helper.h"
#include "message_helper.h"
#include "xio_dispatcher.h"

#define dout_subsys ceph_subsys_xio_client

void usage(ostream& out)
{
  out << "usage: xio_client [options]\n"
"options:\n"
"  --addr X\n"
"  --port X\n"
"  --msgs X\n"
"  --dsize X\n"
"  --nfrags X\n"
"  --dfast\n"
    ;
}

int main(int argc, const char **argv)
{
	vector<const char*> args;
	Messenger* messenger;
	XioDispatcher *dispatcher;
	std::vector<const char*>::iterator arg_iter;
	std::string val;
	entity_addr_t dest_addr;
	ConnectionRef conn;
	int r = 0;

	std::string addr = "localhost";
	std::string port = "1234";
	int n_msgs = 50;
	int n_dsize = 0;
	int n_nfrags = 1;
	bool dfast = false;

	struct timespec ts;
	ts.tv_sec = 5;
	ts.tv_nsec = 0;

	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	CephContext* cct =
	  global_init(NULL, args,
		      CEPH_ENTITY_TYPE_ANY,
		      CODE_ENVIRONMENT_UTILITY, 0);

	for (arg_iter = args.begin(); arg_iter != args.end();) {
	  if (ceph_argparse_witharg(args, arg_iter, &val, "--addr",
				    (char*) NULL)) {
	    addr = val;
	  } else if (ceph_argparse_witharg(args, arg_iter, &val, "--port",
				    (char*) NULL)) {
	    port = val;
	  } else if (ceph_argparse_witharg(args, arg_iter, &val, "--msgs",
				    (char*) NULL)) {
	    n_msgs = atoi(val.c_str());
	  } else if (ceph_argparse_witharg(args, arg_iter, &val, "--dsize",
				    (char*) NULL)) {
	    n_dsize = atoi(val.c_str());
	  } else if (ceph_argparse_witharg(args, arg_iter, &val, "--nfrags",
				    (char*) NULL)) {
	    n_nfrags = atoi(val.c_str());
	  } else if (ceph_argparse_flag(args, &arg_iter, "--dfast",
					(char*) NULL)) {
	    dfast = true;
	  } else {
	    ++arg_iter;
	  }
	};

	if (!args.empty()) {
	  cerr << "What is this? -- " << args[0] << std::endl;
	  usage(cerr);
	  exit(1);
	}

	DispatchStrategy* dstrategy;
	if (dfast)
	  dstrategy = new FastStrategy();
	else
	  dstrategy = new QueueStrategy(2);

        XioMessageFactory factory;
	messenger = new XioMessenger(cct,
				     entity_name_t::GENERIC(),
				     "xio_client",
				     0 /* nonce */,
                                     &factory,
				     0 /* portals */,
				     dstrategy);

	static_cast<XioMessenger*>(messenger)->set_special_handling(
	  MSG_SPECIAL_HANDLING_REDUPE);

	if (n_dsize)
	  (void) static_cast<XioMessenger*>(messenger)->pool_hint(n_dsize);

	// enable timing prints
	static_cast<XioMessenger*>(messenger)->set_magic(MSG_MAGIC_TRACE_CTR);

	messenger->set_default_policy(Messenger::Policy::lossy_client(0, 0));

	string dest_str = "tcp://";
	dest_str += addr;
	dest_str += ":";
	dest_str += port;
	entity_addr_from_url(&dest_addr, dest_str.c_str());
	entity_inst_t dest_server(entity_name_t::GENERIC(), dest_addr);

	dispatcher = new XioDispatcher(messenger);
	messenger->add_dispatcher_head(dispatcher);

	dispatcher->set_active(); // this side is the pinger

	r = messenger->start();
	if (r < 0)
		goto out;

	conn = messenger->get_connection(dest_server);

	// do stuff
	time_t t1, t2;
	t1 = time(NULL);

	int msg_ix;
	for (msg_ix = 0; msg_ix < n_msgs; ++msg_ix) {
	  /* add a data payload if asked */
	  if (! n_dsize) {
	    messenger->send_message(
	      new MPing(), conn);
	  } else {
	  messenger->send_message(
	    new_simple_ping_with_data("xio_client", n_dsize, n_nfrags), conn);
	  }
	}

	// do stuff
	while (conn->is_connected()) {
	  nanosleep(&ts, NULL);
	}

	t2 = time(NULL);
	cout << "Processed " << dispatcher->get_dcount() + n_msgs
	     << " one-way messages in " << t2-t1 << "s"
	     << std::endl;

	conn->put();

	// wait a bit for cleanup to finalize
	ts.tv_sec = 5;
	nanosleep(&ts, NULL);

	messenger->shutdown();

cout << "conn has refs " << conn->nref << std::endl;

out:
	return r;
}
