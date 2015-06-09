// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * CohortFS - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "osdc/RadosClient.h"
using namespace rados;

#include <boost/python.hpp>
using namespace boost::python;

struct DispatcherWrap : Dispatcher, wrapper<Dispatcher>
{
  DispatcherWrap(CephContext *cct_) : Dispatcher(cct_) { }
  bool ms_dispatch(Message *m)
    {
    return this->get_override("ms_dispatch")(m);
    }
  bool ms_handle_reset(Connection *c)
    {
    return this->get_override("ms_handle_reset")(c);
    }
  void ms_handle_remote_reset(Connection *c)
    {
    this->get_override("ms_handle_remote_reset")(c);
    }
};

struct RadosClientWrap : RadosClient, wrapper<RadosClient>
{
  RadosClientWrap(CephContext *cct_) : RadosClient(cct_) { }

  boost::python::tuple mon_command(boost::python::list cmdlist,
		  std::string inbuf)
    {
    std::vector<std::string> cmdvec;
    bufferlist inbl;
    bufferlist outbl;
    std::string outbuf;
    std::string outstring;

    inbl.append(inbuf);
    for (int i = 0; i < len(cmdlist); ++i) {
      cmdvec.push_back(boost::python::extract<std::string>(cmdlist[i]));
    }

    int ret = 0;
    try {
      std::tie(outstring, outbl) = RadosClient::mon_command(cmdvec, inbl);
    } catch (const std::system_error& e) {
      ret = -e.code().value();
    }

    outbl.copy(0, outbl.length(), outbuf);
    return boost::python::make_tuple(ret, outbuf, outstring);
    }
  int parse_args(boost::python::list pyargs)
    {
    std::vector<const char*> args;
    for (int i = 0; i < len(pyargs); ++i) {
      args.push_back(boost::python::extract<const char*>(pyargs[i]));
    }

    int ret = this->cct->_conf->parse_argv(args);
    if (ret) {
      return ret;
    }
    this->cct->_conf->apply_changes(NULL);
    return 0;
    }
  std::string ping_monitor(std::string mon_id)
    {
    std::string result;
    RadosClient::ping_monitor(mon_id, &result);
    return result;
    }
  const char *get_state_string()
    {
    switch (this->get_state()) {
      case DISCONNECTED:
	return "disconnected";
      case CONNECTING:
	return "connecting";
      case CONNECTED:
	return "connected";
    }
    return "unknown";
    }
};

RadosClient *factory(const char *conffile = "/etc/ceph/ceph.conf")
{
  std::vector<const char*>args;

  args.push_back("-c");
  args.push_back(conffile);

  CephContext *cct = global_pre_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
				     CODE_ENVIRONMENT_LIBRARY, 0);

  cct->_conf->parse_env(); // environment variables override
  cct->_conf->apply_changes(NULL);
  return new RadosClientWrap(cct);
}

RadosClient *factory0(void) { return factory(); }


BOOST_PYTHON_MODULE(pyrados)
{
  class_<DispatcherWrap, boost::noncopyable>("Dispatcher", init<CephContext*>())
    .def("ms_dispatch", pure_virtual(&DispatcherWrap::ms_dispatch))
    .def("ms_handle_reset", pure_virtual(&DispatcherWrap::ms_handle_reset))
    .def("ms_handle_remote_reset", pure_virtual(&DispatcherWrap::ms_handle_remote_reset))
    ;

  class_<RadosClientWrap, bases<Dispatcher>, boost::noncopyable>("RadosClient", init<CephContext*>())
    .def("ping_monitor", &RadosClientWrap::ping_monitor)
    .def("connect", &RadosClientWrap::connect)
    .def("shutdown", &RadosClientWrap::shutdown)
    .def("get_instance_id", &RadosClientWrap::get_instance_id)
    .def("wait_for_latest_osdmap", &RadosClientWrap::wait_for_latest_osdmap)
    .def("get_fsid", &RadosClientWrap::get_fsid)
    .def("mon_command", &RadosClientWrap::mon_command)
    .def("parse_args", &RadosClientWrap::parse_args)
    .add_property("state", &RadosClientWrap::get_state_string)
    ;

  def("factory", factory, return_value_policy<manage_new_object>()); // 1 arg
  def("factory", factory0, return_value_policy<manage_new_object>()); // 0 args
}
