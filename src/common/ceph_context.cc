// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <time.h>

#include <mutex>
#include <condition_variable>
#include "common/admin_socket.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/HeartbeatMap.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "log/Log.h"
#include "auth/Crypto.h"
#include "include/str_list.h"

#include <iostream>
#include <pthread.h>

#include "include/Spinlock.h"

using ceph::HeartbeatMap;

class CephContextServiceThread : public Thread
{
public:
  CephContextServiceThread(CephContext *cct)
    : _reopen_logs(false), _exit_thread(false), _cct(cct)
  {
  }

  ~CephContextServiceThread() {}

  void *entry()
  {
    while (1) {
      std::unique_lock<std::mutex> l(_lock);

      if (_cct->_conf->heartbeat_interval) {
	ceph::timespan interval(ceph::span_from_double(
				  _cct->_conf->heartbeat_interval));
	_cond.wait_for(l, interval);
      } else {
	_cond.wait(l);
      }

      if (_exit_thread) {
	break;
      }

      if (_reopen_logs) {
	_cct->_log->reopen_log_file();
	_reopen_logs = false;
      }
      _cct->_heartbeat_map->check_touch_file();
    }
    return NULL;
  }

  void reopen_logs()
  {
    std::unique_lock<std::mutex> l(_lock);
    _reopen_logs = true;
    _cond.notify_all();
  }

  void exit_thread()
  {
    std::unique_lock<std::mutex> l(_lock);
    _exit_thread = true;
    _cond.notify_all();
  }

private:
  std::mutex _lock;
  std::condition_variable _cond;
  bool _reopen_logs;
  bool _exit_thread;
  CephContext *_cct;
};


/**
 * observe logging config changes
 *
 * The logging subsystem sits below most of the ceph code, including
 * the config subsystem, to keep it simple and self-contained.	Feed
 * logging-related config changes to the log.
 */
class LogObs : public md_config_obs_t {
  ceph::log::Log *log;

public:
  LogObs(ceph::log::Log *l) : log(l) {}

  const char** get_tracked_conf_keys() const {
    static const char *KEYS[] = {
      "log_file",
      "log_max_new",
      "log_max_recent",
      "log_to_syslog",
      "err_to_syslog",
      "log_to_stderr",
      "err_to_stderr",
      "log_to_lttng",
      NULL
    };
    return KEYS;
  }

  void handle_conf_change(const md_config_t *conf,
			  const std::set <std::string> &changed) {
    // stderr
    if (changed.count("log_to_stderr") || changed.count("err_to_stderr")) {
      int l = conf->log_to_stderr ? 99 : (conf->err_to_stderr ? -1 : -2);
      log->set_stderr_level(l, l);
    }

    // syslog
    if (changed.count("log_to_syslog")) {
      int l = conf->log_to_syslog ? 99 : (conf->err_to_syslog ? -1 : -2);
      log->set_syslog_level(l, l);
    }

    // file
    if (changed.count("log_file")) {
      log->set_log_file(conf->log_file);
      log->reopen_log_file();
    }

    if (changed.count("log_max_new")) {
      log->set_max_new(conf->log_max_new);
    }

    if (changed.count("log_max_recent")) {
      log->set_max_recent(conf->log_max_recent);
    }

    // lttng
    if (changed.count("log_to_lttng")) {
      log->enable_lttng(conf->log_to_lttng);
    }
  }
};


// hooks

class CephContextHook : public AdminSocketHook {
  CephContext *m_cct;

public:
  CephContextHook(CephContext *cct) : m_cct(cct) {}

  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    m_cct->do_command(command, cmdmap, format, &out);
    return true;
  }
};

void CephContext::do_command(std::string command, cmdmap_t& cmdmap,
			     std::string format, bufferlist *out)
{
  Formatter *f = new_formatter(format);
  if (!f)
    f = new_formatter("json-pretty");
  std::stringstream ss;
  for (cmdmap_t::iterator it = cmdmap.begin(); it != cmdmap.end(); ++it) {
    if (it->first != "prefix") {
      ss << it->first  << ":" << cmd_vartype_stringify(it->second) << " ";
    }
  }
  lgeneric_dout(this, 1) << "do_command '" << command << "' '"
			 << ss.str() << dendl;
  f->open_object_section(command.c_str());
  if (command == "config show") {
    _conf->show_config(f);
  }
  else if (command == "config set") {
    std::string var;
    std::vector<std::string> val;

    if (!(cmd_getval(this, cmdmap, "var", var)) ||
	!(cmd_getval(this, cmdmap, "val", val))) {
      f->dump_string("error", "syntax error: 'config set <var> <value>'");
    } else {
      // val may be multiple words
      string valstr = str_join(val, " ");
      int r = _conf->set_val(var.c_str(), valstr.c_str());
      if (r < 0) {
	f->dump_stream("error") << "error setting '" << var << "' to '" << valstr << "': " << cpp_strerror(r);
      } else {
	ostringstream ss;
	_conf->apply_changes(&ss);
	f->dump_string("success", ss.str());
      }
    }
  } else if (command == "config get") {
    std::string var;
    if (!cmd_getval(this, cmdmap, "var", var)) {
      f->dump_string("error", "syntax error: 'config get <var>'");
    } else {
      char buf[4096];
      memset(buf, 0, sizeof(buf));
      char *tmp = buf;
      int r = _conf->get_val(var.c_str(), &tmp, sizeof(buf));
      if (r < 0) {
	f->dump_stream("error") << "error getting '" << var << "': " << cpp_strerror(r);
      } else {
	f->dump_string(var.c_str(), buf);
      }
    }
  } else if (command == "log flush") {
    _log->flush();
  }
  else if (command == "log dump") {
    _log->dump_recent();
  }
  else if (command == "log reopen") {
    _log->reopen_log_file();
  }
  else {
    assert(0 == "registered under wrong command?");
  }
  f->close_section();

  f->flush(*out);
  delete f;
  lgeneric_dout(this, 1) << "do_command '" << command << "' '" << ss.str()
			 << "result is " << out->length() << " bytes" << dendl;
};


CephContext::CephContext(uint32_t module_type_)
  : nref(1),
    _conf(new md_config_t()),
    _log(NULL),
    _module_type(module_type_),
    _service_thread(NULL),
    _log_obs(NULL),
    _admin_socket(NULL),
    _heartbeat_map(NULL),
    _crypto_none(NULL),
    _crypto_aes(NULL)
{
  _log = new ceph::log::Log(&_conf->subsys, &_conf->name);

  _log_obs = new LogObs(_log);
  _conf->add_observer(_log_obs);

  _admin_socket = new AdminSocket(this);
  _heartbeat_map = new HeartbeatMap(this);

  _admin_hook = new CephContextHook(this);
  _admin_socket->register_command("config show", "config show", _admin_hook, "dump current config settings");
  _admin_socket->register_command("config set", "config set name=var,type=CephString name=val,type=CephString,n=N",  _admin_hook, "config set <field> <val> [<val> ...]: set a config variable");
  _admin_socket->register_command("config get", "config get name=var,type=CephString", _admin_hook, "config get <field>: get the config value");
  _admin_socket->register_command("log flush", "log flush", _admin_hook, "flush log entries to log file");
  _admin_socket->register_command("log dump", "log dump", _admin_hook, "dump recent log entries to log file");
  _admin_socket->register_command("log reopen", "log reopen", _admin_hook, "reopen log file");

  _crypto_none = new CryptoNone;
  _crypto_aes = new CryptoAES;
}

CephContext::~CephContext()
{
  join_service_thread();

  _admin_socket->unregister_command("config show");
  _admin_socket->unregister_command("config set");
  _admin_socket->unregister_command("config get");
  _admin_socket->unregister_command("log flush");
  _admin_socket->unregister_command("log dump");
  _admin_socket->unregister_command("log reopen");
  delete _admin_hook;
  delete _admin_socket;

  delete _heartbeat_map;

  _conf->remove_observer(_log_obs);
  delete _log_obs;
  _log_obs = NULL;

  _log->stop();
  delete _log;
  _log = NULL;

  delete _conf;

  delete _crypto_none;
  delete _crypto_aes;
}

void CephContext::init()
{
  /* We are assured that threads can be created by this point. */
  _log->start();
}

void CephContext::start_service_thread()
{
  {
    std::lock_guard<Spinlock> lock(_service_thread_lock);
    if (_service_thread)
      return;
    _service_thread = new CephContextServiceThread(this);
    _service_thread->create();
  }
  // make logs flush on_exit()
  if (_conf->log_flush_on_exit)
    _log->set_flush_on_exit();

  // Trigger callbacks on any config observers that were waiting for
  // it to become safe to start threads.
  _conf->set_val("internal_safe_to_start_threads", "true");
  _conf->call_all_observers();

  // start admin socket
  if (_conf->admin_socket.length())
    _admin_socket->init(_conf->admin_socket);
}

void CephContext::reopen_logs()
{
  std::lock_guard<Spinlock> lock(_service_thread_lock);
  if (_service_thread)
    _service_thread->reopen_logs();
}

void CephContext::join_service_thread()
{
  CephContextServiceThread *thread = nullptr;
  {
    std::lock_guard<Spinlock> lock(_service_thread_lock);
    std::swap(thread, _service_thread);
  }
  if (!thread)
    return;
  thread->exit_thread();
  thread->join();
  delete thread;
}

uint32_t CephContext::get_module_type() const
{
  return _module_type;
}

AdminSocket *CephContext::get_admin_socket()
{
  return _admin_socket;
}

CryptoHandler *CephContext::get_crypto_handler(int type)
{
  switch (type) {
  case CEPH_CRYPTO_NONE:
    return _crypto_none;
  case CEPH_CRYPTO_AES:
    return _crypto_aes;
  default:
    return NULL;
  }
}
