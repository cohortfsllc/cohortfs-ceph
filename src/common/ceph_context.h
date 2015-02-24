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
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CEPHCONTEXT_H
#define CEPH_CEPHCONTEXT_H

#include <atomic>
#include <iostream>
#include <stdint.h>

#include "include/buffer.h"
#include "common/cmdparse.h"
#include "common/code_environment.h"
#include "include/Spinlock.h"

class AdminSocket;
class CephContextServiceThread;
class md_config_obs_t;
struct md_config_t;
class CephContextHook;
class CryptoNone;
class CryptoAES;
class CryptoHandler;
class CephInitParameters;

namespace ceph {
  class HeartbeatMap;
  namespace log {
    class Log;
  }
}

using ceph::bufferlist;

/* A CephContext represents the context held by a single library user.
 * There can be multiple CephContexts in the same process.
 *
 * For daemons and utility programs, there will be only one CephContext.  The
 * CephContext contains the configuration, the dout object, and anything else
 * that you might want to pass to libcommon with every function call.
 */
class CephContext {
  // ref count!
private:
  ~CephContext();
  std::atomic<uint64_t> nref;
protected:
  CephContext(uint32_t module_type_);

  CephContext *get() {
    ++nref;
    return this;
  }
  void put() {
    if (--nref == 0)
      delete this;
  }

public:
  md_config_t *_conf;
  ceph::log::Log *_log;

  /* Called from global_init() */
  void init();

  /* Start the Ceph Context's service thread */
  void start_service_thread();

  /* Reopen the log files */
  void reopen_logs();

  /* Get the module type (client, mon, osd, mds, etc.) */
  uint32_t get_module_type() const;

  ceph::HeartbeatMap *get_heartbeat_map() {
    return _heartbeat_map;
  }

  /**
   * Get the admin socket associated with this CephContext.
   *
   * Currently there is always an admin socket object,
   * so this will never return NULL.
   *
   * @return the admin socket
   */
  AdminSocket *get_admin_socket();

  /**
   * process an admin socket command
   */
  void do_command(std::string command, cmdmap_t& cmdmap, std::string format,
		  bufferlist *out);

  /**
   * get a crypto handler
   */
  CryptoHandler *get_crypto_handler(int type);

private:
  CephContext(const CephContext &rhs);
  CephContext &operator=(const CephContext &rhs);

  /* Stop and join the Ceph Context's service thread */
  void join_service_thread();

  uint32_t _module_type;

  /* libcommon service thread.
   * SIGHUP wakes this thread, which then reopens logfiles */
  friend class CephContextServiceThread;
  CephContextServiceThread *_service_thread;

  md_config_obs_t *_log_obs;

  /* The admin socket associated with this context */
  AdminSocket *_admin_socket;

  /* lock which protects service thread creation, destruction, etc. */
  ceph_spinlock_t _service_thread_lock;

  CephContextHook *_admin_hook;

  ceph::HeartbeatMap *_heartbeat_map;

  // crypto
  CryptoNone *_crypto_none;
  CryptoAES *_crypto_aes;

  friend CephContext *test_init(enum code_environment_t code_env);
  friend void common_cleanup(CephContext *cct);
  friend CephContext *common_preinit(const CephInitParameters &iparams,
				     enum code_environment_t code_env,
				     int flags);
};

#endif
