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

#include <mutex>
#include <sstream>
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "common/BackTrace.h"
#include "common/config.h"
#include "common/debug.h"
#include "global/pidfile.h"
#include "global/signal_handler.h"


void install_sighandler(int signum, signal_handler_t handler, int flags)
{
  int ret;
  struct sigaction oldact;
  struct sigaction act;
  memset(&act, 0, sizeof(act));

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = flags;

  ret = sigaction(signum, &act, &oldact);
  if (ret != 0) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "install_sighandler: sigaction returned "
	    "%d when trying to install a signal handler for %s\n",
	     ret, sys_siglist[signum]);
    dout_emergency(buf);
    exit(1);
  }
}

/// --- safe handler ---

#include "common/Thread.h"
#include <errno.h>

/**
 * safe async signal handler / dispatcher
 *
 * This is an async unix signal handler based on the design from
 *
 *  http://evbergen.home.xs4all.nl/unix-signals.html
 *
 * Features:
 *   - no unsafe work is done in the signal handler itself
 *   - callbacks are called from a regular thread
 *   - signals are not lost, unless multiple instances of the same signal
 *     are sent twice in quick succession.
 */
struct SignalHandler : public Thread {
  /// to kick the thread, for shutdown, new handlers, etc.
  int pipefd[2];  // write to [1], read from [0]

  /// to signal shutdown
  bool stop;

  /// for an individual signal
  struct safe_handler {
    int pipefd[2];  // write to [1], read from [0]
    signal_handler_t handler;
  };

  /// all handlers
  safe_handler *handlers[32];

  /// to protect the handlers array
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;

  SignalHandler()
    : stop(false)
  {
    for (unsigned i = 0; i < 32; i++)
      handlers[i] = NULL;

    // create signal pipe
    int r = pipe(pipefd);
    assert(r == 0);
    r = fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
    assert(r == 0);

    // create thread
    create();
  }

  ~SignalHandler() {
    shutdown();
  }

  void signal_thread() {
    int r = write(pipefd[1], "\0", 1);
    assert(r == 1);
  }

  void shutdown() {
    stop = true;
    signal_thread();
    join();
  }

  // thread entry point
  void *entry() {
    while (!stop) {
      // build fd list
      struct pollfd fds[33];

      unique_lock l(lock);
      int num_fds = 0;
      fds[num_fds].fd = pipefd[0];
      fds[num_fds].events = POLLIN | POLLERR;
      fds[num_fds].revents = 0;
      ++num_fds;
      for (unsigned i=0; i<32; i++) {
	if (handlers[i]) {
	  fds[num_fds].fd = handlers[i]->pipefd[0];
	  fds[num_fds].events = POLLIN | POLLERR;
	  fds[num_fds].revents = 0;
	  ++num_fds;
	}
      }
      l.unlock();

      // wait for data on any of those pipes
      int r = poll(fds, num_fds, -1);
      if (stop)
	break;
      if (r > 0) {
	char v;

	// consume byte from signal socket, if any.
	r = read(pipefd[0], &v, 1);

	l.lock();
	for (unsigned signum=0; signum<32; signum++) {
	  if (handlers[signum]) {
	    r = read(handlers[signum]->pipefd[0], &v, 1);
	    if (r == 1) {
	      handlers[signum]->handler(signum);
	    }
	  }
	}
	l.unlock();
      } else {
	//cout << "no data, got r=" << r << " errno=" << errno << std::endl;
      }
    }
    return NULL;
  }

  void queue_signal(int signum) {
    // If this signal handler is registered, the callback must be
    // defined.	 We can do this without the lock because we will never
    // have the signal handler defined without the handlers entry also
    // being filled in.
    assert(handlers[signum]);
    int r = write(handlers[signum]->pipefd[1], " ", 1);
    assert(r == 1);
  }

  void register_handler(int signum, signal_handler_t handler, bool oneshot);
  void unregister_handler(int signum, signal_handler_t handler);
};

static SignalHandler *g_signal_handler = NULL;

static void handler_hook(int signum)
{
  g_signal_handler->queue_signal(signum);
}

void SignalHandler::register_handler(int signum, signal_handler_t handler, bool oneshot)
{
  int r;

  assert(signum >= 0 && signum < 32);

  safe_handler *h = new safe_handler;

  r = pipe(h->pipefd);
  assert(r == 0);
  r = fcntl(h->pipefd[0], F_SETFL, O_NONBLOCK);
  assert(r == 0);

  h->handler = handler;
  unique_lock l(lock);
  handlers[signum] = h;
  l.unlock();

  // signal thread so that it sees our new handler
  signal_thread();

  // install our handler
  struct sigaction oldact;
  struct sigaction act;
  memset(&act, 0, sizeof(act));

  act.sa_handler = handler_hook;
  sigfillset(&act.sa_mask);  // mask all signals in the handler
  act.sa_flags = oneshot ? SA_RESETHAND : 0;

  int ret = sigaction(signum, &act, &oldact);
  assert(ret == 0);
}

void SignalHandler::unregister_handler(int signum, signal_handler_t handler)
{
  assert(signum >= 0 && signum < 32);
  safe_handler *h = handlers[signum];
  assert(h);
  assert(h->handler == handler);

  // restore to default
  signal(signum, SIG_DFL);

  // _then_ remove our handlers entry
  unique_lock l(lock);
  handlers[signum] = NULL;
  l.unlock();

  // this will wake up select() so that worker thread sees our handler is gone
  close(h->pipefd[0]);
  close(h->pipefd[1]);
  delete h;
}


// -------

void init_async_signal_handler()
{
  assert(!g_signal_handler);
  g_signal_handler = new SignalHandler;
}

void shutdown_async_signal_handler()
{
  assert(g_signal_handler);
  delete g_signal_handler;
  g_signal_handler = NULL;
}

void queue_async_signal(int signum)
{
  assert(g_signal_handler);
  g_signal_handler->queue_signal(signum);
}

void register_async_signal_handler(int signum, signal_handler_t handler)
{
  assert(g_signal_handler);
  g_signal_handler->register_handler(signum, handler, false);
}

void register_async_signal_handler_oneshot(int signum, signal_handler_t handler)
{
  assert(g_signal_handler);
  g_signal_handler->register_handler(signum, handler, true);
}

void unregister_async_signal_handler(int signum, signal_handler_t handler)
{
  assert(g_signal_handler);
  g_signal_handler->unregister_handler(signum, handler);
}
