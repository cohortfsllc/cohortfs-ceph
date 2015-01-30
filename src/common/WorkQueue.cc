// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <sstream>

#include "include/types.h"
#include "include/ceph_time.h"
#include "WorkQueue.h"

#include "common/config.h"
#include "common/HeartbeatMap.h"
#include "common/dout.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_tp
#undef dout_prefix
#define dout_prefix *_dout << name << " "


ThreadPool::ThreadPool(CephContext *cct_, string nm, int n, const char *option)
  : cct(cct_), name(nm),
    _stop(false),
    _pause(0),
    _draining(0),
    _num_threads(n),
    last_work_queue(0),
    processing(0)
{
  if (option) {
    _thread_num_option = option;
    // set up conf_keys
    _conf_keys = new const char*[2];
    _conf_keys[0] = _thread_num_option.c_str();
    _conf_keys[1] = NULL;
  } else {
    _conf_keys = new const char*[1];
    _conf_keys[0] = NULL;
  }
}

void ThreadPool::TPHandle::suspend_tp_timeout()
{
  cct->get_heartbeat_map()->clear_timeout(hb);
}

void ThreadPool::TPHandle::reset_tp_timeout()
{
  cct->get_heartbeat_map()->reset_timeout(
    hb, grace, suicide_grace);
}

ThreadPool::~ThreadPool()
{
  assert(_threads.empty());
  delete[] _conf_keys;
}

void ThreadPool::handle_conf_change(const struct md_config_t *conf,
				    const std::set <std::string> &changed)
{
  if (changed.count(_thread_num_option)) {
    char *buf;
    int r = conf->get_val(_thread_num_option.c_str(), &buf, -1);
    assert(r >= 0);
    int v = atoi(buf);
    free(buf);
    if (v > 0) {
      std::lock_guard<std::mutex> l(_lock);
      _num_threads = v;
      start_threads();
      _cond.notify_all();
    }
  }
}

void ThreadPool::worker(WorkThread *wt)
{
  std::unique_lock<std::mutex> l(_lock);
  ldout(cct,10) << "worker start" << dendl;

  std::stringstream ss;
  ss << name << " thread " << (void*)pthread_self();
  heartbeat_handle_d *hb = cct->get_heartbeat_map()->add_worker(ss.str());

  while (!_stop) {

    // manage dynamic thread pool
    join_old_threads();
    if (_threads.size() > _num_threads) {
      ldout(cct,1) << " worker shutting down; too many threads (" << _threads.size() << " > " << _num_threads << ")" << dendl;
      _threads.erase(wt);
      _old_threads.push_back(wt);
      break;
    }

    if (!_pause && !work_queues.empty()) {
      WorkQueue_* wq;
      int tries = work_queues.size();
      bool did = false;
      while (tries--) {
	last_work_queue++;
	last_work_queue %= work_queues.size();
	wq = work_queues[last_work_queue];

	void *item = wq->_void_dequeue();
	if (item) {
	  processing++;
	  ldout(cct,12) << "worker wq " << wq->name << " start processing " << item
			<< " (" << processing << " active)" << dendl;
	  TPHandle tp_handle(cct, hb,
			     wq->timeout_interval, wq->suicide_interval);
	  tp_handle.reset_tp_timeout();
	  l.unlock();
	  wq->_void_process(item, tp_handle);
	  l.lock();
	  wq->_void_process_finish(item);
	  processing--;
	  ldout(cct,15) << "worker wq " << wq->name << " done processing " << item
			<< " (" << processing << " active)" << dendl;
	  if (_pause || _draining)
	    _wait_cond.notify_all();
	  did = true;
	  break;
	}
      }
      if (did)
	continue;
    }

    ldout(cct,20) << "worker waiting" << dendl;
    cct->get_heartbeat_map()->reset_timeout(hb, std::chrono::seconds(4),
					    ceph::timespan(0));
    _cond.wait_for(l, 2s);
  }
  ldout(cct,1) << "worker finish" << dendl;

  cct->get_heartbeat_map()->remove_worker(hb);

  l.unlock();
}

void ThreadPool::start_threads()
{
  while (_threads.size() < _num_threads) {
    WorkThread *wt = new WorkThread(this);
    ldout(cct, 10) << "start_threads creating and starting " << wt << dendl;
    _threads.insert(wt);
    wt->create();
  }
}

void ThreadPool::join_old_threads()
{
  while (!_old_threads.empty()) {
    ldout(cct, 10) << "join_old_threads joining and deleting " << _old_threads.front() << dendl;
    _old_threads.front()->join();
    delete _old_threads.front();
    _old_threads.pop_front();
  }
}

void ThreadPool::start()
{
  ldout(cct,10) << "start" << dendl;

  if (_thread_num_option.length()) {
    ldout(cct, 10) << " registering config observer on " << _thread_num_option << dendl;
    cct->_conf->add_observer(this);
  }

  {
    std::lock_guard<std::mutex> l(_lock);
    start_threads();
  }
  ldout(cct,15) << "started" << dendl;
}

void ThreadPool::stop(bool clear_after)
{
  ldout(cct,10) << "stop" << dendl;

  if (_thread_num_option.length()) {
    ldout(cct, 10) << " unregistering config observer on " << _thread_num_option << dendl;
    cct->_conf->remove_observer(this);
  }

  std::unique_lock<std::mutex> l(_lock);
  _stop = true;
  _cond.notify_all();
  join_old_threads();
  l.unlock();
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       ++p) {
    (*p)->join();
    delete *p;
  }
  _threads.clear();
  l.lock();
  for (unsigned i=0; i<work_queues.size(); i++)
    work_queues[i]->_clear();
  _stop = false;
  l.unlock();
  ldout(cct,15) << "stopped" << dendl;
}

void ThreadPool::pause()
{
  ldout(cct,10) << "pause" << dendl;
  std::unique_lock<std::mutex> l(_lock);
  _pause++;
  while (processing)
    _wait_cond.wait(l);
  l.unlock();
  ldout(cct,15) << "paused" << dendl;
}

void ThreadPool::pause_new()
{
  ldout(cct,10) << "pause_new" << dendl;
  std::unique_lock<std::mutex> l(_lock);
  _pause++;
  l.unlock();
}

void ThreadPool::unpause()
{
  ldout(cct,10) << "unpause" << dendl;
  std::unique_lock<std::mutex> l(_lock);
  assert(_pause > 0);
  _pause--;
  _cond.notify_all();
  l.unlock();
}

void ThreadPool::drain(WorkQueue_* wq)
{
  ldout(cct,10) << "drain" << dendl;
  std::unique_lock<std::mutex> l(_lock);
  _draining++;
  while (processing || (wq != NULL && !wq->_empty()))
    _wait_cond.wait(l);
  _draining--;
  l.unlock();
}

