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

#ifndef CEPH_FINISHER_H
#define CEPH_FINISHER_H

#include <mutex>
#include <condition_variable>
#include "include/Context.h"
#include "common/Thread.h"
#include "common/ceph_context.h"

class CephContext;

class Finisher {
  CephContext *cct;
  std::mutex finisher_lock;
  std::condition_variable finisher_cond, finisher_empty_cond;
  bool finisher_stop, finisher_running;
  std::vector<Context*> finisher_queue;
  std::list<std::pair<Context*,int> > finisher_queue_rval;

  void *finisher_thread_entry();

  struct FinisherThread : public Thread {
    Finisher *fin;
    FinisherThread(Finisher *f) : fin(f) {}
    void* entry() { return (void*)fin->finisher_thread_entry(); }
  } finisher_thread;

 public:
  void queue(Context *c, int r = 0) {
    std::unique_lock<std::mutex> l(finisher_lock);
    if (r) {
      finisher_queue_rval.push_back(std::pair<Context*, int>(c, r));
      finisher_queue.push_back(NULL);
    } else
      finisher_queue.push_back(c);
    finisher_cond.notify_all();
    l.unlock();
  }
  void queue(std::vector<Context*>& ls) {
    std::unique_lock<std::mutex> l(finisher_lock);
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    finisher_cond.notify_all();
    l.unlock();
    ls.clear();
  }
  void queue(std::deque<Context*>& ls) {
    std::unique_lock<std::mutex> l(finisher_lock);
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    finisher_cond.notify_all();
    l.unlock();
    ls.clear();
  }

  void start();
  void stop();

  void wait_for_empty();

  Finisher(CephContext *cct_) :
    cct(cct_), finisher_stop(false), finisher_running(false),
    finisher_thread(this) {}
  Finisher(CephContext *cct_, std::string name) :
    cct(cct_),
    finisher_stop(false), finisher_running(false),
    finisher_thread(this) {
  }

  ~Finisher() { }
};

class C_OnFinisher : public Context {
  Context *con;
  Finisher *fin;
public:
  C_OnFinisher(Context *c, Finisher *f) : con(c), fin(f) {}
  void finish(int r) {
    fin->queue(con, r);
  }
};

#endif
