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

#ifndef CEPH_WORKQUEUE_H
#define CEPH_WORKQUEUE_H

#include <condition_variable>
#include <mutex>
#include <condition_variable>
#include "Thread.h"
#include "common/config_obs.h"
#include "common/HeartbeatMap.h"

class CephContext;

class ThreadPool : public md_config_obs_t {
  CephContext *cct;
  std::string name;
  std::mutex _lock;
  std::condition_variable _cond;
  bool _stop;
  int _pause;
  int _draining;
  std::condition_variable _wait_cond;

public:
  class TPHandle {
    friend class ThreadPool;
    CephContext *cct;
    heartbeat_handle_d *hb;
    ceph::timespan grace;
    ceph::timespan suicide_grace;
    TPHandle(
      CephContext *cct,
      heartbeat_handle_d *hb,
      ceph::timespan grace,
      ceph::timespan suicide_grace)
      : cct(cct), hb(hb), grace(grace), suicide_grace(suicide_grace) {}
  public:
    void reset_tp_timeout();
    void suspend_tp_timeout();
  };
private:

  struct WorkQueue_ {
    std::string name;
    ceph::timespan timeout_interval, suicide_interval;
    WorkQueue_(std::string n, ceph::timespan ti, ceph::timespan sti)
      : name(n), timeout_interval(ti), suicide_interval(sti)
    { }
    virtual ~WorkQueue_() {}
    virtual void _clear() = 0;
    virtual bool _empty() = 0;
    virtual void *_void_dequeue() = 0;
    virtual void _void_process(void *item, TPHandle &handle) = 0;
    virtual void _void_process_finish(void *) = 0;
  };

  // track thread pool size changes
  unsigned _num_threads;
  std::string _thread_num_option;
  const char **_conf_keys;

  const char **get_tracked_conf_keys() const {
    return _conf_keys;
  }
  void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed);

public:
  template<class T>
  class BatchWorkQueue : public WorkQueue_ {
    ThreadPool *pool;

    virtual bool _enqueue(T *) = 0;
    virtual void _dequeue(T *) = 0;
    virtual void _dequeue(std::list<T*> *) = 0;
    virtual void _process(const std::list<T*> &) { assert(0); }
    virtual void _process(const std::list<T*> &items, TPHandle &handle) {
      _process(items);
    }
    virtual void _process_finish(const std::list<T*> &) {}

    void *_void_dequeue() {
      std::list<T*> *out(new std::list<T*>);
      _dequeue(out);
      if (!out->empty()) {
	return (void *)out;
      } else {
	delete out;
	return 0;
      }
    }
    void _void_process(void *p, TPHandle &handle) {
      _process(*((std::list<T*>*)p), handle);
    }
    void _void_process_finish(void *p) {
      _process_finish(*(std::list<T*>*)p);
      delete (std::list<T*> *)p;
    }

  public:
    BatchWorkQueue(std::string n, ceph::timespan ti,
		   ceph::timespan sti, ThreadPool* p)
      : WorkQueue_(n, ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~BatchWorkQueue() {
      pool->remove_work_queue(this);
    }

    bool queue(T *item) {
      std::unique_lock<std::mutex> l(pool->_lock);
      bool r = _enqueue(item);
      pool->_cond.notify_one();
      l.unlock();
      return r;
    }
    void dequeue(T *item) {
      std::lock_guard<std::mutex> l(pool->_lock);
      _dequeue(item);
    }
    void clear() {
      std::lock_guard<std::mutex> l(pool->_lock);
      _clear();
    }

    void wake() {
      pool->wake();
    }
    void _wake() {
      pool->_wake();
    }
    void drain() {
      pool->drain(this);
    }

  };
  template<typename T, typename U = T>
  class WorkQueueVal : public WorkQueue_ {
    std::mutex _lock;
    ThreadPool *pool;
    std::list<U> to_process;
    std::list<U> to_finish;
    virtual void _enqueue(T) = 0;
    virtual void _enqueue_front(T) = 0;
    virtual bool _empty() = 0;
    virtual U _dequeue() = 0;
    virtual void _process(U) { assert(0); }
    virtual void _process(U u, TPHandle &) {
      _process(u);
    }
    virtual void _process_finish(U) {}

    void *_void_dequeue() {
      {
	std::lock_guard<std::mutex> l(_lock);
	if (_empty())
	  return 0;
	U u = _dequeue();
	to_process.push_back(u);
      }
      return ((void*)1); // Not used
    }
    void _void_process(void *, TPHandle &handle) {
      std::unique_lock<std::mutex> l(_lock);
      assert(!to_process.empty());
      U u = to_process.front();
      to_process.pop_front();
      l.unlock();

      _process(u, handle);

      l.lock();
      to_finish.push_back(u);
      l.unlock();
    }

    void _void_process_finish(void *) {
      std::unique_lock<std::mutex> l(_lock);
      assert(!to_finish.empty());
      U u = to_finish.front();
      to_finish.pop_front();
      l.unlock();

      _process_finish(u);
    }

    void _clear() {}

  public:
    WorkQueueVal(std::string n, ceph::timespan ti, ceph::timespan sti, ThreadPool *p)
      : WorkQueue_(n, ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueueVal() {
      pool->remove_work_queue(this);
    }
    void queue(T item) {
      std::lock_guard<std::mutex> l(_lock);
      _enqueue(item);
      pool->_cond.notify_one();
    }
    void queue_front(T item) {
      std::lock_guard<std::mutex> l(_lock);
      _enqueue_front(item);
      pool->_cond.notify_one();
    }
    void drain() {
      pool->drain(this);
    }
  };
  template<class T>
  class WorkQueue : public WorkQueue_ {
    ThreadPool *pool;

    virtual bool _enqueue(T *) = 0;
    virtual void _dequeue(T *) = 0;
    virtual T *_dequeue() = 0;
    virtual void _process(T *t) { assert(0); }
    virtual void _process(T *t, TPHandle &) {
      _process(t);
    }
    virtual void _process_finish(T *) {}

    void *_void_dequeue() {
      return (void *)_dequeue();
    }
    void _void_process(void *p, TPHandle &handle) {
      _process(static_cast<T *>(p), handle);
    }
    void _void_process_finish(void *p) {
      _process_finish(static_cast<T *>(p));
    }

  public:
    WorkQueue(std::string n, ceph::timespan ti, ceph::timespan sti, ThreadPool* p)
      : WorkQueue_(n, ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueue() {
      pool->remove_work_queue(this);
    }

    bool queue(T *item) {
      std::unique_lock<std::mutex> l(pool->_lock);
      bool r = _enqueue(item);
      pool->_cond.notify_one();
      l.unlock();
      return r;
    }
    void dequeue(T *item) {
      std::lock_guard<std::mutex> l(pool->_lock);
      _dequeue(item);
    }
    void clear() {
      std::lock_guard<std::mutex> l(pool->_lock);
      _clear();
    }

    /// wake up the thread pool (without lock held)
    void wake() {
      pool->wake();
    }
    /// wake up the thread pool (with lock already held)
    void _wake() {
      pool->_wake();
    }
    void drain() {
      pool->drain(this);
    }

  };

private:
  std::vector<WorkQueue_*> work_queues;
  int last_work_queue;

  // threads
  struct WorkThread : public Thread {
    ThreadPool *pool;
    WorkThread(ThreadPool *p) : pool(p) {}
    void *entry() {
      pool->worker(this);
      return 0;
    }
  };

  std::set<WorkThread*> _threads;
  std::list<WorkThread*> _old_threads;  ///< need to be joined
  int processing;

  void start_threads();
  void join_old_threads();
  void worker(WorkThread *wt);

public:
  ThreadPool(CephContext *cct_, std::string nm, int n,
	     const char *option = NULL);
  ~ThreadPool();

  /// return number of threads currently running
  int get_num_threads() {
    std::lock_guard<std::mutex> l(_lock);
    return _num_threads;
  }

  /// assign a work queue to this thread pool
  void add_work_queue(WorkQueue_* wq) {
    work_queues.push_back(wq);
  }
  /// remove a work queue from this thread pool
  void remove_work_queue(WorkQueue_* wq) {
    unsigned i = 0;
    while (work_queues[i] != wq)
      i++;
    for (i++; i < work_queues.size(); i++)
      work_queues[i-1] = work_queues[i];
    assert(i == work_queues.size());
    work_queues.resize(i-1);
  }

  /// wait for a kick on this thread pool
  void wait(std::condition_variable &c,
	    std::unique_lock<std::mutex> l) {
    c.wait(l);
  }

  /// wake up a waiter (with lock already held)
  void _wake() {
    _cond.notify_all();
  }
  /// wake up a waiter (without lock held)
  void wake() {
    std::lock_guard<std::mutex> l(_lock);
    _cond.notify_all();
  }

  /// start thread pool thread
  void start();
  /// stop thread pool thread
  void stop(bool clear_after=true);
  /// pause thread pool (if it not already paused)
  void pause();
  /// pause initiation of new work
  void pause_new();
  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause();
  /// wait for all work to complete
  void drain(WorkQueue_* wq = 0);
};

#endif
