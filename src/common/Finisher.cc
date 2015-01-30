// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"
#include "Finisher.h"

#include "common/debug.h"
#define dout_subsys ceph_subsys_finisher
#undef dout_prefix
#define dout_prefix *_dout << "finisher(" << this << ") "

void Finisher::start()
{
  finisher_thread.create();
}

void Finisher::stop()
{
  std::unique_lock<std::mutex> l(finisher_lock);
  finisher_stop = true;
  finisher_cond.notify_all();
  l.unlock();
  finisher_thread.join();
}

void Finisher::wait_for_empty()
{
  std::unique_lock<std::mutex> l(finisher_lock);
  while (!finisher_queue.empty() || finisher_running) {
    ldout(cct, 10) << "wait_for_empty waiting" << dendl;
    finisher_empty_cond.wait(l);
  }
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
  l.unlock();
}

void *Finisher::finisher_thread_entry()
{
  std::unique_lock<std::mutex> l(finisher_lock);
  ldout(cct, 10) << "finisher_thread start" << dendl;

  while (!finisher_stop) {
    while (!finisher_queue.empty()) {
      vector<Context*> ls;
      list<pair<Context*,int> > ls_rval;
      ls.swap(finisher_queue);
      ls_rval.swap(finisher_queue_rval);
      finisher_running = true;
      l.unlock();
      ldout(cct, 10) << "finisher_thread doing " << ls << dendl;

      for (vector<Context*>::iterator p = ls.begin();
	   p != ls.end();
	   ++p) {
	if (*p) {
	  (*p)->complete(0);
	} else {
	  assert(!ls_rval.empty());
	  Context *c = ls_rval.front().first;
	  c->complete(ls_rval.front().second);
	  ls_rval.pop_front();
	}
      }
      ldout(cct, 10) << "finisher_thread done with " << ls << dendl;
      ls.clear();

      l.lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_thread empty" << dendl;
    finisher_empty_cond.notify_all();
    if (finisher_stop)
      break;

    ldout(cct, 10) << "finisher_thread sleeping" << dendl;
    finisher_cond.wait(l);
  }
  finisher_empty_cond.notify_all();

  ldout(cct, 10) << "finisher_thread stop" << dendl;
  finisher_stop = false;
  l.unlock();
  return 0;
}

