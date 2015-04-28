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

#include <iostream>
#include "os/file/FileStore.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#undef dout_prefix
#define dout_prefix *_dout

using namespace std;

CephContext* cct;

struct io {
  ceph::mono_time start, ack, commit;
  bool done() {
    return (ack != ceph::mono_time::min() &&
	    commit != ceph::mono_time::min());
  }
};
map<off_t,io> writes;
std::condition_variable cond;
std::mutex test_lock;

unsigned concurrent = 1;
void throttle()
{
  unique_lock<mutex> tl(test_lock);
  cond.wait(tl, [&](){ return writes.size() < concurrent; });
}

ceph::timespan total_ack = 0ns;
ceph::timespan total_commit = 0ns;
int total_num = 0;

void pr(off_t off)
{
  io &i = writes[off];
  if (false) cout << off << "\t"
       << (i.ack - i.start) << "\t"
       << (i.commit - i.start) << std::endl;
  total_num++;
  total_ack += (i.ack - i.start);
  total_commit += (i.commit - i.start);
  writes.erase(off);
  cond.notify_all();
}

void set_start(off_t off, ceph::mono_time t)
{
  lock_guard<mutex> l(test_lock);
  writes[off].start = t;
}

void set_ack(off_t off, ceph::mono_time t)
{
  lock_guard<mutex> l(test_lock);
  //generic_dout(0) << "ack " << off << dendl;
  writes[off].ack = t;
  if (writes[off].done())
    pr(off);
}

void set_commit(off_t off, ceph::mono_time t)
{
  lock_guard<mutex> l(test_lock);
  //generic_dout(0) << "commit " << off << dendl;
  writes[off].commit = t;
  if (writes[off].done())
    pr(off);
}


struct C_Ack : public Context {
  off_t off;
  C_Ack(off_t o) : off(o) {}
  void finish(int r) {
    set_ack(off, ceph::mono_clock::now());
  }
};
struct C_Commit : public Context {
  off_t off;
  C_Commit(off_t o) : off(o) {}
  void finish(int r) {
    set_commit(off, ceph::mono_clock::now());
  }
};


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  // args
  if (args.size() < 3) return -1;
  const char *filename = args[0];
  int seconds = atoi(args[1]);
  int bytes = atoi(args[2]);
  const char *journal = 0;
  if (args.size() >= 4)
    journal = args[3];
  if (args.size() >= 5)
    concurrent = atoi(args[4]);

  cout << "concurrent = " << concurrent << std::endl;

  buffer::ptr bp(bytes);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);

  //float interval = 1.0 / 1000;

  cout << "#dev " << filename
       << ", " << seconds << " seconds, " << bytes << " bytes per write" << std::endl;

  ObjectStore *fs = new FileStore(cct, filename, journal);

  if (fs->mkfs() < 0) {
    cout << "mkfs failed" << std::endl;
    return -1;
  }

  if (fs->mount() < 0) {
    cout << "mount failed" << std::endl;
    return -1;
  }

  Transaction ft;
  ft.create_collection(coll_t());
  fs->apply_transaction(ft);

  ceph::mono_time now = ceph::mono_clock::now();
  ceph::mono_time start = now;
  ceph::mono_time end = now + seconds * 1s;
  off_t pos = 0;

  CollectionHandle ch = fs->open_collection(coll_t());

  //cout << "stop at " << end << std::endl;
  cout << "# offset\tack\tcommit" << std::endl;
  while (now < end) {
    hoid_t oid(oid_t("streamtest"));
    set_start(pos, ceph::mono_clock::now());
    Transaction *t = new Transaction;
    (void) t->push_col(ch);
    (void) t->push_oid(oid);
    t->write(pos, bytes, bl);
    fs->queue_transaction(t, new C_Ack(pos), new C_Commit(pos));
    pos += bytes;

    throttle();

    now = ceph::mono_clock::now();
  }

  cout << "total num " << total_num << std::endl;
  cout << "avg ack\t" << (total_ack / total_num) << std::endl;
  cout << "avg commit\t" << (total_commit / total_num) << std::endl;
  std::chrono::duration<double> elapsed = (end - start);
  cout << "tput\t" << prettybyte_t((double)(total_num * bytes) /
				   elapsed.count()) << "/sec"
       << std::endl;

  fs->close_collection(ch);
  fs->umount();

}

