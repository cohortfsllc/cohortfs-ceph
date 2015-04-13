// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Greg Farnum <gregf@hq.newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "mds/Resetter.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"
#include "mon/MonClient.h"
#include "mds/events/EResetJournal.h"

using rados::CB_Waiter;
using std::cout;

int Resetter::init(int rank)
{
  int r = MDSUtility::init();
  if (r < 0) {
    return r;
  }

  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;
  journaler = new Journaler(ino, mdsmap->get_metadata_volume(objecter),
			    CEPH_FS_ONDISK_MAGIC,
			    objecter, timer);

  return 0;
}

void Resetter::reset()
{
  std::mutex mylock;
  std::condition_variable cond;
  bool done;
  int r;

  unique_lock l(lock);
  journaler->recover(new C_SafeCond(&mylock, &cond, &done, &r));
  l.unlock();

  unique_lock myl(mylock);
  while (!done)
    cond.wait(myl);
  myl.unlock();

  if (r != 0) {
    if (r == -ENOENT) {
      cerr << "journal does not exist on-disk. Did you set a bad rank?"
	   << std::endl;
      shutdown();
      return;
    } else {
      cerr << "got error " << r << "from Journaler, failling" << std::endl;
      shutdown();
      return;
    }
  }

  l.lock();
  uint64_t old_start = journaler->get_read_pos();
  uint64_t old_end = journaler->get_write_pos();
  uint64_t old_len = old_end - old_start;
  cout << "old journal was " << old_start << "~" << old_len << std::endl;

// XXX need better (real) definition for this.  or something mdw 20150215
#define X_layout_period	(1<<22)
  uint64_t new_start = ROUND_UP_TO(old_end+1, X_layout_period);
  cout << "new journal start will be " << new_start
       << " (" << (new_start - old_end) << " bytes past old end)" << std::endl;

  journaler->set_read_pos(new_start);
  journaler->set_write_pos(new_start);
  journaler->set_expire_pos(new_start);
  journaler->set_trimmed_pos(new_start);
  journaler->set_writeable();

  cout << "writing journal head" << std::endl;
  CB_Waiter w;
  journaler->write_head(w);
  lock.unlock();

  r = w.wait();

  l.lock();
  assert(r == 0);

  LogEvent *le = new EResetJournal;

  bufferlist bl;
  le->encode_with_header(bl);

  cout << "writing EResetJournal entry" << std::endl;
  journaler->append_entry(bl);
  journaler->flush(new C_SafeCond(&mylock, &cond, &done,&r));

  l.unlock();

  myl.lock();
  while (!done)
    cond.wait(myl);
  myl.unlock();

  assert(r == 0);

  cout << "done" << std::endl;
}
