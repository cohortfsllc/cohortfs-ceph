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

#ifndef _BACKWARD_BACKWARD_WARNING_H
#define _BACKWARD_BACKWARD_WARNING_H   // make gcc 4.3 shut up about hash_*
#endif

#include "include/compat.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "mds/Dumper.h"
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "osdc/Journaler.h"

#define dout_subsys ceph_subsys_mds


int Dumper::init(int rank_)
{
  rank = rank_;

  int r = MDSUtility::init();
  if (r < 0) {
    return r;
  }

  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;
  journaler = new Journaler(ino, mdsmap->get_metadata_volume(objecter),
			    CEPH_FS_ONDISK_MAGIC, objecter, timer);
  return 0;
}


int Dumper::recover_journal()
{
  bool done = false;
  std::condition_variable cond;
  std::mutex localLock;
  int r;

  unique_lock l(lock);
  journaler->recover(new C_SafeCond(&localLock, &cond, &done, &r));
  l.unlock();

  unique_lock ll(localLock);
  while (!done)
    cond.wait(ll);
  ll.unlock();

  if (r < 0) { // Error
    derr << "error on recovery: " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(10) << "completed journal recovery" << dendl;
    return 0;
  }
}


void Dumper::dump(const char *dump_file)
{
  AVolRef volume(mdsmap->get_metadata_volume(objecter));

  int r = recover_journal();
  if (r) {
    return;
  }
  uint64_t start = journaler->get_read_pos();
  uint64_t end = journaler->get_write_pos();
  uint64_t len = end-start;
  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;

  std::cout << "journal is " << start << "~" << len << std::endl;

  bufferlist bl;

  rados::CB_Waiter w;
  unique_lock l(lock);
  oid_t oid = file_oid(ino, 0);
  objecter->read(oid, volume, start, len, &bl, w);
  l.unlock();

  r = w.wait();

  std::cout << "read " << bl.length() << " bytes at offset " << start << std::endl;

  int fd = ::open(dump_file, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd >= 0) {
    // include an informative header
    char buf[200];
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "Ceph mds%d journal dump\n start offset %" PRIu64
	    " (0x%" PRIx64 ")\n	      length %" PRIu32 " (0x%" PRIx32 ")\n%c",
	    rank, start, start, bl.length(), bl.length(),
	    4);
    int r = safe_write(fd, buf, sizeof(buf));
    if (r)
      abort();

    // write the data
    ::lseek64(fd, start, SEEK_SET);
    bl.write_fd(fd);
    ::close(fd);

    std::cout << "wrote " << bl.length() << " bytes at offset " << start << " to " << dump_file << "\n"
	      << "NOTE: this is a _sparse_ file; you can\n"
	      << "\t$ tar cSzf " << dump_file << ".tgz " << dump_file << "\n"
	      << "	   to efficiently compress it while preserving sparseness." << std::endl;
  } else {
    int err = errno;
    derr << "unable to open " << dump_file << ": " << cpp_strerror(err) << dendl;
  }
}

void Dumper::undump(const char *dump_file)
{
  std::cout << "undump " << dump_file << std::endl;

  int fd = ::open(dump_file, O_RDONLY);
  if (fd < 0) {
    derr << "couldn't open " << dump_file << ": " << cpp_strerror(errno) << dendl;
    return;
  }

  // Ceph mds0 journal dump
  //  start offset 232401996 (0xdda2c4c)
  //	    length 1097504 (0x10bf20)

  char buf[200];
  int r = safe_read(fd, buf, sizeof(buf));
  if (r < 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return;
  }

  long long unsigned start, len;
  sscanf(strstr(buf, "start offset"), "start offset %llu", &start);
  sscanf(strstr(buf, "length"), "length %llu", &len);

  std::cout << "start " << start << " len " << len << std::endl;

  inodeno_t ino = MDS_INO_LOG_OFFSET + rank;

  Journaler::Header h;
  h.trimmed_pos = start;
  h.expire_pos = start;
  h.write_pos = start+len;
  h.magic = CEPH_FS_ONDISK_MAGIC;

  AVolRef volume(mdsmap->get_metadata_volume(objecter));

  bufferlist hbl;
  ::encode(h, hbl);

  oid_t oid = file_oid(ino, 0);

  std::condition_variable cond;

  std::cout << "writing header " << oid << std::endl;
  rados::CB_Waiter w;
  objecter->write_full(oid, volume, hbl, nullptr, w);

  r = w.wait();

  // read
  uint64_t pos = start;
  uint64_t left = len;
  while (left > 0) {
    bufferlist j;
    lseek64(fd, pos, SEEK_SET);
    uint64_t l = MIN(left, 1024*1024);
    j.read_fd(fd, l);
    std::cout << " writing " << pos << "~" << l << std::endl;
    w.reset();
    objecter->write(oid, volume, pos, l, j, nullptr, w);

    r = w.wait();

    pos += l;
    left -= l;
  }

  VOID_TEMP_FAILURE_RETRY(::close(fd));
  std::cout << "done." << std::endl;
}


/**
 * Write JSON-formatted log entries to standard out.
 */
void Dumper::dump_entries()
{
  std::mutex localLock;
  JSONFormatter jf(true);

  int r = recover_journal();
  if (r) {
    return;
  }

  jf.open_array_section("log");
  bool got_data = true;
  unique_lock l(lock);
  // Until the journal is empty, pop an event or wait for one to
  // be available.
  dout(10) << "Journaler read/write/size: "
      << journaler->get_read_pos() << "/" << journaler->get_write_pos()
      << "/" << journaler->get_write_pos() - journaler->get_read_pos() << dendl;
  while (journaler->get_read_pos() != journaler->get_write_pos()) {
    bufferlist entry_bl;
    got_data = journaler->try_read_entry(entry_bl);
    dout(10) << "try_read_entry: " << got_data << dendl;
    if (got_data) {
      LogEvent *le = LogEvent::decode(entry_bl);
      if (!le) {
	dout(0) << "Error decoding LogEvent" << dendl;
	break;
      } else {
	jf.open_object_section("log_event");
	jf.dump_unsigned("type", le->get_type());
	jf.dump_unsigned("start_off", le->get_start_off());
	jf.dump_stream("stamp") << le->get_stamp();
	le->dump(&jf);
	jf.close_section();
	delete le;
      }
    } else {
      bool done = false;
      std::condition_variable cond;

      journaler->wait_for_readable(new C_SafeCond(&localLock, &cond, &done));
      lock.unlock();

      unique_lock ll(localLock);
      while (!done)
	cond.wait(ll);
      ll.unlock();
      l.lock();
    }
  }
  lock.unlock();
  jf.close_section();
  jf.flush(std::cout);
  return;
}
