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


#ifndef CEPH_FILEJOURNAL_H
#define CEPH_FILEJOURNAL_H

#include <condition_variable>
#include <deque>
#include <mutex>
#include <set>

#include "Journal.h"
#include "common/Thread.h"
#include "common/Throttle.h"
#include "common/zipkin_trace.h"

#ifdef HAVE_LIBAIO
# include <libaio.h>
#endif

class ObjectStore;

/**
 * Implements journaling on top of block device or file.
 *
 * Lock ordering is write_lock > aio_lock > finisher_lock
 */
class FileJournal : public Journal {
public:
  /// Protected by finisher_lock
  struct completion_item {
    uint64_t seq;
    Context *finish;
    ceph::mono_time start;
    OpRequestRef op;
    ZTracer::Trace trace;
    completion_item(uint64_t o, Context *c, ceph::mono_time s,
		    OpRequestRef opref, ZTracer::Trace &trace)
      : seq(o), finish(c), start(s), op(opref), trace(trace) {}
    completion_item(uint64_t o = 0)
      : seq(o), finish(0), start(ceph::mono_time::min()) {}
    bool operator<(const completion_item &rhs) const { return seq < rhs.seq; }
  };
  struct write_item {
    uint64_t seq;
    bufferlist bl;
    int alignment;
    OpRequestRef op;
    ZTracer::Trace trace;
    write_item(uint64_t s, bufferlist& b, int al, OpRequestRef opref,
	ZTracer::Trace &trace)
      : seq(s), alignment(al), op(opref), trace(trace) {
      bl.claim(b);
    }
    write_item() : seq(0), alignment(0) {}
  };

  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;

  ObjectStore* os;
  ZTracer::Endpoint trace_endpoint;
  std::mutex finisher_lock;
  std::condition_variable finisher_cond;
  uint64_t journaled_seq;
  bool plug_journal_completions;

  std::mutex writeq_lock;
  std::condition_variable writeq_cond;
  deque<write_item> writeq;
  bool writeq_empty();
  write_item &peek_write();
  void pop_write();

  std::mutex completions_lock;
  std::set<completion_item> completions;
  bool completions_empty() {
    lock_guard l(completions_lock);
    return completions.empty();
  }

  void submit_entry(uint64_t seq, bufferlist& bl, int alignment,
		    Context *oncommit, ZTracer::Trace &trace,
		    OpRequestRef osd_op = OpRequestRef());
  /// End protected by finisher_lock

  /*
   * journal header
   */
  struct header_t {
    enum {
      FLAG_CRC = (1<<0),
      // NOTE: remove kludgey weirdness in read_header() next time a flag is added.
    };

    uint64_t flags;
    boost::uuids::uuid fsid;
    uint32_t block_size;
    uint32_t alignment;
    int64_t max_size;	// max size of journal ring buffer
    int64_t start;	// offset of first entry
    uint64_t committed_up_to; // committed up to

    /**
     * start_seq
     *
     * entry at header.start has sequence >= start_seq
     *
     * Generally, the entry at header.start will have sequence
     * start_seq if it exists.	The only exception is immediately
     * after journal creation since the first sequence number is
     * not known.
     *
     * If the first read on open fails, we can assume corruption
     * if start_seq > committed_up_thru because the entry would have
     * a sequence >= start_seq and therefore > committed_up_thru.
     */
    uint64_t start_seq;

    header_t() :
      flags(0), block_size(0), alignment(0), max_size(0), start(0),
      committed_up_to(0), start_seq(0) {}

    void clear() {
      start = block_size;
    }

    uint64_t get_fsid64() {
      uint64_t fsid64;
      memcpy(&fsid64, &fsid, sizeof(uint64_t));
      return fsid64;
    }

    void encode(bufferlist& bl) const {
      uint32_t v = 4;
      ::encode(v, bl);
      bufferlist em;
      {
	::encode(flags, em);
	::encode(fsid, em);
	::encode(block_size, em);
	::encode(alignment, em);
	::encode(max_size, em);
	::encode(start, em);
	::encode(committed_up_to, em);
	::encode(start_seq, em);
      }
      ::encode(em, bl);
    }
    void decode(bufferlist::iterator& bl) {
      uint32_t v;
      ::decode(v, bl);
      bufferlist em;
      ::decode(em, bl);
      bufferlist::iterator t = em.begin();
      ::decode(flags, t);
      ::decode(fsid, t);
      ::decode(block_size, t);
      ::decode(alignment, t);
      ::decode(max_size, t);
      ::decode(start, t);

      if (v > 2)
	::decode(committed_up_to, t);
      else
	committed_up_to = 0;

      if (v > 3)
	::decode(start_seq, t);
      else
	start_seq = 0;
    }
  } header;

  struct entry_header_t {
    uint64_t seq;     // fs op seq #
    uint32_t crc32c;  // payload only.	not header, pre_pad, post_pad, or footer.
    uint32_t len;
    uint32_t pre_pad, post_pad;
    uint64_t magic1;
    uint64_t magic2;

    void make_magic(off64_t pos, uint64_t fsid) {
      magic1 = pos;
      magic2 = fsid ^ seq ^ len;
    }
    bool check_magic(off64_t pos, uint64_t fsid) {
      return
	magic1 == (uint64_t)pos &&
	magic2 == (fsid ^ seq ^ len);
    }
  } __attribute__((__packed__, aligned(4)));

private:
  string fn;

  char *zero_buf;

  off64_t max_size;
  size_t block_size;
  bool is_bdev;
  bool directio, aio, force_aio;
  bool must_write_header;
  off64_t write_pos;	  // byte where the next entry to be written will go
  off64_t read_pos;

#ifdef HAVE_LIBAIO
  /// state associated with an in-flight aio request
  /// Protected by aio_lock
  struct aio_info {
    struct iocb iocb;
    bufferlist bl;
    struct iovec *iov;
    bool done;
    uint64_t off, len;	  ///< these are for debug only
    uint64_t seq; ///< seq number to complete on aio completion, if non-zero

    aio_info(bufferlist& b, uint64_t o, uint64_t s)
      : iov(NULL), done(false), off(o), len(b.length()), seq(s) {
      bl.claim(b);
      memset((void*)&iocb, 0, sizeof(iocb));
    }
    ~aio_info() {
      delete[] iov;
    }
  };
  std::mutex aio_lock;
  std::condition_variable aio_cond;
  std::condition_variable write_finish_cond;
  io_context_t aio_ctx;
  list<aio_info> aio_queue;
  int aio_num, aio_bytes;
  /// End protected by aio_lock
#endif

  uint64_t last_committed_seq;
  uint64_t journaled_since_start;

  /*
   * full states cycle at the beginnging of each commit epoch, when commit_start()
   * is called.
   *   FULL - we just filled up during this epoch.
   *   WAIT - we filled up last epoch; now we have to wait until everything during
   *	      that epoch commits to the fs before we can start writing over it.
   *   NOTFULL - all good, journal away.
   */
  enum {
    FULL_NOTFULL = 0,
    FULL_FULL = 1,
    FULL_WAIT = 2,
  } full_state;

  int fd;

  // in journal
  deque<pair<uint64_t, off64_t> > journalq;  // track seq offsets, so we can trim later.
  uint64_t writing_seq;


  // throttle
  Throttle throttle_ops, throttle_bytes;

  void put_throttle(uint64_t ops, uint64_t bytes);

  // write thread
  std::mutex write_lock;
  bool write_stop;

  std::condition_variable commit_cond;

  int _open(bool wr, bool create=false);
  int _open_block_device();
  void _check_disk_write_cache() const;
  int _open_file(int64_t oldsize, blksize_t blksize, bool create);
  void print_header();
  int read_header();
  bufferptr prepare_header();
  void start_writer();
  void stop_writer();
  void write_thread_entry();

  void queue_completions_thru(uint64_t seq);

  int check_for_full(uint64_t seq, off64_t pos, off64_t size);
  int prepare_multi_write(bufferlist& bl, uint64_t& orig_ops, uint64_t& orig_bytee);
  int prepare_single_write(bufferlist& bl, off64_t& queue_pos, uint64_t& orig_ops, uint64_t& orig_bytes);
  void do_write(bufferlist& bl, unique_lock& wl);

  void write_finish_thread_entry();
  void check_aio_completion();
  void do_aio_write(bufferlist& bl);
  int write_aio_bl(off64_t& pos, bufferlist& bl, uint64_t seq);


  void align_bl(off64_t pos, bufferlist& bl);
  int write_bl(off64_t& pos, bufferlist& bl);

  /// read len from journal starting at in_pos and wrapping up to len
  void wrap_read_bl(
    off64_t in_pos,   ///< [in] start position
    int64_t len,      ///< [in] length to read
    bufferlist* bl,   ///< [out] result
    off64_t *out_pos  ///< [out] next position to read, will be wrapped
    );

  class Writer : public Thread {
    FileJournal *journal;
  public:
    Writer(FileJournal *fj) : journal(fj) {}
    void *entry() {
      journal->write_thread_entry();
      return 0;
    }
  } write_thread;

  class WriteFinisher : public Thread {
    FileJournal *journal;
  public:
    WriteFinisher(FileJournal *fj) : journal(fj) {}
    void *entry() {
      journal->write_finish_thread_entry();
      return 0;
    }
  } write_finish_thread;

  off64_t get_top() {
    return ROUND_UP_TO(sizeof(header), block_size);
  }

 public:
  FileJournal(CephContext* _cct, ObjectStore* os,
	      const boost::uuids::uuid& fsid,
	      Finisher *fin,
	      std::condition_variable *sync_cond,
	      const char *f,
	      bool dio = false,
	      bool ai = true,
	      bool faio = false) :
    Journal(_cct, fsid, fin, sync_cond),
    os(os),
    trace_endpoint("0.0.0.0", 0, NULL),
    journaled_seq(0),
    plug_journal_completions(false),
    fn(f),
    zero_buf(NULL),
    max_size(0), block_size(0),
    is_bdev(false), directio(dio), aio(ai), force_aio(faio),
    must_write_header(false),
    write_pos(0), read_pos(0),
#ifdef HAVE_LIBAIO
    aio_ctx(0),
    aio_num(0), aio_bytes(0),
#endif
    last_committed_seq(0),
    journaled_since_start(0),
    full_state(FULL_NOTFULL),
    fd(-1),
    writing_seq(0),
    throttle_ops(cct),
    throttle_bytes(cct),
    write_stop(false),
    write_thread(this),
    write_finish_thread(this)
  {
    trace_endpoint.copy_name(string("Journal (") + f + ")");
  }
  ~FileJournal() {
    delete[] zero_buf;
  }

  int check();
  int create();
  int open(uint64_t fs_op_seq);
  void close();
  int peek_fsid(boost::uuids::uuid& fsid);

  int dump(ostream& out);

  void flush();

  void throttle();

  bool is_writeable() {
    return read_pos == 0;
  }
  int make_writeable();

  // writes
  void commit_start(uint64_t seq);
  void committed_thru(uint64_t seq);
  bool should_commit_now() {
    return full_state != FULL_NOTFULL;
  }

  void set_wait_on_full(bool b) { wait_on_full = b; }

  // reads

  /// Result code for read_entry
  enum read_entry_result {
    SUCCESS,
    FAILURE,
    MAYBE_CORRUPT
  };

  /**
   * read_entry
   *
   * Reads next entry starting at pos.	If the entry appears
   * clean, *bl will contain the payload, *seq will contain
   * the sequence number, and *out_pos will reflect the next
   * read position.  If the entry is invalid *ss will contain
   * debug text, while *seq, *out_pos, and *bl will be unchanged.
   *
   * If the entry suggests a corrupt log, *ss will contain debug
   * text, *out_pos will contain the next index to check.  If
   * we find an entry in this way that returns SUCCESS, the journal
   * is most likely corrupt.
   */
  read_entry_result do_read_entry(
    off64_t pos,	  ///< [in] position to read
    off64_t *next_pos,	  ///< [out] next position to read
    bufferlist* bl,	  ///< [out] payload for successful read
    uint64_t *seq,	  ///< [out] seq of successful read
    ostream *ss,	  ///< [out] error output
    entry_header_t *h = 0 ///< [out] header
    ); ///< @return result code

  bool read_entry(
    bufferlist &bl,
    uint64_t &last_seq,
    bool *corrupt
    );

  bool read_entry(
    bufferlist &bl,
    uint64_t &last_seq) {
    return read_entry(bl, last_seq, 0);
  }

  // Debug/Testing
  void get_header(
    uint64_t wanted_seq,
    off64_t *_pos,
    entry_header_t *h);
  void corrupt(
    int wfd,
    off64_t corrupt_at);
  void corrupt_payload(
    int wfd,
    uint64_t seq);
  void corrupt_footer_magic(
    int wfd,
    uint64_t seq);
  void corrupt_header_magic(
    int wfd,
    uint64_t seq);
};

WRITE_CLASS_ENCODER(FileJournal::header_t)

#endif
