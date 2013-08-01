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
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_MDLOG_H
#define CEPH_MDLOG_H

enum {
  l_mdl_first = 5000,
  l_mdl_evadd,
  l_mdl_evex,
  l_mdl_evtrm,
  l_mdl_ev,
  l_mdl_evexg,
  l_mdl_evexd,
  l_mdl_segadd,
  l_mdl_segex,
  l_mdl_segtrm,
  l_mdl_seg,
  l_mdl_segexg,
  l_mdl_segexd,
  l_mdl_expos,
  l_mdl_wrpos,
  l_mdl_rdpos,
  l_mdl_jlat,
  l_mdl_last,
};

#include "include/types.h"
#include "include/Context.h"

#include "common/Thread.h"
#include "common/Cond.h"

#include "LogSegment.h"

#include <list>

class Journaler;
class LogEvent;
class MDS;
class LogSegment;
class ESubtreeMap;

class PerfCounters;

#include <map>
using std::map;


class MDLog {
public:
  MDS *mds;
protected:
  int num_events; // in events

  int unflushed;

  bool capped;

  uuid_d volume;
  inodeno_t ino;
  Journaler *journaler;

  PerfCounters *logger;


  // -- replay --
  Cond replay_cond;

  class ReplayThread : public Thread {
    MDLog *log;
  public:
    ReplayThread(MDLog *l) : log(l) {}
    void* entry() {
      log->_replay_thread();
      return 0;
    }
  } replay_thread;
  bool already_replayed;

  friend class ReplayThread;
  friend class C_MDL_Replay;

  list<Context*> waitfor_replay;

  void _replay();         // old way
  void _replay_thread();  // new way


  // -- segments --
  map<uint64_t,LogSegment*> segments;
  set<LogSegment*> expiring_segments;
  set<LogSegment*> expired_segments;
  int expiring_events;
  int expired_events;

  // -- subtreemaps --
  friend class ESubtreeMap;
  friend class C_MDS_WroteImportMap;
  friend class MDCache;

public:
  uint64_t get_last_segment_offset() {
    assert(!segments.empty());
    return segments.rbegin()->first;
  }
  LogSegment *get_oldest_segment() {
    return segments.begin()->second;
  }
  void remove_oldest_segment() {
    map<uint64_t, LogSegment*>::iterator p = segments.begin();
    delete p->second;
    segments.erase(p);
  }


private:
  void init_journaler();

  struct C_MDL_WriteError : public Context {
    MDLog *mdlog;
    C_MDL_WriteError(MDLog *m) : mdlog(m) {}
    void finish(int r) {
      mdlog->handle_journaler_write_error(r);
    }
  };
  void handle_journaler_write_error(int r);
 
public:
  void create_logger();
  
  // replay state
  map<inodeno_t, set<inodeno_t> >   pending_exports;



public:
  MDLog(MDS *m) : mds(m),
		  num_events(0), 
		  unflushed(0),
		  capped(false),
		  journaler(0),
		  logger(0),
		  replay_thread(this),
		  already_replayed(false),
		  expiring_events(0), expired_events(0),
		  cur_event(NULL) { }		  
  ~MDLog();


  // -- segments --
  void start_new_segment(Context *onsync=0);

  LogSegment *peek_current_segment() {
    return segments.empty() ? NULL : segments.rbegin()->second;
  }

  LogSegment *get_current_segment() { 
    assert(!segments.empty());
    return segments.rbegin()->second;
  }

  LogSegment *get_segment(uint64_t off) {
    if (segments.count(off))
      return segments[off];
    return NULL;
  }

  bool have_any_segments() {
    return !segments.empty();
  }

  void flush_logger();

  size_t get_num_events() { return num_events; }
  size_t get_num_segments() { return segments.size(); }  

  uint64_t get_read_pos();
  uint64_t get_write_pos();
  uint64_t get_safe_pos();
  Journaler *get_journaler() { return journaler; }
  bool empty() { return segments.empty(); }

  bool is_capped() { return capped; }
  void cap();

  // -- events --
private:
  LogEvent *cur_event;
public:
  void start_entry(LogEvent *e);
  void submit_entry(LogEvent *e, Context *c = 0);
  void start_submit_entry(LogEvent *e, Context *c = 0) {
    start_entry(e);
    submit_entry(e, c);
  }
  bool entry_is_open() { return cur_event != NULL; }

  void wait_for_safe( Context *c );
  void flush();
  bool is_flushed() {
    return unflushed == 0;
  }

private:
  class C_MaybeExpiredSegment : public Context {
    MDLog *mdlog;
    LogSegment *ls;
  public:
    C_MaybeExpiredSegment(MDLog *mdl, LogSegment *s) : mdlog(mdl), ls(s) {}
    void finish(int res) {
      mdlog->_maybe_expired(ls);
    }
  };

  void try_expire(LogSegment *ls);
  void _maybe_expired(LogSegment *ls);
  void _expired(LogSegment *ls);
  void _trim_expired_segments();

public:
  void trim(int max=-1);

private:
  void write_head(Context *onfinish);

public:
  void create(uuid_d volume, Context *onfinish);  // fresh, empty log!
  void open(Context *onopen);      // append() or replay() to follow!
  void append();
  void replay(Context *onfinish);

  void standby_trim_segments();
};

#endif
