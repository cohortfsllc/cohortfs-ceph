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

#ifndef CEPH_LOGEVENT_H
#define CEPH_LOGEVENT_H

#define EVENT_STRING       1

#define EVENT_SUBTREEMAP   2
#define EVENT_EXPORT       3
#define EVENT_IMPORTSTART  4
#define EVENT_IMPORTFINISH 5
#define EVENT_FRAGMENT     6

#define EVENT_RESETJOURNAL 9

#define EVENT_SESSION      10
#define EVENT_SESSIONS     11

#define EVENT_UPDATE       20
#define EVENT_SLAVEUPDATE  21
#define EVENT_OPEN         22
#define EVENT_COMMITTED    23

#define EVENT_TABLECLIENT  42
#define EVENT_TABLESERVER  43



#include <string>
using namespace std;

#include "include/buffer.h"
#include "include/Context.h"

class MDS;
class LogSegment;

// generic log event
class LogEvent {
 private:
  __u32 _type;
  loff_t _start_off,_end_off;

protected:
  utime_t stamp;

  friend class MDLog;

 public:
  LogSegment *_segment;

  LogEvent(int t) : 
    _type(t), _start_off(0), _end_off(0), _segment(0) { }
  virtual ~LogEvent() { }

  int get_type() { return _type; }
  loff_t get_start_off() { return _start_off; }
  loff_t get_end_off() { return _end_off; }
  utime_t get_stamp() const { return stamp; }

  void set_stamp(utime_t t) { stamp = t; }

  // encoding
  virtual void encode(bufferlist& bl) const = 0;
  virtual void decode(bufferlist::iterator &bl) = 0;
  static LogEvent *decode(bufferlist &bl);

  void encode_with_header(bufferlist& bl) {
    ::encode(_type, bl);
    encode(bl);
  }

  virtual void print(ostream& out) { 
    out << "event(" << _type << ")";
  }

  /*** live journal ***/
  /* update_segment() - adjust any state we need to in the LogSegment 
   */
  virtual void update_segment() { }

  /*** recovery ***/
  /* replay() - replay given event.  this is idempotent.
   */
  virtual void replay(MDS *m) { assert(0); }


};

inline ostream& operator<<(ostream& out, LogEvent& le) {
  le.print(out);
  return out;
}

#endif
