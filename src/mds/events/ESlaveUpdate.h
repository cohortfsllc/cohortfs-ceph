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

#ifndef CEPH_MDS_ESLAVEUPDATE_H
#define CEPH_MDS_ESLAVEUPDATE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

/*
 * rollback records, for remote/slave updates, which may need to be manually
 * rolled back during journal replay.  (or while active if master fails, but in 
 * that case these records aren't needed.)
 */ 
struct link_rollback {
  metareqid_t reqid;
  inodeno_t ino;
  bool was_inc;
  utime_t ctime;
  inoparent_t parent;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(reqid, bl);
    ::encode(ino, bl);
    ::encode(was_inc, bl);
    ::encode(ctime, bl);
    ::encode(parent, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(reqid, bl);
    ::decode(ino, bl);
    ::decode(was_inc, bl);
    ::decode(ctime, bl);
    ::decode(parent, bl);
  }
};
WRITE_CLASS_ENCODER(link_rollback)

/*
 * this is only used on an empty dir with a dirfrag on a remote node.
 * we are auth for nothing.  all we need to do is relink the directory
 * in the hierarchy properly during replay to avoid breaking the
 * subtree map.
 */
struct rmdir_rollback {
  metareqid_t reqid;
  inoparent_t dn;
  inodeno_t ino; // ino unlinked
  int d_type;
  vector<stripeid_t> stripes; // stripes removed

  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(reqid, bl);
    ::encode(dn, bl);
    ::encode(ino, bl);
    ::encode(d_type, bl);
    ::encode(stripes, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(reqid, bl);
    ::decode(dn, bl);
    ::decode(ino, bl);
    ::decode(d_type, bl);
    ::decode(stripes, bl);
  }
};
WRITE_CLASS_ENCODER(rmdir_rollback)

struct rename_rollback {
  struct drec {
    inoparent_t dn;
    inodeno_t ino;
    char d_type;
    utime_t mtime;
    utime_t rctime;
    utime_t ctime;
    
    void encode(bufferlist &bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(dn, bl);
      ::encode(ino, bl);
      ::encode(d_type, bl);
      ::encode(mtime, bl);
      ::encode(rctime, bl);
      ::encode(ctime, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(dn, bl);
      ::decode(ino, bl);
      ::decode(d_type, bl);
      ::decode(mtime, bl);
      ::decode(rctime, bl);
      ::decode(ctime, bl);
    }
  };
  WRITE_CLASS_MEMBER_ENCODER(drec)

  metareqid_t reqid;
  drec src, dest;
  utime_t ctime;
  vector<stripeid_t> stripes;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(reqid, bl);
    encode(src, bl);
    encode(dest, bl);
    ::encode(ctime, bl);
    ::encode(stripes, bl);
 }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(reqid, bl);
    decode(src, bl);
    decode(dest, bl);
    ::decode(ctime, bl);
    ::decode(stripes, bl);
  }
};
WRITE_CLASS_ENCODER(rename_rollback)


class ESlaveUpdate : public LogEvent {
public:
  const static int OP_PREPARE = 1;
  const static int OP_COMMIT = 2;
  const static int OP_ROLLBACK = 3;
  
  const static int LINK = 1;
  const static int RENAME = 2;
  const static int RMDIR = 3;

  /*
   * we journal a rollback metablob that contains the unmodified metadata
   * too, because we may be updating previously dirty metadata, which 
   * will allow old log segments to be trimmed.  if we end of rolling back,
   * those updates could be lost.. so we re-journal the unmodified metadata,
   * and replay will apply _either_ commit or rollback.
   */
  EMetaBlob commit;
  bufferlist rollback;
  string type;
  metareqid_t reqid;
  __s32 master;
  __u8 op;  // prepare, commit, abort
  __u8 origop; // link | rename

  ESlaveUpdate() : LogEvent(EVENT_SLAVEUPDATE) { }
  ESlaveUpdate(MDLog *mdlog, const char *s, metareqid_t ri, int mastermds, int o, int oo) : 
    LogEvent(EVENT_SLAVEUPDATE), commit(mdlog), 
    type(s),
    reqid(ri),
    master(mastermds),
    op(o), origop(oo) { }
  
  void print(ostream& out) {
    if (type.length())
      out << type << " ";
    out << " " << (int)op;
    if (origop == LINK) out << " link";
    else if (origop == RENAME) out << " rename";
    else if (origop == RMDIR) out << " rmdir";
    out << " " << reqid;
    out << " for mds." << master;
    out << commit;
  }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(stamp, bl);
    ::encode(type, bl);
    ::encode(reqid, bl);
    ::encode(master, bl);
    ::encode(op, bl);
    ::encode(origop, bl);
    ::encode(commit, bl);
    ::encode(rollback, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    if (struct_v >= 2)
      ::decode(stamp, bl);
    ::decode(type, bl);
    ::decode(reqid, bl);
    ::decode(master, bl);
    ::decode(op, bl);
    ::decode(origop, bl);
    ::decode(commit, bl);
    ::decode(rollback, bl);
  }

  void replay(MDS *mds);
};

#endif
