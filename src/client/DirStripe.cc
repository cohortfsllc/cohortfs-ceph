// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "DirStripe.h"
#include "Inode.h"
#include "messages/MClientCaps.h"

ostream& operator<<(ostream &out, DirStripe &stripe)
{
  out << stripe.ds << '('
      << "dns=" << stripe.dentry_map.size()
      << ' ' << &stripe << ')';
  return out;
}

DirStripe::DirStripe(Inode *in, stripeid_t stripeid)
  : CapObject(in->cct, in->vino()),
    parent_inode(in), ds(in->ino, stripeid), version(0),
    release_count(0), max_offset(2)
{
}

void DirStripe::fill_caps(const Cap *cap, MClientCaps *m, int mask)
{
  m->stripe.nfiles = fragstat.nfiles;
  m->stripe.nsubdirs = fragstat.nsubdirs;
  fragstat.mtime.encode_timeval(&m->stripe.mtime);

  m->stripe.rbytes = rstat.rbytes;
  m->stripe.rfiles = rstat.rfiles;
  m->stripe.rsubdirs = rstat.rsubdirs;
  rstat.rctime.encode_timeval(&m->stripe.rctime);
}

