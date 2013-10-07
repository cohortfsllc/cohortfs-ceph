// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "DirStripe.h"
#include "Inode.h"
#include "messages/MClientCaps.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.stripe(" << ds << ") "

void DirStripe::print(ostream &out)
{
  out << ds << '('
      << "dentries=" << dentry_map.size()
      << " complete=" << is_complete()
      << ' ' << this << ')';
}

ostream& operator<<(ostream &out, DirStripe &stripe)
{
  stripe.print(out);
  return out;
}

DirStripe::DirStripe(Inode *in, stripeid_t stripeid)
  : CapObject(in->cct, in->vino()),
    parent_inode(in), ds(in->ino, stripeid), version(0),
    release_count(0), max_offset(2), shared_gen(0), flags(0)
{
}

unsigned DirStripe::caps_wanted() const
{
  return CapObject::caps_wanted() | CEPH_CAP_LINK_SHARED;
}

void DirStripe::on_caps_granted(unsigned issued)
{
  if (issued & CEPH_CAP_LINK_SHARED) {
    shared_gen++;

    if (is_complete()) {
      ldout(cct, 10) << " clearing I_COMPLETE on " << *this << dendl;
      reset_complete();
    }
  }
}

void DirStripe::read_client_caps(const Cap *cap, MClientCaps *m)
{
  fragstat.nfiles = m->stripe.nfiles;
  fragstat.nsubdirs = m->stripe.nsubdirs;
  fragstat.mtime.decode_timeval(&m->stripe.mtime);

  rstat.rbytes = m->stripe.rbytes;
  rstat.rfiles = m->stripe.rfiles;
  rstat.rsubdirs = m->stripe.rsubdirs;
  rstat.rctime.decode_timeval(&m->stripe.rctime);
}

void DirStripe::write_client_caps(const Cap *cap, MClientCaps *m, unsigned mask)
{
  m->stripe.nfiles = fragstat.nfiles;
  m->stripe.nsubdirs = fragstat.nsubdirs;
  fragstat.mtime.encode_timeval(&m->stripe.mtime);

  m->stripe.rbytes = rstat.rbytes;
  m->stripe.rfiles = rstat.rfiles;
  m->stripe.rsubdirs = rstat.rsubdirs;
  rstat.rctime.encode_timeval(&m->stripe.rctime);
}

