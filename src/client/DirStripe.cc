// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "DirStripe.h"
#include "Inode.h"

ostream& operator<<(ostream &out, DirStripe &stripe)
{
  out << stripe.ds << '('
      << "dns=" << stripe.dentry_map.size()
      << ' ' << &stripe << ')';
  return out;
}

DirStripe::DirStripe(Inode *in, stripeid_t stripeid)
  : parent_inode(in), ds(in->ino, stripeid),
    release_count(0), max_offset(2)
{
}

