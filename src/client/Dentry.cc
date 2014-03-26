// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/utime.h"

#include "Dentry.h"
#include "DirStripe.h"
#include "Inode.h"

#include "common/Formatter.h"

void Dentry::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_stream("stripe") << stripe->dirstripe();
  f->dump_stream("vino") << vino;
  f->dump_int("ref", ref);
  if (lease_mds >= 0) {
    f->dump_int("lease_mds", lease_mds);
    f->dump_stream("lease_ttl") << lease_ttl;
    f->dump_int("lease_gen", lease_gen);
    f->dump_int("lease_seq", lease_seq);
  }
  f->dump_int("cap_shared_gen", cap_shared_gen);
}
