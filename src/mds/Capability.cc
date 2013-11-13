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

#include "Capability.h"

#include "common/Formatter.h"


/*
 * CapExport
 */

void CapExport::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(wanted, bl);
  ::encode(issued, bl);
  ::encode(pending, bl);
  ::encode(client_follows, bl);
  ::encode(mseq, bl);
  ::encode(last_issue_stamp, bl);
  ENCODE_FINISH(bl);
}

void CapExport::decode(bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  ::decode(wanted, p);
  ::decode(issued, p);
  ::decode(pending, p);
  ::decode(client_follows, p);
  ::decode(mseq, p);
  ::decode(last_issue_stamp, p);
  DECODE_FINISH(p);
}

void CapExport::dump(Formatter *f) const
{
  f->dump_unsigned("wanted", wanted);
  f->dump_unsigned("issued", issued);
  f->dump_unsigned("pending", pending);
  f->dump_unsigned("client_follows", client_follows);
  f->dump_unsigned("migrate_seq", mseq);
  f->dump_stream("last_issue_stamp") << last_issue_stamp;
}

void CapExport::generate_test_instances(list<CapExport*>& ls)
{
  ls.push_back(new CapExport);
  ls.push_back(new CapExport);
  ls.back()->wanted = 1;
  ls.back()->issued = 2;
  ls.back()->pending = 3;
  ls.back()->client_follows = 4;
  ls.back()->mseq = 5;
  ls.back()->last_issue_stamp = utime_t(6, 7);
}


/*
 * Capability::revoke_info
 */

void Capability::revoke_info::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl)
  ::encode(before, bl);
  ::encode(seq, bl);
  ::encode(last_issue, bl);
  ENCODE_FINISH(bl);
}

void Capability::revoke_info::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(before, bl);
  ::decode(seq, bl);
  ::decode(last_issue, bl);
  DECODE_FINISH(bl);
}

void Capability::revoke_info::dump(Formatter *f) const
{
  f->dump_unsigned("before", before);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("last_issue", last_issue);
}

void Capability::revoke_info::generate_test_instances(list<Capability::revoke_info*>& ls)
{
  ls.push_back(new revoke_info);
  ls.push_back(new revoke_info);
  ls.back()->before = 1;
  ls.back()->seq = 2;
  ls.back()->last_issue = 3;
}


/*
 * Capability
 */

void Capability::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl)
  ::encode(last_sent, bl);
  ::encode(last_issue_stamp, bl);

  ::encode(_wanted, bl);
  ::encode(_pending, bl);
  ::encode(_revokes, bl);
  ENCODE_FINISH(bl);
}

void Capability::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl)
  ::decode(last_sent, bl);
  ::decode(last_issue_stamp, bl);

  ::decode(_wanted, bl);
  ::decode(_pending, bl);
  ::decode(_revokes, bl);
  DECODE_FINISH(bl);
  
  _calc_issued();
}

void Capability::dump(Formatter *f) const
{
  f->dump_unsigned("last_sent", last_sent);
  f->dump_unsigned("last_issue_stamp", last_issue_stamp);
  f->dump_unsigned("wanted", _wanted);
  f->dump_unsigned("pending", _pending);

  f->open_array_section("revokes");
  for (list<revoke_info>::const_iterator p = _revokes.begin(); p != _revokes.end(); ++p) {
    f->open_object_section("revoke");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void Capability::generate_test_instances(list<Capability*>& ls)
{
  ls.push_back(new Capability);
  ls.push_back(new Capability);
  ls.back()->last_sent = 11;
  ls.back()->last_issue_stamp = utime_t(12, 13);
  ls.back()->_wanted = 14;
  ls.back()->_pending = 15;
  ls.back()->_revokes.push_back(revoke_info());
  ls.back()->_revokes.back().before = 16;
  ls.back()->_revokes.back().seq = 17;
  ls.back()->_revokes.back().last_issue = 18;
  ls.back()->_revokes.push_back(revoke_info());
  ls.back()->_revokes.back().before = 19;
  ls.back()->_revokes.back().seq = 20;
  ls.back()->_revokes.back().last_issue = 21;
}
