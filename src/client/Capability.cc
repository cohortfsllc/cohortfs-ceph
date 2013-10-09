// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"

#include "Capability.h"
#include "MetaSession.h"
#include "SnapRealm.h"

#define dout_subsys ceph_subsys_client

void CapObject::print(ostream &out)
{
  out << "refs=" << cap_refs
      << " issued=" << ccap_string(caps_issued())
      << " {";
  for (cap_map::const_iterator i = caps.begin(); i != caps.end(); ++i) {
    if (i != caps.begin()) out << ",";
    out << i->first << "=" << ccap_string(i->second->issued);
  }
  out << '}';
  if (dirty_caps)
    out << " dirty=" << ccap_string(dirty_caps);
  if (flushing_caps)
    out << " flushing=" << ccap_string(flushing_caps);
}

ostream& operator<<(ostream &out, CapObject &o)
{
  o.print(out);
  return out;
}

CapObject::CapObject(CephContext *cct, vinodeno_t vino, stripeid_t stripeid)
  : cct(cct),
    ino(vino.ino),
    stripeid(stripeid),
    snaprealm(NULL),
    snapid(vino.snapid),
    snaprealm_item(this),
    auth_cap(NULL),
    dirty_caps(0),
    flushing_caps(0),
    flushing_cap_seq(0),
    snap_caps(0),
    snap_cap_refs(0),
    exporting_issued(0),
    exporting_mds(-1),
    exporting_mseq(0),
    cap_item(this),
    flushing_cap_item(this),
    last_flush_tid(0)
{
  memset(&flushing_cap_tid, 0, sizeof(flushing_cap_tid));
}

void CapObject::get_cap_ref(unsigned cap)
{
  unsigned n = 0;
  while (cap) {
    if (cap & 1) {
      unsigned c = 1 << n;
      cap_refs[c]++;
    }
    cap >>= 1;
    n++;
  }
}

bool CapObject::put_cap_ref(unsigned cap)
{
  // if cap is always a single bit (which it seems to be)
  // all this logic is equivalent to:
  // if (--cap_refs[c]) return false; else return true;
  bool last = false;
  unsigned n = 0;
  while (cap) {
    if (cap & 1) {
      unsigned c = 1 << n;
      assert(cap_refs[c] > 0);
      if (--cap_refs[c] == 0)
	last = true;
    }
    cap >>= 1;
    n++;
  }
  return last;
}

bool CapObject::is_any_caps() const
{
  return caps.size() || exporting_mds >= 0;
}

bool CapObject::cap_is_valid(Cap *cap)
{
  if ((cap->session->cap_gen <= cap->gen)
      && (ceph_clock_now(cct) < cap->session->cap_ttl)) {
    return true;
  }
  //if we make it here, the capabilities aren't up-to-date
  cap->session->was_stale = true;
  return true;
}

unsigned CapObject::caps_issued(unsigned *implemented)
{
  unsigned c = exporting_issued | snap_caps;
  unsigned i = 0;
  for (cap_map::const_iterator it = caps.begin(); it != caps.end(); ++it)
    if (cap_is_valid(it->second)) {
      c |= it->second->issued;
      i |= it->second->implemented;
    }
  if (implemented)
    *implemented = i;
  return c;
}

void CapObject::touch_cap(Cap *cap)
{
  // move to back of LRU
  cap->session->caps.push_back(&cap->cap_item);
}

void CapObject::try_touch_cap(int mds)
{
  cap_map::iterator c = caps.find(mds);
  if (c != caps.end())
    touch_cap(c->second);
}

bool CapObject::caps_issued_mask(unsigned mask)
{
  unsigned c = exporting_issued | snap_caps;
  if ((c & mask) == mask)
    return true;
  // prefer auth cap
  if (auth_cap &&
      cap_is_valid(auth_cap) &&
      (auth_cap->issued & mask) == mask) {
    touch_cap(auth_cap);
    return true;
  }
  // try any cap
  for (cap_map::iterator i = caps.begin(); i != caps.end(); ++i) {
    if (cap_is_valid(i->second)) {
      if ((i->second->issued & mask) == mask) {
	touch_cap(i->second);
	return true;
      }
      c |= i->second->issued;
    }
  }
  if ((c & mask) == mask) {
    // bah.. touch them all
    for (cap_map::iterator i = caps.begin(); i != caps.end(); ++i)
      touch_cap(i->second);
    return true;
  }
  return false;
}

unsigned CapObject::caps_used() const
{
  unsigned w = 0;
  for (ref_map::const_iterator p = cap_refs.begin(); p != cap_refs.end(); ++p)
    if (p->second)
      w |= p->first;
  return w;
}

unsigned CapObject::caps_wanted() const
{
  return caps_used();
}

unsigned CapObject::caps_dirty() const
{
  return dirty_caps | flushing_caps;
}

bool CapObject::check_cap(const Cap *cap, unsigned retain, bool unmounting) const
{
  unsigned revoking = cap->implemented & ~cap->issued;
  unsigned used = caps_used();

  // completed revocation?
  if (revoking && (revoking & used) == 0) {
    ldout(cct, 10) << "completed revocation of "
        << ccap_string(revoking) << dendl;
    return true;
  }

  if (!revoking && unmounting && used == 0)
    return true;

  if (caps_wanted() == cap->wanted &&  // mds knows what we want.
      ((cap->issued & ~retain) == 0) &&// and we don't have anything we wouldn't like
      !dirty_caps)                     // and we have no dirty caps
    return false;

  if (ceph_clock_now(cct) < hold_caps_until) {
    ldout(cct, 10) << "delaying cap release" << dendl;
    return false;
  }

  return true;
}

void CapObject::dump(Formatter *f) const
{
  f->open_array_section("caps");
  for (cap_map::const_iterator p = caps.begin(); p != caps.end(); ++p) {
    f->open_object_section("cap");
    f->dump_int("mds", p->first);
    if (p->second == auth_cap)
      f->dump_int("auth", 1);
    p->second->dump(f);
    f->close_section();
  }
  f->close_section();
  if (auth_cap)
    f->dump_int("auth_cap", auth_cap->session->mds_num);

  f->dump_stream("dirty_caps") << ccap_string(dirty_caps);
  if (flushing_caps) {
    f->dump_stream("flushings_caps") << ccap_string(flushing_caps);
    f->dump_unsigned("flushing_cap_seq", flushing_cap_seq);
    f->open_object_section("flushing_cap_tid");
    for (unsigned bit = 0; bit < CEPH_CAP_BITS; bit++) {
      if (flushing_caps & (1 << bit)) {
	string n(ccap_string(1 << bit));
	f->dump_unsigned(n.c_str(), flushing_cap_tid[bit]);
      }
    }
    f->close_section();
  }
  if (snap_caps) {
    f->dump_int("snap_caps", snap_caps);
    f->dump_int("snap_cap_refs", snap_cap_refs);
  }
  if (exporting_issued || exporting_mseq) {
    f->dump_stream("exporting_issued") << ccap_string(exporting_issued);
    f->dump_int("exporting_mseq", exporting_mds);
  }

  f->dump_stream("hold_caps_until") << hold_caps_until;
  f->dump_unsigned("last_flush_tid", last_flush_tid);

  if (!cap_refs.empty()) {
    f->open_array_section("cap_refs");
    for (ref_map::const_iterator p = cap_refs.begin(); p != cap_refs.end(); ++p) {
      f->open_object_section("cap_ref");
      f->dump_stream("cap") << ccap_string(p->first);
      f->dump_int("refs", p->second);
      f->close_section();
    }
    f->close_section();
  }

  if (snaprealm) {
    f->open_object_section("snaprealm");
    snaprealm->dump(f);
    f->close_section();
  }
}

void Cap::dump(Formatter *f) const
{
  f->dump_int("mds", session->mds_num);
  f->dump_unsigned("cap_id", cap_id);
  f->dump_stream("issued") << ccap_string(issued);
  if (implemented != issued)
    f->dump_stream("implemented") << ccap_string(implemented);
  f->dump_stream("wanted") << ccap_string(wanted);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("issue_seq", issue_seq);
  f->dump_unsigned("mseq", mseq);
  f->dump_unsigned("gen", gen);
}
