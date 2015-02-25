// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_time.h"
#include "MetaSession.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"

ostream& operator<<(ostream &out, Inode &in)
{
  out << in.vino() << "("
      << "ref=" << in._ref
      << " cap_refs=" << in.cap_refs
      << " open=" << in.open_by_mode
      << " mode=" << std::oct << in.mode << std::dec
      << " size=" << in.size << "/" << in.max_size
      << " mtime=" << in.mtime
      << " caps=" << ccap_string(in.caps_issued());
  if (!in.caps.empty()) {
    out << "(";
    for (map<int,Cap*>::iterator p = in.caps.begin();
	 p != in.caps.end();
	 ++p) {
      if (p != in.caps.begin())
	out << ',';
      out << p->first << '=' << ccap_string(p->second->issued);
    }
    out << ")";
  }
  if (in.dirty_caps)
    out << " dirty_caps=" << ccap_string(in.dirty_caps);
  if (in.flushing_caps)
    out << " flushing_caps=" << ccap_string(in.flushing_caps);

  if (in.flags & I_COMPLETE)
    out << " COMPLETE";

#if 0
  if (in.is_file())
    out << " " << in.oset;
#endif

  if (!in.dn_set.empty())
    out << " parents=" << in.dn_set;

  out << ' ' << &in << ")";
  return out;
}


void Inode::make_long_path(filepath& p)
{
  if (!dn_set.empty()) {
    assert((*dn_set.begin())->dir && (*dn_set.begin())->dir->parent_inode);
    (*dn_set.begin())->dir->parent_inode->make_long_path(p);
    p.push_dentry((*dn_set.begin())->name);
  } else {
    p = filepath(ino);
  }
}

/*
 * make a filepath suitable for an mds request
 */
void Inode::make_relative_path(filepath& p)
{
    p = filepath(ino);
}

void Inode::get_open_ref(int mode)
{
  open_by_mode[mode]++;
}

bool Inode::put_open_ref(int mode)
{
  if (--open_by_mode[mode] == 0)
    return true;
  return false;
}

void Inode::get_cap_ref(int cap)
{
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      cap_refs[c]++;
    }
    cap >>= 1;
    n++;
  }
}

int Inode::put_cap_ref(int cap)
{
  // if cap is always a single bit (which it seems to be)
  // all this logic is equivalent to:
  // if (--cap_refs[c]) return false; else return true;
  int last = 0;
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      if (cap_refs[c] <= 0) {
	lderr(cct) << "put_cap_ref " << ccap_string(c)
		   << " went negative on " << *this << dendl;
	assert(cap_refs[c] > 0);
      }
      if (--cap_refs[c] == 0)
	last |= c;
    }
    cap >>= 1;
    n++;
  }
  return last;
}

bool Inode::is_any_caps()
{
  return caps.size();
}

bool Inode::cap_is_valid(Cap* cap)
{
  /*cout << "cap_gen     " << cap->session-> cap_gen << std::endl
    << "session gen " << cap->gen << std::endl
    << "cap expire  " << cap->session->cap_ttl << std::endl
    << "cur time    " << ceph_clock_now(cct) << std::endl;*/
  if ((cap->session->cap_gen <= cap->gen)
      && (ceph::mono_clock::now() < cap->session->cap_ttl)) {
    return true;
  }
  return true;
}

int Inode::caps_issued(int *implemented)
{
  int c = 0;
  int i = 0;
  for (map<int,Cap*>::iterator it = caps.begin();
       it != caps.end();
       ++it)
    if (cap_is_valid(it->second)) {
      c |= it->second->issued;
      i |= it->second->implemented;
    }
  if (implemented)
    *implemented = i;
  return c;
}

void Inode::touch_cap(Cap *cap)
{
  // move to back of LRU
  cap->session->caps.push_back(&cap->cap_item);
}

void Inode::try_touch_cap(int mds)
{
  if (caps.count(mds))
    touch_cap(caps[mds]);
}

bool Inode::caps_issued_mask(unsigned mask)
{
  int c = 0;
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
  for (map<int,Cap*>::iterator it = caps.begin();
       it != caps.end();
       ++it) {
    if (cap_is_valid(it->second)) {
      if ((it->second->issued & mask) == mask) {
	touch_cap(it->second);
	return true;
      }
      c |= it->second->issued;
    }
  }
  if ((c & mask) == mask) {
    // bah.. touch them all
    for (map<int,Cap*>::iterator it = caps.begin();
	 it != caps.end();
	 ++it)
      touch_cap(it->second);
    return true;
  }
  return false;
}

int Inode::caps_used()
{
  int w = 0;
  for (map<int,int>::iterator p = cap_refs.begin();
       p != cap_refs.end();
       ++p)
    if (p->second)
      w |= p->first;
  return w;
}

int Inode::caps_file_wanted()
{
  int want = 0;
  for (map<int,int>::iterator p = open_by_mode.begin();
       p != open_by_mode.end();
       ++p)
    if (p->second)
      want |= ceph_caps_for_mode(p->first);
  return want;
}

int Inode::caps_wanted()
{
  int want = caps_file_wanted() | caps_used();
  if (want & CEPH_CAP_FILE_BUFFER)
    want |= CEPH_CAP_FILE_EXCL;
  return want;
}

int Inode::caps_dirty()
{
  return dirty_caps | flushing_caps;
}

bool Inode::have_valid_size()
{
  // RD+RDCACHE or WR+WRBUFFER => valid size
  if (caps_issued() & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL))
    return true;
  return false;
}

// open Dir for an inode.  if it's not open, allocated it (and pin
// dentry in memory).
Dir *Inode::open_dir()
{
  if (!dir) {
    dir = new Dir(this);
    lsubdout(cct, mds, 15) << "open_dir " << dir << " on " << this << dendl;
    assert(dn_set.size() < 2); // dirs can't be hard-linked
    if (!dn_set.empty())
      (*dn_set.begin())->get();      // pin dentry
    get(); // pin inode
  }
  return dir;
}

bool Inode::check_mode(uid_t ruid, gid_t rgid, gid_t *sgids, int sgids_count,
		       uint32_t rflags)
{
  unsigned fmode = 0;

  if ((rflags & O_ACCMODE) == O_WRONLY)
      fmode = 2;
  else if ((rflags & O_ACCMODE) == O_RDWR)
      fmode = 6;
  else if ((rflags & O_ACCMODE) == O_RDONLY)
      fmode = 4;

  // if uid is owner, owner entry determines access
  if (uid == ruid) {
    fmode = fmode << 6;
  } else if (gid == rgid) {
    // if a gid or sgid matches the owning group, group entry determines access
    fmode = fmode << 3;
  } else {
    int i = 0;
    for (; i < sgids_count; ++i) {
      if (sgids[i] == gid) {
	fmode = fmode << 3;
	break;
      }
    }
  }

  return (mode & fmode) == fmode;
}


void Inode::dump(Formatter *f) const
{
  f->dump_stream("ino") << ino;
  if (rdev)
    f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("mode") << '0' << std::oct << mode << std::dec;
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("nlink", nlink);

  f->dump_int("size", size);
  f->dump_int("max_size", max_size);
  f->dump_int("truncate_seq", truncate_seq);
  f->dump_int("truncate_size", truncate_size);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_int("time_warp_seq", time_warp_seq);

  f->dump_stream("volume") << volume;

  f->dump_unsigned("version", version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("flags", flags);

  if (is_dir()) {
    if (!dir_contacts.empty()) {
      f->open_object_section("dir_contants");
      for (set<int>::iterator p = dir_contacts.begin();
	   p != dir_contacts.end();
	   ++p)
	f->dump_int("mds", *p);
      f->close_section();
    }
    f->dump_int("dir_hashed", (int)dir_hashed);
    f->dump_int("dir_replicated", (int)dir_replicated);
  }

  f->open_array_section("caps");
  for (map<int,Cap*>::const_iterator p = caps.begin(); p != caps.end(); ++p) {
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
  f->dump_int("shared_gen", shared_gen);
  f->dump_int("cache_gen", cache_gen);

  f->dump_stream("hold_caps_until") << hold_caps_until;
  f->dump_unsigned("last_flush_tid", last_flush_tid);

  // open
  if (!open_by_mode.empty()) {
    f->open_array_section("open_by_mode");
    for (map<int,int>::const_iterator p = open_by_mode.begin();
	 p != open_by_mode.end();
	 ++p) {
      f->open_object_section("ref");
      f->dump_unsigned("mode", p->first);
      f->dump_unsigned("refs", p->second);
      f->close_section();
    }
    f->close_section();
  }
  if (!cap_refs.empty()) {
    f->open_array_section("cap_refs");
    for (map<int,int>::const_iterator p = cap_refs.begin();
	 p != cap_refs.end();
	 ++p) {
      f->open_object_section("cap_ref");
      f->dump_stream("cap") << ccap_string(p->first);
      f->dump_int("refs", p->second);
      f->close_section();
    }
    f->close_section();
  }

  f->dump_unsigned("reported_size", reported_size);
  if (wanted_max_size != max_size)
    f->dump_unsigned("wanted_max_size", wanted_max_size);
  if (requested_max_size != max_size)
    f->dump_unsigned("requested_max_size", requested_max_size);

  f->dump_int("ref", _ref);
  f->dump_int("ll_ref", ll_ref);

  if (!dn_set.empty()) {
    f->open_array_section("parents");
    for (set<Dentry*>::const_iterator p = dn_set.begin();
	 p != dn_set.end();
	 ++p) {
      f->open_object_section("dentry");
      f->dump_stream("dir_ino") << (*p)->dir->parent_inode->ino;
      f->dump_string("name", (*p)->name);
      f->close_section();
    }
    f->close_section();
  }
}

void Cap::dump(Formatter *f) const
{
  f->dump_int("mds", session->mds_num);
  f->dump_stream("ino") << inode->ino;
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
