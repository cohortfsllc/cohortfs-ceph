// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MetaSession.h"
#include "Inode.h"
#include "InodeCache.h"
#include "Dentry.h"
#include "DirStripe.h"
#include "ClientSnapRealm.h"
#include "messages/MClientCaps.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.ino(" << ino << ") "

void Inode::print(ostream &out)
{
  out << vino() << "("
      << "ref=" << _ref
      << " open=" << open_by_mode
      << " mode=" << oct << mode << dec
      << " size=" << size
      << " mtime=" << mtime
      << " caps=(";
  CapObject::print(out);
  out << ')';

  if (is_file())
    out << " " << oset;
  if (is_dir())
    out << " stripes=" << stripes;

  if (!dn_set.empty())
    out << " parents=" << dn_set;

  if (is_dir() && has_dir_layout())
    out << " has_dir_layout";

  out << ' ' << this << ")";
}

ostream& operator<<(ostream &out, Inode &in)
{
  in.print(out);
  return out;
}

void Inode::make_long_path(filepath& p)
{
  if (!dn_set.empty()) {
    Dentry *dn = *dn_set.begin();
    assert(dn->stripe && dn->stripe->parent_inode);
    dn->stripe->parent_inode->make_long_path(p);
    p.push_dentry(dn->name);
  } else if (snapdir_parent) {
    snapdir_parent->make_nosnap_relative_path(p);
    string empty;
    p.push_dentry(empty);
  } else
    p = filepath(ino);
}

/*
 * make a filepath suitable for an mds request:
 *  - if we are non-snapped/live, the ino is sufficient, e.g. #1234
 *  - if we are snapped, make filepath relative to first non-snapped parent.
 */
void Inode::make_nosnap_relative_path(filepath& p)
{
  if (snapid == CEPH_NOSNAP) {
    p = filepath(ino);
  } else if (snapdir_parent) {
    snapdir_parent->make_nosnap_relative_path(p);
    string empty;
    p.push_dentry(empty);
  } else if (!dn_set.empty()) {
    Dentry *dn = *dn_set.begin();
    assert(dn->stripe && dn->stripe->parent_inode);
    dn->stripe->parent_inode->make_nosnap_relative_path(p);
    p.push_dentry(dn->name);
  } else {
    p = filepath(ino);
  }
}

void Inode::get_open_ref(int mode)
{
  open_by_mode[mode]++;
}

bool Inode::put_open_ref(int mode)
{
  //cout << "open_by_mode[" << mode << "] " << open_by_mode[mode] << " -> " << (open_by_mode[mode]-1) << std::endl;
  if (--open_by_mode[mode] == 0)
    return true;
  return false;
}

unsigned Inode::caps_wanted() const
{
  unsigned want = CapObject::caps_wanted();
  for (map<int,int>::const_iterator p = open_by_mode.begin();
       p != open_by_mode.end();
       ++p)
    if (p->second)
      want |= ceph_caps_for_mode(p->first);
  if (want & CEPH_CAP_FILE_BUFFER)
    want |= CEPH_CAP_FILE_EXCL;
  return want;
}

void Inode::read_client_caps(const Cap *cap, MClientCaps *m)
{
  layout = m->get_layout();

  // update inode
  unsigned implemented = 0;
  unsigned issued = caps_issued(&implemented) | caps_dirty();
  issued |= implemented;

  if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
    mode = m->inode.mode;
    uid = m->inode.uid;
    gid = m->inode.gid;
  }
  if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
    nlink = m->inode.nlink;
  }
  if ((issued & CEPH_CAP_XATTR_EXCL) == 0 &&
      m->xattrbl.length() &&
      m->inode.xattr_version > xattr_version) {
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(xattrs, p);
    xattr_version = m->inode.xattr_version;
  }

  update_file_bits(m->get_truncate_seq(), m->get_truncate_size(),
                   m->get_size(), m->get_time_warp_seq(),
                   m->get_inode_ctime(), m->get_inode_mtime(),
                   m->get_inode_atime(), issued);

  // max size
  if (cap == auth_cap &&
      m->get_max_size() != max_size) {
    ldout(cct, 10) << "max_size " << max_size << " -> " << m->get_max_size() << dendl;
    max_size = m->get_max_size();
    if (max_size > wanted_max_size) {
      wanted_max_size = 0;
      requested_max_size = 0;
    }
  }
}

bool Inode::on_caps_revoked(unsigned revoked)
{
  if (revoked & CEPH_CAP_FILE_CACHE)
    cache->release(this);

  if (caps_used() & revoked & CEPH_CAP_FILE_BUFFER)
    return cache->flush(this); // waiting for flush?

  return true;
}

bool Inode::check_cap(const Cap *cap, unsigned retain, bool unmounting) const
{
  if (wanted_max_size > max_size &&
      wanted_max_size > requested_max_size &&
      cap == auth_cap)
    return true;

  /* approaching file_max? */
  if ((cap->issued & CEPH_CAP_FILE_WR) &&
      (size << 1) >= max_size &&
      (reported_size << 1) < max_size &&
      cap == auth_cap) {
    ldout(cct, 10) << "size " << size << " approaching max_size " << max_size
        << ", reported " << reported_size << dendl;
    return true;
  }

  return CapObject::check_cap(cap, retain, unmounting);
}

void Inode::write_client_caps(const Cap *cap, MClientCaps *m, unsigned mask)
{
  m->inode.uid = uid;
  m->inode.gid = gid;
  m->inode.mode = mode;

  m->inode.nlink = nlink;

  if (mask & CEPH_CAP_XATTR_EXCL) {
    ::encode(xattrs, m->xattrbl);
    m->inode.xattr_version = xattr_version;
  }

  m->inode.layout = layout;
  m->inode.size = size;
  m->inode.max_size = max_size;
  m->inode.truncate_seq = truncate_seq;
  m->inode.truncate_size = truncate_size;
  mtime.encode_timeval(&m->inode.mtime);
  atime.encode_timeval(&m->inode.atime);
  ctime.encode_timeval(&m->inode.ctime);
  m->inode.time_warp_seq = time_warp_seq;

  reported_size = size;
  if (cap == auth_cap) {
    m->set_max_size(wanted_max_size);
    requested_max_size = wanted_max_size;
    ldout(cct, 15) << "auth cap, setting max_size = " << requested_max_size << dendl;
  }
}

void Inode::update_file_bits(uint64_t trunc_seq, uint64_t trunc_size,
                             uint64_t sz, uint64_t warp_seq,
                             utime_t ct, utime_t mt, utime_t at,
                             unsigned issued)
{
  bool warn = false;
  ldout(cct, 10) << "update_file_bits " << *this << " " << ccap_string(issued)
	   << " mtime " << mt << dendl;
  ldout(cct, 25) << "truncate_seq: mds " << trunc_seq <<  " local "
	   << truncate_seq << " time_warp_seq: mds " << warp_seq
	   << " local " << time_warp_seq << dendl;
  uint64_t prior_size = size;

  if (trunc_seq > truncate_seq ||
      (trunc_seq == truncate_seq && sz > size)) {
    ldout(cct, 10) << "size " << size << " -> " << sz << dendl;
    size = reported_size = sz;
    if (trunc_seq != truncate_seq) {
      ldout(cct, 10) << "truncate_seq " << truncate_seq << " -> "
	       << trunc_seq << dendl;
      truncate_seq = oset.truncate_seq = trunc_seq;

      // truncate cached file data
      if (prior_size > sz)
	cache->invalidate(this, trunc_size, prior_size - trunc_size, true);
    }
  }
  if (trunc_seq >= truncate_seq && trunc_size != truncate_size) {
    if (is_file()) {
      ldout(cct, 10) << "truncate_size " << truncate_size << " -> "
	       << trunc_size << dendl;
      truncate_size = oset.truncate_size = trunc_size;
    } else {
      ldout(cct, 0) << "Hmmm, truncate_seq && truncate_size changed on non-file inode!" << dendl;
    }
  }
  
  // be careful with size, mtime, atime
  if (issued & (CEPH_CAP_FILE_EXCL|
                CEPH_CAP_FILE_WR|
                CEPH_CAP_FILE_BUFFER|
                CEPH_CAP_AUTH_EXCL|
                CEPH_CAP_XATTR_EXCL)) {
    ldout(cct, 30) << "Yay have enough caps to look at our times" << dendl;
    if (ct > ctime) 
      ctime = ct;
    if (warp_seq > time_warp_seq) {
      ldout(cct, 10) << "mds time_warp_seq " << warp_seq
          << " on inode " << *this << " is higher than local time_warp_seq "
          << time_warp_seq << dendl;
      // the mds updated times, so take those!
      mtime = mt;
      atime = at;
      time_warp_seq = warp_seq;
    } else if (warp_seq == time_warp_seq) {
      // take max times
      if (mt > mtime)
	mtime = mt;
      if (at > atime)
	atime = at;
    } else if (issued & CEPH_CAP_FILE_EXCL) {
      // ignore mds values as we have a higher seq
    } else warn = true;
  } else {
    ldout(cct, 30) << "Don't have enough caps, just taking mds' time values" << dendl;
    if (warp_seq >= time_warp_seq) {
      ctime = ct;
      mtime = mt;
      atime = at;
      time_warp_seq = warp_seq;
    } else warn = true;
  }
  if (warn)
    ldout(cct, 0) << "WARNING: " << *this << " mds time_warp_seq " << warp_seq
        << " is lower than local time_warp_seq " << time_warp_seq << dendl;
}

bool Inode::have_valid_size()
{
  // RD+RDCACHE or WR+WRBUFFER => valid size
  if (caps_issued() & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL))
    return true;
  return false;
}

stripeid_t Inode::pick_stripe(const string &dname)
{
  __u32 dnhash = ceph_str_hash(dir_layout.dl_dir_hash,
                               dname.data(), dname.length());
  return stripeid_t(dnhash % stripe_auth.size());
}

// open DirStripe for an inode.  if it's not open, allocate it (and pin dentry in memory).
DirStripe* Inode::open_stripe(stripeid_t stripeid)
{
  assert(stripeid < stripes.size());
  vector<DirStripe*>::iterator s = stripes.begin() + stripeid;
  if (!*s) {
    *s = new DirStripe(this, stripeid);
    lsubdout(cct, client, 15) << "open_stripe " << **s << " on " << *this << dendl;
    assert(dn_set.size() < 2); // dirs can't be hard-linked
    if (!dn_set.empty())
      (*dn_set.begin())->get();      // pin dentry
    get();                  // pin inode
  }
  return *s;
}

bool Inode::check_mode(uid_t ruid, gid_t rgid, gid_t *sgids, int sgids_count, uint32_t rflags)
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
  f->dump_stream("snapid") << snapid;
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

  f->open_object_section("layout");
  ::dump(layout, f);
  f->close_section();
  if (is_dir()) {
    f->open_object_section("dir_layout");
    ::dump(dir_layout, f);
    f->close_section();

    /* FIXME when wip-mds-encoding is merged ***
    f->open_object_section("dir_stat");
    dirstat.dump(f);
    f->close_section();

    f->open_object_section("rstat");
    rstat.dump(f);
    f->close_section();
    */
  }

  f->dump_unsigned("version", version);
  f->dump_unsigned("xattr_version", xattr_version);

  if (is_dir()) {
    if (!dir_contacts.empty()) {
      f->open_object_section("dir_contacts");
      for (set<int>::iterator p = dir_contacts.begin(); p != dir_contacts.end(); ++p)
	f->dump_int("mds", *p);
      f->close_section();
    }
    f->dump_int("dir_hashed", (int)dir_hashed);
    f->dump_int("dir_replicated", (int)dir_replicated);
  }

  CapObject::dump(f);

  if (!cap_snaps.empty()) {
    for (map<snapid_t,CapSnap*>::const_iterator p = cap_snaps.begin(); p != cap_snaps.end(); ++p) {
      f->open_object_section("cap_snap");
      f->dump_stream("follows") << p->first;
      p->second->dump(f);
      f->close_section();
    }
  }

  // open
  if (!open_by_mode.empty()) {
    f->open_array_section("open_by_mode");
    for (map<int,int>::const_iterator p = open_by_mode.begin(); p != open_by_mode.end(); ++p) {
      f->open_object_section("ref");
      f->dump_unsigned("mode", p->first);
      f->dump_unsigned("refs", p->second);
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
    for (set<Dentry*>::const_iterator p = dn_set.begin(); p != dn_set.end(); ++p) {
      f->open_object_section("dentry");
      f->dump_stream("dir_ino") << (*p)->stripe->ino;
      f->dump_stream("dir_stripe") << (*p)->stripe->stripeid;
      f->dump_string("name", (*p)->name);
      f->close_section();
    }
    f->close_section();
  }
}

void CapSnap::dump(Formatter *f) const
{
  f->dump_stream("ino") << in->ino;
  f->dump_stream("issued") << ccap_string(issued);
  f->dump_stream("dirty") << ccap_string(dirty);
  f->dump_unsigned("size", size);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_int("time_warp_seq", time_warp_seq);
  f->dump_stream("mode") << '0' << std::oct << mode << std::dec;
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  if (!xattrs.empty()) {
    f->open_object_section("xattr_lens");
    for (map<string,bufferptr>::const_iterator p = xattrs.begin(); p != xattrs.end(); ++p)
      f->dump_int(p->first.c_str(), p->second.length());
    f->close_section();
  }
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_int("writing", (int)writing);
  f->dump_int("dirty_data", (int)dirty_data);
  f->dump_unsigned("flush_tid", flush_tid);
}
