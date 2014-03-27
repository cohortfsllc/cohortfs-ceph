// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "DirStripe.h"
#include "Inode.h"
#include "Dentry.h"
#include "messages/MClientCaps.h"

#define dout_subsys ceph_subsys_client

#undef dout_prefix
#define dout_prefix *_dout << "client.stripe(" << dirstripe() << ") "

void DirStripe::print(ostream &out)
{
  out << dirstripe() << '('
      << "dentries=" << dentry_map.size()
      << " complete=" << is_complete()
      << ' ' << fragstat
      << ' ' << rstat
      << " caps=(";
  CapObject::print(out);
  out << ") " << this << ')';
}

ostream& operator<<(ostream &out, DirStripe &stripe)
{
  stripe.print(out);
  return out;
}

DirStripe::DirStripe(Inode *in, stripeid_t stripeid)
  : CapObject(in->cct, in->vino(), stripeid), parent_inode(in), version(0),
    release_count(0), num_nonnull(0), shared_gen(0), flags(0)
{
}

Dentry* DirStripe::link(const string &name)
{
  // find/insert an entry in the hashmap
  pair<dn_hashmap::iterator, bool> result = dentries.insert(
      dn_hashmap::value_type(name, NULL));
  if (result.second) {
    Dentry *dn = new Dentry(cct);
    result.first->second = dn;
    dn->name = name;
    dn->stripe = this;
    dentry_map[name] = dn;
    dn->get();
  }
  return result.first->second;
}

Dentry* DirStripe::link(const string &name, vinodeno_t vino, Dentry *dn)
{
  if (!dn) {
    // create a new Dentry
    dn = link(name);

    ldout(cct, 15) << "link stripe " << dirstripe() << " '" << name
        << "' to vino " << vino << " dn " << dn << " (new dn)" << dendl;
  } else {
    ldout(cct, 15) << "link stripe " << dirstripe() << " '" << name
        << "' to vino " << vino << " dn " << dn << " (old dn)" << dendl;
  }

  if (dn->vino.ino == 0 && vino.ino != 0)
    num_nonnull++;
  else if (dn->vino.ino != 0 && vino.ino == 0)
    num_nonnull--;

  dn->vino = vino;
  return dn;
}

Dentry* DirStripe::link(const string &name, Inode *in, Dentry *dn)
{
  // only one parent for directories!
  if (in->is_dir() && !in->dn_set.empty()) {
    Dentry *olddn = in->get_first_parent();
    ldout(cct, 20) << "link parent=" << dn << " old parent=" << olddn << dendl;
    if (olddn != dn) {
      assert(olddn->stripe != this || olddn->name != name);
      olddn->stripe->unlink(olddn, false);
    }
  }

  // link to inode
  dn = link(name, in->vino(), dn);

  if (in->dn_set.insert(dn).second) // get ref if it wasn't already in dn_set
    dn->get();

  ldout(cct, 20) << "link inode " << in << " parents now " << in->dn_set << dendl;
  return dn;
}

void DirStripe::unlink(Dentry *dn, bool keepempty)
{
  ldout(cct, 15) << "unlink " << dirstripe() << " '" << dn->name
      << "' dn " << dn << " vino " << dn->vino << dendl;

  // unlink from inode
  inode_hashmap::const_iterator i = parent_inode->inodes.find(dn->vino);
  if (i != parent_inode->inodes.end()) {
    Inode *in = i->second;
    set<Dentry*>::const_iterator d = in->dn_set.find(dn);
    if (d != in->dn_set.end()) {
      in->dn_set.erase(d);
      dn->put();
    }
    ldout(cct, 20) << "unlink  inode " << in << " parents now "
        << in->dn_set << dendl;
  }

  if (dn->vino.ino != 0)
    num_nonnull--;

  dn->vino.ino = 0;

  // unlink from dir
  dentries.erase(dn->name);
  dentry_map.erase(dn->name);
  dn->stripe = 0;
  dn->put();

  if (is_empty() && !is_any_caps() && !keepempty)
    parent_inode->close_stripe(this);
}

void DirStripe::unlink_all(bool keepempty)
{
  // unlink any dentries
  dn_map::iterator d = dentry_map.begin();
  while (d != dentry_map.end())
    unlink((d++)->second, true);

  if (!keepempty)
    parent_inode->close_stripe(this);
}

Dentry* DirStripe::lookup(const string &name) const
{
  dn_hashmap::const_iterator d = dentries.find(name);
  return d != dentries.end() ? d->second : NULL;
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

  bufferlist::iterator p = m->dentries.begin();
  vector<stripe_cap_update_t> dns;
  ::decode(dns, p);

  for (vector<stripe_cap_update_t>::iterator d = dns.begin(); d != dns.end(); ++d) {
    ldout(cct, 10) << "read_client_caps dentry update " << *d << dendl;
    if (d->ino)
      link(d->name, vinodeno_t(d->ino, CEPH_NOSNAP));
    else {
      Dentry *dn = lookup(d->name);
      if (dn)
        unlink(dn, true);
    }
  }
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

