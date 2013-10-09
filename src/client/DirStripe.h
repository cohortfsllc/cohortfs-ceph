#ifndef CEPH_CLIENT_DIRSTRIPE_H
#define CEPH_CLIENT_DIRSTRIPE_H

#include "include/types.h"
#include "mds/mdstypes.h"
#include "Capability.h"

class Dentry;
class Inode;

typedef hash_map<string, Dentry*> dn_hashmap;
typedef map<string, Dentry*> dn_map;

#define I_COMPLETE 1

class DirStripe : public CapObject {
 public:
  Inode *parent_inode; // my inode
  version_t version;
  dn_hashmap dentries;
  dn_map dentry_map;
  uint64_t release_count;
  uint64_t max_offset;

  frag_info_t fragstat;
  nest_info_t rstat;
  int shared_gen;
  unsigned flags;

  DirStripe(Inode *in, stripeid_t stripeid);

  dirstripe_t dirstripe() const { return dirstripe_t(ino, stripeid); }

  bool is_empty() const {  return dentries.empty(); }

  bool is_complete() const { return flags & I_COMPLETE; }
  void set_complete() { flags |= I_COMPLETE; }
  void reset_complete() { flags &= ~I_COMPLETE; }

  // CapObject
  virtual unsigned caps_wanted() const;
  virtual void read_client_caps(const Cap *cap, MClientCaps *m);
  virtual void write_client_caps(const Cap *cap, MClientCaps *m, unsigned mask);
  virtual void on_caps_granted(unsigned issued);

  virtual void print(ostream &out);
};

ostream& operator<<(ostream &out, DirStripe &stripe);

#endif
