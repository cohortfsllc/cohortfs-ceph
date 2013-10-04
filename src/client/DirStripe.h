#ifndef CEPH_CLIENT_DIRSTRIPE_H
#define CEPH_CLIENT_DIRSTRIPE_H

#include "include/types.h"
#include "mds/mdstypes.h"
#include "Capability.h"

class Dentry;
class Inode;

typedef hash_map<string, Dentry*> dn_hashmap;
typedef map<string, Dentry*> dn_map;

class DirStripe : public CapObject {
 public:
  Inode *parent_inode;  // my inode
  dirstripe_t ds;
  version_t version;
  dn_hashmap dentries;
  dn_map dentry_map;
  uint64_t release_count;
  uint64_t max_offset;

  frag_info_t fragstat;
  nest_info_t rstat;

  DirStripe(Inode *in, stripeid_t stripeid);

  bool is_empty() {  return dentries.empty(); }

  virtual void fill_caps(const Cap *cap, MClientCaps *m, int mask);
};

ostream& operator<<(ostream &out, DirStripe &stripe);

#endif
