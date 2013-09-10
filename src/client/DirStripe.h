#ifndef CEPH_CLIENT_DIRSTRIPE_H
#define CEPH_CLIENT_DIRSTRIPE_H

#include "include/types.h"
#include "mds/mdstypes.h"

class Dentry;
class Inode;

typedef hash_map<string, Dentry*> dn_hashmap;
typedef map<string, Dentry*> dn_map;

class DirStripe {
 public:
  Inode *parent_inode;  // my inode
  dirstripe_t ds;
  dn_hashmap dentries;
  dn_map dentry_map;
  uint64_t release_count;
  uint64_t max_offset;

  DirStripe(Inode *in, stripeid_t stripeid);

  bool is_empty() {  return dentries.empty(); }
};

ostream& operator<<(ostream &out, DirStripe &stripe);

#endif
