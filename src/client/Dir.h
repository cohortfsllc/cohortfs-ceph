#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

class Inode;

class Dir {
 public:

  Mutex mtx;
  Inode *parent_inode;  // my inode
  hash_map<string, Dentry*> dentries;
  map<string, Dentry*> dentry_map;
  uint64_t release_count;
  uint64_t max_offset;

  Dir(Inode* in) : mtx("Dir lock"), release_count(0), max_offset(2) {
      parent_inode = in;
  }

  bool is_empty() {  return dentries.empty(); }

  void lock() {
    mtx.Lock();
  }

  void unlock() {
    mtx.Unlock();
  }
};

#endif
