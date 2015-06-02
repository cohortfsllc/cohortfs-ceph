/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_FSOBJ_H
#define COHORT_MDS_FSOBJ_H

#include <mutex>

#include "common/cohort_function.h"
#include "include/types.h" // inodeno_t
#include "mds_types.h"

namespace cohort {
namespace mds {

#if 0
class FSObj;
typedef cohort::function<void(int status, FSObj *result)> lookupcb;
typedef cohort::function<int(int status, int count, bool eof)> readdircb;
typedef cohort::function<void(int status, FSObj *result)> createcb;
typedef cohort::function<void(int status, std::string toname)> readlinkcb;
typedef cohort::function<void(int status, accessmask allowed, accessmask denied)> testaccesscb;
typedef cohort::function<void(int status)> getsetattrcb;
typedef cohort::function<void(int status, int readcount, bool eof)> readwritecb;
typedef cohort::function<void(int status, read_delegation *delegation)> delegatedreadcb;
typedef cohort::function<void(int status, write_delegation *delegation)> delegatedwritecb;
#endif

class FSObj {
 public:
  const _inodeno_t ino;
 private:
  mutable std::mutex mtx;
  ObjAttr attr;

  struct Dir {
    Dir() : gen(0) {}
    mutable std::mutex mtx;
    std::map<std::string, FSObj*> entries;
    uint64_t gen; // for readdir verf
  } dir;
 public:
  FSObj(_inodeno_t ino, const identity &who, int type);

  // attr.type is immutable, so we don't need to lock these
  bool is_reg() const { return S_ISREG(attr.type); }
  bool is_dir() const { return S_ISDIR(attr.type); }

  int adjust_nlinks(int n) { return attr.nlinks += n; }

  int getattr(int mask, ObjAttr &attrs) const;
  int setattr(int mask, const ObjAttr &attrs);

  int lookup(const std::string &name, FSObj **obj) const;
  int link(const std::string &name, FSObj *obj);
  int unlink(const std::string &name, FSObj **obj);
#if 0
  int lookup(identity *who, const std::string path, lookupcb * lookres);
  int readdir(dirptr *where, unsigned char *buf, int bufsize,
              readdircb * readdirres);
  int create(identity *who, const std::string name, ObjAttr *attrs,
             createcb * createres);
  // create is also mkdir, mknod
  int symlink(identity *who, const std::string name, const std::string toname, ObjAttr *attrs,
              createcb * symlinkres);
  int readlink(identity *who, readlinkcb *result);
  int testaccess(identity *who, int accesstype, testaccesscb *testaccessres);
  int setattr(identity *who, int mask, ObjAttr *attrs, getsetattrcb *setattrres);
  int getattr(identity *who, int mask, ObjAttr *attrs, getsetattrcb *getattrres);
  int link(identity *who, FSObj *destdir, std::string name, getsetattrcb *linkres);
  int rename(identity *who, std::string oldname, FSObj *newdir, std::string newname,
             getsetattrcb *linkres);
  int unlink(identity *who, std::string name, getsetattrcb *linkres);
  int read(bufferlist bl, int flags, readwritecb *readres);
  int write(bufferlist bl, int flags, readwritecb *readres);
  int prepare_delegated_read(int flags, delegatedreadcb *delegatedreadres);
  int release_delegated_read(read_delegation *delegation, getsetattrcb *releasecb);
  int prepare_delegated_write(int flags, delegatedwritecb *delegatedwriteres);
  int release_delegated_write(write_delegation *delegation, getsetattrcb *releasecb);
  char * get_oid_name();	// not in delegation?
#endif
};

} // namespace mds
} // namespace cohort

#endif /* COHORT_MDS_FSOBJ_H */
