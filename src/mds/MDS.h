#ifndef _MDS_H
#define _MDS_H 1
#include <boost/function.hpp>
#ifdef IN_TREE_BUILD
#include "include/ceph_time.h"
#include "include/buffer.h"
#else
#include "ceph_time.h"
#include "rados/buffer.h"
#endif

#define CEPH_MDS_PROTOCOL    23 /* cluster internal */

class MDS {
protected:
    MDS() { }
    ~MDS() { }
public:
};

class MDSVol {
protected:
    MDSVol() {
    }
    ~MDSVol() { }
public:
    void release();
};

struct ObjAttr {
//    int mask;
    uint64_t filesize;
    int mode;
    int user, group;
    ceph::real_clock atime, mtime, ctime;
    int nlinks;
    int type;
    int rawdev;
};
#define MDS_ATTR_SIZE	1
#define MDS_ATTR_MODE	2
#define MDS_ATTR_GROUP	4
#define MDS_ATTR_OWNER	8
#define MDS_ATTR_ATIME	16
#define MDS_ATTR_MTIME	32
#define MDS_ATTR_CTIME	64
#define MDS_ATTR_NLINKS	128
#define MDS_ATTR_TYPE	256
#define	MDS_ATTR_RAWDEV	512

struct dirptr {
    int64_t cookie;
    unsigned char verifier[8];
};

struct read_delegation {
	int foo;	// need something here
	// probably a list of segments and indication which osd to go to.
};

struct write_delegation {
	int foo;	// need something here
};

#define MDS_MAX_NGROUPS 32

struct identity {
	int uid;
	int gid;
	int ngroups;
	int groups[MDS_MAX_NGROUPS];
};

typedef int accessmask;
#define MDS_ACCESS_READ	1
#define MDS_ACCESS_WRITE 2

class FSObj;
typedef boost::function<void(int status, FSObj *result)>lookupcb;
typedef boost::function<int(int status, int count, bool eof)>readdircb;
typedef boost::function<void(int status, FSObj *result)>createcb;
typedef boost::function<void(int status, std::string toname)>readlinkcb;
typedef boost::function<void(int status, accessmask allowed, accessmask denied)>testaccesscb;
typedef boost::function<void(int status)>getsetattrcb;
typedef boost::function<void(int status, int readcount, bool eof)>readwritecb;
typedef boost::function<void(int status, read_delegation *delegation)>delegatedreadcb;
typedef boost::function<void(int status, write_delegation *delegation)>delegatedwritecb;

class FSObj {
protected:
    FSObj() {
    }
    ~FSObj() { }
public:
    void release();
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
};
#endif /* _MDS_H */
