class MDS : public Dispatcher {
public:
    string name;
    int whoami;
    Messenger *messenger;
    MonClient *monc;
    MDSMap *mdsmap;
    Objecter *objecter;
    Finisher *finisher;
protected:
    int last_state, state, want_state;
    Context *createwaitingforosd;
    ceph_tid_t last_tid;
public:
    int get_state() { return state; }
    int get_want_state() { return want_state; }
    void request_state(int s);
    ceph_tid_t issue_tid() { return ++last_tid; }
    version_t beacon_last_seq;
    bool was_laggy;
    uint64_t beacon_sender;
public:
    MDS(const std::string &n, Messenger *m, MonClient *mc);
    ~MDS();
    int get_nodeid() { return whoami; }
    MDSMap *get_mds_map() { return mdsmap; }
    int init(int wanted_state = MDSMap::STATE_BOOT);
    void beacon_start();
    void beacon_send();
    // void handle_mds_beacon(MMDSBeacon *m);
    void handle_signal(int signum);
    bool ms_dispatch(Message *m);
    bool ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con);
    bool shutdown();
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
    int lookup(const std::string path, lookupcb * lookres);
    int readdir(dirptr *where, unsigned char *buf, int bufsize,
	readdircb * readdirres);
    int create(const std::string name, ObjAttr *attrs,
	createcb * createres);
	// create is also mkdir, mknod
    int symlink(const std::string name, const std::string toname, ObjAttr *attrs,
	createcb * symlinkres);
    int readlink(readlinkcb *result);
    int testaccess(int accesstype, testaccesscb *testaccessres);
    int setattr(int mask, ObjAttr *attrs, getsetattrcb *setattrres);
    int getattr(int mask, ObjAttr *attrs, getsetattrcb *getattrres);
    int link(FSObj *destdir, std::string name, getsetattrcb *linkres);
    int rename(std::string oldname, FSObj *newdir, std::string newname,
	getsetattrcb *linkres);
    int unlink(std::string name, getsetattrcb *linkres);
    int read(bufferlist bl, int flags, readwritecb *readres);
    int write(bufferlist bl, int flags, readwritecb *readres);
    int prepare_delegated_read(int flags, delegatedreadcb *delegatedreadres);
    int release_delegated_read(read_delegation *delegation, getsetattrcb *releasecb);
    int prepare_delegated_write(int flags, delegatedwritecb *delegatedwriteres);
    int release_delegated_write(write_delegation *delegation, getsetattrcb *releasecb);
    char * get_oid_name();	// not in delegation?
};
