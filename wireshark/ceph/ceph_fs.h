// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * ceph_fs.h - Ceph constants and data types to share between kernel and
 * user space.
 *
 * Most types in this file are defined as little-endian, and are
 * primarily intended to describe data structures that pass over the
 * wire or that are stored on disk.
 *
 * LGPL2
 */

#ifndef CEPH_FS_H
#define CEPH_FS_H

#include "msgr.h"
#include "rados.h"

/*
 * subprotocol versions.  when specific messages types or high-level
 * protocols change, bump the affected components.  we keep rev
 * internal cluster protocols separately from the public,
 * client-facing protocol.
 */
#define CEPH_OSD_PROTOCOL     8 /* cluster internal */
#define CEPH_MDS_PROTOCOL    12 /* cluster internal */
#define CEPH_MON_PROTOCOL     5 /* cluster internal */
#define CEPH_OSDC_PROTOCOL   24 /* server/client */
#define CEPH_MDSC_PROTOCOL   32 /* server/client */
#define CEPH_MONC_PROTOCOL   15 /* server/client */


#define CEPH_INO_ROOT  1
#define CEPH_INO_CEPH  2	/* hidden .ceph dir */

/* arbitrary limit on max # of monitors (cluster of 3 is typical) */
#define CEPH_MAX_MON   31


/*
 * feature bits
 */
#define CEPH_FEATURE_UID	    (1<<0)
#define CEPH_FEATURE_NOSRCADDR	    (1<<1)
#define CEPH_FEATURE_MONCLOCKCHECK  (1<<2)
#define CEPH_FEATURE_FLOCK	    (1<<3)
#define CEPH_FEATURE_SUBSCRIBE2	    (1<<4)
#define CEPH_FEATURE_MONNAMES	    (1<<5)
#define CEPH_FEATURE_RECONNECT_SEQ  (1<<6)
#define CEPH_FEATURE_DIRLAYOUTHASH  (1<<7)


/*
 * ceph_file_layout - describe data layout for a file/inode
 */
struct ceph_file_layout {
	/* file -> object mapping */
	uint32_t fl_stripe_unit;	   /* stripe unit, in bytes.  must be multiple
				      of page size. */
	uint32_t fl_stripe_count;	   /* over this many objects */
	uint32_t fl_object_size;	   /* until objects are this big, then move to
				      new objects */
	uint32_t fl_cas_hash;	   /* UNUSED.  0 = none; 1 = sha256 */

	/* pg -> disk layout */
	uint32_t fl_object_stripe_unit;  /* UNUSED.  for per-object parity, if any */

	/* object -> pg layout */
	uint32_t fl_pg_preferred; /* preferred primary for pg (-1 for none) */
	uint32_t fl_pg_pool;	/* namespace, crush ruleset, rep level */
};

#define CEPH_MIN_STRIPE_UNIT 65536

int ceph_file_layout_is_valid(const struct ceph_file_layout *layout);

struct ceph_dir_layout {
	uint8_t	  dl_dir_hash;	 /* see ceph_hash.h for ids */
	uint8_t	  dl_unused1;
	uint16_t  dl_unused2;
	uint32_t  dl_unused3;
};

/* crypto algorithms */
#define CEPH_CRYPTO_NONE 0x0
#define CEPH_CRYPTO_AES	 0x1

#define CEPH_AES_IV "cephsageyudagreg"

/* security/authentication protocols */
#define CEPH_AUTH_UNKNOWN	0x0
#define CEPH_AUTH_NONE		0x1
#define CEPH_AUTH_CEPHX		0x2

#define CEPH_AUTH_UID_DEFAULT ((uint64_t) -1)


/*********************************************
 * message layer
 */

/*
 * message types
 */

/* misc */
#define CEPH_MSG_SHUTDOWN		1
#define CEPH_MSG_PING			2

/* client <-> monitor */
#define CEPH_MSG_MON_MAP		4
#define CEPH_MSG_MON_GET_MAP		5
#define CEPH_MSG_STATFS			13
#define CEPH_MSG_STATFS_REPLY		14
#define CEPH_MSG_MON_SUBSCRIBE		15
#define CEPH_MSG_MON_SUBSCRIBE_ACK	16
#define CEPH_MSG_AUTH			17
#define CEPH_MSG_AUTH_REPLY		18

/* client <-> mds */
#define CEPH_MSG_MDS_MAP		21

#define CEPH_MSG_CLIENT_SESSION		22
#define CEPH_MSG_CLIENT_RECONNECT	23

#define CEPH_MSG_CLIENT_REQUEST		24
#define CEPH_MSG_CLIENT_REQUEST_FORWARD 25
#define CEPH_MSG_CLIENT_REPLY		26
#define CEPH_MSG_CLIENT_CAPS		0x310
#define CEPH_MSG_CLIENT_LEASE		0x311
#define CEPH_MSG_CLIENT_CAPRELEASE	0x313

/* pool ops */
#define CEPH_MSG_POOLOP_REPLY		48
#define CEPH_MSG_POOLOP			49


/* osd */
#define CEPH_MSG_OSD_MAP		41
#define CEPH_MSG_OSD_OP			42
#define CEPH_MSG_OSD_OPREPLY		43
#define CEPH_MSG_WATCH_NOTIFY		44


/* watch-notify operations */
enum {
  WATCH_NOTIFY				= 1, /* notifying watcher */
  WATCH_NOTIFY_COMPLETE			= 2, /* notifier notified when done */
};


/* pool operations */
enum {
  POOL_OP_CREATE			= 0x01,
  POOL_OP_DELETE			= 0x02,
  POOL_OP_AUID_CHANGE			= 0x03,
};

struct ceph_mon_request_header {
	uint64_t have_version;
	uint16_t session_mon;
	uint64_t session_mon_tid;
};

struct ceph_mon_statfs {
	struct ceph_mon_request_header monhdr;
	struct ceph_fsid fsid;
};

struct ceph_statfs {
	uint64_t kb, kb_used, kb_avail;
	uint64_t num_objects;
};

struct ceph_mon_statfs_reply {
	struct ceph_fsid fsid;
	uint64_t version;
	struct ceph_statfs st;
};

const char *ceph_pool_op_name(int op);

struct ceph_mon_poolop {
	struct ceph_mon_request_header monhdr;
	struct ceph_fsid fsid;
	uint32_t pool;
	uint32_t op;
	uint64_t auid;
	uint32_t name_len;
};

struct ceph_mon_poolop_reply {
	struct ceph_mon_request_header monhdr;
	struct ceph_fsid fsid;
	uint32_t reply_code;
	uint32_t epoch;
	char has_data;
	char data[0];
};

struct ceph_osd_getmap {
	struct ceph_mon_request_header monhdr;
	struct ceph_fsid fsid;
	uint32_t start;
};

struct ceph_mds_getmap {
	struct ceph_mon_request_header monhdr;
	struct ceph_fsid fsid;
};

struct ceph_client_mount {
	struct ceph_mon_request_header monhdr;
};

#define CEPH_SUBSCRIBE_ONETIME	  1  /* i want only 1 update after have */

struct ceph_mon_subscribe_item {
	uint64_t have_version;	uint64_t have;
	uint8_t onetime;
};

struct ceph_mon_subscribe_ack {
	uint32_t duration;	 /* seconds */
	struct ceph_fsid fsid;
};

/*
 * mds states
 *   > 0 -> in
 *  <= 0 -> out
 */
#define CEPH_MDS_STATE_DNE	    0  /* down, does not exist. */
#define CEPH_MDS_STATE_STOPPED	   -1  /* down, once existed, but no subtrees.
					  empty log. */
#define CEPH_MDS_STATE_BOOT	   -4  /* up, boot announcement. */
#define CEPH_MDS_STATE_STANDBY	   -5  /* up, idle.  waiting for assignment. */
#define CEPH_MDS_STATE_CREATING	   -6  /* up, creating MDS instance. */
#define CEPH_MDS_STATE_STARTING	   -7  /* up, starting previously stopped mds */
#define CEPH_MDS_STATE_STANDBY_REPLAY -8 /* up, tailing active node's journal */

#define CEPH_MDS_STATE_REPLAY	    8  /* up, replaying journal. */
#define CEPH_MDS_STATE_RESOLVE	    9  /* up, disambiguating distributed
					  operations (import, rename, etc.) */
#define CEPH_MDS_STATE_RECONNECT    10 /* up, reconnect to clients */
#define CEPH_MDS_STATE_REJOIN	    11 /* up, rejoining distributed cache */
#define CEPH_MDS_STATE_CLIENTREPLAY 12 /* up, replaying client operations */
#define CEPH_MDS_STATE_ACTIVE	    13 /* up, active */
#define CEPH_MDS_STATE_STOPPING	    14 /* up, but exporting metadata */

extern const char *ceph_mds_state_name(int s);


/*
 * metadata lock types.
 *  - these are bitmasks.. we can compose them
 *  - they also define the lock ordering by the MDS
 *  - a few of these are internal to the mds
 */
#define CEPH_LOCK_DVERSION    1
#define CEPH_LOCK_DN	      2
#define CEPH_LOCK_IVERSION    32    /* mds internal */
#define CEPH_LOCK_IFILE	      64
#define CEPH_LOCK_IAUTH	      128
#define CEPH_LOCK_ILINK	      256
#define CEPH_LOCK_IDFT	      512   /* dir frag tree */
#define CEPH_LOCK_INEST	      1024  /* mds internal */
#define CEPH_LOCK_IXATTR      2048
#define CEPH_LOCK_IFLOCK      4096  /* advisory file locks */
#define CEPH_LOCK_INO	      8192  /* immutable inode bits; not a lock */

/* client_session ops */
enum {
	CEPH_SESSION_REQUEST_OPEN,
	CEPH_SESSION_OPEN,
	CEPH_SESSION_REQUEST_CLOSE,
	CEPH_SESSION_CLOSE,
	CEPH_SESSION_REQUEST_RENEWCAPS,
	CEPH_SESSION_RENEWCAPS,
	CEPH_SESSION_STALE,
	CEPH_SESSION_RECALL_STATE,
};

extern const char *ceph_session_op_name(int op);

struct ceph_mds_session_head {
	uint32_t op;
	uint64_t seq;
	struct ceph_timerep stamp;
	uint32_t max_caps, max_leases;
};

/* client_request */
/*
 * metadata ops.
 *  & 0x001000 -> write op
 *  & 0x010000 -> follow symlink (e.g. stat(), not lstat()).
 &  & 0x100000 -> use weird ino/path trace
 */
#define CEPH_MDS_OP_WRITE	 0x001000
enum {
	CEPH_MDS_OP_LOOKUP     = 0x00100,
	CEPH_MDS_OP_GETATTR    = 0x00101,
	CEPH_MDS_OP_LOOKUPHASH = 0x00102,
	CEPH_MDS_OP_LOOKUPPARENT = 0x00103,
	CEPH_MDS_OP_LOOKUPINO  = 0x00104,
	CEPH_MDS_OP_LOOKUPNAME = 0x00105,

	CEPH_MDS_OP_SETXATTR   = 0x01105,
	CEPH_MDS_OP_RMXATTR    = 0x01106,
	CEPH_MDS_OP_SETLAYOUT  = 0x01107,
	CEPH_MDS_OP_SETATTR    = 0x01108,
	CEPH_MDS_OP_SETFILELOCK= 0x01109,
	CEPH_MDS_OP_GETFILELOCK= 0x00110,
	CEPH_MDS_OP_SETDIRLAYOUT=0x0110a,

	CEPH_MDS_OP_MKNOD      = 0x01201,
	CEPH_MDS_OP_LINK       = 0x01202,
	CEPH_MDS_OP_UNLINK     = 0x01203,
	CEPH_MDS_OP_RENAME     = 0x01204,
	CEPH_MDS_OP_MKDIR      = 0x01220,
	CEPH_MDS_OP_RMDIR      = 0x01221,
	CEPH_MDS_OP_SYMLINK    = 0x01222,

	CEPH_MDS_OP_CREATE     = 0x01301,
	CEPH_MDS_OP_OPEN       = 0x00302,
	CEPH_MDS_OP_READDIR    = 0x00305,
};

extern const char *ceph_mds_op_name(int op);


#define CEPH_SETATTR_MODE   1
#define CEPH_SETATTR_UID    2
#define CEPH_SETATTR_GID    4
#define CEPH_SETATTR_MTIME  8
#define CEPH_SETATTR_ATIME 16
#define CEPH_SETATTR_SIZE  32
#define CEPH_SETATTR_CTIME 64

union ceph_mds_request_args {
	struct {
		uint32_t mask;		     /* CEPH_CAP_* */
	} getattr;
	struct {
		uint32_t mode;
		uint32_t uid;
		uint32_t gid;
		struct ceph_timerep mtime;
		struct ceph_timerep atime;
		uint64_t size, old_size;	     /* old_size needed by truncate */
		uint32_t mask;		     /* CEPH_SETATTR_* */
	} setattr;
	struct {
		uint32_t frag;		     /* which dir fragment */
		uint32_t max_entries;	     /* how many dentries to grab */
		uint32_t max_bytes;
	} readdir;
	struct {
		uint32_t mode;
		uint32_t rdev;
	} mknod;
	struct {
		uint32_t mode;
	} mkdir;
	struct {
		uint32_t flags;
		uint32_t mode;
		uint32_t stripe_unit;	     /* layout for newly created file */
		uint32_t stripe_count;	     /* ... */
		uint32_t object_size;
		uint32_t file_replication;
		uint32_t preferred;
	} open;
	struct {
		uint32_t flags;
	} setxattr;
	struct {
		struct ceph_file_layout layout;
	} setlayout;
	struct {
		uint8_t rule; /* currently fcntl or flock */
		uint8_t type; /* shared, exclusive, remove*/
		uint64_t owner; /* who requests/holds the lock */
		uint64_t pid; /* process id requesting the lock */
		uint64_t start; /* initial location to lock */
		uint64_t length; /* num bytes to lock from start */
		uint8_t wait; /* will caller wait for lock to become available? */
	} filelock_change;
};

#define CEPH_MDS_FLAG_REPLAY	    1  /* this is a replayed op */
#define CEPH_MDS_FLAG_WANT_DENTRY   2  /* want dentry in reply */

struct ceph_mds_request_head {
	uint64_t oldest_client_tid;
	uint32_t mdsmap_epoch;	       /* on client */
	uint32_t flags;		       /* CEPH_MDS_FLAG_* */
	uint8_t num_retry, num_fwd;	  /* count retry, fwd attempts */
	uint16_t num_releases;	       /* # include cap/lease release records */
	uint32_t op;		       /* mds op code */
	uint32_t caller_uid, caller_gid;
	uint64_t ino;		       /* use this ino for openc, mkdir, mknod,
					  etc. (if replaying) */
	union ceph_mds_request_args args;
};

/* cap/lease release record */
struct ceph_mds_request_release {
	uint64_t ino, cap_id;	       /* ino and unique cap id */
	uint32_t caps, wanted;	       /* new issued, wanted */
	uint32_t seq, issue_seq, mseq;
	uint32_t dname_seq;	       /* if releasing a dentry lease, a */
	uint32_t dname_len;	       /* string follows. */
};

/* client reply */
struct ceph_mds_reply_head {
	uint32_t op;
	uint32_t result;
	uint32_t mdsmap_epoch;
	uint8_t safe;			  /* true if committed to disk */
	uint8_t is_dentry, is_target;	  /* true if dentry, target inode records
					  are included with reply */
};

/* one for each node split */
struct ceph_frag_tree_split {
	uint32_t frag;		       /* this frag splits... */
	uint32_t by;		       /* ...by this many bits */
};

struct ceph_frag_tree_head {
	uint32_t nsplits;		       /* num ceph_frag_tree_split records */
	struct ceph_frag_tree_split splits[];
};

/* capability issue, for bundling with mds reply */
struct ceph_mds_reply_cap {
	uint32_t caps, wanted;	       /* caps issued, wanted */
	uint64_t cap_id;
	uint32_t seq, mseq;
	uint8_t flags;			  /* CEPH_CAP_FLAG_* */
};

#define CEPH_CAP_FLAG_AUTH  1	       /* cap is issued by auth mds */

/* inode record, for bundling with mds reply */
struct ceph_mds_reply_inode {
	uint64_t ino;
	uint32_t rdev;
	uint64_t version;		       /* inode version */
	uint64_t xattr_version;	       /* version for xattr blob */
	struct ceph_mds_reply_cap cap; /* caps issued for this inode */
	struct ceph_file_layout layout;
	struct ceph_timerep ctime, mtime, atime;
	uint32_t time_warp_seq;
	uint64_t size, max_size, truncate_size;
	uint32_t truncate_seq;
	uint32_t mode, uid, gid;
	uint32_t nlink;
	uint64_t files, subdirs, rbytes, rfiles, rsubdirs;  /* dir stats */
	struct ceph_timerep rctime;
	struct ceph_frag_tree_head fragtree;  /* (must be at end of struct) */
};
/* followed by frag array, symlink string, dir layout, xattr blob */

/* reply_lease follows dname, and reply_inode */
struct ceph_mds_reply_lease {
	uint16_t mask;		/* lease type(s) */
	uint32_t duration_ms;	/* lease duration */
	uint32_t seq;
};

struct ceph_mds_reply_dirfrag {
	uint32_t frag;		/* fragment */
	uint32_t auth;		/* auth mds, if this is a delegation point */
	uint32_t ndist;		/* number of mds' this is replicated on */
	uint32_t dist[];
};

#define CEPH_LOCK_FCNTL	   1
#define CEPH_LOCK_FLOCK	   2

#define CEPH_LOCK_SHARED   1
#define CEPH_LOCK_EXCL	   2
#define CEPH_LOCK_UNLOCK   4

struct ceph_filelock {
	uint64_t start;/* file offset to start lock at */
	uint64_t length; /* num bytes to lock; 0 for all following start */
	uint64_t client; /* which client holds the lock */
	uint64_t owner; /* who requests/holds the lock */
	uint64_t pid; /* process id holding the lock on the client */
	uint8_t type; /* shared lock, exclusive lock, or unlock */
};


/* file access modes */
#define CEPH_FILE_MODE_PIN	  0
#define CEPH_FILE_MODE_RD	  1
#define CEPH_FILE_MODE_WR	  2
#define CEPH_FILE_MODE_RDWR	  3  /* RD | WR */
#define CEPH_FILE_MODE_LAZY	  4  /* lazy io */
#define CEPH_FILE_MODE_NUM	  8  /* bc these are bit fields.. mostly */

int ceph_flags_to_mode(int flags);


/* capability bits */
#define CEPH_CAP_PIN	     1	/* no specific capabilities beyond the pin */

/* generic cap bits */
#define CEPH_CAP_GSHARED     1	/* client can reads */
#define CEPH_CAP_GEXCL	     2	/* client can read and update */
#define CEPH_CAP_GCACHE	     4	/* (file) client can cache reads */
#define CEPH_CAP_GRD	     8	/* (file) client can read */
#define CEPH_CAP_GWR	    16	/* (file) client can write */
#define CEPH_CAP_GBUFFER    32	/* (file) client can buffer writes */
#define CEPH_CAP_GWREXTEND  64	/* (file) client can extend EOF */
#define CEPH_CAP_GLAZYIO   128	/* (file) client can perform lazy io */

/* per-lock shift */
#define CEPH_CAP_SAUTH	    2
#define CEPH_CAP_SLINK	    4
#define CEPH_CAP_SXATTR	    6
#define CEPH_CAP_SFILE	    8
#define CEPH_CAP_SFLOCK	   20

#define CEPH_CAP_BITS	    22

/* composed values */
#define CEPH_CAP_AUTH_SHARED  (CEPH_CAP_GSHARED	 << CEPH_CAP_SAUTH)
#define CEPH_CAP_AUTH_EXCL     (CEPH_CAP_GEXCL	   << CEPH_CAP_SAUTH)
#define CEPH_CAP_LINK_SHARED  (CEPH_CAP_GSHARED	 << CEPH_CAP_SLINK)
#define CEPH_CAP_LINK_EXCL     (CEPH_CAP_GEXCL	   << CEPH_CAP_SLINK)
#define CEPH_CAP_XATTR_SHARED (CEPH_CAP_GSHARED	 << CEPH_CAP_SXATTR)
#define CEPH_CAP_XATTR_EXCL    (CEPH_CAP_GEXCL	   << CEPH_CAP_SXATTR)
#define CEPH_CAP_FILE(x)    (x << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_SHARED   (CEPH_CAP_GSHARED   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_EXCL     (CEPH_CAP_GEXCL	   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_CACHE    (CEPH_CAP_GCACHE	   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_RD       (CEPH_CAP_GRD	   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_WR       (CEPH_CAP_GWR	   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_BUFFER   (CEPH_CAP_GBUFFER   << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_WREXTEND (CEPH_CAP_GWREXTEND << CEPH_CAP_SFILE)
#define CEPH_CAP_FILE_LAZYIO   (CEPH_CAP_GLAZYIO   << CEPH_CAP_SFILE)
#define CEPH_CAP_FLOCK_SHARED  (CEPH_CAP_GSHARED   << CEPH_CAP_SFLOCK)
#define CEPH_CAP_FLOCK_EXCL    (CEPH_CAP_GEXCL	   << CEPH_CAP_SFLOCK)


/* cap masks (for getattr) */
#define CEPH_STAT_CAP_INODE    CEPH_CAP_PIN
#define CEPH_STAT_CAP_TYPE     CEPH_CAP_PIN  /* mode >> 12 */
#define CEPH_STAT_CAP_SYMLINK  CEPH_CAP_PIN
#define CEPH_STAT_CAP_UID      CEPH_CAP_AUTH_SHARED
#define CEPH_STAT_CAP_GID      CEPH_CAP_AUTH_SHARED
#define CEPH_STAT_CAP_MODE     CEPH_CAP_AUTH_SHARED
#define CEPH_STAT_CAP_NLINK    CEPH_CAP_LINK_SHARED
#define CEPH_STAT_CAP_LAYOUT   CEPH_CAP_FILE_SHARED
#define CEPH_STAT_CAP_MTIME    CEPH_CAP_FILE_SHARED
#define CEPH_STAT_CAP_SIZE     CEPH_CAP_FILE_SHARED
#define CEPH_STAT_CAP_ATIME    CEPH_CAP_FILE_SHARED  /* fixme */
#define CEPH_STAT_CAP_XATTR    CEPH_CAP_XATTR_SHARED
#define CEPH_STAT_CAP_INODE_ALL (CEPH_CAP_PIN |			\
				 CEPH_CAP_AUTH_SHARED |	\
				 CEPH_CAP_LINK_SHARED |	\
				 CEPH_CAP_FILE_SHARED |	\
				 CEPH_CAP_XATTR_SHARED)

#define CEPH_CAP_ANY_SHARED (CEPH_CAP_AUTH_SHARED |			\
			      CEPH_CAP_LINK_SHARED |			\
			      CEPH_CAP_XATTR_SHARED |			\
			      CEPH_CAP_FILE_SHARED)
#define CEPH_CAP_ANY_RD	  (CEPH_CAP_ANY_SHARED | CEPH_CAP_FILE_RD |	\
			   CEPH_CAP_FILE_CACHE)

#define CEPH_CAP_ANY_EXCL (CEPH_CAP_AUTH_EXCL |		\
			   CEPH_CAP_LINK_EXCL |		\
			   CEPH_CAP_XATTR_EXCL |	\
			   CEPH_CAP_FILE_EXCL)
#define CEPH_CAP_ANY_FILE_WR (CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER |	\
			      CEPH_CAP_FILE_EXCL)
#define CEPH_CAP_ANY_WR	  (CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_FILE_WR)
#define CEPH_CAP_ANY	  (CEPH_CAP_ANY_RD | CEPH_CAP_ANY_EXCL | \
			   CEPH_CAP_ANY_FILE_WR | CEPH_CAP_FILE_LAZYIO | \
			   CEPH_CAP_PIN)

#define CEPH_CAP_LOCKS (CEPH_LOCK_IFILE | CEPH_LOCK_IAUTH | CEPH_LOCK_ILINK | \
			CEPH_LOCK_IXATTR)

int ceph_caps_for_mode(int mode);

enum {
	CEPH_CAP_OP_GRANT,	   /* mds->client grant */
	CEPH_CAP_OP_REVOKE,	   /* mds->client revoke */
	CEPH_CAP_OP_TRUNC,	   /* mds->client trunc notify */
	CEPH_CAP_OP_EXPORT,	   /* mds has exported the cap */
	CEPH_CAP_OP_IMPORT,	   /* mds has imported the cap */
	CEPH_CAP_OP_UPDATE,	   /* client->mds update */
	CEPH_CAP_OP_DROP,	   /* client->mds drop cap bits */
	CEPH_CAP_OP_FLUSH,	   /* client->mds cap writeback */
	CEPH_CAP_OP_FLUSH_ACK,	   /* mds->client flushed */
	CEPH_CAP_OP_RELEASE,	   /* client->mds release (clean) cap */
	CEPH_CAP_OP_RENEW,	   /* client->mds renewal request */
};

extern const char *ceph_cap_op_name(int op);

/*
 * caps message, used for capability callbacks, acks, requests, etc.
 */
struct ceph_mds_caps {
	uint32_t op;		    /* CEPH_CAP_OP_* */
	uint64_t ino, realm;
	uint64_t cap_id;
	uint32_t seq, issue_seq;
	uint32_t caps, wanted, dirty; /* latest issued/wanted/dirty */
	uint32_t migrate_seq;

	/* authlock */
	uint32_t uid, gid, mode;

	/* linklock */
	uint32_t nlink;

	/* xattrlock */
	uint32_t xattr_len;
	uint64_t xattr_version;

	/* filelock */
	uint64_t size, max_size, truncate_size;
	uint32_t truncate_seq;
	struct ceph_timerep mtime, atime, ctime;
	struct ceph_file_layout layout;
	uint32_t time_warp_seq;
};

/* cap release msg head */
struct ceph_mds_cap_release {
	uint32_t num;		   /* number of cap_items that follow */
};

struct ceph_mds_cap_item {
	uint64_t ino;
	uint64_t cap_id;
	uint32_t migrate_seq, seq;
};

#define CEPH_MDS_LEASE_REVOKE		1  /*	 mds  -> client */
#define CEPH_MDS_LEASE_RELEASE		2  /* client  -> mds	*/
#define CEPH_MDS_LEASE_RENEW		3  /* client <-> mds	*/
#define CEPH_MDS_LEASE_REVOKE_ACK	4  /* client  -> mds	*/

extern const char *ceph_lease_op_name(int o);

/* lease msg header */
struct ceph_mds_lease {
	uint8_t action;		   /* CEPH_MDS_LEASE_* */
	uint16_t mask;		/* which lease */
	uint64_t ino;
	uint32_t seq;
	uint32_t duration_ms;	/* duration of renewal */
};
/* followed by a uint32_t+string for dname */

/* client reconnect */
struct ceph_mds_cap_reconnect {
	uint64_t cap_id;
	uint32_t wanted;
	uint32_t issued;
	uint64_t pathbase;	/* base ino for our path to this ino */
	uint32_t flock_len;	/* size of flock state blob, if any */
};
/* followed by flock blob */

struct ceph_mds_cap_reconnect_v1 {
	uint64_t cap_id;
	uint32_t wanted;
	uint32_t issued;
	uint64_t size;
	struct ceph_timerep mtime, atime;
	uint64_t pathbase;	/* base ino for our path to this ino */
};

#endif
