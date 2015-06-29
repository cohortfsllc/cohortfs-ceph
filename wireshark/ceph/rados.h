#ifndef CEPH_RADOS_H
#define CEPH_RADOS_H

/*
 * Data types for the Ceph distributed object storage layer RADOS
 * (Reliable Autonomic Distributed Object Store).
 */

#include "msgr.h"

/*
 * osdmap encoding versions
 */
#define CEPH_OSDMAP_INC_VERSION	    5
#define CEPH_OSDMAP_INC_VERSION_EXT 6
#define CEPH_OSDMAP_VERSION	    5
#define CEPH_OSDMAP_VERSION_EXT	    6

/*
 * fs id
 */
struct ceph_fsid {
	unsigned char fsid[16];
};

static inline int ceph_fsid_compare(const struct ceph_fsid *a,
				    const struct ceph_fsid *b)
{
	return memcmp(a, b, sizeof(*a));
}

/*
 * ino, object, etc.
 */
typedef uint64_t ceph_snapid_t;
#define CEPH_SNAPDIR ((uint64_t)(-1))  /* reserved for hidden .snap dir */
#define CEPH_NOSNAP  ((uint64_t)(-2))  /* "head", "live" revision */
#define CEPH_MAXSNAP ((uint64_t)(-3))  /* largest valid snapid */

struct ceph_timerep {
	uint32_t tv_sec;
	uint32_t tv_nsec;
};


/*
 * object layout - how objects are mapped into PGs
 */
#define CEPH_OBJECT_LAYOUT_HASH	    1
#define CEPH_OBJECT_LAYOUT_LINEAR   2
#define CEPH_OBJECT_LAYOUT_HASHINO  3

/*
 * pg layout -- how PGs are mapped onto (sets of) OSDs
 */
#define CEPH_PG_LAYOUT_CRUSH  0
#define CEPH_PG_LAYOUT_HASH   1
#define CEPH_PG_LAYOUT_LINEAR 2
#define CEPH_PG_LAYOUT_HYBRID 3

#define CEPH_PG_MAX_SIZE      16  /* max # osds in a single pg */

/*
 * placement group.
 * we encode this into one uint64_t.
 */
struct ceph_pg {
	uint16_t preferred; /* preferred primary osd */
	uint16_t ps;	  /* placement seed */
	uint32_t pool;	  /* object pool */
};

/*
 * object layout - how a given object should be stored.
 */
struct ceph_object_layout {
	uint32_t ol_stripe_unit;	  /* for per-object parity, if any */
};

/*
 * compound epoch+version, used by storage layer to serialize mutations
 */
struct ceph_eversion {
	uint32_t epoch;
	uint64_t version;
};

/*
 * osd map bits
 */

/* status bits */
#define CEPH_OSD_EXISTS 1
#define CEPH_OSD_UP	2

/* osd weights.	 fixed point value: 0x10000 == 1.0 ("in"), 0 == "out" */
#define CEPH_OSD_IN  0x10000
#define CEPH_OSD_OUT 0


/*
 * osd map flag bits
 */
#define CEPH_OSDMAP_NEARFULL (1<<0)  /* sync writes (near ENOSPC) */
#define CEPH_OSDMAP_FULL     (1<<1)  /* no data writes (ENOSPC) */
#define CEPH_OSDMAP_PAUSERD  (1<<2)  /* pause all reads */
#define CEPH_OSDMAP_PAUSEWR  (1<<3)  /* pause all writes */
#define CEPH_OSDMAP_PAUSEREC (1<<4)  /* pause recovery */

/*
 * osd ops
 */
#define CEPH_OSD_OP_MODE       0xf000
#define CEPH_OSD_OP_MODE_RD    0x1000
#define CEPH_OSD_OP_MODE_WR    0x2000
#define CEPH_OSD_OP_MODE_RMW   0x3000
#define CEPH_OSD_OP_MODE_SUB   0x4000

#define CEPH_OSD_OP_TYPE       0x0f00
#define CEPH_OSD_OP_TYPE_LOCK  0x0100
#define CEPH_OSD_OP_TYPE_DATA  0x0200
#define CEPH_OSD_OP_TYPE_ATTR  0x0300
#define CEPH_OSD_OP_TYPE_EXEC  0x0400
#define CEPH_OSD_OP_TYPE_PG    0x0500

enum {
	/** data **/
	/* read */
	CEPH_OSD_OP_READ      = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 1,
	CEPH_OSD_OP_STAT      = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 2,

	/* fancy read */
	CEPH_OSD_OP_MASKTRUNC	= CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 4,
	CEPH_OSD_OP_SPARSE_READ = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 5,

	CEPH_OSD_OP_NOTIFY    = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 6,
	CEPH_OSD_OP_NOTIFY_ACK = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 7,

	/* versioning */
	CEPH_OSD_OP_ASSERT_VER = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 8,

	/* write */
	CEPH_OSD_OP_WRITE     = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 1,
	CEPH_OSD_OP_WRITEFULL = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 2,
	CEPH_OSD_OP_TRUNCATE  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 3,
	CEPH_OSD_OP_ZERO      = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 4,
	CEPH_OSD_OP_DELETE    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 5,

	/* fancy write */
	CEPH_OSD_OP_APPEND    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 6,
	CEPH_OSD_OP_STARTSYNC = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 7,
	CEPH_OSD_OP_SETTRUNC  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 8,
	CEPH_OSD_OP_TRIMTRUNC = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 9,

	CEPH_OSD_OP_CREATE  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 13,
	CEPH_OSD_OP_ROLLBACK= CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 14,

	CEPH_OSD_OP_WATCH   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 15,

	/** attrs **/
	/* read */
	CEPH_OSD_OP_GETXATTR  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 1,
	CEPH_OSD_OP_GETXATTRS = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 2,
	CEPH_OSD_OP_CMPXATTR  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_ATTR | 3,

	/* write */
	CEPH_OSD_OP_SETXATTR  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 1,
	CEPH_OSD_OP_SETXATTRS = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 2,
	CEPH_OSD_OP_RESETXATTRS = CEPH_OSD_OP_MODE_WR|CEPH_OSD_OP_TYPE_ATTR | 3,
	CEPH_OSD_OP_RMXATTR   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_ATTR | 4,

	/** subop **/
	CEPH_OSD_OP_PULL	    = CEPH_OSD_OP_MODE_SUB | 1,
	CEPH_OSD_OP_PUSH	    = CEPH_OSD_OP_MODE_SUB | 2,
	CEPH_OSD_OP_BALANCEREADS    = CEPH_OSD_OP_MODE_SUB | 3,
	CEPH_OSD_OP_UNBALANCEREADS  = CEPH_OSD_OP_MODE_SUB | 4,
	CEPH_OSD_OP_SCRUB	    = CEPH_OSD_OP_MODE_SUB | 5,
	CEPH_OSD_OP_SCRUB_RESERVE   = CEPH_OSD_OP_MODE_SUB | 6,
	CEPH_OSD_OP_SCRUB_UNRESERVE = CEPH_OSD_OP_MODE_SUB | 7,
	CEPH_OSD_OP_SCRUB_STOP	    = CEPH_OSD_OP_MODE_SUB | 8,

	/** lock **/
	CEPH_OSD_OP_WRLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 1,
	CEPH_OSD_OP_WRUNLOCK  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 2,
	CEPH_OSD_OP_RDLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 3,
	CEPH_OSD_OP_RDUNLOCK  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 4,
	CEPH_OSD_OP_UPLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 5,
	CEPH_OSD_OP_DNLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 6,

	/** exec **/
	CEPH_OSD_OP_CALL    = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_EXEC | 1,

	/** pg **/
	CEPH_OSD_OP_PGLS      = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_PG | 1,
};

static inline int ceph_osd_op_type_lock(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_LOCK;
}
static inline int ceph_osd_op_type_data(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_DATA;
}
static inline int ceph_osd_op_type_attr(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_ATTR;
}
static inline int ceph_osd_op_type_exec(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_EXEC;
}
static inline int ceph_osd_op_type_pg(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_PG;
}

static inline int ceph_osd_op_mode_subop(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_SUB;
}
static inline int ceph_osd_op_mode_read(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_RD;
}
static inline int ceph_osd_op_mode_modify(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_WR;
}

extern const char *ceph_osd_op_name(int op);


/*
 * osd op flags
 *
 * An op may be READ, WRITE, or READ|WRITE.
 */
enum {
	CEPH_OSD_FLAG_ACK = 1,		/* want (or is) "ack" ack */
	CEPH_OSD_FLAG_ONNVRAM = 2,	/* want (or is) "onnvram" ack */
	CEPH_OSD_FLAG_ONDISK = 4,	/* want (or is) "ondisk" ack */
	CEPH_OSD_FLAG_RETRY = 8,	/* resend attempt */
	CEPH_OSD_FLAG_READ = 16,	/* op may read */
	CEPH_OSD_FLAG_WRITE = 32,	/* op may write */
	CEPH_OSD_FLAG_ORDERSNAP = 64,	/* EOLDSNAP if snapc is out of order */
	CEPH_OSD_FLAG_PEERSTAT = 128,	/* msg includes osd_peer_stat */
	CEPH_OSD_FLAG_BALANCE_READS = 256,
	CEPH_OSD_FLAG_PARALLELEXEC = 512, /* execute op in parallel */
	CEPH_OSD_FLAG_PGOP = 1024,	/* pg op, no object */
	CEPH_OSD_FLAG_EXEC = 2048,	/* op may exec */
	CEPH_OSD_FLAG_EXEC_PUBLIC = 4096, /* op may exec (public) */
};

enum {
	CEPH_OSD_OP_FLAG_EXCL = 1,	/* EXCL object create */
};

#define EOLDSNAPC    ERESTART  /* ORDERSNAP flag set; writer has old snapc*/
#define EBLACKLISTED ESHUTDOWN /* blacklisted */

/* xattr comparison */
enum {
	CEPH_OSD_CMPXATTR_OP_EQ	 = 1,
	CEPH_OSD_CMPXATTR_OP_NE	 = 2,
	CEPH_OSD_CMPXATTR_OP_GT	 = 3,
	CEPH_OSD_CMPXATTR_OP_GTE = 4,
	CEPH_OSD_CMPXATTR_OP_LT	 = 5,
	CEPH_OSD_CMPXATTR_OP_LTE = 6
};

enum {
	CEPH_OSD_CMPXATTR_MODE_STRING = 1,
	CEPH_OSD_CMPXATTR_MODE_U64    = 2
};

#define RADOS_NOTIFY_VER	1

/*
 * an individual object operation.  each may be accompanied by some data
 * payload
 */
struct ceph_osd_op {
	uint16_t op;	     /* CEPH_OSD_OP_* */
	uint32_t flags;	     /* CEPH_OSD_FLAG_* */
	union {
		struct {
			uint64_t offset, length;
			uint64_t truncate_size;
			uint32_t truncate_seq;
		} extent;
		struct {
			uint32_t name_len;
			uint32_t value_len;
			uint8_t cmp_op;	      /* CEPH_OSD_CMPXATTR_OP_* */
			uint8_t cmp_mode;     /* CEPH_OSD_CMPXATTR_MODE_* */
		} xattr;
		struct {
			uint8_t class_len;
			uint8_t method_len;
			uint8_t argc;
			uint32_t indata_len;
		} cls;
		struct {
			uint64_t cookie;
			uint64_t ver;
			uint8_t flag;	/* 0 = unwatch, 1 = watch */
		} watch;
};
	uint32_t payload_len;
};

/*
 * osd request message header.	each request may include multiple
 * ceph_osd_op object operations.
 */
struct ceph_osd_request_head {
	uint32_t client_inc;		   /* client incarnation */
	struct ceph_object_layout layout;  /* pgid */
	uint32_t osdmap_epoch;		   /* client's osdmap epoch */

	uint32_t flags;

	struct ceph_timerep mtime;	   /* for mutations only */
	struct ceph_eversion reassert_version; /* if we are replaying op */

	uint32_t object_len;     /* length of object name */

	uint64_t snapid;	       /* snapid to read */
	uint64_t snap_seq;       /* writer's snap context */
	uint32_t num_snaps;

	uint16_t num_ops;
	struct ceph_osd_op ops[];  /* followed by ops[], obj, ticket, snaps */
};

struct ceph_osd_reply_head {
	uint32_t client_inc;		  /* client incarnation */
	uint32_t flags;
	struct ceph_object_layout layout;
	uint32_t osdmap_epoch;
	struct ceph_eversion reassert_version; /* for replaying uncommitted */

	uint32_t result;			  /* result code */

	uint32_t object_len;		  /* length of object name */
	uint32_t num_ops;
	struct ceph_osd_op ops[0];  /* ops[], object */
};



#endif
