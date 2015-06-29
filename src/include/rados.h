#ifndef CEPH_RADOS_H
#define CEPH_RADOS_H

/*
 * Data types for the Ceph distributed object storage layer RADOS
 * (Reliable Autonomic Distributed Object Store).
 */

#include "msgr.h"

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

typedef uint64_t ceph_timerep;


/*
 * stable_mod func is used to control number of placement groups.
 * similar to straight-up modulo, but produces a stable mapping as b
 * increases over time.	 b is the number of bins, and bmask is the
 * containing power of 2 minus 1.
 *
 * b <= bmask and bmask=(2**n)-1
 * e.g., b=12 -> bmask=15, b=123 -> bmask=127
 */
static inline int ceph_stable_mod(int x, int b, int bmask)
{
	if ((x & bmask) < b)
		return x & bmask;
	else
		return x & (bmask >> 1);
}

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
#define CEPH_OSD_EXISTS	 (1<<0)
#define CEPH_OSD_UP	 (1<<1)
#define CEPH_OSD_AUTOOUT (1<<2)	 /* osd was automatically marked out */
#define CEPH_OSD_NEW	 (1<<3)	 /* osd is new, never marked in */

extern const char *ceph_osd_state_name(int s);

/* osd weights.	 fixed point value: 0x10000 == 1.0 ("in"), 0 == "out" */
#define CEPH_OSD_IN  0x10000
#define CEPH_OSD_OUT 0

#define CEPH_OSD_MAX_PRIMARY_AFFINITY 0x10000
#define CEPH_OSD_DEFAULT_PRIMARY_AFFINITY 0x10000


/*
 * osd map flag bits
 */
#define CEPH_OSDMAP_NEARFULL (1<<0)  /* sync writes (near ENOSPC) */
#define CEPH_OSDMAP_FULL     (1<<1)  /* no data writes (ENOSPC) */
#define CEPH_OSDMAP_PAUSERD  (1<<2)  /* pause all reads */
#define CEPH_OSDMAP_PAUSEWR  (1<<3)  /* pause all writes */
#define CEPH_OSDMAP_PAUSEREC (1<<4)  /* pause recovery */
#define CEPH_OSDMAP_NOUP     (1<<5)  /* block osd boot */
#define CEPH_OSDMAP_NODOWN   (1<<6)  /* block osd mark-down/failure */
#define CEPH_OSDMAP_NOOUT    (1<<7)  /* block osd auto mark-out */
#define CEPH_OSDMAP_NOIN     (1<<8)  /* block osd auto mark-in */

/*
 * The error code to return when an OSD can't handle a write
 * because it is too large.
 */
#define OSD_WRITETOOBIG EMSGSIZE

/*
 * osd ops
 *
 * WARNING: do not use these op codes directly.	 Use the helpers
 * defined below instead.  In certain cases, op code behavior was
 * redefined, resulting in special-cases in the helpers.
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
#define CEPH_OSD_OP_TYPE_MULTI 0x0600 /* multiobject */

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

	CEPH_OSD_OP_LIST_WATCHERS = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 9,

	/* sync */
	CEPH_OSD_OP_SYNC_READ = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 11,

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

	CEPH_OSD_OP_WATCH   = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 15,

	/* omap */
	CEPH_OSD_OP_OMAPGETKEYS	  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 17,
	CEPH_OSD_OP_OMAPGETVALS	  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 18,
	CEPH_OSD_OP_OMAPGETHEADER = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 19,
	CEPH_OSD_OP_OMAPGETVALSBYKEYS  =
	  CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 20,
	CEPH_OSD_OP_OMAPSETVALS	  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 21,
	CEPH_OSD_OP_OMAPSETHEADER = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 22,
	CEPH_OSD_OP_OMAPCLEAR	  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 23,
	CEPH_OSD_OP_OMAPRMKEYS	  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 24,
	CEPH_OSD_OP_OMAP_CMP	  = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_DATA | 25,

	/* hints */
	CEPH_OSD_OP_SETALLOCHINT = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 35,

	/** multi **/
	CEPH_OSD_OP_ASSERT_SRC_VERSION = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_MULTI | 2,
	CEPH_OSD_OP_SRC_CMPXATTR = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_MULTI | 3,

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
	CEPH_OSD_OP_SCRUB_MAP	  = CEPH_OSD_OP_MODE_SUB | 9,

	/** lock **/
	CEPH_OSD_OP_WRLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 1,
	CEPH_OSD_OP_WRUNLOCK  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 2,
	CEPH_OSD_OP_RDLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 3,
	CEPH_OSD_OP_RDUNLOCK  = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 4,
	CEPH_OSD_OP_UPLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 5,
	CEPH_OSD_OP_DNLOCK    = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_LOCK | 6,

	/** exec **/
	/* note: the RD bit here is wrong; see special-case below in helper */
	CEPH_OSD_OP_CALL    = CEPH_OSD_OP_MODE_RD | CEPH_OSD_OP_TYPE_EXEC | 1,
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
static inline int ceph_osd_op_type_multi(int op)
{
	return (op & CEPH_OSD_OP_TYPE) == CEPH_OSD_OP_TYPE_MULTI;
}

static inline int ceph_osd_op_mode_subop(int op)
{
	return (op & CEPH_OSD_OP_MODE) == CEPH_OSD_OP_MODE_SUB;
}
static inline int ceph_osd_op_mode_read(int op)
{
	return (op & CEPH_OSD_OP_MODE_RD) &&
		op != CEPH_OSD_OP_CALL;
}
static inline int ceph_osd_op_mode_modify(int op)
{
	return op & CEPH_OSD_OP_MODE_WR;
}

extern const char *ceph_osd_op_name(int op);

/*
 * osd op flags
 *
 * An op may be READ, WRITE, or READ|WRITE.
 */
enum {
	CEPH_OSD_FLAG_ACK =	       0x0001,	/* want (or is) "ack" ack */
	CEPH_OSD_FLAG_ONNVRAM =	       0x0002,	/* want (or is) "onnvram" ack */
	CEPH_OSD_FLAG_ONDISK =	       0x0004,	/* want (or is) "ondisk" ack */
	CEPH_OSD_FLAG_RETRY =	       0x0008,	/* resend attempt */
	CEPH_OSD_FLAG_READ =	       0x0010,	/* op may read */
	CEPH_OSD_FLAG_WRITE =	       0x0020,	/* op may write */
	CEPH_OSD_FLAG_PEERSTAT_OLD =   0x0080,	/* DEPRECATED msg includes osd_peer_stat */
	CEPH_OSD_FLAG_PARALLELEXEC =   0x0200,	/* execute op in parallel */
	CEPH_OSD_FLAG_EXEC =	       0x0800,	/* op may exec */
	CEPH_OSD_FLAG_EXEC_PUBLIC =    0x1000,	/* DEPRECATED op may exec (public) */
	CEPH_OSD_FLAG_RWORDERED =      0x4000,	/* order wrt concurrent reads */
	CEPH_OSD_FLAG_SKIPRWLOCKS =   0x10000,	/* skip rw locks */
};

enum {
	CEPH_OSD_OP_FLAG_EXCL = 1,	/* EXCL object create */
	CEPH_OSD_OP_FLAG_FAILOK = 2,	/* continue despite failure */
};

#define EBLACKLISTED 108 /* blacklisted */

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
			uint64_t total_real_length;
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
		struct {
			uint64_t unused;
			uint64_t ver;
		} assert_ver;
		struct {
			uint64_t max;	/* max data in reply */
		} copy_get;
		struct {
			uint64_t src_version;
			uint8_t flags;
		} copy_from;
		struct {
			ceph_timerep stamp;
		} hit_set_get;
		struct {
			uint8_t flags;
		} tmap2omap;
		struct {
			uint64_t expected_object_size;
			uint64_t expected_write_size;
		} alloc_hint;
	};
	uint32_t payload_len;
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
