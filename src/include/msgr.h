#ifndef CEPH_MSGR_H
#define CEPH_MSGR_H

#ifdef __cplusplus
#include <sys/socket.h> // for struct sockaddr_storage
#endif

/*
 * Data types for message passing layer used by Ceph.
 */

#define CEPH_MON_PORT	 6789  /* default monitor port */

/*
 * client-side processes will try to bind to ports in this
 * range, simply for the benefit of tools like nmap or wireshark
 * that would like to identify the protocol.
 */
#define CEPH_PORT_FIRST	 6789

/*
 * tcp connection banner.  include a protocol version. and adjust
 * whenever the wire protocol changes.	try to keep this string length
 * constant.
 */
#define CEPH_BANNER "ceph v027"
#define CEPH_BANNER_MAX_LEN 30


/*
 * Rollover-safe type and comparator for 32-bit sequence numbers.
 * Comparator returns -1, 0, or 1.
 */
typedef uint32_t ceph_seq_t;

static inline int32_t ceph_seq_cmp(uint32_t a, uint32_t b)
{
       return (int32_t)a - (int32_t)b;
}


/*
 * entity_name -- logical name for a process participating in the
 * network, e.g. 'mds0' or 'osd3'.
 */
struct ceph_entity_name {
	uint8_t type;	   /* CEPH_ENTITY_TYPE_* */
	int64_t num;
};

#define CEPH_ENTITY_TYPE_MON	0x01
#define CEPH_ENTITY_TYPE_MDS	0x02
#define CEPH_ENTITY_TYPE_OSD	0x04
#define CEPH_ENTITY_TYPE_CLIENT 0x08
/* where is 0x10? */
#define CEPH_ENTITY_TYPE_AUTH	0x20
#define CEPH_ENTITY_TYPE_GENERIC_SERVER	  0x40

#define CEPH_ENTITY_TYPE_ANY	0xFF

extern const char *ceph_entity_type_name(int type);

/*
 * entity_addr -- network address
 */
struct ceph_entity_addr {
	uint32_t type;
	uint32_t nonce;  /* unique id for process (e.g. pid) */
	struct sockaddr_storage in_addr;
};

struct ceph_entity_inst {
	struct ceph_entity_name name;
	struct ceph_entity_addr addr;
};


/* used by message exchange protocol */
#define CEPH_MSGR_TAG_READY	    1  /* server->client: ready for messages */
#define CEPH_MSGR_TAG_RESETSESSION  2  /* server->client: reset, try again */
#define CEPH_MSGR_TAG_WAIT	    3  /* server->client: wait for racing
					  incoming connection */
#define CEPH_MSGR_TAG_RETRY_SESSION 4  /* server->client + cseq: try again
					  with higher cseq */
#define CEPH_MSGR_TAG_RETRY_GLOBAL  5  /* server->client + gseq: try again
					  with higher gseq */
#define CEPH_MSGR_TAG_CLOSE	    6  /* closing pipe */
#define CEPH_MSGR_TAG_MSG	    7  /* message */
#define CEPH_MSGR_TAG_ACK	    8  /* message ack */
#define CEPH_MSGR_TAG_KEEPALIVE	    9  /* just a keepalive byte! */
#define CEPH_MSGR_TAG_BADPROTOVER  10  /* bad protocol version */
#define CEPH_MSGR_TAG_BADAUTHORIZER 11 /* bad authorizer */
#define CEPH_MSGR_TAG_FEATURES	    12 /* insufficient features */
#define CEPH_MSGR_TAG_SEQ	    13 /* 64-bit int follows with seen seq number */
#define CEPH_MSGR_TAG_KEEPALIVE2     14
#define CEPH_MSGR_TAG_KEEPALIVE2_ACK 15	 /* keepalive reply */


/*
 * connection negotiation
 */
struct ceph_msg_connect {
	uint64_t features;     /* supported feature bits */
	uint32_t host_type;    /* CEPH_ENTITY_TYPE_* */
	uint32_t global_seq;   /* count connections initiated by this host */
	uint32_t connect_seq;  /* count connections initiated in this session */
	uint32_t protocol_version;
	uint32_t authorizer_protocol;
	uint32_t authorizer_len;
	uint8_t	 flags;		/* CEPH_MSG_CONNECT_* */
};

struct ceph_msg_connect_reply {
	uint8_t tag;
	uint64_t features;     /* feature bits for this session */
	uint32_t global_seq;
	uint32_t connect_seq;
	uint32_t protocol_version;
	uint32_t authorizer_len;
	uint8_t flags;
};

#define CEPH_MSG_CONNECT_LOSSY	1  /* messages i send may be safely dropped */


/*
 * message header
 */
struct ceph_msg_header_old {
	uint64_t seq;	  /* message seq# for this session */
	uint64_t tid;	  /* transaction id */
	uint16_t type;	  /* message type */
	uint16_t priority;  /* priority.	higher value == higher priority */
	uint16_t version;	  /* version of message encoding */

	uint32_t front_len; /* bytes in main payload */
	uint32_t middle_len;/* bytes in middle payload */
	uint32_t data_len;  /* bytes of data payload */
	uint16_t data_off;  /* sender: include full offset;
			     receiver: mask against ~PAGE_MASK */

	struct ceph_entity_inst src, orig_src;
	uint32_t reserved;
	uint32_t crc;	  /* header crc32c */
};

struct ceph_msg_header {
	uint64_t seq;	  /* message seq# for this session */
	uint64_t tid;	  /* transaction id */
	uint16_t type;	  /* message type */
	uint16_t priority;  /* priority.	higher value == higher priority */
	uint16_t version;	  /* version of message encoding */

	uint32_t front_len; /* bytes in main payload */
	uint32_t middle_len;/* bytes in middle payload */
	uint32_t data_len;  /* bytes of data payload */
	uint16_t data_off;  /* sender: include full offset;
			     receiver: mask against ~PAGE_MASK */

	struct ceph_entity_name src;

	/* oldest code we think can decode this.  unknown if zero. */
	uint16_t compat_version;
	uint16_t reserved;
	uint32_t crc;	  /* header crc32c */
};

#define CEPH_MSG_PRIO_LOW     64
#define CEPH_MSG_PRIO_DEFAULT 127
#define CEPH_MSG_PRIO_HIGH    196
#define CEPH_MSG_PRIO_HIGHEST 255

/*
 * follows data payload
 * ceph_msg_footer_old does not support digital signatures on messages PLR
 */

struct ceph_msg_footer_old {
	uint32_t front_crc, middle_crc, data_crc;
	uint8_t flags;
};

struct ceph_msg_footer {
	uint32_t front_crc, middle_crc, data_crc;
	// sig holds the 64 bits of the digital signature for the message PLR
	uint64_t	sig;
	uint8_t flags;
};

#define CEPH_MSG_FOOTER_COMPLETE  (1<<0)   /* msg wasn't aborted */
#define CEPH_MSG_FOOTER_NOCRC	  (1<<1)   /* no data crc */
#define CEPH_MSG_FOOTER_SIGNED	  (1<<2)   /* msg was signed */


#endif
