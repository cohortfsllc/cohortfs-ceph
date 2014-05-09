#ifndef CEPH_CRUSH_CRUSH_H
#define CEPH_CRUSH_CRUSH_H

#include <inttypes.h>
#if defined(__linux__)
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#endif

/*
 * CRUSH is a pseudo-random data distribution algorithm that
 * efficiently distributes input values (typically, data objects)
 * across a heterogeneous, structured storage cluster.
 *
 * The algorithm was originally described in detail in this paper
 * (although the algorithm has evolved somewhat since then):
 *
 *     http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 *
 * LGPL2
 */


#define CRUSH_MAGIC 0x00010000ul   /* for detecting algorithm revisions */

#define CRUSH_MAX_DEPTH 10  /* max crush hierarchy depth */

#define CRUSH_MAX_DEVICE_WEIGHT (100u * 0x10000u)
#define CRUSH_MAX_BUCKET_WEIGHT (65535u * 0x10000u)

#define CRUSH_ITEM_UNDEF  0x7ffffffe  /* undefined result (internal use only) */
#define CRUSH_ITEM_NONE   0x7fffffff  /* no result */

/*
 * CRUSH uses user-defined "rules" to describe how inputs should be
 * mapped to devices.  A rule consists of sequence of steps to perform
 * to generate the set of output devices.
 */
struct crush_rule_step {
	uint32_t op;
	int32_t arg1;
	int32_t arg2;
};

/* step op codes */
enum {
	CRUSH_RULE_NOOP = 0,
	CRUSH_RULE_TAKE = 1,          /* arg1 = value to start with */
	CRUSH_RULE_CHOOSE_FIRSTN = 2, /* arg1 = num items to pick */
				      /* arg2 = type */
	CRUSH_RULE_CHOOSE_INDEP = 3,  /* same */
	CRUSH_RULE_EMIT = 4,          /* no args */
	CRUSH_RULE_CHOOSELEAF_FIRSTN = 6,
	CRUSH_RULE_CHOOSELEAF_INDEP = 7,

	CRUSH_RULE_SET_CHOOSE_TRIES = 8, /* override choose_total_tries */
	CRUSH_RULE_SET_CHOOSELEAF_TRIES = 9, /* override chooseleaf_descend_once */
	CRUSH_RULE_SET_CHOOSE_LOCAL_TRIES = 10,
	CRUSH_RULE_SET_CHOOSE_LOCAL_FALLBACK_TRIES = 11,
	CRUSH_RULE_SET_CHOOSELEAF_VARY_R = 12
};

/*
 * for specifying choose num (arg1) relative to the max parameter
 * passed to do_rule
 */
#define CRUSH_CHOOSE_N            0
#define CRUSH_CHOOSE_N_MINUS(x)   (-(x))

/*
 * The rule mask is used to describe what the rule is intended for.
 * Given a ruleset and size of output set, we search through the
 * rule list for a matching rule_mask.
 */
struct crush_rule_mask {
	uint8_t ruleset;
	uint8_t type;
	uint8_t min_size;
	uint8_t max_size;
};

struct crush_rule {
	uint32_t len;
	struct crush_rule_mask mask;
	struct crush_rule_step steps[0];
};

#define crush_rule_size(len) (sizeof(struct crush_rule) + \
			      (len)*sizeof(struct crush_rule_step))



/*
 * A bucket is a named container of other items (either devices or
 * other buckets).  Items within a bucket are chosen using one of a
 * few different algorithms.  The table summarizes how the speed of
 * each option measures up against mapping stability when items are
 * added or removed.
 *
 *  Bucket Alg     Speed       Additions    Removals
 *  ------------------------------------------------
 *  uniform         O(1)       poor         poor
 *  list            O(n)       optimal      poor
 *  tree            O(log n)   good         good
 *  straw           O(n)       optimal      optimal
 */
enum {
	CRUSH_BUCKET_UNIFORM = 1,
	CRUSH_BUCKET_LIST = 2,
	CRUSH_BUCKET_TREE = 3,
	CRUSH_BUCKET_STRAW = 4
};
extern const char *crush_bucket_alg_name(int alg);

struct crush_bucket {
	int32_t id;        /* this'll be negative */
	uint16_t type;      /* non-zero; type=0 is reserved for devices */
	uint8_t alg;        /* one of CRUSH_BUCKET_* */
	uint8_t hash;       /* which hash function to use, CRUSH_HASH_* */
	uint32_t weight;    /* 16-bit fixed point */
	uint32_t size;      /* num items */
	int32_t *items;

	/*
	 * cached random permutation: used for uniform bucket and for
	 * the linear search fallback for the other bucket types.
	 */
	uint32_t perm_x;  /* @x for which *perm is defined */
	uint32_t perm_n;  /* num elements of *perm that are permuted/defined */
	uint32_t *perm;
};

struct crush_bucket_uniform {
	struct crush_bucket h;
	uint32_t item_weight;  /* 16-bit fixed point; all items equally weighted */
};

struct crush_bucket_list {
	struct crush_bucket h;
	uint32_t *item_weights;  /* 16-bit fixed point */
	uint32_t *sum_weights;   /* 16-bit fixed point.  element i is sum
				 of weights 0..i, inclusive */
};

struct crush_bucket_tree {
	struct crush_bucket h;  /* note: h.size is _tree_ size, not number of
				   actual items */
	uint8_t num_nodes;
	uint32_t *node_weights;
};

struct crush_bucket_straw {
	struct crush_bucket h;
	uint32_t *item_weights;   /* 16-bit fixed point */
	uint32_t *straws;         /* 16-bit fixed point */
};



/*
 * CRUSH map includes all buckets, rules, etc.
 */
struct crush_map {
	struct crush_bucket **buckets;
	struct crush_rule **rules;

	int32_t max_buckets;
	uint32_t max_rules;
	int32_t max_devices;

	/* choose local retries before re-descent */
	uint32_t choose_local_tries;
	/* choose local attempts using a fallback permutation before
	 * re-descent */
	uint32_t choose_local_fallback_tries;
	/* choose attempts before giving up */
	uint32_t choose_total_tries;
	/* attempt chooseleaf inner descent once for firstn mode; on
	 * reject retry outer descent.  Note that this does *not*
	 * apply to a collision: in that case we will retry as we used
	 * to. */
	uint32_t chooseleaf_descend_once;

	/* if non-zero, feed r into chooseleaf, bit-shifted right by (r-1)
	 * bits.  a value of 1 is best for new clusters.  for legacy clusters
	 * that want to limit reshuffling, a value of 3 or 4 will make the
	 * mappings line up a bit better with previous mappings. */
	uint8_t chooseleaf_vary_r;

	uint32_t *choose_tries;
};


/* crush.c */
extern int crush_get_bucket_item_weight(const struct crush_bucket *b, int pos);
extern int crush_addition_is_unsafe(uint32_t a, uint32_t b);
extern int crush_multiplication_is_unsafe(uint32_t  a, uint32_t b);
extern void crush_destroy_bucket_uniform(struct crush_bucket_uniform *b);
extern void crush_destroy_bucket_list(struct crush_bucket_list *b);
extern void crush_destroy_bucket_tree(struct crush_bucket_tree *b);
extern void crush_destroy_bucket_straw(struct crush_bucket_straw *b);
extern void crush_destroy_bucket(struct crush_bucket *b);
extern void crush_destroy_rule(struct crush_rule *r);
extern void crush_destroy(struct crush_map *map);

static inline int crush_calc_tree_node(int i)
{
	return ((i+1) << 1)-1;
}

#endif
