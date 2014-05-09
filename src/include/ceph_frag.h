#ifndef FS_CEPH_FRAG_H
#define FS_CEPH_FRAG_H

/*
 * "Frags" are a way to describe a subset of a 32-bit number space,
 * using a mask and a value to match against that mask.  Any given frag
 * (subset of the number space) can be partitioned into 2^n sub-frags.
 *
 * Frags are encoded into a 32-bit word:
 *   8 upper bits = "bits"
 *  24 lower bits = "value"
 * (We could go to 5+27 bits, but who cares.)
 *
 * We use the _most_ significant bits of the 24 bit value.  This makes
 * values logically sort.
 *
 * Unfortunately, because the "bits" field is still in the high bits, we
 * can't sort encoded frags numerically.  However, it does allow you
 * to feed encoded frags as values into frag_contains_value.
 */
static inline uint32_t ceph_frag_make(uint32_t b, uint32_t v)
{
	return (b << 24) |
		(v & (0xffffffu << (24-b)) & 0xffffffu);
}
static inline uint32_t ceph_frag_bits(uint32_t f)
{
	return f >> 24;
}
static inline uint32_t ceph_frag_value(uint32_t f)
{
	return f & 0xffffffu;
}
static inline uint32_t ceph_frag_mask(uint32_t f)
{
	return (0xffffffu << (24-ceph_frag_bits(f))) & 0xffffffu;
}
static inline uint32_t ceph_frag_mask_shift(uint32_t f)
{
	return 24 - ceph_frag_bits(f);
}

static inline int ceph_frag_contains_value(uint32_t f, uint32_t v)
{
	return (v & ceph_frag_mask(f)) == ceph_frag_value(f);
}
static inline int ceph_frag_contains_frag(uint32_t f, uint32_t sub)
{
	/* is sub as specific as us, and contained by us? */
	return ceph_frag_bits(sub) >= ceph_frag_bits(f) &&
	       (ceph_frag_value(sub) & ceph_frag_mask(f)) == ceph_frag_value(f);
}

static inline uint32_t ceph_frag_parent(uint32_t f)
{
	return ceph_frag_make(ceph_frag_bits(f) - 1,
			 ceph_frag_value(f) & (ceph_frag_mask(f) << 1));
}
static inline int ceph_frag_is_left_child(uint32_t f)
{
	return ceph_frag_bits(f) > 0 &&
		(ceph_frag_value(f) & (0x1000000 >> ceph_frag_bits(f))) == 0;
}
static inline int ceph_frag_is_right_child(uint32_t f)
{
	return ceph_frag_bits(f) > 0 &&
		(ceph_frag_value(f) & (0x1000000 >> ceph_frag_bits(f))) == 1;
}
static inline uint32_t ceph_frag_sibling(uint32_t f)
{
	return ceph_frag_make(ceph_frag_bits(f),
		      ceph_frag_value(f) ^ (0x1000000 >> ceph_frag_bits(f)));
}
static inline uint32_t ceph_frag_left_child(uint32_t f)
{
	return ceph_frag_make(ceph_frag_bits(f)+1, ceph_frag_value(f));
}
static inline uint32_t ceph_frag_right_child(uint32_t f)
{
	return ceph_frag_make(ceph_frag_bits(f)+1,
	      ceph_frag_value(f) | (0x1000000 >> (1+ceph_frag_bits(f))));
}
static inline uint32_t ceph_frag_make_child(uint32_t f, int by, int i)
{
	int newbits = ceph_frag_bits(f) + by;
	return ceph_frag_make(newbits,
			 ceph_frag_value(f) | (i << (24 - newbits)));
}
static inline int ceph_frag_is_leftmost(uint32_t f)
{
	return ceph_frag_value(f) == 0;
}
static inline int ceph_frag_is_rightmost(uint32_t f)
{
	return ceph_frag_value(f) == ceph_frag_mask(f);
}
static inline uint32_t ceph_frag_next(uint32_t f)
{
	return ceph_frag_make(ceph_frag_bits(f),
			 ceph_frag_value(f) + (0x1000000 >> ceph_frag_bits(f)));
}

/*
 * comparator to sort frags logically, as when traversing the
 * number space in ascending order...
 */
int ceph_frag_compare(uint32_t a, uint32_t b);

#endif
