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

typedef uint32_t ceph_frag_t;

#define FRAG_SHIFT 24
#define FRAG_BIT1 (1u << FRAG_SHIFT)
#define FRAG_MASK (FRAG_BIT1 - 1)

static inline ceph_frag_t ceph_frag_make(ceph_frag_t b, ceph_frag_t v)
{
	return (b << FRAG_SHIFT) |
		(v & (FRAG_MASK << (FRAG_SHIFT-b)) & FRAG_MASK);
}
static inline ceph_frag_t ceph_frag_bits(ceph_frag_t f)
{
	return f >> FRAG_SHIFT;
}
static inline ceph_frag_t ceph_frag_value(ceph_frag_t f)
{
	return f & FRAG_MASK;
}
static inline ceph_frag_t ceph_frag_mask(ceph_frag_t f)
{
	return (FRAG_MASK << (FRAG_SHIFT-ceph_frag_bits(f))) & FRAG_MASK;
}
static inline ceph_frag_t ceph_frag_mask_shift(ceph_frag_t f)
{
	return FRAG_SHIFT - ceph_frag_bits(f);
}

static inline int ceph_frag_contains_value(ceph_frag_t f, ceph_frag_t v)
{
	return (v & ceph_frag_mask(f)) == ceph_frag_value(f);
}
static inline int ceph_frag_contains_frag(ceph_frag_t f, ceph_frag_t sub)
{
	/* is sub as specific as us, and contained by us? */
	return ceph_frag_bits(sub) >= ceph_frag_bits(f) &&
	       (ceph_frag_value(sub) & ceph_frag_mask(f)) == ceph_frag_value(f);
}

static inline ceph_frag_t ceph_frag_parent(ceph_frag_t f)
{
	return ceph_frag_make(ceph_frag_bits(f) - 1,
			 ceph_frag_value(f) & (ceph_frag_mask(f) << 1));
}
static inline int ceph_frag_is_left_child(ceph_frag_t f)
{
	return ceph_frag_bits(f) > 0 &&
		(ceph_frag_value(f) & (FRAG_BIT1 >> ceph_frag_bits(f))) == 0;
}
static inline int ceph_frag_is_right_child(ceph_frag_t f)
{
	return ceph_frag_bits(f) > 0 &&
		(ceph_frag_value(f) & (FRAG_BIT1 >> ceph_frag_bits(f))) == 1;
}
static inline ceph_frag_t ceph_frag_sibling(ceph_frag_t f)
{
	return ceph_frag_make(ceph_frag_bits(f),
		      ceph_frag_value(f) ^ (FRAG_BIT1 >> ceph_frag_bits(f)));
}
static inline ceph_frag_t ceph_frag_left_child(ceph_frag_t f)
{
	return ceph_frag_make(ceph_frag_bits(f)+1, ceph_frag_value(f));
}
static inline ceph_frag_t ceph_frag_right_child(ceph_frag_t f)
{
	return ceph_frag_make(ceph_frag_bits(f)+1,
	      ceph_frag_value(f) | (FRAG_BIT1 >> (1+ceph_frag_bits(f))));
}
static inline ceph_frag_t ceph_frag_make_child(ceph_frag_t f, ceph_frag_t by, ceph_frag_t i)
{
	ceph_frag_t newbits = ceph_frag_bits(f) + by;
	return ceph_frag_make(newbits,
			 ceph_frag_value(f) | (i << (FRAG_SHIFT - newbits)));
}
static inline int ceph_frag_is_leftmost(ceph_frag_t f)
{
	return ceph_frag_value(f) == 0;
}
static inline int ceph_frag_is_rightmost(ceph_frag_t f)
{
	return ceph_frag_value(f) == ceph_frag_mask(f);
}
static inline ceph_frag_t ceph_frag_next(ceph_frag_t f)
{
	return ceph_frag_make(ceph_frag_bits(f),
			 ceph_frag_value(f) + (FRAG_BIT1 >> ceph_frag_bits(f)));
}

/*
 * comparator to sort frags logically, as when traversing the
 * number space in ascending order...
 */
int ceph_frag_compare(ceph_frag_t a, ceph_frag_t b);

#endif
