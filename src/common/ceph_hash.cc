
#include "include/types.h"

/*
 * Robert Jenkin's hash function.
 * http://burtleburtle.net/bob/hash/evahash.html
 * This is in the public domain.
 */
#define mix(a, b, c)						\
	do {							\
		a = a - b;  a = a - c;	a = a ^ (c >> 13);	\
		b = b - c;  b = b - a;	b = b ^ (a << 8);	\
		c = c - a;  c = c - b;	c = c ^ (b >> 13);	\
		a = a - b;  a = a - c;	a = a ^ (c >> 12);	\
		b = b - c;  b = b - a;	b = b ^ (a << 16);	\
		c = c - a;  c = c - b;	c = c ^ (b >> 5);	\
		a = a - b;  a = a - c;	a = a ^ (c >> 3);	\
		b = b - c;  b = b - a;	b = b ^ (a << 10);	\
		c = c - a;  c = c - b;	c = c ^ (b >> 15);	\
	} while (0)

unsigned ceph_str_hash_rjenkins(const char *str, unsigned length)
{
	const unsigned char *k = (const unsigned char *)str;
	uint32_t a, b, c;  /* the internal state */
	uint32_t len;	   /* how many key bytes still need mixing */

	/* Set up the internal state */
	len = length;
	a = 0x9e3779b9;	     /* the golden ratio; an arbitrary value */
	b = a;
	c = 0;		     /* variable initialization of internal state */

	/* handle most of the key */
	while (len >= 12) {
		a = a + (k[0] + ((uint32_t)k[1] << 8) + ((uint32_t)k[2] << 16) +
			 ((uint32_t)k[3] << 24));
		b = b + (k[4] + ((uint32_t)k[5] << 8) + ((uint32_t)k[6] << 16) +
			 ((uint32_t)k[7] << 24));
		c = c + (k[8] + ((uint32_t)k[9] << 8) + ((uint32_t)k[10] << 16) +
			 ((uint32_t)k[11] << 24));
		mix(a, b, c);
		k = k + 12;
		len = len - 12;
	}

	/* handle the last 11 bytes */
	c = c + length;
	switch (len) {		  /* all the case statements fall through */
	case 11:
		c = c + ((uint32_t)k[10] << 24);
	case 10:
		c = c + ((uint32_t)k[9] << 16);
	case 9:
		c = c + ((uint32_t)k[8] << 8);
		/* the first byte of c is reserved for the length */
	case 8:
		b = b + ((uint32_t)k[7] << 24);
	case 7:
		b = b + ((uint32_t)k[6] << 16);
	case 6:
		b = b + ((uint32_t)k[5] << 8);
	case 5:
		b = b + k[4];
	case 4:
		a = a + ((uint32_t)k[3] << 24);
	case 3:
		a = a + ((uint32_t)k[2] << 16);
	case 2:
		a = a + ((uint32_t)k[1] << 8);
	case 1:
		a = a + k[0];
		/* case 0: nothing left to add */
	}
	mix(a, b, c);

	return c;
}

/*
 * linux dcache hash
 */
unsigned ceph_str_hash_linux(const char *str, unsigned length)
{
	unsigned long hash = 0;

	while (length--) {
		unsigned char c = *str++;
		hash = (hash + (c << 4) + (c >> 4)) * 11;
	}
	return hash;
}

const char *ceph_str_hash_name(int type)
{
	switch (type) {
	case CEPH_STR_HASH_XXHASH:
	    return "xxhash";
	case CEPH_STR_HASH_LINUX:
	    return "linux";
	case CEPH_STR_HASH_RJENKINS:
	    return "rjenkins";
	default:
		return "unknown";
	}
}
