#if defined(__linux__)
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#endif

#include "hash.h"

/*
 * Robert Jenkins' function for mixing 32-bit values
 * http://burtleburtle.net/bob/hash/evahash.html
 * a, b = random bits, c = input and output
 */
#define crush_hashmix(a, b, c) do {			\
		a = a-b;  a = a-c;  a = a^(c>>13);	\
		b = b-c;  b = b-a;  b = b^(a<<8);	\
		c = c-a;  c = c-b;  c = c^(b>>13);	\
		a = a-b;  a = a-c;  a = a^(c>>12);	\
		b = b-c;  b = b-a;  b = b^(a<<16);	\
		c = c-a;  c = c-b;  c = c^(b>>5);	\
		a = a-b;  a = a-c;  a = a^(c>>3);	\
		b = b-c;  b = b-a;  b = b^(a<<10);	\
		c = c-a;  c = c-b;  c = c^(b>>15);	\
	} while (0)

#define crush_hash_seed 1315423911

static uint32_t crush_hash32_rjenkins1(uint32_t a)
{
	uint32_t hash = crush_hash_seed ^ a;
	uint32_t b = a;
	uint32_t x = 231232;
	uint32_t y = 1232;
	crush_hashmix(b, x, hash);
	crush_hashmix(y, a, hash);
	return hash;
}

static uint32_t crush_hash32_rjenkins1_2(uint32_t a, uint32_t b)
{
	uint32_t hash = crush_hash_seed ^ a ^ b;
	uint32_t x = 231232;
	uint32_t y = 1232;
	crush_hashmix(a, b, hash);
	crush_hashmix(x, a, hash);
	crush_hashmix(b, y, hash);
	return hash;
}

static uint32_t crush_hash32_rjenkins1_3(uint32_t a, uint32_t b, uint32_t c)
{
	uint32_t hash = crush_hash_seed ^ a ^ b ^ c;
	uint32_t x = 231232;
	uint32_t y = 1232;
	crush_hashmix(a, b, hash);
	crush_hashmix(c, x, hash);
	crush_hashmix(y, a, hash);
	crush_hashmix(b, x, hash);
	crush_hashmix(y, c, hash);
	return hash;
}

static uint32_t crush_hash32_rjenkins1_4(uint32_t a, uint32_t b, uint32_t c, uint32_t d)
{
	uint32_t hash = crush_hash_seed ^ a ^ b ^ c ^ d;
	uint32_t x = 231232;
	uint32_t y = 1232;
	crush_hashmix(a, b, hash);
	crush_hashmix(c, d, hash);
	crush_hashmix(a, x, hash);
	crush_hashmix(y, b, hash);
	crush_hashmix(c, x, hash);
	crush_hashmix(y, d, hash);
	return hash;
}

static uint32_t crush_hash32_rjenkins1_5(uint32_t a, uint32_t b, uint32_t c, uint32_t d,
				      uint32_t e)
{
	uint32_t hash = crush_hash_seed ^ a ^ b ^ c ^ d ^ e;
	uint32_t x = 231232;
	uint32_t y = 1232;
	crush_hashmix(a, b, hash);
	crush_hashmix(c, d, hash);
	crush_hashmix(e, x, hash);
	crush_hashmix(y, a, hash);
	crush_hashmix(b, x, hash);
	crush_hashmix(y, c, hash);
	crush_hashmix(d, x, hash);
	crush_hashmix(y, e, hash);
	return hash;
}


uint32_t crush_hash32(int type, uint32_t a)
{
	switch (type) {
	case CRUSH_HASH_RJENKINS1:
		return crush_hash32_rjenkins1(a);
	default:
		return 0;
	}
}

uint32_t crush_hash32_2(int type, uint32_t a, uint32_t b)
{
	switch (type) {
	case CRUSH_HASH_RJENKINS1:
		return crush_hash32_rjenkins1_2(a, b);
	default:
		return 0;
	}
}

uint32_t crush_hash32_3(int type, uint32_t a, uint32_t b, uint32_t c)
{
	switch (type) {
	case CRUSH_HASH_RJENKINS1:
		return crush_hash32_rjenkins1_3(a, b, c);
	default:
		return 0;
	}
}

uint32_t crush_hash32_4(int type, uint32_t a, uint32_t b, uint32_t c, uint32_t d)
{
	switch (type) {
	case CRUSH_HASH_RJENKINS1:
		return crush_hash32_rjenkins1_4(a, b, c, d);
	default:
		return 0;
	}
}

uint32_t crush_hash32_5(int type, uint32_t a, uint32_t b, uint32_t c, uint32_t d, uint32_t e)
{
	switch (type) {
	case CRUSH_HASH_RJENKINS1:
		return crush_hash32_rjenkins1_5(a, b, c, d, e);
	default:
		return 0;
	}
}

const char *crush_hash_name(int type)
{
	switch (type) {
	case CRUSH_HASH_RJENKINS1:
		return "rjenkins1";
	default:
		return "unknown";
	}
}
