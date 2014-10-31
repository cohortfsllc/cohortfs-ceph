/*
 * byteorder.h
 *
 * LGPL 2
 */

#ifndef CEPH_BYTEORDER_H
#define CEPH_BYTEORDER_H

#include <stdint.h>
#include <sys/param.h>

#if defined(__APPLE__)
# if __DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN
#  define CEPH_LITTLE_ENDIAN
# elif __DARWIN_BYTE_ORDER == __DARWIN_BIG_ENDIAN
#  define CEPH_BIG_ENDIAN
# endif
#endif

#if defined(__FreeBSD__)
# if _BYTE_ORDER == _LITTLE_ENDIAN
#  define CEPH_LITTLE_ENDIAN
# elif _BYTE_ORDER == _BIG_ENDIAN
#  define CEPH_BIG_ENDIAN
# endif
#endif

#if defined(__linux__)
# if BYTE_ORDER == LITTLE_ENDIAN
#  define CEPH_LITTLE_ENDIAN
# elif BYTE_ORDER == BIG_ENDIAN
#  define CEPH_BIG_ENDIAN
# endif
#endif

static __inline__ uint16_t swab16(uint16_t val)
{
  return (val >> 8) | (val << 8);
}
static __inline__ uint32_t swab32(uint32_t val)
{
  return (( val >> 24) |
	  ((val >> 8)  & 0xff00) |
	  ((val << 8)  & 0xff0000) |
	  ((val << 24)));
}
static __inline__ uint64_t swab64(uint64_t val)
{
  return (( val >> 56) |
	  ((val >> 40) & 0xff00ull) |
	  ((val >> 24) & 0xff0000ull) |
	  ((val >> 8)  & 0xff000000ull) |
	  ((val << 8)  & 0xff00000000ull) |
	  ((val << 24) & 0xff0000000000ull) |
	  ((val << 40) & 0xff000000000000ull) |
	  ((val << 56)));
}

// mswab == maybe swab (if not LE)
#ifdef CEPH_BIG_ENDIAN
# define mswab64(a) swab64(a)
# define mswab32(a) swab32(a)
# define mswab16(a) swab16(a)
#elif defined(CEPH_LITTLE_ENDIAN)
# define mswab64(a) (a)
# define mswab32(a) (a)
# define mswab16(a) (a)
#else
# error "Could not determine endianess"
#endif

#endif
