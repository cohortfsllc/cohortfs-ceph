#ifndef FS_CEPH_HASH_H
#define FS_CEPH_HASH_H

#include "xxHash-r39/xxhash.h"

#define CEPH_STR_HASH_LINUX      0x001  /* linux dcache hash */
#define CEPH_STR_HASH_RJENKINS   0x002  /* robert jenkins' */
#define CEPH_STR_HASH_XXHASH     0x010

extern unsigned ceph_str_hash_linux(const char *s, unsigned len);
extern unsigned ceph_str_hash_rjenkins(const char *s, unsigned len);

static inline uint64_t
ceph_str_hash(int type, const char *s, unsigned len, uint64_t seed = 667)
{
  switch (type) {
  case CEPH_STR_HASH_XXHASH:
    return XXH64(s, len, seed);
  case CEPH_STR_HASH_LINUX:
    return ceph_str_hash_linux(s, len);
  case CEPH_STR_HASH_RJENKINS:
    return ceph_str_hash_rjenkins(s, len);
  default:
    return -1;
  }
}

extern const char *ceph_str_hash_name(int type);

#endif
