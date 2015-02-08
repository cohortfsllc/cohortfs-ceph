#ifndef FS_CEPH_HASH_H
#define FS_CEPH_HASH_H

#define CEPH_STR_HASH_LINUX      0x001  /* linux dcache hash */
#define CEPH_STR_HASH_RJENKINS   0x002  /* robert jenkins' */
#define CEPH_STR_HASH_XXHASH     0x010

extern unsigned ceph_str_hash_linux(const char *s, unsigned len);
extern unsigned ceph_str_hash_rjenkins(const char *s, unsigned len);

extern unsigned ceph_str_hash(int type, const char *s, unsigned len);
extern const char *ceph_str_hash_name(int type);

#endif
