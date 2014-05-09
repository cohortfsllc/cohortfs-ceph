#ifndef CEPH_CRUSH_HASH_H
#define CEPH_CRUSH_HASH_H

#include <inttypes.h>

#define CRUSH_HASH_RJENKINS1   0

#define CRUSH_HASH_DEFAULT CRUSH_HASH_RJENKINS1

extern const char *crush_hash_name(int type);

extern uint32_t crush_hash32(int type, uint32_t a);
extern uint32_t crush_hash32_2(int type, uint32_t a, uint32_t b);
extern uint32_t crush_hash32_3(int type, uint32_t a, uint32_t b, uint32_t c);
extern uint32_t crush_hash32_4(int type, uint32_t a, uint32_t b, uint32_t c, uint32_t d);
extern uint32_t crush_hash32_5(int type, uint32_t a, uint32_t b, uint32_t c, uint32_t d,
			    uint32_t e);

#endif
