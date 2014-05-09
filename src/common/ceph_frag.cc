/*
 * Ceph 'frag' type
 */
#include "include/types.h"

int ceph_frag_compare(uint32_t a, uint32_t b)
{
	unsigned va = ceph_frag_value(a);
	unsigned vb = ceph_frag_value(b);
	if (va < vb)
		return -1;
	if (va > vb)
		return 1;
	va = ceph_frag_bits(a);
	vb = ceph_frag_bits(b);
	if (va < vb)
		return -1;
	if (va > vb)
		return 1;
	return 0;
}
