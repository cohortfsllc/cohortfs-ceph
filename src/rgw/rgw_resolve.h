#ifndef CEPH_RGW_RESOLVE_H
#define CEPH_RGW_RESOLVE_H

#include "rgw_common.h"

class RGWDNSResolver;

class RGWResolver {
  RGWDNSResolver *resolver;

public:
  RGWResolver(CephContext* _cct);
  int resolve_cname(const string& hostname, string& cname, bool *found);
  ~RGWResolver();
};

extern void rgw_init_resolver(CephContext* cct);
extern void rgw_shutdown_resolver(void);
extern RGWResolver *rgw_resolver;

#endif
