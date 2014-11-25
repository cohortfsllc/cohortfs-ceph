// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_CONTEXT_H
#define CEPH_LIBOSD_CONTEXT_H

class CephContext;
struct md_config_t;

namespace ceph
{
namespace osd
{
class Context {
 public:
  CephContext *cct;
  md_config_t *conf;

  Context() : cct(nullptr), conf(nullptr) {}
  ~Context();

  int create(int id, const char *config, const char *cluster);
};

} // namespace osd
} // namespace ceph

#endif // CEPH_LIBOSD_CONTEXT_H
