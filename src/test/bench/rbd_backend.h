// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef CEPH_TEST_SMALLIOBENCH_RBD_BACKEND_H
#define CEPH_TEST_SMALLIOBENCH_RBD_BACKEND_H

#include "backend.h"
#include "librbd/Image.h"

using namespace ceph;

class RBDBackend : public Backend {
  std::map<std::string, std::shared_ptr<librbd::Image> > *m_images;
public:
  RBDBackend(std::map<std::string, std::shared_ptr<librbd::Image> > *images)
    : m_images(images) {}
  void write(
    const std::string &oid_t,
    uint64_t offset,
    const bufferlist &bl,
    rados::op_callback&& on_applied,
    rados::op_callback&& on_commit);

  void read(
    const std::string &oid_t,
    uint64_t offset,
    uint64_t length,
    bufferlist *bl,
    rados::op_callback&& on_complete);
};

#endif
