// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef CEPH_TEST_SMALLIOBENCH_RBD_BACKEND_H
#define CEPH_TEST_SMALLIOBENCH_RBD_BACKEND_H

#include "backend.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"

using namespace ceph;

class RBDBackend : public Backend {
  std::map<std::string, std::shared_ptr<librbd::Image> > *m_images;
public:
  RBDBackend(std::map<std::string, std::shared_ptr<librbd::Image> > *images)
    : m_images(images) {}
  void write(
    const std::string &oid,
    uint64_t offset,
    const bufferlist &bl,
    Context *on_applied,
    Context *on_commit);

  void read(
    const std::string &oid,
    uint64_t offset,
    uint64_t length,
    bufferlist *bl,
    Context *on_complete);
};

#endif
