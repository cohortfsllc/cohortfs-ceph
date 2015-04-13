// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "rbd_backend.h"
#include <boost/tuple/tuple.hpp>

typedef boost::tuple<rados::op_callback&&, rados::op_callback&&> arg_type;

void RBDBackend::write(
  const std::string &oid,
  uint64_t offset,
  const bufferlist &bl,
  rados::op_callback&& on_write_applied,
  rados::op_callback&& on_commit)
{
  bufferlist &bl_non_const = const_cast<bufferlist&>(bl);
  std::shared_ptr<librbd::Image> image = (*m_images)[oid];
  try {
    image->write(offset, bl_non_const.length(),
		 bl_non_const, std::move(on_write_applied),
		 std::move(on_commit));
  } catch (std::error_condition &e) {
    std::cerr << "Error writing to RBD image: "
	      << e.message() << std::endl;
  }
}

void RBDBackend::read(
  const std::string &oid,
  uint64_t offset,
  uint64_t length,
  bufferlist *bl,
  rados::op_callback&& on_read_complete)
{
  std::shared_ptr<librbd::Image> image = (*m_images)[oid];
  try{
    image->read(offset, length, bl, std::move(on_read_complete));
  } catch (std::error_condition &e) {
    std::cerr << "Error writing to RBD image: "
	      << e.message() << std::endl;
  }
}
