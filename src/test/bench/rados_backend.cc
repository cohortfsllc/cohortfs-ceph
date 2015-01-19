// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "rados_backend.h"
#include <boost/tuple/tuple.hpp>

typedef boost::tuple<OSDC::op_callback, OSDC::op_callback,
		     librados::AioCompletion*> arg_type;

void on_applied(void *completion, void *_arg) {
  arg_type *arg = static_cast<arg_type*>(_arg);
  arg->get<1>()(0);
}

void on_complete(void *completion, void *_arg) {
  arg_type *arg = static_cast<arg_type*>(_arg);
  arg->get<0>()(0);
  arg->get<2>()->release();
  delete arg;
}

void RadosBackend::write(
  const std::string &oid_t,
  uint64_t offset,
  const bufferlist &bl,
  OSDC::op_callback&& on_write_applied,
  OSDC::op_callback&& on_commit)
{
  librados::AioCompletion *completion = librados::Rados::aio_create_completion();


  void *arg = static_cast<void *>(new arg_type(on_commit, on_write_applied,
					       completion));

  completion->set_safe_callback(
    arg,
    on_complete);

  completion->set_complete_callback(
    arg,
    on_applied);

  ioctx->aio_write(oid_t, completion, bl, bl.length(), offset);
}

void RadosBackend::read(
  const std::string &oid_t,
  uint64_t offset,
  uint64_t length,
  bufferlist *bl,
  OSDC::op_callback&& on_read_complete)
{
  librados::AioCompletion *completion = librados::Rados::aio_create_completion();


  void *arg = static_cast<void *>(new arg_type(on_read_complete, 0,
					       completion));

  completion->set_complete_callback(
    arg,
    on_complete);

  ioctx->aio_read(oid_t, completion, bl, length, offset);
}
