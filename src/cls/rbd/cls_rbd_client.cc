// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/lock/cls_lock_client.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/rbd_types.h"

#include "cls_rbd_client.h"

#include <errno.h>

namespace librbd {
  namespace cls_client {
    int get_immutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			       std::string *object_prefix, uint8_t *order)
    {
      assert(object_prefix);
      assert(order);

      librados::ObjectReadOperation op;
      bufferlist bl, empty;
      op.exec("rbd", "get_size", bl);
      op.exec("rbd", "get_object_prefix", empty);

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	uint64_t size;
	// get_size
	::decode(*order, iter);
	::decode(size, iter);
	// get_object_prefix
	::decode(*object_prefix, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int get_mutable_metadata(librados::IoCtx *ioctx, const std::string &oid,
			     uint64_t *size, uint64_t *features,
			     uint64_t *incompatible_features,
			     map<rados::cls::lock::locker_id_t,
				 rados::cls::lock::locker_info_t> *lockers,
			     bool *exclusive_lock,
			     string *lock_tag)
    {
      assert(size);
      assert(features);
      assert(incompatible_features);
      assert(lockers);
      assert(exclusive_lock);

      librados::ObjectReadOperation op;
      bufferlist sizebl, featuresbl;
      op.exec("rbd", "get_size", sizebl);
      op.exec("rbd", "get_features", featuresbl);
      rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	uint8_t order;
	// get_size
	::decode(order, iter);
	::decode(*size, iter);
	// get_features
	::decode(*features, iter);
	::decode(*incompatible_features, iter);

	// get_lock_info
	ClsLockType lock_type = LOCK_NONE;
	r = rados::cls::lock::get_lock_info_finish(&iter, lockers, &lock_type,
						   lock_tag);

	// see comment in ictx_refresh().  Ugly conflation of
	// EOPNOTSUPP and EIO.

	if (r < 0 && ((r != -EOPNOTSUPP) && (r != -EIO)))
	  return r;

	*exclusive_lock = (lock_type == LOCK_EXCLUSIVE);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int create_image(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t size, uint8_t order, uint64_t features,
		     const std::string &object_prefix)
    {
      bufferlist bl, bl2;
      ::encode(size, bl);
      ::encode(order, bl);
      ::encode(features, bl);
      ::encode(object_prefix, (bl));

      return ioctx->exec(oid, "rbd", "create", bl, bl2);
    }

    int get_features(librados::IoCtx *ioctx, const std::string &oid,
		     uint64_t *features)
    {
      bufferlist inbl, outbl;

      int r = ioctx->exec(oid, "rbd", "get_features", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*features, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int get_object_prefix(librados::IoCtx *ioctx, const std::string &oid,
			  std::string *object_prefix)
    {
      bufferlist inbl, outbl;
      int r = ioctx->exec(oid, "rbd", "get_object_prefix", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*object_prefix, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int get_size(librados::IoCtx *ioctx, const std::string &oid,
		 uint64_t *size, uint8_t *order)
    {
      bufferlist inbl, outbl;

      int r = ioctx->exec(oid, "rbd", "get_size", inbl, outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*order, iter);
	::decode(*size, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_size(librados::IoCtx *ioctx, const std::string &oid,
		 uint64_t size)
    {
      bufferlist bl, bl2;
      ::encode(size, bl);

      return ioctx->exec(oid, "rbd", "set_size", bl, bl2);
    }

    int get_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t *stripe_unit, uint64_t *stripe_count)
    {
      assert(stripe_unit);
      assert(stripe_count);

      librados::ObjectReadOperation op;
      bufferlist empty;
      op.exec("rbd", "get_stripe_unit_count", empty);

      bufferlist outbl;
      int r = ioctx->operate(oid, &op, &outbl);
      if (r < 0)
	return r;

      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(*stripe_unit, iter);
	::decode(*stripe_count, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_stripe_unit_count(librados::IoCtx *ioctx, const std::string &oid,
			      uint64_t stripe_unit, uint64_t stripe_count)
    {
      bufferlist in, out;
      ::encode(stripe_unit, in);
      ::encode(stripe_count, in);
      return ioctx->exec(oid, "rbd", "set_stripe_unit_count", in, out);
    }


    /************************ rbd_id object methods ************************/

    int get_id(librados::IoCtx *ioctx, const std::string &oid, std::string *id)
    {
      bufferlist in, out;
      int r = ioctx->exec(oid, "rbd", "get_id", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*id, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int set_id(librados::IoCtx *ioctx, const std::string &oid, std::string id)
    {
      bufferlist in, out;
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "set_id", in, out);
    }

    /******************** rbd_directory object methods ********************/

    int dir_get_id(librados::IoCtx *ioctx, const std::string &oid,
		   const std::string &name, std::string *id)
    {
      bufferlist in, out;
      ::encode(name, in);
      int r = ioctx->exec(oid, "rbd", "dir_get_id", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*id, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int dir_get_name(librados::IoCtx *ioctx, const std::string &oid,
		     const std::string &id, std::string *name)
    {
      bufferlist in, out;
      ::encode(id, in);
      int r = ioctx->exec(oid, "rbd", "dir_get_name", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*name, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int dir_list(librados::IoCtx *ioctx, const std::string &oid,
		 const std::string &start, uint64_t max_return,
		 map<string, string> *images)
    {
      bufferlist in, out;
      ::encode(start, in);
      ::encode(max_return, in);
      int r = ioctx->exec(oid, "rbd", "dir_list", in, out);
      if (r < 0)
	return r;

      bufferlist::iterator iter = out.begin();
      try {
	::decode(*images, iter);
      } catch (const buffer::error &err) {
	return -EBADMSG;
      }

      return 0;
    }

    int dir_add_image(librados::IoCtx *ioctx, const std::string &oid,
		      const std::string &name, const std::string &id)
    {
      bufferlist in, out;
      ::encode(name, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "dir_add_image", in, out);
    }

    int dir_remove_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &name, const std::string &id)
    {
      bufferlist in, out;
      ::encode(name, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "dir_remove_image", in, out);
    }

    int dir_rename_image(librados::IoCtx *ioctx, const std::string &oid,
			 const std::string &src, const std::string &dest,
			 const std::string &id)
    {
      bufferlist in, out;
      ::encode(src, in);
      ::encode(dest, in);
      ::encode(id, in);
      return ioctx->exec(oid, "rbd", "dir_rename_image", in, out);
    }
  } // namespace cls_client
} // namespace librbd
