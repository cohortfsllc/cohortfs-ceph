// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <limits.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "cls/lock/cls_lock_client.h"
#include "include/stringify.h"

#include "librbd/AioCompletion.h"
#include "librbd/AioRequest.h"
#include "librbd/ImageCtx.h"

#include "librbd/internal.h"
#include "include/util.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

#define rbd_howmany(x, y)  (((x) + (y) - 1) / (y))

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;
// list binds to list() here, so std::list is explicitly used below

using ceph::bufferlist;
using librados::IoCtx;
using librados::Rados;

namespace librbd {
  const string id_obj_name(const string &name)
  {
    return RBD_ID_PREFIX + name;
  }

  const string header_name(const string &image_id)
  {
    return RBD_HEADER_PREFIX + image_id;
  }

  const string old_header_name(const string &image_name)
  {
    return image_name + RBD_SUFFIX;
  }

  int detect_format(IoCtx &io_ctx, const string &name,
		    bool *old_format, uint64_t *size)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    if (old_format)
      *old_format = true;
    int r = io_ctx.stat(old_header_name(name), size, NULL);
    if (r < 0) {
      if (old_format)
	*old_format = false;
      r = io_ctx.stat(id_obj_name(name), size, NULL);
      if (r < 0)
	return r;
    }

    ldout(cct, 20) << "detect format of " << name << " : "
		   << (old_format ? (*old_format ? "old" : "new") :
		       "don't care")  << dendl;
    return 0;
  }

  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
		       uint64_t size, int order, uint64_t bid)
  {
    uint32_t hi = bid >> 32;
    uint32_t lo = bid & 0xFFFFFFFF;
    uint32_t extra = rand() % 0xFFFFFFFF;
    memset(&ondisk, 0, sizeof(ondisk));

    memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
    memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE,
	   sizeof(RBD_HEADER_SIGNATURE));
    memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

    snprintf(ondisk.block_name, sizeof(ondisk.block_name), "rb.%x.%x.%x",
	     hi, lo, extra);

    ondisk.image_size = size;
    ondisk.options.order = order;
  }

  void image_info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    int obj_order = ictx->order;
    ictx->md_lock.get_read();
    info.size = ictx->get_image_size();
    ictx->md_lock.put_read();
    info.obj_size = 1ULL << obj_order;
    info.num_objs = rbd_howmany(info.size, ictx->get_object_size());
    info.order = obj_order;
    memcpy(&info.block_name_prefix, ictx->object_prefix.c_str(),
	   min((size_t)RBD_MAX_BLOCK_NAME_SIZE,
	       ictx->object_prefix.length() + 1));
  }

  uint64_t oid_to_object_no(const string& oid, const string& object_prefix)
  {
    istringstream iss(oid);
    // skip object prefix and separator
    iss.ignore(object_prefix.length() + 1);
    uint64_t num;
    iss >> std::hex >> num;
    return num;
  }

  int init_rbd_info(struct rbd_info *info)
  {
    memset(info, 0, sizeof(*info));
    return 0;
  }

  void trim_image(ImageCtx *ictx, uint64_t newsize, ProgressContext& prog_ctx)
  {
    CephContext *cct = (CephContext *)ictx->data_ctx.cct();

    uint64_t size = ictx->get_current_size();
    uint64_t period = ictx->get_stripe_period();
    uint64_t num_period = ((newsize + period - 1) / period);
    uint64_t delete_off = MIN(num_period * period, size);
    // first object we can delete free and clear
    uint64_t delete_start = num_period * ictx->get_stripe_count();
    uint64_t num_objects = ictx->get_num_objects();
    uint64_t object_size = ictx->get_object_size();

    ldout(cct, 10) << "trim_image " << size << " -> " << newsize
		   << " periods " << num_period
		   << " discard to offset " << delete_off
		   << " delete objects " << delete_start
		   << " to " << (num_objects-1)
		   << dendl;

    SimpleThrottle throttle(cct->_conf->rbd_concurrent_management_ops, true);
    if (delete_start < num_objects) {
      ldout(cct, 2) << "trim_image objects " << delete_start << " to "
		    << (num_objects - 1) << dendl;
      for (uint64_t i = delete_start; i < num_objects; ++i) {
	string oid = ictx->get_object_name(i);
	Context *req_comp = new C_SimpleThrottle(&throttle);
	librados::AioCompletion *rados_completion =
	  librados::Rados::aio_create_completion(req_comp, NULL, rados_ctx_cb);
	ictx->data_ctx.aio_remove(oid, rados_completion);
	rados_completion->release();
	prog_ctx.update_progress((i - delete_start) * object_size,
				 (num_objects - delete_start) * object_size);
      }
    }

    // discard the weird boundary, if any
    if (delete_off > newsize) {
      vector<ObjectExtent> extents;
      Striper::file_to_extents(ictx->cct, ictx->format_string,
			       &ictx->layout, newsize,
			       delete_off - newsize, 0, extents);

      for (vector<ObjectExtent>::iterator p = extents.begin();
	   p != extents.end(); ++p) {
	ldout(ictx->cct, 20) << " ex " << *p << dendl;
	Context *req_comp = new C_SimpleThrottle(&throttle);
	librados::AioCompletion *rados_completion =
	  librados::Rados::aio_create_completion(req_comp, NULL, rados_ctx_cb);
	if (p->offset == 0) {
	  ictx->data_ctx.aio_remove(p->oid.name, rados_completion);
	} else {
	  librados::ObjectWriteOperation op;
	  op.truncate(p->offset);
	  ictx->data_ctx.aio_operate(p->oid.name, rados_completion, &op);
	}
	rados_completion->release();
      }
    }
    int r = throttle.wait_for_ret();
    if (r < 0) {
      lderr(cct) << "warning: failed to remove some object(s): "
		 << cpp_strerror(r) << dendl;
    }
  }

  int read_rbd_info(IoCtx& io_ctx, const string& info_oid,
		    struct rbd_info *info)
  {
    int r;
    bufferlist bl;
    r = io_ctx.read(info_oid, bl, sizeof(*info), 0);
    if (r < 0)
      return r;
    if (r == 0) {
      return init_rbd_info(info);
    }

    if (r < (int)sizeof(*info))
      return -EIO;

    memcpy(info, bl.c_str(), r);
    return 0;
  }

  int read_header_bl(IoCtx& io_ctx, const string& header_oid,
		     bufferlist& header)
  {
    int r;
    uint64_t off = 0;
#define READ_SIZE 4096
    do {
      bufferlist bl;
      r = io_ctx.read(header_oid, bl, READ_SIZE, off);
      if (r < 0)
	return r;
      header.claim_append(bl);
      off += r;
    } while (r == READ_SIZE);

    if (memcmp(RBD_HEADER_TEXT, header.c_str(), sizeof(RBD_HEADER_TEXT))) {
      CephContext *cct = (CephContext *)io_ctx.cct();
      lderr(cct) << "unrecognized header format" << dendl;
      return -ENXIO;
    }

    return 0;
  }

  int notify_change(IoCtx& io_ctx, const string& oid, ImageCtx *ictx)
  {
    if (ictx) {
      ictx->refresh_lock.Lock();
      ldout(ictx->cct, 20) << "notify_change refresh_seq = " << ictx->refresh_seq
			   << " last_refresh = " << ictx->last_refresh << dendl;
      ++ictx->refresh_seq;
      ictx->refresh_lock.Unlock();
    }

    bufferlist bl;
    io_ctx.notify(oid, bl);
    return 0;
  }

  int read_header(IoCtx& io_ctx, const string& header_oid,
		  struct rbd_obj_header_ondisk *header)
  {
    bufferlist header_bl;
    int r = read_header_bl(io_ctx, header_oid, header_bl);
    if (r < 0)
      return r;
    if (header_bl.length() < (int)sizeof(*header))
      return -EIO;
    memcpy(header, header_bl.c_str(), sizeof(*header));

    return 0;
  }

  int write_header(IoCtx& io_ctx, const string& header_oid, bufferlist& header)
  {
    bufferlist bl;
    int r = io_ctx.write(header_oid, header, header.length(), 0);

    notify_change(io_ctx, header_oid, NULL);

    return r;
  }

  int tmap_set(IoCtx& io_ctx, const string& imgname)
  {
    bufferlist cmdbl, emptybl;
    uint8_t c = CEPH_OSD_TMAP_SET;
    ::encode(c, cmdbl);
    ::encode(imgname, cmdbl);
    ::encode(emptybl, cmdbl);
    return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
  }

  int tmap_rm(IoCtx& io_ctx, const string& imgname)
  {
    bufferlist cmdbl;
    uint8_t c = CEPH_OSD_TMAP_RM;
    ::encode(c, cmdbl);
    ::encode(imgname, cmdbl);
    return io_ctx.tmap_update(RBD_DIRECTORY, cmdbl);
  }

  int list(IoCtx& io_ctx, vector<string>& names)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "list " << &io_ctx << dendl;

    bufferlist bl;
    int r = io_ctx.read(RBD_DIRECTORY, bl, 0, 0);
    if (r < 0)
      return r;

    // old format images are in a tmap
    if (bl.length()) {
      bufferlist::iterator p = bl.begin();
      bufferlist header;
      map<string,bufferlist> m;
      ::decode(header, p);
      ::decode(m, p);
      for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); ++q) {
	names.push_back(q->first);
      }
    }

    // new format images are accessed by class methods
    int max_read = 1024;
    string last_read = "";
    do {
      map<string, string> images;
      cls_client::dir_list(&io_ctx, RBD_DIRECTORY,
			   last_read, max_read, &images);
      for (map<string, string>::const_iterator it = images.begin();
	   it != images.end(); ++it) {
	names.push_back(it->first);
      }
      if (!images.empty()) {
	last_read = images.rbegin()->first;
      }
      r = images.size();
    } while (r == max_read);

    return 0;
  }

  int create_v1(IoCtx& io_ctx, const char *imgname, uint64_t bid,
		uint64_t size, int order)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 2) << "adding rbd image to directory..." << dendl;
    int r = tmap_set(io_ctx, imgname);
    if (r < 0) {
      lderr(cct) << "error adding image to directory: " << cpp_strerror(r)
		 << dendl;
      return r;
    }

    ldout(cct, 2) << "creating rbd image..." << dendl;
    struct rbd_obj_header_ondisk header;
    init_rbd_header(header, size, order, bid);

    bufferlist bl;
    bl.append((const char *)&header, sizeof(header));

    string header_oid = old_header_name(imgname);
    r = io_ctx.write(header_oid, bl, bl.length(), 0);
    if (r < 0) {
      lderr(cct) << "Error writing image header: " << cpp_strerror(r)
		 << dendl;
      int remove_r = tmap_rm(io_ctx, imgname);
      if (remove_r < 0) {
	lderr(cct) << "Could not remove image from directory after "
		   << "header creation failed: "
		   << cpp_strerror(r) << dendl;
      }
      return r;
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }

  int create_v2(IoCtx& io_ctx, const char *imgname, uint64_t bid, uint64_t size,
		int order, uint64_t features, uint64_t stripe_unit,
		uint64_t stripe_count)
  {
    ostringstream bid_ss;
    uint32_t extra;
    string id, id_obj, header_oid;
    int remove_r;
    ostringstream oss;
    CephContext *cct = (CephContext *)io_ctx.cct();

    id_obj = id_obj_name(imgname);

    int r = io_ctx.create(id_obj, true);
    if (r < 0) {
      lderr(cct) << "error creating rbd id object: " << cpp_strerror(r)
		 << dendl;
      return r;
    }

    extra = rand() % 0xFFFFFFFF;
    bid_ss << std::hex << bid << std::hex << extra;
    id = bid_ss.str();
    r = cls_client::set_id(&io_ctx, id_obj, id);
    if (r < 0) {
      lderr(cct) << "error setting image id: " << cpp_strerror(r) << dendl;
      goto err_remove_id;
    }

    ldout(cct, 2) << "adding rbd image to directory..." << dendl;
    r = cls_client::dir_add_image(&io_ctx, RBD_DIRECTORY, imgname, id);
    if (r < 0) {
      lderr(cct) << "error adding image to directory: " << cpp_strerror(r)
		 << dendl;
      goto err_remove_id;
    }

    oss << RBD_DATA_PREFIX << id;
    header_oid = header_name(id);
    r = cls_client::create_image(&io_ctx, header_oid, size, order,
				 features, oss.str());
    if (r < 0) {
      lderr(cct) << "error writing header: " << cpp_strerror(r) << dendl;
      goto err_remove_from_dir;
    }

    if ((stripe_unit || stripe_count) &&
	(stripe_count != 1 || stripe_unit != (1ull << order))) {
      r = cls_client::set_stripe_unit_count(&io_ctx, header_oid,
					    stripe_unit, stripe_count);
      if (r < 0) {
	lderr(cct) << "error setting striping parameters: "
		   << cpp_strerror(r) << dendl;
	goto err_remove_header;
      }
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;

  err_remove_header:
    remove_r = io_ctx.remove(header_oid);
    if (remove_r < 0) {
      lderr(cct) << "error cleaning up image header after creation failed: "
		 << dendl;
    }
  err_remove_from_dir:
    remove_r = cls_client::dir_remove_image(&io_ctx, RBD_DIRECTORY,
					    imgname, id);
    if (remove_r < 0) {
      lderr(cct) << "error cleaning up image from rbd_directory object "
		 << "after creation failed: " << cpp_strerror(remove_r)
		 << dendl;
    }
  err_remove_id:
    remove_r = io_ctx.remove(id_obj);
    if (remove_r < 0) {
      lderr(cct) << "error cleaning up id object after creation failed: "
		 << cpp_strerror(remove_r) << dendl;
    }

    return r;
  }

  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size,
	     int *order)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    bool old_format = cct->_conf->rbd_default_format == 1;
    uint64_t features = old_format ? 0 : cct->_conf->rbd_default_features;
    return create(io_ctx, imgname, size, old_format, features, order, 0, 0);
  }

  int create(IoCtx& io_ctx, const char *imgname, uint64_t size,
	     bool old_format, uint64_t features, int *order,
	     uint64_t stripe_unit, uint64_t stripe_count)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "create " << &io_ctx << " name = " << imgname
		   << " size = " << size << " old_format = " << old_format
		   << " features = " << features << " order = " << *order
		   << " stripe_unit = " << stripe_unit
		   << " stripe_count = " << stripe_count
		   << dendl;


    if (features & ~RBD_FEATURES_ALL) {
      lderr(cct) << "librbd does not support requested features." << dendl;
      return -ENOSYS;
    }

    // make sure it doesn't already exist, in either format
    int r = detect_format(io_ctx, imgname, NULL, NULL);
    if (r != -ENOENT) {
      if (r) {
	lderr(cct) << "Could not tell if " << imgname << " already exists" << dendl;
	return r;
      }
      lderr(cct) << "rbd image " << imgname << " already exists" << dendl;
      return -EEXIST;
    }

    if (!order)
      return -EINVAL;

    if (!*order)
      *order = cct->_conf->rbd_default_order;
    if (!*order)
      *order = RBD_DEFAULT_OBJ_ORDER;

    if (*order && (*order > 64 || *order < 12)) {
      lderr(cct) << "order must be in the range [12, 64]" << dendl;
      return -EDOM;
    }

    Rados rados(io_ctx);
    uint64_t bid = rados.get_instance_id();

    // if striping is enabled, use possibly custom defaults
    if (!old_format && (features & RBD_FEATURE_STRIPINGV2) &&
	!stripe_unit && !stripe_count) {
      stripe_unit = cct->_conf->rbd_default_stripe_unit;
      stripe_count = cct->_conf->rbd_default_stripe_count;
    }

    // normalize for default striping
    if (stripe_unit == (1ull << *order) && stripe_count == 1) {
      stripe_unit = 0;
      stripe_count = 0;
    }
    if ((stripe_unit || stripe_count) &&
	(features & RBD_FEATURE_STRIPINGV2) == 0) {
      lderr(cct) << "STRIPINGV2 and format 2 or later required for non-default striping" << dendl;
      return -EINVAL;
    }
    if ((stripe_unit && !stripe_count) ||
	(!stripe_unit && stripe_count))
      return -EINVAL;

    if (old_format) {
      if (stripe_unit && stripe_unit != (1ull << *order))
	return -EINVAL;
      if (stripe_count && stripe_count != 1)
	return -EINVAL;

      return create_v1(io_ctx, imgname, bid, size, *order);
    } else {
      return create_v2(io_ctx, imgname, bid, size, *order, features,
		       stripe_unit, stripe_count);
    }
  }

  int rename(IoCtx& io_ctx, const char *srcname, const char *dstname)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "rename " << &io_ctx << " " << srcname << " -> "
		   << dstname << dendl;

    bool old_format;
    uint64_t src_size;
    int r = detect_format(io_ctx, srcname, &old_format, &src_size);
    if (r < 0) {
      lderr(cct) << "error finding source object: " << cpp_strerror(r) << dendl;
      return r;
    }

    string src_oid =
      old_format ? old_header_name(srcname) : id_obj_name(srcname);
    string dst_oid =
      old_format ? old_header_name(dstname) : id_obj_name(dstname);

    string id;
    if (!old_format) {
      r = cls_client::get_id(&io_ctx, src_oid, &id);
      if (r < 0) {
	lderr(cct) << "error reading image id: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    bufferlist databl;
    map<string, bufferlist> omap_values;
    r = io_ctx.read(src_oid, databl, src_size, 0);
    if (r < 0) {
      lderr(cct) << "error reading source object: " << src_oid << ": "
		 << cpp_strerror(r) << dendl;
      return r;
    }

    int MAX_READ = 1024;
    string last_read = "";
    do {
      map<string, bufferlist> outbl;
      r = io_ctx.omap_get_vals(src_oid, last_read, MAX_READ, &outbl);
      if (r < 0) {
	lderr(cct) << "error reading source object omap values: "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      omap_values.insert(outbl.begin(), outbl.end());
      if (!outbl.empty())
	last_read = outbl.rbegin()->first;
    } while (r == MAX_READ);

    r = detect_format(io_ctx, dstname, NULL, NULL);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error checking for existing image called "
		 << dstname << ":" << cpp_strerror(r) << dendl;
      return r;
    }
    if (r == 0) {
      lderr(cct) << "rbd image " << dstname << " already exists" << dendl;
      return -EEXIST;
    }

    librados::ObjectWriteOperation op;
    op.create(true);
    op.write_full(databl);
    if (!omap_values.empty())
      op.omap_set(omap_values);
    r = io_ctx.operate(dst_oid, &op);
    if (r < 0) {
      lderr(cct) << "error writing destination object: " << dst_oid << ": "
		 << cpp_strerror(r) << dendl;
      return r;
    }

    if (old_format) {
      r = tmap_set(io_ctx, dstname);
      if (r < 0) {
	io_ctx.remove(dst_oid);
	lderr(cct) << "couldn't add " << dstname << " to directory: "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      r = tmap_rm(io_ctx, srcname);
      if (r < 0) {
	lderr(cct) << "warning: couldn't remove old entry from directory ("
		   << srcname << ")" << dendl;
      }
    } else {
      r = cls_client::dir_rename_image(&io_ctx, RBD_DIRECTORY,
				       srcname, dstname, id);
      if (r < 0) {
	lderr(cct) << "error updating directory: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    r = io_ctx.remove(src_oid);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "warning: couldn't remove old source object ("
		 << src_oid << ")" << dendl;
    }

    if (old_format) {
      notify_change(io_ctx, old_header_name(srcname), NULL);
    }

    return 0;
  }


  int info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    ldout(ictx->cct, 20) << "info " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    image_info(ictx, info, infosize);
    return 0;
  }

  int get_old_format(ImageCtx *ictx, uint8_t *old)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    *old = ictx->old_format;
    return 0;
  }

  int get_size(ImageCtx *ictx, uint64_t *size)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    RWLock::RLocker l(ictx->md_lock);
    *size = ictx->get_image_size();
    return 0;
  }

  int get_features(ImageCtx *ictx, uint64_t *features)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    RWLock::RLocker l(ictx->md_lock);
    return ictx->get_features(features);
  }

  int remove(IoCtx& io_ctx, const char *imgname, ProgressContext& prog_ctx)
  {
    CephContext *cct((CephContext *)io_ctx.cct());
    ldout(cct, 20) << "remove " << &io_ctx << " " << imgname << dendl;

    string id;
    bool old_format = false;
    bool unknown_format = true;
    ImageCtx *ictx = new ImageCtx(imgname, "", io_ctx, false);
    int r = open_image(ictx);
    if (r < 0) {
      ldout(cct, 2) << "error opening image: " << cpp_strerror(-r) << dendl;
    } else {
      string header_oid = ictx->header_oid;
      old_format = ictx->old_format;
      unknown_format = false;
      id = ictx->id;

      std::list<obj_watch_t> watchers;
      r = io_ctx.list_watchers(header_oid, &watchers);
      if (r < 0) {
	lderr(cct) << "error listing watchers" << dendl;
	close_image(ictx);
	return r;
      }
      if (watchers.size() > 1) {
	lderr(cct) << "image has watchers - not removing" << dendl;
	close_image(ictx);
	return -EBUSY;
      }
      assert(watchers.size() == 1);

      ictx->md_lock.get_read();
      trim_image(ictx, 0, prog_ctx);
      ictx->md_lock.put_read();

      close_image(ictx);

      ldout(cct, 2) << "removing header..." << dendl;
      r = io_ctx.remove(header_oid);
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
	return r;
      }
    }

    if (old_format || unknown_format) {
      ldout(cct, 2) << "removing rbd image from directory..." << dendl;
      r = tmap_rm(io_ctx, imgname);
      old_format = (r == 0);
      if (r < 0 && !unknown_format) {
	lderr(cct) << "error removing img from old-style directory: "
		   << cpp_strerror(-r) << dendl;
	return r;
      }
    }
    if (!old_format) {
      ldout(cct, 2) << "removing id object..." << dendl;
      r = io_ctx.remove(id_obj_name(imgname));
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error removing id object: " << cpp_strerror(r) << dendl;
	return r;
      }

      r = cls_client::dir_get_id(&io_ctx, RBD_DIRECTORY, imgname, &id);
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error getting id of image" << dendl;
	return r;
      }

      ldout(cct, 2) << "removing rbd image from directory..." << dendl;
      r = cls_client::dir_remove_image(&io_ctx, RBD_DIRECTORY, imgname, id);
      if (r < 0) {
	lderr(cct) << "error removing img from new-style directory: "
		   << cpp_strerror(-r) << dendl;
	return r;
      }
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }

  int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
  {
    CephContext *cct = ictx->cct;

    if (size == ictx->size) {
      ldout(cct, 2) << "no change in size (" << ictx->size << " -> " << size
		    << ")" << dendl;
      return 0;
    }

    if (size > ictx->size) {
      ldout(cct, 2) << "expanding image " << ictx->size << " -> " << size
		    << dendl;
      // TODO: make ictx->set_size
    } else {
      ldout(cct, 2) << "shrinking image " << ictx->size << " -> " << size
		    << dendl;
      trim_image(ictx, size, prog_ctx);
    }
    ictx->size = size;

    int r;
    if (ictx->old_format) {
      // rewrite header
      bufferlist bl;
      ictx->header.image_size = size;
      bl.append((const char *)&(ictx->header), sizeof(ictx->header));
      r = ictx->md_ctx.write(ictx->header_oid, bl, bl.length(), 0);
    } else {
      r = cls_client::set_size(&(ictx->md_ctx), ictx->header_oid, size);
    }

    // TODO: remove this useless check
    if (r == -ERANGE)
      lderr(cct) << "operation might have conflicted with another client!"
		 << dendl;
    if (r < 0) {
      lderr(cct) << "error writing header: " << cpp_strerror(-r) << dendl;
      return r;
    } else {
      notify_change(ictx->md_ctx, ictx->header_oid, ictx);
    }

    return 0;
  }

  int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "resize " << ictx << " " << ictx->size << " -> "
		   << size << dendl;

    if (ictx->read_only)
      return -EROFS;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    RWLock::WLocker l(ictx->md_lock);
    if (size < ictx->size && ictx->object_cacher) {
      // need to invalidate since we're deleting objects, and
      // ObjectCacher doesn't track non-existent objects
      ictx->invalidate_cache();
    }
    resize_helper(ictx, size, prog_ctx);

    ldout(cct, 2) << "done." << dendl;

    ictx->perfcounter->inc(l_librbd_resize);
    return 0;
  }

  int ictx_check(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "ictx_check " << ictx << dendl;
    ictx->refresh_lock.Lock();
    bool needs_refresh = ictx->last_refresh != ictx->refresh_seq;
    ictx->refresh_lock.Unlock();

    if (needs_refresh) {
      RWLock::WLocker l(ictx->md_lock);

      int r = ictx_refresh(ictx);
      if (r < 0) {
	lderr(cct) << "Error re-reading rbd header: " << cpp_strerror(-r)
		   << dendl;
	return r;
      }
    }
    return 0;
  }

  int ictx_refresh(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    bufferlist bl, bl2;

    ldout(cct, 20) << "ictx_refresh " << ictx << dendl;

    ictx->refresh_lock.Lock();
    int refresh_seq = ictx->refresh_seq;
    ictx->refresh_lock.Unlock();

    {
      int r;
      ictx->lockers.clear();
      if (ictx->old_format) {
	r = read_header(ictx->md_ctx, ictx->header_oid, &ictx->header);
	if (r < 0) {
	  lderr(cct) << "Error reading header: " << cpp_strerror(r) << dendl;
	  return r;
	}
	ClsLockType lock_type = LOCK_NONE;
	r = rados::cls::lock::get_lock_info(&ictx->md_ctx, ictx->header_oid,
					    RBD_LOCK_NAME, &ictx->lockers,
					    &lock_type, &ictx->lock_tag);

	// If EOPNOTSUPP, treat image as if there are no locks (we can't
	// query them).

	// Ugly: OSDs prior to eed28daaf8927339c2ecae1b1b06c1b63678ab03
	// return EIO when the class isn't present; should be EOPNOTSUPP.
	// Treat EIO or EOPNOTSUPP the same for now, as LOCK_NONE.  Blech.

	if (r < 0 && ((r != -EOPNOTSUPP) && (r != -EIO))) {
	  lderr(cct) << "Error getting lock info: " << cpp_strerror(r)
		     << dendl;
	  return r;
	}
	ictx->exclusive_locked = (lock_type == LOCK_EXCLUSIVE);
	ictx->order = ictx->header.options.order;
	ictx->size = ictx->header.image_size;
	ictx->object_prefix = ictx->header.block_name;
	ictx->init_layout();
      } else {
	do {
	  uint64_t incompatible_features;
	  r = cls_client::get_mutable_metadata(&ictx->md_ctx, ictx->header_oid,
					       &ictx->size, &ictx->features,
					       &incompatible_features,
					       &ictx->lockers,
					       &ictx->exclusive_locked,
					       &ictx->lock_tag);
	  if (r < 0) {
	    lderr(cct) << "Error reading mutable metadata: " << cpp_strerror(r)
		       << dendl;
	    return r;
	  }

	  uint64_t unsupported = incompatible_features & ~RBD_FEATURES_ALL;
	  if (unsupported) {
	    lderr(ictx->cct) << "Image uses unsupported features: "
			     << unsupported << dendl;
	    return -ENOSYS;
	  }

	} while (r == -ENOENT);
      }
    }

    ictx->refresh_lock.Lock();
    ictx->last_refresh = refresh_seq;
    ictx->refresh_lock.Unlock();

    return 0;
  }

  struct CopyProgressCtx {
    CopyProgressCtx(ProgressContext &p)
      : destictx(NULL), src_size(0), prog_ctx(p)
    { }

    ImageCtx *destictx;
    uint64_t src_size;
    ProgressContext &prog_ctx;
  };

  int do_copy_extent(uint64_t offset, size_t len, const char *buf, void *data)
  {
    CopyProgressCtx *cp = reinterpret_cast<CopyProgressCtx*>(data);
    cp->prog_ctx.update_progress(offset, cp->src_size);
    int ret = 0;
    if (buf) {
      ret = write(cp->destictx, offset, len, buf);
    }
    return ret;
  }

  int copy(ImageCtx *src, IoCtx& dest_md_ctx, const char *destname,
	   ProgressContext &prog_ctx)
  {
    CephContext *cct = (CephContext *)dest_md_ctx.cct();
    ldout(cct, 20) << "copy " << src->name
		   << " -> " << destname << dendl;
    int order = src->order;

    src->md_lock.get_read();
    uint64_t src_size = src->get_image_size();
    src->md_lock.put_read();

    int r = create(dest_md_ctx, destname, src_size, src->old_format,
		   src->features, &order, src->stripe_unit, src->stripe_count);
    if (r < 0) {
      lderr(cct) << "header creation failed" << dendl;
      return r;
    }

    ImageCtx *dest = new librbd::ImageCtx(destname, "", dest_md_ctx, false);
    r = open_image(dest);
    if (r < 0) {
      lderr(cct) << "failed to read newly created header" << dendl;
      return r;
    }

    r = copy(src, dest, prog_ctx);
    close_image(dest);
    return r;
  }

  class C_CopyWrite : public Context {
  public:
    C_CopyWrite(SimpleThrottle *throttle, bufferlist *bl)
      : m_throttle(throttle), m_bl(bl) {}
    virtual void finish(int r) {
      delete m_bl;
      m_throttle->end_op(r);
    }
  private:
    SimpleThrottle *m_throttle;
    bufferlist *m_bl;
  };

  class C_CopyRead : public Context {
  public:
    C_CopyRead(SimpleThrottle *throttle, ImageCtx *dest, uint64_t offset,
	       bufferlist *bl)
      : m_throttle(throttle), m_dest(dest), m_offset(offset), m_bl(bl) {
      m_throttle->start_op();
    }
    virtual void finish(int r) {
      if (r < 0) {
	lderr(m_dest->cct) << "error reading from source image at offset "
			   << m_offset << ": " << cpp_strerror(r) << dendl;
	delete m_bl;
	m_throttle->end_op(r);
	return;
      }
      assert(m_bl->length() == (size_t)r);

      if (m_bl->is_zero()) {
	delete m_bl;
	m_throttle->end_op(r);
	return;
      }

      Context *ctx = new C_CopyWrite(m_throttle, m_bl);
      AioCompletion *comp = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_write(m_dest, m_offset, m_bl->length(), m_bl->c_str(), comp);
      if (r < 0) {
	ctx->complete(r);
	comp->release();
	lderr(m_dest->cct) << "error writing to destination image at offset "
			   << m_offset << ": " << cpp_strerror(r) << dendl;
      }
    }
  private:
    SimpleThrottle *m_throttle;
    ImageCtx *m_dest;
    uint64_t m_offset;
    bufferlist *m_bl;
  };

  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx)
  {
    src->md_lock.get_read();
    uint64_t src_size = src->get_image_size();
    src->md_lock.put_read();

    dest->md_lock.get_read();
    uint64_t dest_size = dest->get_image_size();
    dest->md_lock.put_read();

    CephContext *cct = src->cct;
    if (dest_size < src_size) {
      lderr(cct) << " src size " << src_size << " >= dest size "
		 << dest_size << dendl;
      return -EINVAL;
    }
    int r;
    SimpleThrottle throttle(cct->_conf->rbd_concurrent_management_ops, false);
    uint64_t period = src->get_stripe_period();
    for (uint64_t offset = 0; offset < src_size; offset += period) {
      uint64_t len = min(period, src_size - offset);
      bufferlist *bl = new bufferlist();
      Context *ctx = new C_CopyRead(&throttle, dest, offset, bl);
      AioCompletion *comp = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_read(src, offset, len, NULL, bl, comp);
      if (r < 0) {
	ctx->complete(r);
	comp->release();
	throttle.wait_for_ret();
	lderr(cct) << "could not read from source image from "
		   << offset << " to " << offset + len << ": "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      prog_ctx.update_progress(offset, src_size);
    }

    r = throttle.wait_for_ret();
    if (r >= 0)
      prog_ctx.update_progress(src_size, src_size);
    return r;
  }

  int open_image(ImageCtx *ictx)
  {
    ldout(ictx->cct, 20) << "open_image: ictx = " << ictx
			 << " name = '" << ictx->name
			 << "' id = '" << ictx->id
			 << dendl;
    int r = ictx->init();
    if (r < 0)
      return r;

    if (!ictx->read_only) {
      r = ictx->register_watch();
      if (r < 0) {
	lderr(ictx->cct) << "error registering a watch: " << cpp_strerror(r)
			 << dendl;
	goto err_close;
      }
    }

    ictx->md_lock.get_write();
    r = ictx_refresh(ictx);
    ictx->md_lock.put_write();
    if (r < 0)
      goto err_close;

    return 0;

  err_close:
    close_image(ictx);
    return r;
  }

  void close_image(ImageCtx *ictx)
  {
    ldout(ictx->cct, 20) << "close_image " << ictx << dendl;
    if (ictx->object_cacher)
      ictx->shutdown_cache(); // implicitly flushes
    else
      flush(ictx);

    if (ictx->wctx)
      ictx->unregister_watch();

    delete ictx;
  }

  int list_lockers(ImageCtx *ictx,
		   std::list<locker_t> *lockers,
		   bool *exclusive,
		   string *tag)
  {
    ldout(ictx->cct, 20) << "list_locks on image " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    RWLock::RLocker locker(ictx->md_lock);
    if (exclusive)
      *exclusive = ictx->exclusive_locked;
    if (tag)
      *tag = ictx->lock_tag;
    if (lockers) {
      lockers->clear();
      map<rados::cls::lock::locker_id_t,
	  rados::cls::lock::locker_info_t>::const_iterator it;
      for (it = ictx->lockers.begin(); it != ictx->lockers.end(); ++it) {
	locker_t locker;
	locker.client = stringify(it->first.locker);
	locker.cookie = it->first.cookie;
	locker.address = stringify(it->second.addr);
	lockers->push_back(locker);
      }
    }

    return 0;
  }

  int lock(ImageCtx *ictx, bool exclusive, const string& cookie,
	   const string& tag)
  {
    ldout(ictx->cct, 20) << "lock image " << ictx << " exclusive=" << exclusive
			 << " cookie='" << cookie << "' tag='" << tag << "'"
			 << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    /**
     * If we wanted we could do something more intelligent, like local
     * checks that we think we will succeed. But for now, let's not
     * duplicate that code.
     */
    RWLock::RLocker locker(ictx->md_lock);
    r = rados::cls::lock::lock(&ictx->md_ctx, ictx->header_oid, RBD_LOCK_NAME,
			       exclusive ? LOCK_EXCLUSIVE : LOCK_SHARED,
			       cookie, tag, "", utime_t(), 0);
    if (r < 0)
      return r;
    notify_change(ictx->md_ctx, ictx->header_oid, ictx);
    return 0;
  }

  int unlock(ImageCtx *ictx, const string& cookie)
  {
    ldout(ictx->cct, 20) << "unlock image " << ictx
			 << " cookie='" << cookie << "'" << dendl;


    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    RWLock::RLocker locker(ictx->md_lock);
    r = rados::cls::lock::unlock(&ictx->md_ctx, ictx->header_oid,
				 RBD_LOCK_NAME, cookie);
    if (r < 0)
      return r;
    notify_change(ictx->md_ctx, ictx->header_oid, ictx);
    return 0;
  }

  int break_lock(ImageCtx *ictx, const string& client,
		 const string& cookie)
  {
    ldout(ictx->cct, 20) << "break_lock image " << ictx << " client='" << client
			 << "' cookie='" << cookie << "'" << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    entity_name_t lock_client;
    if (!lock_client.parse(client)) {
      lderr(ictx->cct) << "Unable to parse client '" << client
		       << "'" << dendl;
      return -EINVAL;
    }
    RWLock::RLocker locker(ictx->md_lock);
    r = rados::cls::lock::break_lock(&ictx->md_ctx, ictx->header_oid,
				     RBD_LOCK_NAME, cookie, lock_client);
    if (r < 0)
      return r;
    notify_change(ictx->md_ctx, ictx->header_oid, ictx);
    return 0;
  }

  void rbd_ctx_cb(completion_t cb, void *arg)
  {
    Context *ctx = reinterpret_cast<Context *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    ctx->complete(comp->get_return_value());
    comp->release();
  }

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, uint64_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg)
  {
    utime_t start_time, elapsed;

    ldout(ictx->cct, 20) << "read_iterate " << ictx << " off = " << off
			 << " len = " << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    uint64_t mylen = len;
    r = clip_io(ictx, off, &mylen);
    if (r < 0)
      return r;

    int64_t total_read = 0;
    uint64_t period = ictx->get_stripe_period();
    uint64_t left = mylen;

    start_time = ceph_clock_now(ictx->cct);
    while (left > 0) {
      uint64_t period_off = off - (off % period);
      uint64_t read_len = min(period_off + period - off, left);

      bufferlist bl;

      Mutex mylock("IoCtxImpl::write::mylock");
      Cond cond;
      bool done;
      int ret;

      Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
      AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_read(ictx, off, read_len, NULL, &bl, c);
      if (r < 0) {
	c->release();
	delete ctx;
	return r;
      }

      mylock.Lock();
      while (!done)
	cond.Wait(mylock);
      mylock.Unlock();

      if (ret < 0)
	return ret;

      r = cb(total_read, ret, bl.c_str(), arg);
      if (r < 0)
	return r;

      total_read += ret;
      left -= ret;
      off += ret;
    }

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    ictx->perfcounter->tinc(l_librbd_rd_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_rd);
    ictx->perfcounter->inc(l_librbd_rd_bytes, mylen);
    return total_read;
  }

  int simple_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg)
  {
    char *dest_buf = (char *)arg;
    if (buf)
      memcpy(dest_buf + ofs, buf, len);
    else
      memset(dest_buf + ofs, 0, len);

    return 0;
  }

  ssize_t read(ImageCtx *ictx, uint64_t ofs, size_t len, char *buf)
  {
    vector<pair<uint64_t,uint64_t> > extents;
    extents.push_back(make_pair(ofs, len));
    return read(ictx, extents, buf, NULL);
  }

  ssize_t read(ImageCtx *ictx,
	       const vector<pair<uint64_t,uint64_t> >& image_extents,
	       char *buf, bufferlist *pbl)
  {
    Mutex mylock("IoCtxImpl::write::mylock");
    Cond cond;
    bool done;
    int ret;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    int r = aio_read(ictx, image_extents, buf, pbl, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    return ret;
  }

  ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf)
  {
    utime_t start_time, elapsed;
    ldout(ictx->cct, 20) << "write " << ictx << " off = " << off << " len = "
			 << len << dendl;

    start_time = ceph_clock_now(ictx->cct);
    Mutex mylock("librbd::write::mylock");
    Cond cond;
    bool done;
    int ret;

    uint64_t mylen = len;
    int r = clip_io(ictx, off, &mylen);
    if (r < 0)
      return r;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    r = aio_write(ictx, off, mylen, buf, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    if (ret < 0)
      return ret;

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    ictx->perfcounter->tinc(l_librbd_wr_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_wr);
    ictx->perfcounter->inc(l_librbd_wr_bytes, mylen);
    return mylen;
  }

  int discard(ImageCtx *ictx, uint64_t off, uint64_t len)
  {
    utime_t start_time, elapsed;
    ldout(ictx->cct, 20) << "discard " << ictx << " off = " << off << " len = "
			 << len << dendl;

    start_time = ceph_clock_now(ictx->cct);
    Mutex mylock("librbd::discard::mylock");
    Cond cond;
    bool done;
    int ret;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    int r = aio_discard(ictx, off, len, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    if (ret < 0)
      return ret;

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    ictx->perfcounter->inc(l_librbd_discard_latency, elapsed);
    ictx->perfcounter->inc(l_librbd_discard);
    ictx->perfcounter->inc(l_librbd_discard_bytes, len);
    return len;
  }

  ssize_t handle_sparse_read(CephContext *cct,
			     bufferlist data_bl,
			     uint64_t block_ofs,
			     const map<uint64_t, uint64_t> &data_map,
			     uint64_t buf_ofs,   // offset into buffer
			     size_t buf_len,     // length in buffer (not size of buffer!)
			     char *dest_buf)
  {
    uint64_t bl_ofs = 0;
    size_t buf_left = buf_len;

    for (map<uint64_t, uint64_t>::const_iterator iter = data_map.begin();
	 iter != data_map.end();
	 ++iter) {
      uint64_t extent_ofs = iter->first;
      size_t extent_len = iter->second;

      ldout(cct, 10) << "extent_ofs=" << extent_ofs
		     << " extent_len=" << extent_len << dendl;
      ldout(cct, 10) << "block_ofs=" << block_ofs << dendl;

      /* a hole? */
      if (extent_ofs > block_ofs) {
	uint64_t gap = extent_ofs - block_ofs;
	ldout(cct, 10) << "<1>zeroing " << buf_ofs << "~" << gap << dendl;
	memset(dest_buf + buf_ofs, 0, gap);

	buf_ofs += gap;
	buf_left -= gap;
	block_ofs = extent_ofs;
      } else if (extent_ofs < block_ofs) {
	assert(0 == "osd returned data prior to what we asked for");
	return -EIO;
      }

      if (bl_ofs + extent_len > (buf_ofs + buf_left)) {
	assert(0 == "osd returned more data than we asked for");
	return -EIO;
      }

      /* data */
      ldout(cct, 10) << "<2>copying " << buf_ofs << "~" << extent_len
		     << " from ofs=" << bl_ofs << dendl;
      memcpy(dest_buf + buf_ofs, data_bl.c_str() + bl_ofs, extent_len);

      bl_ofs += extent_len;
      buf_ofs += extent_len;
      assert(buf_left >= extent_len);
      buf_left -= extent_len;
      block_ofs += extent_len;
    }

    /* last hole */
    if (buf_left > 0) {
      ldout(cct, 10) << "<3>zeroing " << buf_ofs << "~" << buf_left << dendl;
      memset(dest_buf + buf_ofs, 0, buf_left);
    }

    return buf_len;
  }

  void rados_req_cb(rados_completion_t c, void *arg)
  {
    AioRequest *req = reinterpret_cast<AioRequest *>(arg);
    req->complete(rados_aio_get_return_value(c));
  }

  void rados_ctx_cb(rados_completion_t c, void *arg)
  {
    Context *comp = reinterpret_cast<Context *>(arg);
    comp->complete(rados_aio_get_return_value(c));
  }

  // validate extent against image size; clip to image size if necessary
  int clip_io(ImageCtx *ictx, uint64_t off, uint64_t *len)
  {
    ictx->md_lock.get_read();
    uint64_t image_size = ictx->get_image_size();
    ictx->md_lock.put_read();

    // special-case "len == 0" requests: always valid
    if (*len == 0)
      return 0;

    // can't start past end
    if (off >= image_size)
      return -EINVAL;

    // clip requests that extend past end to just end
    if ((off + *len) > image_size)
      *len = (size_t)(image_size - off);

    return 0;
  }

  int aio_flush(ImageCtx *ictx, AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_flush " << ictx << " completion " << c <<  dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    ictx->user_flushed();

    c->get();
    c->add_request();
    c->init_time(ictx, AIO_TYPE_FLUSH);
    C_AioWrite *req_comp = new C_AioWrite(cct, c);
    if (ictx->object_cacher) {
      ictx->flush_cache_aio(req_comp);
    } else {
      librados::AioCompletion *rados_completion =
	librados::Rados::aio_create_completion(req_comp, NULL, rados_ctx_cb);
      ictx->data_ctx.aio_flush_async(rados_completion);
      rados_completion->release();
    }
    c->finish_adding_requests(cct);
    c->put();
    ictx->perfcounter->inc(l_librbd_aio_flush);

    return 0;
  }

  int flush(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "flush " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    ictx->user_flushed();
    r = _flush(ictx);
    ictx->perfcounter->inc(l_librbd_flush);
    return r;
  }

  int _flush(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    int r;
    // flush any outstanding writes
    if (ictx->object_cacher) {
      r = ictx->flush_cache();
    } else {
      r = ictx->data_ctx.aio_flush();
    }

    if (r)
      lderr(cct) << "_flush " << ictx << " r = " << r << dendl;

    return r;
  }

  int aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
		AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_write " << ictx << " off = " << off << " len = "
		   << len << " buf = " << (void*)buf << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    uint64_t mylen = len;
    r = clip_io(ictx, off, &mylen);
    if (r < 0)
      return r;

    if (ictx->read_only)
      return -EROFS;

    // map
    vector<ObjectExtent> extents;
    if (len > 0) {
      Striper::file_to_extents(ictx->cct, ictx->format_string,
			       &ictx->layout, off, mylen, 0, extents);
    }

    c->get();
    c->init_time(ictx, AIO_TYPE_WRITE);
    for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); ++p) {
      ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~" << p->length
		     << " from " << p->buffer_extents << dendl;
      // assemble extent
      bufferlist bl;
      for (vector<pair<uint64_t,uint64_t> >::iterator q = p->buffer_extents.begin();
	   q != p->buffer_extents.end();
	   ++q) {
	bl.append(buf + q->first, q->second);
      }

      C_AioWrite *req_comp = new C_AioWrite(cct, c);
      if (ictx->object_cacher) {
	c->add_request();
	ictx->write_to_cache(p->oid, bl, p->length, p->offset, req_comp);
      } else {
	vector<pair<uint64_t,uint64_t> > objectx;
	Striper::extent_to_file(ictx->cct, &ictx->layout,
				p->objectno, 0, ictx->layout.fl_object_size,
				objectx);
	AioWrite *req = new AioWrite(ictx, p->oid.name, p->objectno, p->offset,
				     objectx,
				     bl, req_comp);
	c->add_request();
	r = req->send();
	if (r < 0)
	  goto done;
      }
    }
  done:
    c->finish_adding_requests(ictx->cct);
    c->put();

    ictx->perfcounter->inc(l_librbd_aio_wr);
    ictx->perfcounter->inc(l_librbd_aio_wr_bytes, mylen);

    /* FIXME: cleanup all the allocated stuff */
    return r;
  }

  int aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_discard " << ictx << " off = " << off << " len = "
		   << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    r = clip_io(ictx, off, &len);
    if (r < 0)
      return r;

    if (ictx->read_only)
      return -EROFS;

    // map
    vector<ObjectExtent> extents;
    if (len > 0) {
      Striper::file_to_extents(ictx->cct, ictx->format_string,
			       &ictx->layout, off, len, 0, extents);
    }

    c->get();
    c->init_time(ictx, AIO_TYPE_DISCARD);
    for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); ++p) {
      ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~" << p->length
		     << " from " << p->buffer_extents << dendl;
      C_AioWrite *req_comp = new C_AioWrite(cct, c);
      AbstractWrite *req;
      c->add_request();

      vector<pair<uint64_t,uint64_t> > objectx;
      Striper::extent_to_file(ictx->cct, &ictx->layout,
			      p->objectno, 0, ictx->layout.fl_object_size,
			      objectx);

      if (p->offset == 0 && p->length == ictx->layout.fl_object_size) {
	req = new AioRemove(ictx, p->oid.name, p->objectno, objectx,
			    req_comp);
      } else if (p->offset + p->length == ictx->layout.fl_object_size) {
	req = new AioTruncate(ictx, p->oid.name, p->objectno, p->offset, objectx,
			      req_comp);
      } else {
	req = new AioZero(ictx, p->oid.name, p->objectno, p->offset, p->length,
			  objectx,
			  req_comp);
      }

      r = req->send();
      if (r < 0)
	goto done;
    }
    r = 0;
  done:
    if (ictx->object_cacher) {
      Mutex::Locker l(ictx->cache_lock);
      ictx->object_cacher->discard_set(ictx->object_set, extents);
    }

    c->finish_adding_requests(ictx->cct);
    c->put();

    ictx->perfcounter->inc(l_librbd_aio_discard);
    ictx->perfcounter->inc(l_librbd_aio_discard_bytes, len);

    /* FIXME: cleanup all the allocated stuff */
    return r;
  }

  void rbd_req_cb(completion_t cb, void *arg)
  {
    AioRequest *req = reinterpret_cast<AioRequest *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    req->complete(comp->get_return_value());
  }

  int aio_read(ImageCtx *ictx, uint64_t off, size_t len,
	       char *buf, bufferlist *bl,
	       AioCompletion *c)
  {
    vector<pair<uint64_t,uint64_t> > image_extents(1);
    image_extents[0] = make_pair(off, len);
    return aio_read(ictx, image_extents, buf, bl, c);
  }

  int aio_read(ImageCtx *ictx, const vector<pair<uint64_t,uint64_t> >& image_extents,
	       char *buf, bufferlist *pbl, AioCompletion *c)
  {
    ldout(ictx->cct, 20) << "aio_read " << ictx << " completion " << c << " " << image_extents << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    // map
    map<object_t,vector<ObjectExtent> > object_extents;

    uint64_t buffer_ofs = 0;
    for (vector<pair<uint64_t,uint64_t> >::const_iterator p = image_extents.begin();
	 p != image_extents.end();
	 ++p) {
      uint64_t len = p->second;
      r = clip_io(ictx, p->first, &len);
      if (r < 0)
	return r;
      if (len == 0)
	continue;

      Striper::file_to_extents(ictx->cct, ictx->format_string, &ictx->layout,
			       p->first, len, 0, object_extents, buffer_ofs);
      buffer_ofs += len;
    }

    int64_t ret;

    c->read_buf = buf;
    c->read_buf_len = buffer_ofs;
    c->read_bl = pbl;

    c->get();
    c->init_time(ictx, AIO_TYPE_READ);
    for (map<object_t,vector<ObjectExtent> >::iterator p = object_extents.begin(); p != object_extents.end(); ++p) {
      for (vector<ObjectExtent>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
	ldout(ictx->cct, 20) << " oid " << q->oid << " " << q->offset << "~" << q->length
			     << " from " << q->buffer_extents << dendl;

	C_AioRead *req_comp = new C_AioRead(ictx->cct, c);
	AioRead *req = new AioRead(ictx, q->oid.name, 
				   q->objectno, q->offset, q->length,
				   q->buffer_extents,
				   true, req_comp);
	req_comp->set_req(req);
	c->add_request();

	if (ictx->object_cacher) {
	  C_CacheRead *cache_comp = new C_CacheRead(req);
	  ictx->aio_read_from_cache(q->oid, &req->data(),
				    q->length, q->offset,
				    cache_comp);
	} else {
	  r = req->send();
	  if (r < 0 && r == -ENOENT)
	    r = 0;
	  if (r < 0) {
	    ret = r;
	    goto done;
	  }
	}
      }
    }
    ret = buffer_ofs;
  done:
    c->finish_adding_requests(ictx->cct);
    c->put();

    ictx->perfcounter->inc(l_librbd_aio_rd);
    ictx->perfcounter->inc(l_librbd_aio_rd_bytes, buffer_ofs);

    return ret;
  }

  AioCompletion *aio_create_completion() {
    AioCompletion *c = new AioCompletion();
    return c;
  }

  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete) {
    AioCompletion *c = new AioCompletion();
    c->set_complete_cb(cb_arg, cb_complete);
    return c;
  }

  AioCompletion *aio_create_completion_internal(void *cb_arg,
						callback_t cb_complete) {
    AioCompletion *c = aio_create_completion(cb_arg, cb_complete);
    c->rbd_comp = c;
    return c;
  }
}
