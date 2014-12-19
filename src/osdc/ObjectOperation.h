// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_OSDC_OBJECTOPERATION_H
#define CEPH_OSDC_OBJECTOPERATION_H

#include <memory>
#include "include/utime.h"
#include "include/Context.h"
#include "include/rados/rados_types.h"
#include "osd/osd_types.h"
#include "common/hobject.h"


class ObjOp {
protected:
  uint64_t budget; // Should wrap calls for budget.
  C_Contexts *handler;

  ObjOp() : budget(0), flags(0), priority(0) {
    handler = new C_Contexts(&handler);
  }

public:
  int flags;
  int priority;

  virtual ~ObjOp() {
    delete handler; // delete NULL has no effect
  }

  virtual std::unique_ptr<ObjOp> clone() = 0;
  void add_handler(Context *extra) {
    handler->add(extra);
  }

  uint64_t get_budget() {
    return budget;
  }
  virtual size_t size() = 0;
  virtual size_t width() = 0;
  virtual void add_op(const int op) = 0;
  virtual void add_version(const uint64_t ver) = 0;
  virtual void add_oid(const hobject_t &oid) = 0;
  virtual void add_single_return(bufferlist* bl, int* rval = NULL,
				 Context* ctx = NULL) = 0;
  virtual void add_replicated_data(const bufferlist& bl) = 0;
  virtual void add_striped_data(const uint64_t off,
				const bufferlist& bl) = 0;
  virtual void add_striped_range(const uint64_t off,
				 const uint64_t len) = 0;
  virtual void add_xattr(const string &name, bufferlist* data = NULL) = 0;
  virtual void add_xattr(const string &name, const bufferlist &data) = 0;
  virtual void add_xattr_cmp(const string &name, uint8_t cmp_op,
			     const uint8_t cmp_mode,
			     const bufferlist& data) = 0;
  virtual void add_call(const string &cname, const string &method,
			const bufferlist &indata, bufferlist *const outbl,
			Context *const ctx, int *const rval) = 0;
  virtual void add_watch(const uint64_t cookie, const uint64_t ver,
			 const uint8_t flag, const bufferlist& inbl) = 0;
  virtual void add_alloc_hint(const uint64_t expected_object_size,
			      const uint64_t expected_write_size) = 0;
  virtual void add_truncate(const uint64_t truncate_size,
			    const uint32_t truncate_seq) = 0;
  virtual void set_op_flags(const uint32_t flags) = 0;
  virtual void clear_op_flags(const uint32_t flags) = 0;
  virtual void add_stat_ctx(uint64_t *s, utime_t *m, int *rval,
			    Context *ctx = NULL) = 0;
  virtual void add_read_ctx(uint64_t off, uint64_t len, bufferlist *bl,
			    int *rval, Context *ctx = NULL) = 0;
  virtual void add_sparse_read_ctx(uint64_t off, uint64_t len,
				   std::map<uint64_t,uint64_t> *m,
				   bufferlist *data_bl, int *rval,
				   Context *ctx) = 0;

  // ------

  void create(int create_flags) {
    add_op(CEPH_OSD_OP_CREATE);
    set_op_flags(create_flags);
  }

  void create(bool excl) {
    add_op(CEPH_OSD_OP_CREATE);
    if (excl)
      set_op_flags(CEPH_OSD_OP_FLAG_EXCL);
  }
 void create(bool excl, const string& category) {
    add_op(CEPH_OSD_OP_CREATE);
    if (excl)
      set_op_flags(CEPH_OSD_OP_FLAG_EXCL);
    bufferlist bl;
    ::encode(category, bl);
    add_replicated_data(bl);
  }

  void stat(uint64_t *size, utime_t *mtime, int *rval = NULL) {
    add_op(CEPH_OSD_OP_STAT);
    add_stat_ctx(size, mtime, rval);
  }

  // object data
  void read(uint64_t off, uint64_t len, bufferlist *bl,
	    int *rval = NULL, Context* ctx = NULL) {
    add_op(CEPH_OSD_OP_READ);
    add_read_ctx(off, len, bl, rval, ctx);
  }

  // object data
  void read(uint64_t off, uint64_t len, bufferlist *bl,
	    uint64_t truncate_size, uint32_t truncate_seq, int *rval = NULL,
	    Context* ctx = NULL) {
    add_op(CEPH_OSD_OP_READ);
    add_read_ctx(off, len, bl, rval, ctx);
    add_truncate(truncate_size, truncate_seq);
  }

  void sparse_read(uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
		   bufferlist *bl, int *rval) {
    add_op(CEPH_OSD_OP_SPARSE_READ);
    add_sparse_read_ctx(off, len, m, bl, rval, NULL);
  }

  void write(uint64_t off, const bufferlist& bl,
	     uint64_t truncate_size = 0,
	     uint32_t truncate_seq = 0) {
    add_op(CEPH_OSD_OP_WRITE);
    add_striped_data(off, bl);
    add_truncate(truncate_size, truncate_seq);
  }
  void write(uint64_t off, uint64_t len, const bufferlist& bl,
	     uint64_t truncate_size = 0,
	     uint32_t truncate_seq = 0) {
    add_op(CEPH_OSD_OP_WRITE);
    bufferlist buf;
    buf.substr_of(bl, 0, len);
    add_striped_data(off, buf);
    add_truncate(truncate_size, truncate_seq);
  }
  void write_full(const bufferlist& bl) {
    add_op(CEPH_OSD_OP_WRITEFULL);
    add_striped_data(0, bl);
  }
  void append(const bufferlist& bl) {
    add_op(CEPH_OSD_OP_APPEND);
    add_striped_data(0, bl);
  }
  void append(const uint64_t len, const bufferlist& bl) {
    add_op(CEPH_OSD_OP_APPEND);
    bufferlist buf;
    buf.substr_of(buf, 0, len);
    add_striped_data(0,  bl);
  }
  void zero(uint64_t off, uint64_t len) {
    add_op(CEPH_OSD_OP_ZERO);
    add_striped_range(off, len);
  }
  void truncate(uint64_t len) {
    add_op(CEPH_OSD_OP_TRUNCATE);
    add_striped_range(0, len);
  }
  void truncate(uint64_t len, uint32_t truncate_seq) {
    add_op(CEPH_OSD_OP_TRUNCATE);
    add_striped_range(0, len);
    add_truncate(len, truncate_seq);
  }
  void remove() {
    add_op(CEPH_OSD_OP_DELETE);
  }

  // object attrs
  void getxattr(const char *name, bufferlist *bl, int *rval = NULL) {
    add_op(CEPH_OSD_OP_GETXATTR);
    add_xattr(name);
    add_single_return(bl, rval);
  }
  struct C_ObjectOperation_decodevals : public Context {
    bufferlist bl;
    std::map<std::string,bufferlist> &attrs;
    int *rval;
    C_ObjectOperation_decodevals(std::map<std::string,bufferlist> &a, int *r)
      : attrs(a), rval(r) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  ::decode(attrs, p);
	}
	catch (ceph::buffer::error& e) {
	  if (rval)
	    *rval = -EIO;
	}
      }
    }
  };
  struct C_ObjectOperation_decodekeys : public Context {
    bufferlist bl;
    std::set<std::string> *attrs;
    int *rval;
    C_ObjectOperation_decodekeys(std::set<std::string> *a, int *r)
      : attrs(a), rval(r) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  if (attrs)
	    ::decode(*attrs, p);
	}
	catch (ceph::buffer::error& e) {
	  if (rval)
	    *rval = -EIO;
	}
      }
    }
  };
  struct C_ObjectOperation_decodewatchers : public Context {
    bufferlist bl;
    list<obj_watch_t> *watchers;
    int *rval;
    C_ObjectOperation_decodewatchers(list<obj_watch_t> *w, int *r)
      : watchers(w), rval(r) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  obj_list_watch_response_t resp;
	  ::decode(resp, p);
	  if (watchers) {
	    for (list<watch_item_t>::iterator i = resp.entries.begin() ;
		 i != resp.entries.end() ; ++i) {
	      obj_watch_t ow;
	      ostringstream sa;
	      sa << i->addr;
	      strncpy(ow.addr, sa.str().c_str(), 256);
	      ow.watcher_id = i->name.num();
	      ow.cookie = i->cookie;
	      ow.timeout_seconds = i->timeout_seconds;
	      watchers->push_back(ow);
	    }
	  }
	}
	catch (ceph::buffer::error& e) {
	  if (rval)
	    *rval = -EIO;
	}
      }
    }
  };
  void getxattrs(std::map<std::string, bufferlist>& attrs,
		 int *rval = NULL) {
    add_op(CEPH_OSD_OP_GETXATTRS);
    C_ObjectOperation_decodevals *h
      = new C_ObjectOperation_decodevals(attrs, rval);
    add_single_return(&h->bl, rval, h);
  }
  void setxattr(const char *name, const bufferlist& bl) {
    add_op(CEPH_OSD_OP_SETXATTR);
    add_xattr(name, bl);
  }
  void setxattr(const char *name, const string& s) {
    bufferlist bl;
    bl.append(s);
    setxattr(name, bl);
  }
  void cmpxattr(const char *name, uint8_t cmp_op, uint8_t cmp_mode, const bufferlist& bl) {
    add_op(CEPH_OSD_OP_CMPXATTR);
    add_xattr_cmp(name, cmp_op, cmp_mode, bl);
  }
  void rmxattr(const char *name) {
    add_op(CEPH_OSD_OP_RMXATTR);
    add_xattr(name);
  }
  void setxattrs(map<string, bufferlist>& attrs) {
    bufferlist bl;
    ::encode(attrs, bl);
    add_op(CEPH_OSD_OP_RESETXATTRS);
    add_xattr(NULL, bl);
  }
  void resetxattrs(const char *prefix, map<string, bufferlist>& attrs) {
    bufferlist bl;
    ::encode(attrs, bl);
    add_op(CEPH_OSD_OP_RESETXATTRS);
    add_xattr(prefix, bl);
  }


  // objectmap
  void omap_get_keys(const string &start_after,
		     uint64_t max_to_get,
		     std::set<std::string> *out_set,
		     int *rval) {
    add_op(CEPH_OSD_OP_OMAPGETKEYS);
    bufferlist bl;
    ::encode(start_after, bl);
    ::encode(max_to_get, bl);
    add_replicated_data(bl);
    if (rval || out_set) {
      C_ObjectOperation_decodekeys *h =
	new C_ObjectOperation_decodekeys(out_set, rval);
      add_single_return(&h->bl, rval, h);
    }
  }

  void omap_get_vals(const string &start_after,
		     const string &filter_prefix,
		     uint64_t max_to_get,
		     std::map<std::string, bufferlist> &out_set,
		     int *rval) {
    add_op(CEPH_OSD_OP_OMAPGETVALS);
    bufferlist bl;
    ::encode(start_after, bl);
    ::encode(max_to_get, bl);
    ::encode(filter_prefix, bl);
    add_replicated_data(bl);
    C_ObjectOperation_decodevals *h =
      new C_ObjectOperation_decodevals(out_set, rval);
    add_single_return(&h->bl, rval, h);
  }

  void omap_get_vals_by_keys(const std::set<std::string> &to_get,
			    std::map<std::string, bufferlist> &out_set,
			    int *rval) {
    add_op(CEPH_OSD_OP_OMAPGETVALSBYKEYS);
    bufferlist bl;
    ::encode(to_get, bl);
    add_replicated_data(bl);
    C_ObjectOperation_decodevals *h =
      new C_ObjectOperation_decodevals(out_set, rval);
    add_single_return(&h->bl, rval, h);
  }

  void omap_cmp(const std::map<std::string, pair<bufferlist, int> > &assertions,
		int *rval) {
    add_op(CEPH_OSD_OP_OMAP_CMP);
    bufferlist bl;
    ::encode(assertions, bl);
    add_replicated_data(bl);
    if (rval) {
      add_single_return(NULL, rval, NULL);
    }
  }

  void omap_get_header(bufferlist *bl, int *rval) {
    add_op(CEPH_OSD_OP_OMAPGETHEADER);
    add_single_return(bl, rval, NULL);
  }

  void omap_set(const map<string, bufferlist> &map) {
    bufferlist bl;
    ::encode(map, bl);
    add_op(CEPH_OSD_OP_OMAPSETVALS);
    add_replicated_data(bl);
  }

  void omap_set_header(bufferlist &bl) {
    add_op(CEPH_OSD_OP_OMAPSETHEADER);
    add_replicated_data(bl);
  }

  void omap_clear() {
    add_op(CEPH_OSD_OP_OMAPCLEAR);
  }

  void omap_rm_keys(const std::set<std::string> &to_remove) {
    bufferlist bl;
    ::encode(to_remove, bl);
    add_op(CEPH_OSD_OP_OMAPRMKEYS);
    add_replicated_data(bl);
  }

  // object classes
  void call(const char *cname, const char *method, bufferlist &indata) {
    add_op(CEPH_OSD_OP_CALL);
    add_call(cname, method, indata, NULL, NULL, NULL);
  }

  void call(const char *cname, const char *method, bufferlist &indata,
	    bufferlist *outdata, Context *ctx, int *rval) {
    add_op(CEPH_OSD_OP_CALL);
    add_call(cname, method, indata, outdata, ctx, rval);
  }

  // watch/notify
  void watch(uint64_t cookie, uint64_t ver, bool set) {
    bufferlist inbl;
    add_op(CEPH_OSD_OP_WATCH);
    add_watch(cookie, ver, (set ? 1 : 0), inbl);
  }

  void notify(uint64_t cookie, bufferlist& inbl) {
    add_op(CEPH_OSD_OP_NOTIFY);
    add_watch(cookie, 0, 1, inbl);
  }

  void notify_ack(uint64_t notify_id, uint64_t ver, uint64_t cookie) {
    bufferlist bl;
    ::encode(notify_id, bl);
    ::encode(cookie, bl);
    add_op(CEPH_OSD_OP_NOTIFY_ACK);
    add_watch(notify_id, ver, 0, bl);
  }

  void list_watchers(list<obj_watch_t> *out, int *rval) {
    add_op(CEPH_OSD_OP_LIST_WATCHERS);
    if (rval || out) {
      C_ObjectOperation_decodewatchers *h =
	new C_ObjectOperation_decodewatchers(out, rval);
      add_single_return(&h->bl, rval, h);
    }
  }

  void assert_version(uint64_t ver) {
    add_op(CEPH_OSD_OP_ASSERT_VER);
    add_version(ver);
  }

  void assert_src_version(const object_t& srcoid, uint64_t ver) {
    bufferlist bl;
    add_op(CEPH_OSD_OP_ASSERT_SRC_VERSION);
    add_watch(0, ver, 0, bl);
    add_oid(srcoid);
  }

  void cmpxattr(const char *name, const bufferlist& val,
		int op, int mode) {
    add_op(CEPH_OSD_OP_CMPXATTR);
    add_xattr_cmp(name, op, mode, val);
  }

  void src_cmpxattr(const object_t& srcoid,
		    const char *name, const bufferlist& val,
		    int op, int mode) {
    add_op(CEPH_OSD_OP_SRC_CMPXATTR);
    add_xattr_cmp(name, op, mode, val);
    add_oid(srcoid);
  }

  void set_alloc_hint(uint64_t expected_object_size,
		      uint64_t expected_write_size ) {
    add_op(CEPH_OSD_OP_SETALLOCHINT);
    add_alloc_hint(expected_object_size, expected_write_size);

    // CEPH_OSD_OP_SETALLOCHINT op is advisory and therefore deemed
    // not worth a feature bit.	 Set FAILOK per-op flag to make
    // sure older osds don't trip over an unsupported opcode.
    set_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
  }

  virtual void realize(
    const object_t& oid,
    const std::function<void(hobject_t&&, vector<OSDOp>&&)>& f) = 0;
};

#endif // !CEPH_OSDC_OBJECTOPERATION_H
