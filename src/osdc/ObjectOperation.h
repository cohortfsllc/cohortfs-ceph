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
#include "osd/osd_types.h"
#include "common/oid.h"

namespace rados {

  typedef cohort::function<void(std::error_code, bufferlist&)> read_callback;
  typedef cohort::function<void(std::error_code, uint64_t s,
				ceph::real_time m)> stat_callback;
  typedef cohort::function<void(std::error_code, std::string&)> key_callback;
  typedef cohort::function<void(std::error_code, std::string&,
				bufferlist&)> keyval_callback;

  namespace detail {
    struct CB_DecodeKeys {
      key_callback f;
      CB_DecodeKeys(key_callback&& _f) : f(_f) { }
      void operator()(std::error_code err, bufferlist& bl) {
	if (!err) {
	  bufferlist::iterator p = bl.begin();
	  try {
	    uint32_t n;
	    ::decode(n, p);
	    for (; n > 0; --n) {
	      string s;
	      ::decode(s, p);
	      f(err, s);
	    }
	  }
	  catch (std::system_error& e) {
	    string s;
	    f(e.code(), s);
	  }
	} else {
	  string s;
	  f(err, s);
	}
      }
    };

    struct CB_DecodePairs {
      keyval_callback f;
      CB_DecodePairs(keyval_callback&& _f) : f(_f) { }
      void operator()(std::error_code err, bufferlist& bl) {
	if (!err) {
	  bufferlist::iterator p = bl.begin();
	  try {
	    uint32_t n;
	    ::decode(n, p);
	    for (; n > 0; --n) {
	      string s;
	      bufferlist bl;
	      ::decode(s, p);
	      ::decode(bl, p);
	      f(std::error_code(), s, bl);
	    }
	  } catch (std::system_error& e) {
	    string s;
	    bufferlist bl;
	    f(e.code(), s, bl);
	  }
	} else {
	  string s;
	  bufferlist bl;
	  f(err, s, bl);
	}
      }
    };

    struct CB_FillMap {
      map<string, bufferlist>& out;
      CB_FillMap(map<string, bufferlist>& _out) : out(_out) { }
      void operator()(std::error_code err, bufferlist& bl) {
	if (!err) {
	  bufferlist::iterator p = bl.begin();
	  uint32_t n;
	  ::decode(n, p);
	  for (; n > 0; --n) {
	    string s;
	    bufferlist bl;
	    ::decode(s, p);
	    ::decode(bl, p);
	    out.insert(std::make_pair(s, bl));
	  }
	}
      }
    };
  };

  class ObjOp {
  protected:
    uint64_t budget; // Should wrap calls for budget.

    ObjOp() : budget(0), flags(0), priority(0) {}

  public:
    int flags;
    int priority;

    virtual ~ObjOp() {
    }

    virtual std::unique_ptr<ObjOp> clone() = 0;

    uint64_t get_budget() {
      return budget;
    }
    virtual size_t size() = 0;
    virtual size_t width() = 0;
    virtual void add_op(const int op) = 0;
    virtual void add_version(const uint64_t ver) = 0;
    virtual void add_obj(const oid_t &oid) = 0;
    virtual void add_single_return(OSDOp::opfun_t&& f) = 0;
    /* Add metadata (or commands) to an op */
    virtual void add_metadata(const bufferlist& bl) = 0;
    /* Add a metadata offset/length */
    virtual void add_metadata_range(const uint64_t off,
				    const uint64_t len) = 0;
    /* Add data to an op */
    virtual void add_data(const uint64_t off, const bufferlist& bl) = 0;
    /* Add a data offset/length */
    virtual void add_data_range(const uint64_t off, const uint64_t len) = 0;

    virtual void add_xattr(const string &name) = 0;
    virtual void add_xattr(const string &name, const bufferlist &data) = 0;
    virtual void add_xattr_cmp(const string &name, uint8_t cmp_op,
			       const uint8_t cmp_mode,
			       const bufferlist& data) = 0;
    virtual void add_call(const string &cname, const string &method,
			  const bufferlist &indata,
			  read_callback&& cb) = 0;
    virtual void add_watch(const uint64_t cookie, const uint64_t ver,
			   const uint8_t flag, const bufferlist& inbl) = 0;
    virtual void add_alloc_hint(const uint64_t expected_object_size,
				const uint64_t expected_write_size) = 0;
    virtual void add_truncate(const uint64_t truncate_size,
			      const uint32_t truncate_seq) = 0;
    virtual void set_op_flags(const uint32_t flags) = 0;
    virtual void clear_op_flags(const uint32_t flags) = 0;
    virtual void add_stat_cb(stat_callback&& f) = 0;

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
      add_metadata(bl);
    }

    void stat(stat_callback&& f) {
      add_op(CEPH_OSD_OP_STAT);
      add_stat_cb(std::move(f));
    }

    // object data
    void read(uint64_t off, uint64_t len,
	      read_callback&& f) {
      read(off, len, 0, 0, std::move(f));
    };

    void read(uint64_t off, uint64_t len,
	      bufferlist& bl) {
      read(off, len,
	   [&bl](std::error_code err, bufferlist& _bl) {
	     if (!err)
	       bl.claim_append(bl);
	   });
    }

    virtual void read(uint64_t off, uint64_t len, uint64_t truncate_size,
		      uint32_t truncate_seq, read_callback&& f) = 0;
    virtual void read_full(read_callback&& f) = 0;

    void sparse_read(uint64_t off, uint64_t len,
		     std::map<uint64_t,uint64_t> *m,
		     bufferlist *bl, int *rval) {
      add_op(CEPH_OSD_OP_SPARSE_READ);
      //add_sparse_read_ctx(off, len, m, bl, rval, NULL);
    }

    void write(uint64_t off, const bufferlist& bl,
	       uint64_t truncate_size = 0,
	       uint32_t truncate_seq = 0) {
      add_op(CEPH_OSD_OP_WRITE);
      add_data(off, bl);
      add_truncate(truncate_size, truncate_seq);
    }
    void write(uint64_t off, uint64_t len, const bufferlist& bl,
	       uint64_t truncate_size = 0,
	       uint32_t truncate_seq = 0) {
      add_op(CEPH_OSD_OP_WRITE);
      bufferlist buf;
      buf.substr_of(bl, 0, len);
      add_data(off, buf);
      add_truncate(truncate_size, truncate_seq);
    }
    void write_full(const bufferlist& bl) {
      add_op(CEPH_OSD_OP_WRITEFULL);
      add_data(0, bl);
    }
    void append(const bufferlist& bl) {
      add_op(CEPH_OSD_OP_APPEND);
      add_data(0, bl);
    }
    void append(const uint64_t len, const bufferlist& bl) {
      add_op(CEPH_OSD_OP_APPEND);
      bufferlist buf;
      buf.substr_of(buf, 0, len);
      add_data(0,  bl);
    }
    void zero(uint64_t off, uint64_t len) {
      add_op(CEPH_OSD_OP_ZERO);
      add_data_range(off, len);
    }
    void truncate(uint64_t len) {
      add_op(CEPH_OSD_OP_TRUNCATE);
      add_data_range(0, len);
    }
    void truncate(uint64_t len, uint32_t truncate_seq) {
      add_op(CEPH_OSD_OP_TRUNCATE);
      add_data_range(0, len);
      add_truncate(len, truncate_seq);
    }
    void remove() {
      add_op(CEPH_OSD_OP_DELETE);
    }

    // object attrs
    void getxattr(const string& name,
		  OSDOp::opfun_t&& f) {
      add_op(CEPH_OSD_OP_GETXATTR);
      add_xattr(name);
      add_single_return(std::move(f));
    }

    void getxattrs(keyval_callback&& f) {
      add_op(CEPH_OSD_OP_GETXATTRS);
      add_single_return(detail::CB_DecodePairs(std::move(f)));
    }
    void getxattrs(map<string, bufferlist>& out) {
      add_op(CEPH_OSD_OP_GETXATTRS);
      add_single_return(detail::CB_FillMap(out));
    }
    void setxattr(const string& name, const bufferlist& bl) {
      add_op(CEPH_OSD_OP_SETXATTR);
      add_xattr(name, bl);
    }
    void setxattr(const string& name, const string& s) {
      bufferlist bl;
      bl.append(s);
      setxattr(name, bl);
    }
    void cmpxattr(const string& name, uint8_t cmp_op, uint8_t cmp_mode,
		  const bufferlist& bl) {
      add_op(CEPH_OSD_OP_CMPXATTR);
      add_xattr_cmp(name, cmp_op, cmp_mode, bl);
    }
    void rmxattr(const string& name) {
      add_op(CEPH_OSD_OP_RMXATTR);
      add_xattr(name);
    }

    template<typename InputIterator>
    void setxattrs(InputIterator begin, InputIterator end) {
      bufferlist bl;
      static_assert(
	std::is_same<
	typename std::iterator_traits<InputIterator>::value_type,
	typename std::pair<std::string, bufferlist>>(),
	"Input must be (string, bufferlist) pairs.");
      ::encode(std::distance(begin, end));
      for (;begin != end; ++begin)
	::encode(*begin, bl);
      add_op(CEPH_OSD_OP_RESETXATTRS);
      add_xattr(NULL, bl);
    }
    void resetxattrs(const string& prefix, map<string, bufferlist>& attrs) {
      bufferlist bl;
      ::encode(attrs, bl);
      add_op(CEPH_OSD_OP_RESETXATTRS);
      add_xattr(prefix, bl);
    }


    // objectmap
    void omap_get_keys(const string &start_after,
		       uint64_t max_to_get,
		       key_callback&& f) {
      add_op(CEPH_OSD_OP_OMAPGETKEYS);
      bufferlist bl;
      ::encode(start_after, bl);
      ::encode(max_to_get, bl);
      add_metadata(bl);
      add_single_return(detail::CB_DecodeKeys(std::move(f)));
    }

    void omap_get_vals(const string &start_after,
		       const string &filter_prefix,
		       uint64_t max_to_get,
		       map<string, bufferlist>& out) {
      add_op(CEPH_OSD_OP_OMAPGETVALS);
      bufferlist bl;
      ::encode(start_after, bl);
      ::encode(max_to_get, bl);
      ::encode(filter_prefix, bl);
      add_metadata(bl);
      add_single_return(detail::CB_FillMap(out));
    }

    void omap_get_vals(const string &start_after,
		       const string &filter_prefix,
		       uint64_t max_to_get, keyval_callback&& f) {
      add_op(CEPH_OSD_OP_OMAPGETVALS);
      bufferlist bl;
      ::encode(start_after, bl);
      ::encode(max_to_get, bl);
      ::encode(filter_prefix, bl);
      add_metadata(bl);
      add_single_return(detail::CB_DecodePairs(std::move(f)));
    }

    template<typename InputIterator>
    void omap_get_vals_by_keys(InputIterator begin, InputIterator end,
			       keyval_callback&& f) {
      static_assert(std::is_same<typename std::iterator_traits<
		    InputIterator>::value_type,
		    typename std::string>(),
		    "Keys must be strings.");
      add_op(CEPH_OSD_OP_OMAPGETVALSBYKEYS);
      bufferlist bl;
      ::encode(std::distance(begin, end), bl);
      for (;begin != end; ++begin)
	::encode(*begin, bl);
      add_metadata(bl);
      add_single_return(detail::CB_DecodePairs(std::move(f)));
    }

    void omap_cmp(const std::map<std::string,
		  pair<bufferlist, int>> &assertions,
		  int *rval) {
      add_op(CEPH_OSD_OP_OMAP_CMP);
      bufferlist bl;
      ::encode(assertions, bl);
      add_metadata(bl);
      if (rval) {
	// add_single_return(NULL, rval, NULL);
      }
    }

    void omap_get_header(read_callback&& cb) {
      add_op(CEPH_OSD_OP_OMAPGETHEADER);
      add_single_return(std::move(cb));
    }

    void omap_get_header(bufferlist& out) {
      add_op(CEPH_OSD_OP_OMAPGETHEADER);
      add_single_return(
	[&out](std::error_code err, bufferlist& bl) {
	  if (!err)
	    out.claim_append(bl);
	});
    }

    template<typename InputIterator>
    void omap_get_vals_by_keys(InputIterator begin, InputIterator end) {
      bufferlist bl;
      static_assert(std::is_same<typename std::iterator_traits<
		    InputIterator>::value_type,
		    typename std::string>(),
		    "Input must be strings pairs.");
      ::encode(std::distance(begin, end));
      for (;begin != end; ++begin)
	::encode(*begin, bl);
      add_op(CEPH_OSD_OP_OMAPSETVALS);
      add_metadata(bl);
    }

    void omap_set_header(const bufferlist &bl) {
      add_op(CEPH_OSD_OP_OMAPSETHEADER);
      add_metadata(bl);
    }

    template<typename InputIterator>
    void omap_set(InputIterator begin, InputIterator end) {
      static_assert(
	std::is_convertible<
	typename std::iterator_traits<InputIterator>::value_type,
	typename std::pair<std::string, bufferlist>>(),
	"Input must be (string, bufferlist) pairs.");
      bufferlist bl;
      ::encode(std::distance(begin, end), bl);
      for (;begin != end; ++begin)
	::encode(*begin, bl);
      add_op(CEPH_OSD_OP_OMAPSETVALS);
      add_metadata(bl);
    }

    void omap_clear() {
      add_op(CEPH_OSD_OP_OMAPCLEAR);
    }

    template<typename InputIterator>
    void omap_rm_keys(InputIterator begin, InputIterator end) {
      static_assert(
	std::is_convertible<
	typename std::iterator_traits<InputIterator>::value_type,
	typename std::string>(),
	"Input must be strings.");
      bufferlist bl;
      ::encode(std::distance(begin, end), bl);
      for (;begin != end; ++begin)
	::encode(*begin, bl);
      add_op(CEPH_OSD_OP_OMAPRMKEYS);
      add_metadata(bl);
    }

    void call(const string& cname, const string& method, bufferlist &indata,
	      read_callback&& cb = nullptr) {
      add_op(CEPH_OSD_OP_CALL);
      add_call(cname, method, indata, std::move(cb));
    }


    void assert_version(uint64_t ver) {
      add_op(CEPH_OSD_OP_ASSERT_VER);
      add_version(ver);
    }

    void assert_src_version(const oid_t& srcoid, uint64_t ver) {
      bufferlist bl;
      add_op(CEPH_OSD_OP_ASSERT_SRC_VERSION);
      add_watch(0, ver, 0, bl);
      add_obj(srcoid);
    }

    void cmpxattr(const string& name, const bufferlist& val,
		  int op, int mode) {
      add_op(CEPH_OSD_OP_CMPXATTR);
      add_xattr_cmp(name, op, mode, val);
    }

    void src_cmpxattr(const oid_t& srcoid,
		      const string& name, const bufferlist& val,
		      int op, int mode) {
      add_op(CEPH_OSD_OP_SRC_CMPXATTR);
      add_xattr_cmp(name, op, mode, val);
      add_obj(srcoid);
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
      const oid_t& oid,
      const cohort::function<void(oid_t&&, vector<OSDOp>&&)>& f) = 0;
  };

  typedef std::unique_ptr<ObjOp> ObjectOperation;
  typedef const std::unique_ptr<ObjOp>& ObjOpUse;
  typedef std::unique_ptr<ObjOp>& ObjOpOwn;

};

#endif // !CEPH_OSDC_OBJECTOPERATION_H
