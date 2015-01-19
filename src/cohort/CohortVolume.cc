// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#include <atomic>
#include <cstring>
#include <cstdio>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <dlfcn.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "osd/OSDMap.h"
#include "CohortVolume.h"
#include "osdc/Objecter.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "include/str_map.h"

using std::shared_ptr;
using std::to_string;
using std::min;
using std::unique_ptr;

const uint64_t CohortVolume::one_op = 4194304;

typedef void (*place_func)(void*, const uint8_t[16], size_t, const char*,
			   bool(*)(void*, int),
			   bool(*)(void*, int));

VolumeRef CohortVolFactory(bufferlist::iterator& bl, uint8_t v, vol_type t)
{
  CohortVolume *vol = new CohortVolume(t);
  vol->decode_payload(bl, v);
  return VolumeRef(vol);
}

/* Epoch should be the current epoch of the OSDMap. */

int CohortVolume::compile(std::stringstream &ss) const
{
  const char namelate[] = "/tmp/cohortplacerXXXXXX";
  char cfilename[sizeof(namelate) + 10];
  char objfilename[sizeof(namelate) + 10];
  char sofilename[sizeof(namelate) + 10];
  pid_t child;

  strcpy(cfilename, namelate);
  int fd = mkstemp(cfilename);
  if (fd < 0) {
    ss << "Unable to generate a temporary filename.";
    return -errno;
  }
  close(fd);
  unlink(cfilename);

  strcpy(objfilename, cfilename);
  strcpy(sofilename, cfilename);

  strcat(cfilename, ".c");
  strcat(objfilename, ".o");
  strcat(sofilename, ".so");

  const char *cargv[] = {
    [0] = "gcc", [1] = "-std=c11", [2] = "-O3", [3] = "-fPIC",
    [4] = "-c", [5] = cfilename, [6] = "-o", [7] = objfilename,
    [8] = NULL
  };

  const char *largv[] = {
    [0] = "gcc", [1] = "-shared", [2] = "-o",
    [3] = sofilename, [4] = objfilename, [5] = "-lm",
    [6] = "-lc", [7] = NULL
  };

  /* Better error handling, when we figure out what to do on
     error. Also figure out some directory we should be using,
     possibly under /var/lib.  Also come back and deal with
     concurrency.  We don't want to restrict this to a single thread
     but we don't want a placement function jumping through here while
     we're messing with it. */

  if (place_shared) {
    dlclose(place_shared); /* It's not like we can do anything on error. */
    place_shared = NULL;
  }

  place_text.write_file(cfilename);

  child = fork();
  if (!child) {
    execvp("gcc", (char **)cargv);
  } else {
  cretry:
    int status = 0;
    waitpid(child, &status, 0);
    if (!(WIFEXITED(status) || WIFSIGNALED(status))) {
      goto cretry;
    } else if (WIFSIGNALED(status)) {
      ss << "gcc died with signal ";
      return -EDOM;
    } else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
      ss << "gcc returned failing status " << WTERMSIG(status);
      return -EDOM;
    }
  }

  unlink(cfilename);

  child = fork();
  if (!child) {
    execvp("gcc", (char **)largv);
  } else {
  lretry:
    int status = 0;
    waitpid(child, &status, 0);
    if (!(WIFEXITED(status) || WIFSIGNALED(status))) {
      goto lretry;
    } else if (WIFSIGNALED(status)) {
      ss << "gcc died with signal " << WTERMSIG(status);
      return -EDOM;
    } else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
      ss << "gcc returned failing status " << WTERMSIG(status);
      return -EDOM;
    }
  }

  unlink(objfilename);
  place_shared = dlopen(sofilename, RTLD_LAZY | RTLD_GLOBAL);
  if (!place_shared) {
    ss << "failed loading library: " << dlerror();
    return -EDOM;
  }
  //unlink(sofilename);

  for(vector<string>::size_type i = 0;
      i < symbols.size();
      ++i) {
    void *sym = dlsym(place_shared, symbols[i].c_str());
    if (!sym) {
      ss << "failed loading symbol: " << dlerror();
      return -EDOM;
    }
    entry_points.push_back(sym);
  }

  return 0;
}

int CohortVolume::_attach(std::stringstream &ss) const
{
  Mutex::Locker l(lock);
  int r;

  if (attached)
    return 0;

  r = compile(ss);
  if (r < 0) {
    return r;
  }

  // This is sort of a brokenness in how the erasure code interface
  // handles things.
  map<string, string> copy_params(erasure_params);
  copy_params["directory"] = g_conf->osd_erasure_code_directory;
  ceph::ErasureCodePluginRegistry::instance().factory(
    erasure_plugin,
    copy_params,
    &erasure,
    ss);

  if (!erasure) {
    // So we don't leave things hanging around on error
    if (place_shared) {
      dlclose(place_shared);
      place_shared = NULL;
    }
    return -EDOM;
  }

  stripe_unit = erasure->get_chunk_size(suggested_unit *
					erasure->get_data_chunk_count());
  attached = true;
  return 0;
}

void CohortVolume::detach()
{
  Mutex::Locker l(lock);
  if (!attached)
    return;

  erasure.reset();
  if (place_shared) {
    dlclose(place_shared);
    place_shared = NULL;
  }
  stripe_unit = 0;
  attached = false;
}

CohortVolume::~CohortVolume(void)
{
  detach();
}

int CohortVolume::update(const shared_ptr<const Volume>& v)
{
  return 0;
}

uint32_t CohortVolume::num_rules(void)
{
  return entry_points.size();
}

struct placement_context
{
  const OSDMap* map;
  const void* f;
  ssize_t* count;
};

/* Return 'true' if the OSD is marked as 'in' */

static bool test_osd(void *data, int osd)
{
  placement_context *context = (placement_context *)data;
  return context->map->is_in(osd);
}

/* This function adds an OSD to the list returned to the client ONLY
   if the OSD is marked in. */

static bool return_osd(void *data, int osd)
{
  placement_context *context = (placement_context *)data;
  if (context->map->is_up(osd)) {
    (*(std::function<void(int)>*) context->f)(osd);
    return true;
  } else {
    (*(std::function<void(int)>*) context->f)(-1);
    return true;
  }
}


ssize_t CohortVolume::place(const object_t& object,
			    const OSDMap& map,
			    const std::function<void(int)>& f) const
{
  ssize_t count = 0;
  placement_context context = {
    .map = &map,
    .f = (void *) &f,
    .count = &count
  };

  if (!attached) {
    std::stringstream ss;
    int r = _attach(ss);
    if (r < 0)
      return r;
  }


  place_func entry_point = (place_func) entry_points[0];

  entry_point(&context, id.data, object.name.length(), object.name.data(),
	      test_osd, return_osd);

  return count;
}

void CohortVolume::dump(Formatter *f) const
{
  inherited::dump(f);
  f->dump_stream("place_text") << place_text;
  f->dump_stream("symbols") << symbols;
  f->dump_stream("erasure") << erasure;
}

void CohortVolume::decode_payload(bufferlist::iterator& bl, uint8_t v)
{
  inherited::decode_payload(bl, v);

  ::decode(place_text, bl);
  ::decode(symbols, bl);
  entry_points.reserve(symbols.size());
  ::decode(erasure_plugin, bl);
  ::decode(erasure_params, bl);
  ::decode(suggested_unit, bl);
}

void CohortVolume::encode(bufferlist& bl) const
{
  inherited::encode(bl);

  ::encode(place_text, bl);
  ::encode(symbols, bl);
  ::encode(erasure_plugin, bl);
  ::encode(erasure_params, bl);
  ::encode(suggested_unit, bl);
}

static string indent(size_t k)
{
  return string(k / 8, '\t') + string(k % 8, ' ');
}

static void default_placer(uint32_t blocks,
			   bufferlist& text,
			   vector<std::string>& sym)
{
  const string funcname = "placer";
  sym.push_back(funcname);
  text.append(
    "#include <stddef.h>\n"
    "#include <stdbool.h>\n\n"

    "int " + funcname + "(void *ctx, const char* uuid, size_t size, const char* id,\n"
    + indent(sizeof("int ") + funcname.length())
    + "bool(*test)(void*, int), bool(*place)(void*, int))\n"
    "{\n"
    "\tfor(int i = 0; i < " + to_string(blocks) + "; ++i) {\n"
    "\t\tplace(ctx, i);\n"
    "\t}\n"
    "\treturn 0;\n"
    "}\n");
}

VolumeRef CohortVolume::create(const string& name,
			       const int64_t _suggested_unit,
			       const string& erasure_plugin,
			       const string& erasure_paramstring,
			       const string& place_text, const string& sym_str,
			       std::stringstream& ss)

{
  CohortVolume *v = new CohortVolume(CohortVol);
  map<string, string> copy_params;
  std::stringstream es;

  if (!valid_name(name, ss)) {
    goto error;
  }

  v->id = boost::uuids::random_generator()();
  v->name = name;

  if ((place_text.empty() && !sym_str.empty()) ||
      (sym_str.empty() && !place_text.empty())) {
    ss << "If you have symbols you must have place text and vice versa.";
    goto error;
  }

  v->erasure_plugin = erasure_plugin;
  get_str_map(erasure_paramstring, &v->erasure_params);
  v->suggested_unit = _suggested_unit;

  // This stuff is in attach, too, but we need to initialize the
  // erasure code plugin before we can create the default placer.

  copy_params = v->erasure_params;
  copy_params["directory"] = g_conf->osd_erasure_code_directory;
  ceph::ErasureCodePluginRegistry::instance().factory(
    v->erasure_plugin,
    copy_params,
    &v->erasure,
    es);

  if (!v->erasure) {
    ss << es.str(); // Factory writes output even on success, which is
		    // kind of confusing if an error happens later.
    goto error;
  }

  v->stripe_unit = v->erasure->get_chunk_size(
    v->suggested_unit * v->erasure->get_data_chunk_count());
  if (!place_text.empty()) {
    v->place_text.append(place_text);
    boost::algorithm::split(v->symbols, sym_str,
			    boost::algorithm::is_any_of(" \t"));
  } else {
    default_placer(v->erasure->get_chunk_count(), v->place_text, v->symbols);
  }
  if (v->compile(ss) < 0) {
    goto error;
  }

  v->attached = true;
  v->detach();

  return VolumeRef(v);

error:

  delete v;
  return VolumeRef();
}

struct C_GetAttrs : public Context {
  bufferlist bl;
  map<string,bufferlist>& attrset;
  Context *fin;
  C_GetAttrs(map<string, bufferlist>& set, Context *c)
    : attrset(set), fin(c) {}
  void finish(int r) {
    if (r >= 0) {
      bufferlist::iterator p = bl.begin();
      ::decode(attrset, p);
    }
    fin->complete(r);
  }
};

CohortVolume::StripulatedOp::StripulatedOp(
  const CohortVolume& v) : v(v), logical_operations(0)

{
  ops.resize(v.erasure->get_chunk_count());
}

void CohortVolume::StripulatedOp::add_op(const int op)
{
  ++logical_operations;
  for (auto &v : ops)
    v.emplace_back(op);
}

void CohortVolume::StripulatedOp::add_version(const uint64_t ver)
{
  for (auto &v : ops)
    v.back().op.assert_ver.ver = ver;
}

void CohortVolume::StripulatedOp::add_oid(const hobject_t &oid)
{
  for (auto &v : ops)
    v.back().oid = oid;
}

void CohortVolume::StripulatedOp::add_single_return(bufferlist* bl,
						    int* rval,
						    Context *ctx)
{
  ops[0].back().ctx = ctx;
  ops[0].back().out_bl = bl;
  ops[0].back().out_rval = rval;
}

void CohortVolume::StripulatedOp::add_replicated_data(const bufferlist& bl)
{
  budget += bl.length() * ops.size();
  for (auto &v : ops) {
    v.back().indata.append(bl);
  }
}

void CohortVolume::StripulatedOp::add_striped_data(const uint64_t off,
						   const bufferlist& in)
{
  bufferlist bl(in);
  const uint32_t stripe_width = v.stripe_unit *
    v.erasure->get_data_chunk_count();
  set<int> want;
  for (unsigned i = 0; i < v.erasure->get_chunk_count(); ++i) {
    want.insert(i);
  }
  budget += bl.length();
  assert(bl.length());
  assert(off % stripe_width == 0);

  // Pad
  if (bl.length() % stripe_width)
    bl.append_zero(stripe_width - ((off + bl.length()) % stripe_width));

  for (uint64_t i = 0; i < bl.length(); i += stripe_width) {
    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(bl, i, stripe_width);
    int r = v.erasure->encode(want, buf, &encoded);
    assert(r == 0);
    for (auto &p : encoded) {
      assert(p.second.length() == v.stripe_unit);
      ops[p.first].back().indata.claim_append(p.second);
    }
  }

  for (auto &o : ops) {
    o.back().op.extent.offset = off / v.erasure->get_data_chunk_count();
    o.back().op.extent.length = bl.length()
      / v.erasure->get_data_chunk_count();
  }
}

void CohortVolume::StripulatedOp::add_striped_range(const uint64_t off,
						    const uint64_t len)
{
  const uint32_t stripe_width = v.stripe_unit *
    v.erasure->get_data_chunk_count();
  uint64_t actual_len = len;
  assert(off % stripe_width == 0);
  if (len % stripe_width)
    actual_len += stripe_width - ((off + len % stripe_width));

  for (auto &o : ops) {
    o.back().op.extent.offset = off / v.erasure->get_data_chunk_count();
    o.back().op.extent.length = actual_len
      / v.erasure->get_data_chunk_count();
  }
}

void CohortVolume::StripulatedOp::add_truncate(const uint64_t truncate_size,
					       const uint32_t truncate_seq)
{
  // The truncate_size is the truncate_size of the entire OBJECT, not
  // of any individual stride. This seems the only sane way to go
  // about things when we have padding to length and the like.
  for (auto &v : ops) {
    v.back().op.extent.truncate_size = truncate_size;
    v.back().op.extent.truncate_seq = truncate_seq;
  }
}

struct C_MultiRead : public Context {
  const CohortVolume& v;
  map<int, bufferlist> resultbl;
  bufferlist *bl;
  int* rval;
  Context *onfinish;
  C_MultiRead(const CohortVolume& v, map<int, bufferlist>& _resultbl,
	      bufferlist *bl, int* rval, Context *onfinish) :
    v(v), bl(bl), rval(rval), onfinish(onfinish) {
    resultbl.swap(_resultbl);
  }
  void finish(int r) {
    uint64_t stride_size
      = std::max_element(resultbl.begin(), resultbl.end(),
			 [](pair<int, bufferlist> x, pair<int, bufferlist> y) {
			   return x.second.length() < y.second.length();
			 })->second.length();

    for (uint64_t i = 0; i < stride_size; i += v.stripe_unit) {
      map<int, bufferlist> chunks;
      for (auto &p : resultbl) {
	if (chunks.size() == v.erasure->get_data_chunk_count()) {
	  break;
	}
	if (p.second.length() < i + v.stripe_unit) {
	  continue;
	}
	chunks[p.first].substr_of(p.second, i, v.stripe_unit);
      }
      if (chunks.size() < v.erasure->get_data_chunk_count()) {
	if (rval)
	  *rval = r;
	if (onfinish)
	  onfinish->complete(r);
	return;
      }
      bufferlist stripebuf;
      int s = v.erasure->decode_concat(chunks, &stripebuf);
      if (s != 0) {
	r = s;
	goto done;
      }
      bl->claim_append(stripebuf);
    }

    r = bl->length();

  done:

    if (rval) {
      *rval = bl->length();
    }

    if (onfinish) {
      onfinish->complete(bl->length());
    }
  }
};

void CohortVolume::StripulatedOp::add_read_ctx(const uint64_t off,
					       const uint64_t len,
					       bufferlist *bl,
					       int *rval, Context *ctx)
{
  budget += len;
  const uint32_t stripe_width = v.stripe_unit *
    v.erasure->get_data_chunk_count();
  uint64_t actual_len = len;
  assert(off % stripe_width == 0);
  if (len % stripe_width)
    actual_len += stripe_width - ((off + len % stripe_width));

  map<int, bufferlist> resultbl;
  C_GatherBuilder gather;
  for (uint32_t stride = 0; stride < v.erasure->get_chunk_count(); ++stride) {
    ops[stride].back().op.extent.offset = off
      / v.erasure->get_data_chunk_count();
    ops[stride].back().op.extent.length = actual_len
      / v.erasure->get_data_chunk_count();
    ops[stride].back().ctx = gather.new_sub();
    auto p = resultbl.emplace(stride, len / stripe_width);
    ops[stride].back().out_bl = &(p.first->second);
  }
  gather.set_finisher(new C_MultiRead(v, resultbl, bl, rval, ctx));
  gather.activate();
}

void CohortVolume::StripulatedOp::add_sparse_read_ctx(
  uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
  bufferlist *data_bl, int *rval, Context *ctx)
{
  puts("Sparse read is currently not supported.");
  abort();
}


void CohortVolume::StripulatedOp::add_xattr(const string &name,
					    const bufferlist& data)
{
  // At least for the moment, just send the same attribute on every stride
  budget += (name.length() + data.length()) * ops.size();
  for (auto &v : ops) {
    OSDOp &op = v.back();
    op.op.xattr.name_len = name.length();
    op.op.xattr.value_len = data.length();
    op.indata.append(name);
    op.indata.append(data);
  }
}

void CohortVolume::StripulatedOp::add_xattr(const string &name,
					    bufferlist* data)
{
  // At least for the moment, just read the same attribute on every stride
  budget += name.length();
  OSDOp &op = ops[0].back();
  op.op.xattr.name_len = name.length();
  op.indata.append(name);
  op.out_bl = data;
}

void CohortVolume::StripulatedOp::add_xattr_cmp(const string &name,
						const uint8_t cmp_op,
						const uint8_t cmp_mode,
						const bufferlist& data)
{
  // Only do anything on the first For client-side recovery we
  // probably want to do it on ALL and just have the completion find
  // the first success and use it.
  budget += (name.length() + data.length());
  OSDOp &op = ops[0].back();
  op.op.xattr.name_len = name.length();
  op.op.xattr.value_len = data.length();
  op.op.xattr.cmp_op = cmp_op;
  op.op.xattr.cmp_mode = cmp_mode;
  op.indata.append(name);
  op.indata.append(data);

  // Do nothing in all the other ops.
  for (auto i = (ops.begin() + 1); i != ops.end(); ++i)
    i->back().op.op = 0;
}

void CohortVolume::StripulatedOp::add_call(const string &cname,
					   const string &method,
					   const bufferlist &indata,
					   bufferlist *const outbl,
					   Context *const ctx,
					   int *const rval)
{
  // Calls are hard.
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.cls.class_len = cname.length();
    osd_op.op.cls.method_len = method.length();
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname.data(), osd_op.op.cls.class_len);
    osd_op.indata.append(method.data(), osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
    osd_op.ctx = ctx;
    osd_op.out_bl = outbl;
    osd_op.out_rval = rval;
  }
}

void CohortVolume::StripulatedOp::add_watch(const uint64_t cookie,
						   const uint64_t ver,
						   const uint8_t flag,
						   const bufferlist& inbl)
{
  // Watches might be hard
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.watch.cookie = cookie;
    osd_op.op.watch.ver = ver;
    osd_op.op.watch.flag = flag;
    osd_op.indata.append(inbl);
  }
}

void CohortVolume::StripulatedOp::add_alloc_hint(
  const uint64_t expected_object_size, const uint64_t expected_write_size)
{
  const uint32_t stripe_width = v.stripe_unit *
    v.erasure->get_data_chunk_count();
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.alloc_hint.expected_object_size
      = expected_object_size / stripe_width;
    osd_op.op.alloc_hint.expected_write_size
      = expected_write_size / stripe_width;
  }
}

void CohortVolume::StripulatedOp::set_op_flags(const uint32_t flags)
{
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.flags = osd_op.op.flags | flags;
  }
}

void CohortVolume::StripulatedOp::clear_op_flags(const uint32_t flags)
{
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.flags = osd_op.op.flags & ~flags;
  }
}

struct C_MultiStat : public Context {
  const CohortVolume &v;
  vector<bufferlist> bls;
  uint64_t *s;
  utime_t *m;
  int *rval;
  Context *ctx;

  C_MultiStat(const CohortVolume &v, uint64_t *_s, utime_t *_m, int *_rval,
	      Context *_ctx)
    : v(v), bls(v.erasure->get_chunk_count()), s(_s), m(_m), rval(_rval),
      ctx(_ctx) { }

  void finish(int r) {
    if (rval)
      *rval = r;

    if (r >= 0) {
     for (auto &b : bls) {
	bufferlist::iterator p = b.begin();
	try {
	  uint64_t size;
	  utime_t mtime;
	  ::decode(size, p);
	  ::decode(mtime, p);
	  if (s)
	    *s += size;
	  if (m)
	    *m = std::max(mtime, *m);
	} catch (ceph::buffer::error& e) {
	  if (rval)
	    *rval = -EDOM;
	}
      }
    }
    if (ctx)
      ctx->complete(r);
  }
};

void CohortVolume::StripulatedOp::add_stat_ctx(uint64_t *s, utime_t *m,
					       int *rval, Context *ctx)
{
  C_GatherBuilder gather;
  C_MultiStat *f = new C_MultiStat(v, s, m, rval, ctx);
  for (uint32_t stride = 0; stride < v.erasure->get_chunk_count(); ++stride) {
    OSDOp &osd_op = ops[stride].back();
    osd_op.out_bl = &(f->bls[stride]);
    osd_op.ctx = gather.new_sub();
  }
  gather.set_finisher(f);
  gather.activate();
}

unique_ptr<ObjOp> CohortVolume::StripulatedOp::clone()
{
  return unique_ptr<ObjOp>(new StripulatedOp(*this));
}


unique_ptr<ObjOp> CohortVolume::op() const
{
  if (!attached) {
    std::stringstream ss;
    if (_attach(ss) < 0)
      return nullptr;
  }
  return unique_ptr<ObjOp>(new StripulatedOp(*this));
}

void CohortVolume::StripulatedOp::realize(
  const object_t& oid,
  const std::function<void(hobject_t&&, vector<OSDOp>&&)>& f)
{
  for(size_t i = 0; i < ops.size(); ++i) {
    f(hobject_t(oid,
		i < v.erasure->get_data_chunk_count() ? DATA : ECC,
		i),
      std::move(ops[i]));
  }
}
