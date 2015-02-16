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
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <dlfcn.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "osd/OSDMap.h"
#include "ErasureCPlacer.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "include/str_map.h"

using std::shared_ptr;
using std::to_string;
using std::min;
using std::unique_ptr;

const uint64_t ErasureCPlacer::one_op = 4194304;

typedef void (*place_func)(void*, const uint8_t[16], size_t, const char*,
			  bool(*)(void*, int),
			  bool(*)(void*, int));

PlacerRef ErasureCPlacerFactory(bufferlist::iterator& bl, uint8_t v)
{
  ErasureCPlacer *placer = new ErasureCPlacer();
  placer->decode_payload(bl, v);
  return PlacerRef(placer);
}

/* Epoch should be the current epoch of the OSDMap. */

int ErasureCPlacer::compile(CephContext* cct) const
{
  const char namelate[] = "/tmp/cohortplacerXXXXXX";
  char cfilename[sizeof(namelate) + 10];
  char objfilename[sizeof(namelate) + 10];
  char sofilename[sizeof(namelate) + 10];
  pid_t child;

  strcpy(cfilename, namelate);
  int fd = mkstemp(cfilename);
  if (fd < 0) {
    lsubdout(cct, volume, -1) << "Unable to generate a temporary filename."
			      << dendl;
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
      lsubdout(cct, volume, -1)
	<< "gcc died with signal " << WTERMSIG(status)<< dendl;
      return -EDOM;
    } else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
      lsubdout(cct, volume, -1)
	<< "gcc returned failing status " << WEXITSTATUS(status) << dendl;
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
      lsubdout(cct, volume, -1)
	<< "gcc died with signal " << WTERMSIG(status) << dendl;
      return -EDOM;
    } else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
      lsubdout(cct, volume, -1)
	<< "gcc returned failing status " << WEXITSTATUS(status) << dendl;
      return -EDOM;
    }
  }

  unlink(objfilename);
  place_shared = dlopen(sofilename, RTLD_LAZY | RTLD_GLOBAL);
  if (!place_shared) {
    lsubdout(cct, volume, -1)
      << "failed loading library: " << dlerror() << dendl;
    return -EDOM;
  }
  unlink(sofilename);

  for(vector<string>::size_type i = 0;
      i < symbols.size();
      ++i) {
    void *sym = dlsym(place_shared, symbols[i].c_str());
    if (!sym) {
      lsubdout(cct, volume, -1)
	<< "failed loading symbol: " << dlerror() << dendl;
      return -EDOM;
    }
    entry_points.push_back(sym);
  }

  return 0;
}

int ErasureCPlacer::_attach(CephContext* cct, std::stringstream* ss) const
{
  Mutex::Locker l(lock);
  int r;
  stringstream rs;

  if (attached)
    return 0;

  r = compile(cct);
  if (r < 0) {
    return r;
  }

  // This is sort of a brokenness in how the erasure code interface
  // handles things.
  map<string, string> copy_params(erasure_params);
  copy_params["directory"] = cct->_conf->osd_erasure_code_directory;
  ceph::ErasureCodePluginRegistry::instance().factory(
      cct,
      erasure_plugin,
      copy_params,
      &erasure,
      rs);

  if (ss) {
    *ss << rs.str();
  }

  if (!erasure) {
    // So we don't leave things hanging around on error
    if (place_shared) {
      dlclose(place_shared);
      place_shared = NULL;
      lsubdout(cct, volume, -1) << rs.str() << dendl;

    }
    return -EDOM;
  }

  stripe_unit = erasure->get_chunk_size(suggested_unit *
      erasure->get_data_chunk_count());
  attached = true;
  return 0;
}

void ErasureCPlacer::detach()
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

ErasureCPlacer::~ErasureCPlacer(void)
{
  detach();
}

int ErasureCPlacer::update(const shared_ptr<const Placer>& pl)
{
  return 0;
}

uint32_t ErasureCPlacer::num_rules(void)
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

  return false;
}


ssize_t ErasureCPlacer::place(const object_t& object,
			     const OSDMap& map,
			     const std::function<void(int)>& f) const
{
  ssize_t count = 0;
  placement_context context = {
    .map = &map,
    .f = (void *) &f,
    .count = &count
  };

  assert(attached);

  place_func entry_point = (place_func) entry_points[0];

  entry_point(&context, id.data, object.name.length(), object.name.data(),
	      test_osd, return_osd);

  return count;
}

void ErasureCPlacer::dump(Formatter *f) const
{
  inherited::dump(f);
  f->dump_stream("place_text") << place_text.convert_to_string();
  f->dump_stream("symbols") << symbols;
  f->dump_stream("erasure") << erasure;
}

void ErasureCPlacer::decode_payload(bufferlist::iterator& bl, uint8_t v)
{
  inherited::decode_payload(bl, v);

  ::decode(place_text, bl);
  ::decode(symbols, bl);
  entry_points.reserve(symbols.size());
  ::decode(erasure_plugin, bl);
  ::decode(erasure_params, bl);
  ::decode(suggested_unit, bl);
}

void ErasureCPlacer::encode(bufferlist& bl) const
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

PlacerRef ErasureCPlacer::create(CephContext *cct,
				 const string& name,
				 const int64_t _suggested_unit,
				 const string& erasure_plugin,
				 const string& erasure_paramstring,
				 const string& place_text, const string& sym_str,
				 std::stringstream& ss)
{
  ErasureCPlacer *v = new ErasureCPlacer();
  map<string, string> copy_params;
  std::stringstream es;

  if (!valid_name(name, ss)) {
    goto error;
  }

  if ((place_text.empty() && !sym_str.empty()) ||
      (sym_str.empty() && !place_text.empty())) {
    ss << "If you have symbols you must have place text and vice versa.";
    goto error;
  }

  v->id = boost::uuids::random_generator()();
  v->name = name;
  v->erasure_plugin = erasure_plugin;
  get_str_map(erasure_paramstring, &v->erasure_params);
  v->suggested_unit = _suggested_unit;

  // This stuff is in attach, too, but we need to initialize the
  // erasure code plugin before we can create the default placer.

  copy_params = v->erasure_params;
  copy_params["directory"] = cct->_conf->osd_erasure_code_directory;
  ceph::ErasureCodePluginRegistry::instance().factory(
      cct,
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
  if (v->compile(cct) < 0) {
    goto error;
  }

  v->attached = true;
  v->detach();

  return PlacerRef(v);

error:

  delete v;
  return PlacerRef();
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

void ErasureCPlacer::stride_extent(const uint64_t off, const uint64_t len,
		   const size_t stride, uint64_t &strideoff,
		   uint64_t &stridelen) {
  size_t first = stride_idx(off);
  size_t span = extent_units(off, len);
  if ((len == 0) ||
      ((span < erasure->get_data_chunk_count()) &&
       (stride > ((first + span - 1) % erasure->get_data_chunk_count())))) {
    strideoff = 0;
    stridelen = 0;
    return;
  }
  uint64_t last_byte = off + len - 1;
  size_t last = stride_idx(last_byte);
  uint64_t stride_last_byte;

  if (first == stride) {
    strideoff = stride_offset(off);
  } else {
    strideoff = stride_offset(
			      (off / stripe_unit) * stripe_unit +
			      stripe_unit *
			      (stride > first ? stride - first : erasure->get_data_chunk_count() -
			       first + stride));
  }

  if (len == 0) {
    stridelen = 0;
    return;
  }

  if (last == stride) {
    stride_last_byte = stride_offset(last_byte);
  } else {
    uint64_t filled_byte = (last_byte + 1) % stripe_unit == 0 ?
      last_byte : (last_byte / stripe_unit + 1) * stripe_unit - 1;
    stride_last_byte = stride_offset(
				     filled_byte - stripe_unit *
				     (stride < last ? last - stride : last +
				      erasure->get_data_chunk_count() - stride));
  }
  stridelen = stride_last_byte + 1 - strideoff;
}

void ErasureCPlacer::make_strides(const object_t& oid,
				  uint64_t offset, uint64_t len,
				  uint64_t truncate_size, uint32_t truncate_seq,
				  vector<StrideExtent>& strides)
{
#if 0
  buffer::list::iterator i(&blin);
  size_t stride;
  uint64_t thislen;
  len = min(len, (uint64_t)blin.length());

  for (stride = 0; stride < erasure->get_data_chunk_count(); ++stride) {
    stride_extent(offset, len, stride, strides[stride].offset,
		  strides[stride].length);
    assert(strides[stride].length <= one_op);
  }

  stride = stride_idx(offset);
  /* Special case on incomplete first block */
  if (offset % stripe_unit != 0) {
    thislen = min(len, (uint64_t) (stripe_unit - offset % stripe_unit));
  } else {
    thislen = min(len, (uint64_t)stripe_unit);
  }
  i.copy(thislen, strides[stride].bl);
  stride = (stride + 1) % erasure->get_data_chunk_count();

  while (i.get_off() < len) {
    uint64_t thislen = min(i.get_remaining(), stripe_unit);
    i.copy(thislen, strides[stride].bl);
    stride = (stride + 1) % erasure->get_data_chunk_count();
  }
#endif
}

void ErasureCPlacer::repair(vector<StrideExtent>& extents,
	    const OSDMap& map)
{
  return;
}

void ErasureCPlacer::serialize_data(bufferlist &bl)
{
  return;
}

void ErasureCPlacer::serialize_code(bufferlist &bl)
{
  return;
}

void ErasureCPlacer::add_data(const uint64_t off, bufferlist& in,
			      vector<StrideExtent>& out) const
{
  const uint32_t stripe_size = get_stripe_unit() * get_data_chunk_count();
  set<int> want;
  for (unsigned i = 0; i < get_chunk_count(); ++i) {
    want.insert(i);
  }
  assert(in.length());
  assert(off % stripe_size == 0);
  assert(out.size() == get_chunk_count());

  // Pad
  if (in.length() % stripe_size)
    in.append_zero(stripe_size - ((off + in.length()) % stripe_size));

  for (uint64_t i = 0; i < in.length(); i += stripe_size) {
    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(in, i, stripe_size);
    int r = erasure->encode(want, buf, &encoded);
    assert(r == 0);
    for (auto &p : encoded) {
      assert(p.second.length() == get_stripe_unit());
      assert(p.first <= (int)out.size());
      out[p.first].bl.claim_append(p.second);
      out[p.first].offset = off / get_data_chunk_count();
      out[p.first].length = in.length() / get_data_chunk_count();
    }
  }
}

int ErasureCPlacer::get_data(map<int, bufferlist> &strides,
			     bufferlist *decoded) const
{
  uint64_t stride_size
    = std::max_element(strides.begin(), strides.end(),
		       [](pair<int, bufferlist> x, pair<int, bufferlist> y) {
		       return x.second.length() < y.second.length();
		       })->second.length();

  for (uint64_t i = 0; i < stride_size; i += get_stripe_unit()) {
    map<int, bufferlist> chunks;
    for (auto &p : strides) {
      if (chunks.size() == get_data_chunk_count()) {
	break;
      }
      if (p.second.length() < i + get_stripe_unit()) {
	continue;
      }
      chunks[p.first].substr_of(p.second, i, get_stripe_unit());
    }
    if (chunks.size() < get_data_chunk_count()) {
      return -1;
    }
    bufferlist stripebuf;
    int s = erasure->decode_concat(chunks, &stripebuf);
    if (s != 0)
      return s;
    decoded->claim_append(stripebuf);
  }

  return 0;
};
