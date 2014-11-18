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
#include "CohortVolume.h"
#include "osdc/Objecter.h"

const uint64_t CohortVolume::one_op = 4194304;

typedef int (*place_func)(void*, const uint8_t[16], size_t, const char*,
			  struct erasure_params *,
			  bool(*)(void*, int),
			  bool(*)(void*, int));

VolumeRef CohortVolFactory(bufferlist::iterator& bl, uint8_t v, vol_type t)
{
  CohortVolume *vol = new CohortVolume(t);
  vol->decode_payload(bl, v);
  return VolumeRef(vol);
}

/* Epoch should be the current epoch of the OSDMap. */

void CohortVolume::compile(epoch_t epoch)
{
  const size_t uuid_strlen = 37;
  char uuidstring[uuid_strlen];
  char cfilename[uuid_strlen + 10];
  char objfilename[uuid_strlen + 10];
  char sofilename[uuid_strlen + 10];

  pid_t child;

  strcpy(uuidstring, to_string(id).c_str());
  strcpy(cfilename, "/tmp/");
  strcat(cfilename, uuidstring);
  strcpy(objfilename, cfilename);
  strcpy(sofilename, cfilename);
  strcat(cfilename, ".c");
  strcat(objfilename, ".o");
  strcat(sofilename, ".so");

  unlink(sofilename);

  const char *cargv[] = {
    [0] = "gcc", [1] = "-O3", [2] = "-fPIC",
    [3] = "-c", [4] = cfilename, [5] = "-o",
    [6] = objfilename, [7] = NULL
  };

  const char *largv[] = {
    [0] = "gcc", [1] = "-shared", [2] = "-o",
    [3] = sofilename, [4] = objfilename, [5] = "-lm",
    [6] = "-lc", [7] = NULL
  };

  if (compiled_epoch >= last_update) {
    return;
  }

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
    }
  }

  unlink(objfilename);
  place_shared = dlopen(sofilename, RTLD_LAZY | RTLD_GLOBAL);

  for(vector<string>::size_type i = 0;
      i < symbols.size();
      ++i) {
    entry_points.push_back(dlsym(place_shared, symbols[i].c_str()));
  }

  compiled_epoch = epoch;
}

CohortVolume::~CohortVolume(void)
{
  if (place_shared) {
    dlclose(place_shared); /* It's not like we can do anything on error. */
    place_shared = NULL;
  }
}

int CohortVolume::update(VolumeCRef v)
{
  return 0;
}

uint32_t CohortVolume::num_rules(void)
{
  return entry_points.size();
}

struct placement_context
{
  const OSDMap *map;
  vector<int> *osds;
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
  if (context->map->is_in(osd)) {
    context->osds->push_back(osd);
    return true;
  }

  return false;
}


int CohortVolume::place(const object_t& object,
			const OSDMap& map,
			const unsigned int rule_index,
			vector<int>& osds)
{
  placement_context context = {
    .map = &map,
    .osds = &osds
  };

  compile_lock.get_read();
  if ((compiled_epoch < last_update) || !place_shared) {
    compile_lock.unlock();
    compile_lock.get_write();
    compile(map.get_epoch());
  }

  if (rule_index >= entry_points.size()) {
    return -1;
  }

  place_func entry_point = (place_func) entry_points[rule_index];

  int rc = entry_point(&context, id.data,
		       object.name.length(), object.name.data(),
		       &erasure, test_osd, return_osd);
  compile_lock.unlock();

  return rc;
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
  ::decode(erasure, bl);
}

void CohortVolume::encode(bufferlist& bl) const
{
  inherited::encode(bl);

  ::encode(place_text, bl);
  ::encode(symbols, bl);
  ::encode(erasure, bl);
}

VolumeRef CohortVolume::create(const string& name,
			       const epoch_t last_update,
			       const string& place_text,
			       const string& sym_str,
			       const string& erasure_type,
			       int64_t data_blocks,
			       int64_t code_blocks,
			       int64_t word_size,
			       int64_t packet_size,
			       int64_t size,
			       string& error_message)
{
  CohortVolume *v = new CohortVolume(CohortVol);

  if (!valid_name(name, error_message))
    goto error;

  v->id = boost::uuids::random_generator()();
  v->name = name;
  v->last_update = last_update;
  v->place_text.append(place_text);

  boost::algorithm::split(v->symbols, sym_str, boost::algorithm::is_any_of(" \t"));

  if (!erasure_params::fill_out(erasure_type, data_blocks,
				code_blocks, word_size,
				packet_size, size,
				v->erasure,
				error_message))
    goto error;

  return VolumeRef(v);

error:

  delete v;
  return VolumeRef();
}

class C_MultiCond : public Context {
  Mutex lock; ///< Mutex to take
  int rval; ///< return value (optional)
  std::atomic<uint64_t> refcnt; ///< call dependent context when this at zero
  Context *dependent; ///< dependent context

public:
  C_MultiCond(size_t ref, Context *dep)
    : rval(0), refcnt(ref), dependent(dep) {
  }
  void finish(int r) {
    lock.Lock();
    if (rval >= 0) {
      if (r >= 0) {
	rval += r;
      } else {
	rval = r;
      }
    }
    lock.Unlock();
  }
  void complete(int r) {
    finish(r);
    if (--refcnt == 0) {
      dependent->complete(rval);
      delete this;
    }
  }
};

int CohortVolume::create(const object_t& oid, utime_t mtime,
			 int global_flags, Context *onack,
			 Context *oncommit, Objecter *objecter)
{
  /* I'm not entirely sure about this vector thing, but I don't like
     it. Look at it as part of the refactor in the future. */
  vector<int> osds;
  const size_t stripes = erasure.k + erasure.m;
  C_MultiCond *multiack = (onack ? new C_MultiCond(stripes, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(stripes, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == stripes);

  for(size_t stripe = 0; stripe < stripes; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_CREATE;
    ops[0].op.flags = 0;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, global_flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

struct writestripe {
  uint64_t off;
  uint64_t len;
  bufferlist bl;

  writestripe() {
    off = 0;
    len = 0;
  }
};

void CohortVolume::make_stripes(uint64_t off, unsigned len,
				  bufferlist &blin,
				  vector<writestripe> &stripes)
{
  buffer::list::iterator i(&blin);
  size_t stripe;
  uint64_t thislen;
  len = min(len, blin.length());

  for (stripe = 0; stripe < erasure.k; ++stripe) {
    stripe_extent(off, len, stripe, stripes[stripe].off,
		  stripes[stripe].len);
    assert(stripes[stripe].len <= one_op);
  }

  stripe = ostripe(off);
  /* Special case on incomplete first block */
  if (off % erasure.size != 0) {
    thislen = min(len,
		  (unsigned) (erasure.size - off % erasure.size));
  } else {
    thislen = min(len, erasure.size);
  }
  i.copy(thislen, stripes[stripe].bl);
  stripe = (stripe + 1) % erasure.k;

  while (i.get_off() < len) {
    uint64_t thislen = min(i.get_remaining(), erasure.size);
    i.copy(thislen, stripes[stripe].bl);
    stripe = (stripe + 1) % erasure.k;
  }
}

int CohortVolume::write(const object_t& oid, uint64_t off, uint64_t len,
			const bufferlist &bl,
			utime_t mtime, int flags, Context *onack,
			Context *oncommit, Objecter *objecter)
{
  vector<int> osds;
  vector<writestripe> stripes(erasure.k);
  make_stripes(off, len, const_cast<bufferlist&>(bl), stripes);

  C_MultiCond *multiack = (onack ? new C_MultiCond(erasure.k, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(erasure.k, oncommit) : NULL);

  if (len > op_size())
    len = op_size();

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITE;
    ops[0].op.extent.offset = stripes[stripe].off;
    ops[0].op.extent.length = stripes[stripe].len;
    assert(ops[0].op.extent.length <= one_op);
    ops[0].op.extent.truncate_size = 0;
    ops[0].op.extent.truncate_seq = 0;
    ops[0].indata = stripes[stripe].bl;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    int rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

int CohortVolume::append(const object_t& oid, uint64_t len,
			 const bufferlist &bl,
			 utime_t mtime, int flags, Context *onack,
			 Context *oncommit, Objecter *objecter)
{
  /* A bit gross, but expedient */
  Mutex mylock;
  Cond cond;
  bool done;
  int r;
  Context *gotit = new C_SafeCond(&mylock, &cond, &done, &r);
  uint64_t size;

  stat(oid, &size, NULL, flags, gotit, objecter);

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r < 0) {
    return r;
  }

  return write(oid, size ? size - 1 : 0, len, bl,
	       mtime, flags, onack, oncommit, objecter);
}

int CohortVolume::write_full(const object_t& oid,
			     const bufferlist &bl, utime_t mtime, int flags,
			     Context *onack, Context *oncommit,
			     Objecter *objecter)
{
  vector<int> osds;
  vector<writestripe> stripes(erasure.k);
  uint64_t len = bl.length();
  if (len > op_size())
    len = op_size();

  make_stripes(0, len, const_cast<bufferlist&>(bl),
	       stripes);

  C_MultiCond *multiack = (onack ? new C_MultiCond(erasure.k, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(erasure.k, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[0].op.extent.offset = stripes[stripe].off;
    ops[0].op.extent.length = stripes[stripe].len;
    assert(ops[0].op.extent.length <= one_op);
    ops[0].indata = stripes[stripe].bl;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    int rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

/* I know it's stupid, I'm sorry, but I just want to fix this bug now,
   then I'll come back and do this right. */
static bool stupid_end_test(vector<bufferlist::iterator>& cursors)
{
  bool done = true;
  for (vector<bufferlist::iterator>::iterator i = cursors.begin();
       i != cursors.end();
       ++i) {
    if (!(i->end())) {
      done =  false;
      break;
    }
  }
  return done;
}

class C_MultiRead : public Context {
  Mutex lock; ///< Mutex to take
  int rval; ///< return value
  std::atomic<uint64_t> refcnt; ///< Gather when this gets to 0
  Context *dependent; ///< Dependent context
  bufferlist *bl; //< Bufferlist to gather into
  const erasure_params *erasure; //< Erasure parameters
  uint64_t off; //< Offset of total read
  uint64_t len; //< Length of total read

public:
  vector<bufferlist> reads; //< Bufferlists to gather from

  C_MultiRead(size_t ref, Context *dep, bufferlist *b,
	      const erasure_params *e, uint64_t o, uint64_t l)
    : rval(0), refcnt(ref), dependent(dep),
      bl(b), erasure(e), off(o), len(l), reads(ref) { }

  size_t ostripe(const uint64_t o) {
    return (o / erasure->size) % erasure->k;
  }

  void finish(int r) {
    lock.Lock();
    if ((rval == 0) && (r != -ENOENT)) {
      rval = r;
    }
    lock.Unlock();
  }

  void complete(int r) {
    finish(r);
    if (--refcnt == 0) {
      if ((rval >= 0) && (len > 0)) {
	try {
	  uint32_t real_len = 0;
	  uint32_t shortread = off % erasure->size ?
	    erasure->size - off % erasure->size : 0;
	  uint32_t stripe = ostripe(off);
	  vector<bufferlist::iterator> cursors;
	  for (vector<bufferlist>::iterator i = reads.begin();
	       i != reads.end();
	       ++i)
	    cursors.push_back(bufferlist::iterator(&(*i)));
	  if (shortread) {
	    if (cursors[stripe].get_remaining() >= shortread) {
	      cursors[stripe].copy(shortread, *bl);
	      real_len += shortread;
	    } else {
	      uint32_t blen = cursors[stripe].get_remaining();
	      cursors[stripe].copy(blen, *bl);
	      real_len += blen;
	      if (!stupid_end_test(cursors)) {
		bl->append_zero(shortread - blen);
		real_len += shortread - blen;
	      }
	    }
	    stripe = (stripe + 1) % erasure->k;
	  }
	  while (!stupid_end_test(cursors)) {
	    uint32_t blen = cursors[stripe].get_remaining();
	    if (blen >= erasure->size) {
	      cursors[stripe].copy(erasure->size, *bl);
	      real_len += erasure->size;
	    } else {
	      cursors[stripe].copy(blen, *bl);
	      real_len += blen;
	      if (!stupid_end_test(cursors)) {
		bl->append_zero(erasure->size - blen);
		real_len += erasure->size - blen;
	      }
	    }
	    stripe = (stripe + 1) % erasure->k;
	    assert(real_len <= len);
	  }
	  rval = real_len;
	} catch (buffer::end_of_buffer &e) {
	  puts("This ought not happen.");
	}
      }
      dependent->complete(rval);
      delete this;
    }
  }
};

/* This is only for headers, omaps, and similar. Attempts to read
   object data will are undefined. */

int CohortVolume::md_read(const object_t& oid, ObjectOperation& op,
			  bufferlist *pbl, int flags,
			  Context *onack, Objecter *objecter)
{
  vector<int> osds;
  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);

  Objecter::Op *o
    = new Objecter::Op(hobject_t(oid, DATA, 0), id, op.ops,
		       flags | CEPH_OSD_FLAG_READ, onack, NULL, NULL);
  o->priority = op.priority;
  o->outbl = pbl;
  o->out_bl.swap(op.out_bl);
  o->out_handler.swap(op.out_handler);
  o->out_rval.swap(op.out_rval);
  o->osd = osds[0];
  rc = objecter->op_submit_special(o);
  if (rc < 0)
    return rc;
  return 0;
}

int CohortVolume::read(const object_t& oid, uint64_t off, uint64_t len,
		       bufferlist *pbl, int flags,
		       Context *onfinish, Objecter *objecter)
{
  vector<int> osds;
  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);
  if (len > op_size())
    len = op_size();
  C_MultiRead *mr = new C_MultiRead(erasure.k, onfinish, pbl,
				    &erasure, off, len);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_READ;
    stripe_extent(off, len, stripe, ops[0].op.extent.offset,
		  ops[0].op.extent.length);
    assert(ops[0].op.extent.length <= one_op);
    ops[0].op.extent.truncate_size = 0;
    ops[0].op.extent.truncate_seq = 0;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id, ops,
			 flags | CEPH_OSD_FLAG_READ, mr, 0, NULL);
    o->outbl = &mr->reads[stripe];
    o->osd = osds[stripe];
    rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

struct C_MultiStat : public Context {
  vector<bufferlist> reads; //< Bufferlists to gather from
  uint64_t *psize;
  utime_t *pmtime;
  std::atomic<uint64_t> refcnt; ///< Gather when this gets to 0
  Context *fin;

  C_MultiStat(uint64_t *ps, utime_t *pm, size_t ref, Context *c) :
    reads(ref), psize(ps), pmtime(pm), refcnt(ref),
    fin(c) {
    if (pmtime != NULL)
      *pmtime = utime_t();
    if (psize)
      *psize = 0;
  }

  void finish(int r) {
  }

  void complete(int r) {
    bool foundone = false;;
    if (--refcnt == 0) {
      for (vector<bufferlist>::iterator i = reads.begin();
	   i != reads.end();
	   ++i) {
	bufferlist::iterator p = i->begin();
	try {
	  uint64_t s;
	  utime_t m;
	  ::decode(s, p);
	  ::decode(m, p);
	  if (psize)
	    *psize += s;
	  if (pmtime)
	    *pmtime = max(*pmtime, m);
	  foundone = true;
	} catch (buffer::end_of_buffer &e) { }
      }
      fin->complete(foundone ? 0 : -ENOENT);
      delete this;
    }
  }
};

int CohortVolume::stat(const object_t& oid,
		       uint64_t *psize, utime_t *pmtime, int flags,
		       Context *onfinish, Objecter *objecter)
{
  vector<int> osds;
  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);
  C_MultiStat *ms = new C_MultiStat(psize, pmtime, erasure.k,
				    onfinish);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {


    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_STAT;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_READ, ms, 0, NULL);
    o->outbl = &ms->reads[stripe];
    o->osd = osds[stripe];
    rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

int CohortVolume::remove(const object_t& oid,
			 utime_t mtime, int flags,
			 Context *onack, Context *oncommit,
			 Objecter *objecter)
{
  vector<int> osds;
  const size_t stripes = erasure.k + erasure.m;
  C_MultiCond *multiack = (onack ? new C_MultiCond(stripes, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(stripes, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == stripes);

  for(size_t stripe = 0; stripe < stripes; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_DELETE;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
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


int CohortVolume::getxattr(const object_t& oid, const char *name,
			   bufferlist *pbl, int flags,
			   Context *onfinish, Objecter *objecter)
{
  vector<int> osds;
  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_GETXATTR;
  ops[0].op.xattr.name_len = (name ? strlen(name) : 0);
  ops[0].op.xattr.value_len = 0;
  if (name)
    ops[0].indata.append(name);
  Objecter::Op *o
    = new Objecter::Op(hobject_t(oid, DATA, 0), id,
		       ops, flags | CEPH_OSD_FLAG_READ, onfinish, 0,
		       NULL);
  o->outbl = pbl;
  o->osd = osds[0];
  return objecter->op_submit_special(o);
}

int CohortVolume::removexattr(const object_t& oid, const char *name,
			      utime_t mtime,
			      int flags, Context *onack, Context *oncommit,
			      Objecter *objecter)
{
  vector<int> osds;
  const size_t stripes = erasure.k + erasure.m;
  C_MultiCond *multiack = (onack ? new C_MultiCond(stripes, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(stripes, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == stripes);

  for(size_t stripe = 0; stripe < stripes; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_RMXATTR;
    ops[0].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[0].op.xattr.value_len = 0;
    if (name)
      ops[0].indata.append(name);
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

int CohortVolume::setxattr(const object_t& oid, const char *name,
			   const bufferlist &bl,
			   utime_t mtime, int flags, Context *onack,
			   Context *oncommit, Objecter *objecter)
{
  vector<int> osds;
  const size_t stripes = erasure.k + erasure.m;
  C_MultiCond *multiack = (onack ? new C_MultiCond(stripes, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(stripes, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == stripes);

  for(size_t stripe = 0; stripe < stripes; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_SETXATTR;
    ops[0].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[0].op.xattr.value_len = bl.length();
    if (name)
      ops[0].indata.append(name);
    ops[0].indata.append(bl);
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

int CohortVolume::getxattrs(const object_t& oid,
			    map<string, bufferlist>& attrset, int flags,
			    Context *onfinish, Objecter *objecter)
{
  vector<int> osds;
  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  C_GetAttrs *fin = new C_GetAttrs(attrset, onfinish);
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_GETXATTRS;
  Objecter::Op *o
    = new Objecter::Op(hobject_t(oid, DATA, 0), id,
		       ops, flags | CEPH_OSD_FLAG_READ, fin, 0,
		       NULL);
  o->outbl = &fin->bl;
  o->osd = osds[0];
  return objecter->op_submit_special(o);
}

int CohortVolume::trunc(const object_t& oid,
			utime_t mtime, int flags,
			uint64_t trunc_size, uint32_t trunc_seq,
			Context *onack, Context *oncommit,
			Objecter *objecter)
{
  vector<int> osds;
  C_MultiCond *multiack = (onack ? new C_MultiCond(erasure.k, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(erasure.k, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {
    uint64_t zero;
    uint64_t stripetrunclen;
    stripe_extent(0, trunc_size, stripe, zero, stripetrunclen);
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
    ops[0].op.extent.offset = stripetrunclen;
    ops[0].op.extent.truncate_size = stripetrunclen;
    ops[0].op.extent.truncate_seq = trunc_seq;
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    int rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}

int CohortVolume::zero(const object_t& oid, uint64_t off, uint64_t len,
		       utime_t mtime, int flags,
		       Context *onack, Context *oncommit, Objecter *objecter)
{
  vector<int> osds;
  C_MultiCond *multiack = (onack ? new C_MultiCond(erasure.k, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(erasure.k, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {
    vector<OSDOp> ops(1);
    ops[0].op.op = CEPH_OSD_OP_ZERO;
    ops[0].op.extent.offset = off;
    ops[0].op.extent.length = len;
    stripe_extent(off, len, stripe, ops[0].op.extent.offset,
		  ops[0].op.extent.length);
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id,
			 ops, flags | CEPH_OSD_FLAG_WRITE,
			 multiack, multicommit, NULL);
    o->mtime = mtime;
    o->osd = osds[stripe];
    int rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}



/* This is for metadata operations only. */

int CohortVolume::mutate_md(const object_t& oid, ObjectOperation& op,
			    utime_t mtime, int flags, Context *onack,
			    Context *oncommit, Objecter *objecter)
{
  vector<int> osds;
  C_MultiCond *multiack = (onack ? new C_MultiCond(erasure.k, onack) : NULL);
  C_MultiCond *multicommit
    = (oncommit ? new C_MultiCond(erasure.k, oncommit) : NULL);

  int rc = place(oid, *objecter->osdmap, 0, osds);
  if (rc < 0)
    return rc;
  assert(osds.size() == erasure.k);
  /* We don't have erasure coding yet, but WE WILL */
  assert(erasure.m == 0);

  for (size_t stripe = 0; stripe < erasure.k; ++stripe) {
    Objecter::Op *o
      = new Objecter::Op(hobject_t(oid, DATA, stripe), id, op.ops, flags |
			 CEPH_OSD_FLAG_WRITE, multiack, multicommit);
    o->mtime = mtime;
    o->osd = osds[stripe];
    int rc = objecter->op_submit_special(o);
    if (rc < 0)
      return rc;
  }
  return 0;
}
