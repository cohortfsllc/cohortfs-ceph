// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-


#include <condition_variable>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include "Object.h"
#include "TestOpStat.h"
#include "test/librados/test.h"
#include "common/sharedptr_registry.hpp"
#include "common/errno.h"
#include "include/rados/librados.hpp"

#ifndef RADOSMODEL_H
#define RADOSMODEL_H

class RadosTestContext;
class TestOpStat;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) ++retval;
  return retval;
}

enum TestOpType {
  TEST_OP_READ,
  TEST_OP_WRITE,
  TEST_OP_DELETE,
  TEST_OP_SNAP_CREATE,
  TEST_OP_SNAP_REMOVE,
  TEST_OP_ROLLBACK,
  TEST_OP_SETATTR,
  TEST_OP_RMATTR,
  TEST_OP_WATCH,
  TEST_OP_COPY_FROM,
  TEST_OP_HIT_SET_LIST,
  TEST_OP_UNDIRTY,
  TEST_OP_IS_DIRTY,
  TEST_OP_CACHE_FLUSH,
  TEST_OP_CACHE_TRY_FLUSH,
  TEST_OP_CACHE_EVICT,
  TEST_OP_APPEND
};

class TestWatchContext : public librados::WatchCtx {
  TestWatchContext(const TestWatchContext&);
public:
  std::condition_variable cond;
  uint64_t handle;
  bool waiting;
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  TestWatchContext() : handle(0), waiting(false) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist &bl) {
    lock_guard l(lock);
    waiting = false;
    cond.notify_all();
  }
  void start() {
    lock_guard l(lock);
    waiting = true;
  }
  void wait(unique_lock& l) {
    cond.wait(l, [&](){ return !waiting; });
  }
  uint64_t &get_handle() {
    return handle;
  }
};

class TestOp {
public:
  int num;
  RadosTestContext *context;
  TestOpStat *stat;
  bool done;
  TestOp(int n, RadosTestContext *context,
	 TestOpStat *stat = 0)
    : num(n),
      context(context),
      stat(stat),
      done(0)
  {}

  virtual ~TestOp() {};

  /**
   * This struct holds data to be passed by a callback
   * to a TestOp::finish method.
   */
  struct CallbackInfo {
    uint64_t id;
    CallbackInfo(uint64_t id) : id(id) {}
    virtual ~CallbackInfo() {};
  };

  virtual void _begin() = 0;

  /**
   * Called when the operation completes.
   * This should be overridden by asynchronous operations.
   *
   * @param info information stored by a callback, or NULL -
   *		 useful for multi-operation TestOps
   */
  virtual void _finish(CallbackInfo *info)
  {
    return;
  }
  virtual string getType() = 0;
  virtual bool finished()
  {
    return true;
  }

  void begin();
  void finish(CallbackInfo *info);
  virtual bool must_quiesce_other_ops() { return false; }
};

class TestOpGenerator {
public:
  virtual ~TestOpGenerator() {};
  virtual TestOp *next(RadosTestContext &context) = 0;
};

class RadosTestContext {
public:
  std::mutex state_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  std::condition_variable wait_cond;
  map<string,ObjectDesc> volume_obj_cont;
  set<string> oid_in_use;
  set<string> oid_not_in_use;
  set<string> oid_flushing;
  set<string> oid_not_flushing;
  string volume_name;
  librados::IoCtx io_ctx;
  librados::Rados rados;
  int next_oid;
  string prefix;
  int errors;
  int max_in_flight;
  int seq_num;
  uint64_t seq;
  const char *rados_id;
  bool initialized;
  map<string, TestWatchContext*> watches;
  const uint64_t max_size;
  const uint64_t min_stride_size;
  const uint64_t max_stride_size;
  AttrGenerator attr_gen;
  const bool no_omap;

  RadosTestContext(const string &volume_name,
		   int max_in_flight,
		   uint64_t max_size,
		   uint64_t min_stride_size,
		   uint64_t max_stride_size,
		   bool no_omap,
		   const char *id = 0) :
    volume_obj_cont(),
    volume_name(volume_name),
    next_oid(0),
    errors(0),
    max_in_flight(max_in_flight),
    seq_num(0), seq(0),
    rados_id(id), initialized(false),
    max_size(max_size),
    min_stride_size(min_stride_size), max_stride_size(max_stride_size),
    attr_gen(2000),
    no_omap(no_omap)
  {
  }

  int init()
  {
    int r = rados.init(rados_id);
    if (r < 0)
      return r;
    r = rados.conf_read_file(NULL);
    if (r < 0)
      return r;
    r = rados.conf_parse_env(NULL);
    if (r < 0)
      return r;
    r = rados.connect();
    if (r < 0)
      return r;
    r = rados.ioctx_create(volume_name.c_str(), io_ctx);
    if (r < 0) {
      rados.shutdown();
      return r;
    }
    char hostname_cstr[100];
    gethostname(hostname_cstr, 100);
    stringstream hostpid;
    hostpid << hostname_cstr << getpid() << "-";
    prefix = hostpid.str();
    assert(!initialized);
    initialized = true;
    return 0;
  }

  void shutdown()
  {
    if (initialized) {
      rados.shutdown();
    }
  }

  void loop(TestOpGenerator *gen)
  {
    assert(initialized);
    list<TestOp*> inflight;
    unique_lock sl(state_lock);

    TestOp *next = gen->next(*this);
    TestOp *waiting = NULL;

    while (next || !inflight.empty()) {
      if (next && next->must_quiesce_other_ops() && !inflight.empty()) {
	waiting = next;
	next = NULL;   // Force to wait for inflight to drain
      }
      if (next) {
	inflight.push_back(next);
      }
      sl.unlock();
      if (next) {
	(*inflight.rbegin())->begin();
      }
      sl.lock();
      while (1) {
	for (list<TestOp*>::iterator i = inflight.begin();
	     i != inflight.end();) {
	  if ((*i)->finished()) {
	    std::cout << (*i)->num << ": done (" << (inflight.size()-1)
		      << " left)" << std::endl;
	    delete *i;
	    inflight.erase(i++);
	  } else {
	    ++i;
	  }
	}

	if (inflight.size() >= (unsigned) max_in_flight ||
	    (!next && !inflight.empty())) {
	  std::cout << " waiting on " << inflight.size() << std::endl;
	  wait(sl);
	} else {
	  break;
	}
      }
      if (waiting) {
	next = waiting;
	waiting = NULL;
      } else {
	next = gen->next(*this);
      }
    }
    sl.unlock();
  }

  void wait(unique_lock& sl)
  {
    wait_cond.wait(sl);
  }

  void kick()
  {
    wait_cond.notify_all();
  }

  TestWatchContext *get_watch_context(const string &oid) {
    return watches.count(oid) ? watches[oid] : 0;
  }

  TestWatchContext *watch(const string &oid) {
    assert(!watches.count(oid));
    return (watches[oid] = new TestWatchContext);
  }

  void unwatch(const string &oid) {
    assert(watches.count(oid));
    delete watches[oid];
    watches.erase(oid);
  }

  ObjectDesc get_most_recent(const string &oid) {
    ObjectDesc new_obj;
    auto j = volume_obj_cont.find(oid);
    if (j != volume_obj_cont.end())
      new_obj = j->second;
    return new_obj;
  }

  void rm_object_attrs(const string &oid, const set<string> &attrs)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    for (set<string>::const_iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      new_obj.attrs.erase(*i);
    }
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void remove_object_header(const string &oid)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.header = bufferlist();
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, new_obj));
  }


  void update_object_header(const string &oid, const bufferlist &bl)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.header = bl;
    new_obj.exists = true;
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object_attrs(const string &oid, const map<string, ContDesc> &attrs)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    for (map<string, ContDesc>::const_iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      new_obj.attrs[i->first] = i->second;
    }
    new_obj.exists = true;
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object(std::shared_ptr<ContentsGenerator> cont_gen,
		     const string &oid, const ContDesc &contents)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.exists = true;
    new_obj.update(cont_gen,
		   contents);
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object_full(const string &oid, const ObjectDesc &contents)
  {
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, contents));
  }

  void update_object_version(const string &oid, uint64_t version)
  {
    auto j = volume_obj_cont.find(oid);
    if (j != volume_obj_cont.end()) {
      if (version)
	j->second.version = version;
      std::cout << __func__ << " oid " << oid
		<< " v " << version << " " << j->second.most_recent()
		<< " " << (j->second.exists ? "exists" : "dne")
		<< std::endl;
    }
  }

  void remove_object(const string &oid)
  {
    assert(!get_watch_context(oid));
    ObjectDesc new_obj;
    volume_obj_cont.erase(oid);
    volume_obj_cont.insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  bool find_object(const string &oid, ObjectDesc *contents) const
  {
    auto i = volume_obj_cont.find(oid);
    if (i != volume_obj_cont.end()) {
      *contents = i->second;
      return true;
    } else {
      return false;
    }
  }
};

void read_callback(librados::completion_t comp, void *arg);
void write_callback(librados::completion_t comp, void *arg);

class RemoveAttrsOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  bool done;
  RemoveAttrsOp(int n, RadosTestContext *context,
		const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat), oid(oid), op(context->io_ctx), comp(NULL), done(false)
  {}

  void _begin()
  {
    ContDesc cont;
    set<string> to_remove;
    {
      RadosTestContext::lock_guard l(context->state_lock);
      ObjectDesc obj;
      if (!context->find_object(oid, &obj)) {
	context->kick();
	done = true;
	return;
      }
      cont = ContDesc(context->seq_num, context->seq_num, "");
      context->oid_in_use.insert(oid);
      context->oid_not_in_use.erase(oid);

      if (rand() % 30) {
	ContentsGenerator::iterator iter = context->attr_gen.get_iterator(cont);
	for (map<string, ContDesc>::iterator i = obj.attrs.begin();
	     i != obj.attrs.end();
	     ++i, ++iter) {
	  if (!(*iter % 3)) {
	    //op.rmxattr(i->first.c_str());
	    to_remove.insert(i->first);
	    op.rmxattr(i->first.c_str());
	  }
	}
	if (to_remove.empty()) {
	  context->kick();
	  context->oid_in_use.erase(oid);
	  context->oid_not_in_use.insert(oid);
	  done = true;
	  return;
	}
	if (!context->no_omap) {
	  op.omap_rm_keys(to_remove);
	}
      } else {
	if (!context->no_omap) {
	  op.omap_clear();
	}
	for (map<string, ContDesc>::iterator i = obj.attrs.begin();
	     i != obj.attrs.end();
	     ++i) {
	  op.rmxattr(i->first.c_str());
	  to_remove.insert(i->first);
	}
	context->remove_object_header(oid);
      }
      context->rm_object_attrs(oid, to_remove);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, NULL,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info)
  {
    RadosTestContext::lock_guard l(context->state_lock);
    done = true;
    context->update_object_version(oid, comp->get_version());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "RemoveAttrsOp";
  }
};

class SetAttrsOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  bool done;
  SetAttrsOp(int n,
	     RadosTestContext *context,
	     const string &oid,
	     TestOpStat *stat)
    : TestOp(n, context, stat),
      oid(oid), op(context->io_ctx), comp(NULL), done(false)
  {}

  void _begin()
  {
    ContDesc cont;
    {
      RadosTestContext::lock_guard l(context->state_lock);
      cont = ContDesc(context->seq_num, context->seq_num, "");
      context->oid_in_use.insert(oid);
      context->oid_not_in_use.erase(oid);
    }

    map<string, bufferlist> omap_contents;
    map<string, ContDesc> omap;
    bufferlist header;
    ContentsGenerator::iterator keygen = context->attr_gen.get_iterator(cont);
    op.create(false);
    while (!*keygen) ++keygen;
    while (*keygen) {
      if (*keygen != '_')
	header.append(*keygen);
      ++keygen;
    }
    for (int i = 0; i < 20; ++i) {
      string key;
      while (!*keygen) ++keygen;
      while (*keygen && key.size() < 40) {
	key.push_back((*keygen % 20) + 'a');
	++keygen;
      }
      ContDesc val(cont);
      val.seqnum += (unsigned)(*keygen);
      val.prefix = ("oid: " + oid);
      omap[key] = val;
      bufferlist val_buffer = context->attr_gen.gen_bl(val);
      omap_contents[key] = val_buffer;
      op.setxattr(key.c_str(), val_buffer);
    }
    if (!context->no_omap) {
      op.omap_set_header(header);
      op.omap_set(omap_contents);
    }

    {
      RadosTestContext::lock_guard l(context->state_lock);
      context->update_object_header(oid, header);
      context->update_object_attrs(oid, omap);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, NULL,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix + oid, comp, &op);
  }

  void _finish(CallbackInfo *info)
  {
    RadosTestContext::lock_guard l(context->state_lock);
    int r;
    if ((r = comp->get_return_value())) {
      cerr << "err " << r << std::endl;
      assert(0);
    }
    done = true;
    context->update_object_version(oid, comp->get_version());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "SetAttrsOp";
  }
};

class WriteOp : public TestOp {
public:
  string oid;
  ContDesc cont;
  set<librados::AioCompletion *> waiting;
  librados::AioCompletion *rcompletion;
  uint64_t waiting_on;
  uint64_t last_acked_tid;

  librados::ObjectReadOperation read_op;
  librados::ObjectWriteOperation write_op;
  bufferlist rbuffer;

  bool do_append;

  WriteOp(int n,
	  RadosTestContext *context,
	  const string &oid,
	  bool do_append,
	  TestOpStat *stat = 0)
    : TestOp(n, context, stat), oid(oid), waiting_on(0), last_acked_tid(0),
      read_op(context->io_ctx), write_op(context->io_ctx),
      do_append(do_append)
  {}

  void _begin()
  {
    RadosTestContext::unique_lock csl(context->state_lock);
    done = 0;
    stringstream acc;
    acc << context->prefix << "OID: " << oid << std::endl;
    string prefix = acc.str();

    cont = ContDesc(context->seq_num, context->seq_num, prefix);

    std::shared_ptr<ContentsGenerator> cont_gen;
    if (do_append) {
      ObjectDesc old_value;
      bool found = context->find_object(oid, &old_value);
      uint64_t prev_length = found && old_value.has_contents() ?
	old_value.most_recent_gen()->get_length(old_value.most_recent()) :
	0;
      cont_gen.reset(new AppendGenerator(
		       prev_length, context->min_stride_size,
		       context->max_stride_size, 3));
    } else {
      cont_gen.reset(new VarLenGenerator(
		       context->max_size, context->min_stride_size,
		       context->max_stride_size));
    }
    context->update_object(cont_gen, oid, cont);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    map<uint64_t, uint64_t> ranges;

    cont_gen->get_ranges_map(cont, ranges);
    std::cout << num << ":  seq_num " << context->seq_num << " ranges "
	      << ranges << std::endl;
    context->seq_num++;

    waiting_on = ranges.size();
    ContentsGenerator::iterator gen_pos = cont_gen->get_iterator(cont);
    uint64_t tid = 1;
    for (map<uint64_t, uint64_t>::iterator i = ranges.begin();
	 i != ranges.end();
	 ++i, ++tid) {
      bufferlist to_write;
      gen_pos.seek(i->first);
      for (uint64_t k = 0; k != i->second; ++k, ++gen_pos) {
	to_write.append(*gen_pos);
      }
      assert(to_write.length() == i->second);
      assert(to_write.length() > 0);
      std::cout << num << ":  writing " << context->prefix+oid
		<< " from " << i->first
		<< " to " << i->first + i->second << " tid " << tid << std::endl;
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(tid));
      librados::AioCompletion *completion =
	context->rados.aio_create_completion((void*) cb_arg, NULL,
					     &write_callback);
      waiting.insert(completion);
      librados::ObjectWriteOperation op(context->io_ctx);
      if (do_append) {
	op.append(to_write);
      } else {
	op.write(i->first, to_write);
      }
      context->io_ctx.aio_operate(
	context->prefix + oid, completion,
	&op);
    }

    bufferlist contbl;
    ::encode(cont, contbl);
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    librados::AioCompletion *completion = context->rados.aio_create_completion(
      (void*) cb_arg, NULL, &write_callback);
    waiting.insert(completion);
    waiting_on++;
    write_op.setxattr("_header", contbl);
    if (!do_append) {
      write_op.truncate(cont_gen->get_length(cont));
    }
    context->io_ctx.aio_operate(
      context->prefix+oid, completion, &write_op);

    cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    rcompletion = context->rados.aio_create_completion(
	 (void*) cb_arg, NULL, &write_callback);
    waiting_on++;
    read_op.read(0, 1, &rbuffer, 0);
    context->io_ctx.aio_operate(
      context->prefix + oid, rcompletion,
      &read_op,
      librados::OPERATION_ORDER_READS_WRITES,  // order wrt previous write/update
      0);
    csl.unlock();
  }

  void _finish(CallbackInfo *info)
  {
    assert(info);
    RadosTestContext::unique_lock csl(context->state_lock);
    uint64_t tid = info->id;

    std::cout << num << ":  finishing write tid " << tid << " to "
	      << context->prefix + oid << std::endl;

    if (tid <= last_acked_tid) {
      cerr << "Error: finished tid " << tid
	   << " when last_acked_tid was " << last_acked_tid << std::endl;
      assert(0);
    }
    last_acked_tid = tid;

    assert(!done);
    waiting_on--;
    if (waiting_on == 0) {
      uint64_t version = 0;
      for (set<librados::AioCompletion *>::iterator i = waiting.begin();
	   i != waiting.end();
	   ) {
	assert((*i)->is_complete());
	if (int err = (*i)->get_return_value()) {
	  cerr << "Error: oid " << oid << " write returned error code "
	       << err << std::endl;
	}
	if ((*i)->get_version() > version)
	  version = (*i)->get_version();
	(*i)->release();
	waiting.erase(i++);
      }

      context->update_object_version(oid, version);
      if (rcompletion->get_version() != version) {
	cerr << "Error: racing read on " << oid << " returned version "
	     << rcompletion->get_version() << " rather than version "
	     << version << std::endl;
	assert(0 == "racing read got wrong version");
      }
      rcompletion->release();
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
      context->kick();
      done = true;
    }
    csl.unlock();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "WriteOp";
  }
};

class DeleteOp : public TestOp {
public:
  string oid;

  DeleteOp(int n,
	   RadosTestContext *context,
	   const string &oid,
	   TestOpStat *stat = 0)
    : TestOp(n, context, stat), oid(oid)
  {}

  void _begin()
  {
    RadosTestContext::unique_lock csl(context->state_lock);
    if (context->get_watch_context(oid)) {
      context->kick();
      csl.unlock();
      return;
    }

    ObjectDesc contents;
    context->find_object(oid, &contents);
    bool present = !contents.deleted();

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->seq_num++;

    context->remove_object(oid);

    interval_set<uint64_t> ranges;
    csl.unlock();

    int r = context->io_ctx.remove(context->prefix + oid);
    if (r && !(r == -ENOENT && !present)) {
      cerr << "r is " << r << " while deleting " << oid << " and present is "
	   << present << std::endl;
      assert(0);
    }

    csl.lock();
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
    csl.unlock();
  }

  string getType()
  {
    return "DeleteOp";
  }
};

class ReadOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  ObjectDesc old_value;

  std::shared_ptr<int> in_use;

  bufferlist result;
  int retval;

  map<string, bufferlist> attrs;
  int attrretval;

  set<string> omap_requested_keys;
  map<string, bufferlist> omap_returned_values;
  set<string> omap_keys;
  map<string, bufferlist> omap;
  bufferlist header;

  map<string, bufferlist> xattrs;
  ReadOp(int n,
	 RadosTestContext *context,
	 const string &oid,
	 TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completion(NULL),
      op(context->io_ctx),
      oid(oid),
      retval(0),
      attrretval(0)
  {}

  void _begin()
  {
    RadosTestContext::unique_lock csl(context->state_lock);
    std::cout << num << ": read oid " << oid << std::endl;
    done = 0;
    completion = context->rados.aio_create_completion(
      (void *) this, &read_callback, 0);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    assert(context->find_object(oid, &old_value));

    TestWatchContext *ctx = context->get_watch_context(oid);
    csl.unlock();
    if (ctx) {
      assert(old_value.exists);
      TestAlarm alarm;
      std::cerr << num << ":  about to start" << std::endl;
      ctx->start();
      std::cerr << num << ":  started" << std::endl;
      bufferlist bl;
      context->io_ctx.set_notify_timeout(10min);
      int r = context->io_ctx.notify(context->prefix + oid, bl);
      if (r < 0) {
	std::cerr << "r is " << r << std::endl;
	assert(0);
      }
      std::cerr << num << ":  notified, waiting" << std::endl;
      ctx->wait(csl);
    }

    op.read(0,
	    !old_value.has_contents() ? 0 :
	    old_value.most_recent_gen()->get_length(old_value.most_recent()),
	    &result,
	    &retval);

    for (map<string, ContDesc>::iterator i = old_value.attrs.begin();
	 i != old_value.attrs.end();
	 ++i) {
      if (rand() % 2) {
	string key = i->first;
	if (rand() % 2)
	  key.push_back((rand() % 26) + 'a');
	omap_requested_keys.insert(key);
      }
    }
    if (!context->no_omap) {
      op.omap_get_vals_by_keys(omap_requested_keys, omap_returned_values, 0);

      op.omap_get_keys("", -1, &omap_keys, 0);
      op.omap_get_vals("", -1, omap, 0);
      op.omap_get_header(&header, 0);
    }
    op.getxattrs(xattrs, 0);
    assert(!context->io_ctx.aio_operate(context->prefix + oid,
					completion, &op, 0));
  }

  void _finish(CallbackInfo *info)
  {
    RadosTestContext::unique_lock csl(context->state_lock);
    assert(!done);
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    assert(completion->is_complete());
    uint64_t version = completion->get_version();
    if (int err = completion->get_return_value()) {
      if (!(err == -ENOENT && old_value.deleted())) {
	cerr << num << ": Error: oid " << oid << " read returned error code "
	     << err << std::endl;
	assert(0);
      }
    } else {
      map<string, bufferlist>::iterator iter = xattrs.find("_header");
      bufferlist headerbl;
      if (iter == xattrs.end()) {
	if (old_value.has_contents()) {
	  cerr << num << ": Error: did not find header attr, has_contents: "
	       << old_value.has_contents()
	       << std::endl;
	  assert(!old_value.has_contents());
	}
      } else {
	headerbl = iter->second;
	xattrs.erase(iter);
      }
      std::cout << num << ":	 expect " << old_value.most_recent()
		<< std::endl;
      assert(!old_value.deleted());
      if (old_value.has_contents()) {
	ContDesc to_check;
	bufferlist::iterator p = headerbl.begin();
	::decode(to_check, p);
	if (to_check != old_value.most_recent()) {
	  cerr << num << ": oid " << oid << " found incorrect object contents "
	       << to_check
	       << ", expected " << old_value.most_recent() << std::endl;
	  context->errors++;
	}
	if (!old_value.check(result)) {
	  cerr << num << ": oid " << oid << " contents " << to_check
	       << " corrupt" << std::endl;
	  context->errors++;
	}
	if (context->errors) assert(0);
      }

      // Attributes
      if (!context->no_omap) {
	if (!(old_value.header == header)) {
	  cerr << num << ": oid " << oid
	       << " header does not match, old size: "
	       << old_value.header.length() << " new size " << header.length()
	       << std::endl;
	  assert(old_value.header == header);
	}
	if (omap.size() != old_value.attrs.size()) {
	  cerr << num << ": oid " << oid << " omap.size() is " << omap.size()
	       << " and old is " << old_value.attrs.size() << std::endl;
	  assert(omap.size() == old_value.attrs.size());
	}
	if (omap_keys.size() != old_value.attrs.size()) {
	  cerr << num << ": oid " << oid << " omap.size() is " << omap_keys.size()
	       << " and old is " << old_value.attrs.size() << std::endl;
	  assert(omap_keys.size() == old_value.attrs.size());
	}
      }
      if (xattrs.size() != old_value.attrs.size()) {
	cerr << num << ": oid " << oid << " xattrs.size() is " << xattrs.size()
	     << " and old is " << old_value.attrs.size() << std::endl;
	assert(xattrs.size() == old_value.attrs.size());
      }
      if (version != old_value.version) {
	cerr << num << ": oid " << oid << " version is " << version
	     << " and expected " << old_value.version << std::endl;
	assert(version == old_value.version);
      }
      for (map<string, ContDesc>::iterator iter = old_value.attrs.begin();
	   iter != old_value.attrs.end();
	   ++iter) {
	bufferlist bl = context->attr_gen.gen_bl(
	  iter->second);
	if (!context->no_omap) {
	  map<string, bufferlist>::iterator omap_iter = omap.find(iter->first);
	  assert(omap_iter != omap.end());
	  assert(bl.length() == omap_iter->second.length());
	  bufferlist::iterator k = bl.begin();
	  for(bufferlist::iterator l = omap_iter->second.begin();
	      !k.end() && !l.end();
	      ++k, ++l) {
	    assert(*l == *k);
	  }
	}
	map<string, bufferlist>::iterator xattr_iter = xattrs.find(iter->first);
	assert(xattr_iter != xattrs.end());
	assert(bl.length() == xattr_iter->second.length());
	bufferlist::iterator k = bl.begin();
	for (bufferlist::iterator j = xattr_iter->second.begin();
	     !k.end() && !j.end();
	     ++j, ++k) {
	  assert(*j == *k);
	}
      }
      if (!context->no_omap) {
	for (set<string>::iterator i = omap_requested_keys.begin();
	     i != omap_requested_keys.end();
	     ++i) {
	  if (!omap_returned_values.count(*i))
	    assert(!old_value.attrs.count(*i));
	  if (!old_value.attrs.count(*i))
	    assert(!omap_returned_values.count(*i));
	}
	for (map<string, bufferlist>::iterator i = omap_returned_values.begin();
	     i != omap_returned_values.end();
	     ++i) {
	  assert(omap_requested_keys.count(i->first));
	  assert(omap.count(i->first));
	  assert(old_value.attrs.count(i->first));
	  assert(i->second == omap[i->first]);
	}
      }
    }
    context->kick();
    done = true;
    csl.unlock();
  }

  bool finished()
  {
    return done && completion->is_complete();
  }

  string getType()
  {
    return "ReadOp";
  }
};

class WatchOp : public TestOp {
  string oid;
public:
  WatchOp(int n,
	  RadosTestContext *context,
	  const string &_oid,
	  TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(_oid)
  {}

  void _begin()
  {
    RadosTestContext::unique_lock csl(context->state_lock);
    ObjectDesc contents;
    context->find_object(oid, &contents);
    if (contents.deleted()) {
      context->kick();
      csl.unlock();
      return;
    }
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    TestWatchContext *ctx = context->get_watch_context(oid);
    csl.unlock();
    int r;
    if (!ctx) {
      {
	ctx = context->watch(oid);
      }

      r = context->io_ctx.watch(context->prefix + oid,
				0,
				&ctx->get_handle(),
				ctx);
    } else {
      r = context->io_ctx.unwatch(context->prefix + oid,
				  ctx->get_handle());
      {
	RadosTestContext::lock_guard l(context->state_lock);
	context->unwatch(oid);
      }
    }

    if (r) {
      cerr << "r is " << r << std::endl;
      assert(0);
    }

    {
      RadosTestContext::lock_guard l(context->state_lock);
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
    }
  }

  string getType()
  {
    return "WatchOp";
  }
};

#endif
