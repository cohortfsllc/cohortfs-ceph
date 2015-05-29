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

#include <cerrno>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <locale>
#include <mutex>
#include <sstream>
#include <stdexcept>

#include <boost/intrusive/slist.hpp>

#include <dirent.h>

#include "include/types.h"
#include "include/compat.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/obj_bencher.h"
#include "common/config.h"
#include "common/strtol.h"
#include "common/ceph_argparse.h"

#include "auth/Crypto.h"
#include "global/global_init.h"

#include "osdc/RadosClient.h"
#include "cls/lock/cls_lock_client.h"


using std::cout;
using std::cerr;
using std::endl;

using namespace rados;
namespace bi = boost::intrusive;


CephContext* cct;

int rados_tool_sync(const std::map < std::string, std::string > &opts,
		    std::vector<const char*> &args);

// two steps seem to be necessary to do this right
#define STR(x) _STR(x)
#define _STR(x) #x

void usage(ostream& out)
{
  out <<					\
"usage: rados [options] [commands]\n"
"OBJECT COMMANDS\n"
"   get <oid-name> [outfile]	     fetch object\n"
"   put <oid-name> [infile]	     write object\n"
"   truncate <oid-name> length	     truncate object\n"
"   create <oid-name>		     create object\n"
"   rm <oid-name> ...		     remove object(s)\n"
"   cp <oid-name> [target-oid]	     copy object\n"
"   listxattr <oid-name>\n"
"   getxattr <oid-name> attr\n"
"   setxattr <oid-name> attr val\n"
"   rmxattr <oid-name> attr\n"
"   stat objname		     stat the named object\n"
"   bench <seconds> write|seq|rand [-t concurrent_operations] [--no-cleanup] [--run-name run_name]\n"
"				     default is 16 concurrent IOs and 4 MB ops\n"
"				     default is to clean up after write benchmark\n"
"				     default run-name is 'benchmark_last_metadata'\n"
"   cleanup [--run-name run_name] [--prefix prefix]\n"
"				     clean up a previous benchmark operation\n"
"				     default run-name is 'benchmark_last_metadata'\n"
"   load-gen [options]		     generate load on the cluster\n"
"   listomapkeys <oid-name>	     list the keys in the object map\n"
"   listomapvals <oid-name>	     list the keys and vals in the object map \n"
"   getomapval <oid-name> <key> [file] show the value for the specified key\n"
"				     in the object's object map\n"
"   setomapval <oid-name> <key> <val>\n"
"   rmomapkey <oid-name> <key>\n"
"   getomapheader <oid-name> [file]\n"
"   setomapheader <oid-name> <val>\n"
"   set-alloc-hint <oid-name> <expected-object-size> <expected-write-size>\n"
"				     set allocation hint for an object\n"
"\n"
"ADVISORY LOCKS\n"
"   lock list <oid-name>\n"
"	List all advisory locks on an object\n"
"   lock get <oid-name> <lock-name>\n"
"	Try to acquire a lock\n"
"   lock break <oid-name> <lock-name> <locker-name>\n"
"	Try to break a lock acquired by another client\n"
"   lock info <oid-name> <lock-name>\n"
"	Show lock information\n"
"   options:\n"
"	--lock-tag		     Lock tag, all locks operation should use\n"
"				     the same tag\n"
"	--lock-cookie		     Locker cookie\n"
"	--lock-description	     Description of lock\n"
"	--lock-duration		     Lock duration (in seconds)\n"
"	--lock-type		     Lock type (shared, exclusive)\n"
"\n"
"GLOBAL OPTIONS:\n"
"   -v volume\n"
"   --volume=volume\n"
"	 select given volume by name or UUID\n"
"   --target-volume=volume\n"
"	 select target volume by name or UUID\n"
"   -b op_size\n"
"	 set the size of write ops for put or benchmarking\n"
"   -i infile\n"
"\n"
"BENCH OPTIONS:\n"
"   -t N\n"
"   --concurrent-ios=N\n"
"	 Set number of concurrent I/O operations\n"
"   --show-time\n"
"	 prefix output with date/time\n"
"\n"
"LOAD GEN OPTIONS:\n"
"   --num-objects		     total number of objects\n"
"   --min-object-size		     min object size\n"
"   --max-object-size		     max object size\n"
"   --min-ops			     min number of operations\n"
"   --max-ops			     max number of operations\n"
"   --max-backlog		     max backlog (in MB)\n"
"   --percent			     percent of operations that are read\n"
"   --target-throughput		     target throughput (in MB)\n"
"   --run-length		     total time (in seconds)\n"
    ;
}

static void usage_exit()
{
  usage(cerr);
  exit(1);
}


static int dump_data(std::string const &filename, bufferlist const &data)
{
  int fd;
  if (filename == "-") {
    fd = 1;
  } else {
    fd = TEMP_FAILURE_RETRY(::open(filename.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644));
    if (fd < 0) {
      int err = errno;
      cerr << "failed to open file: " << cpp_strerror(err) << endl;
      return -err;
    }
  }

  int r = data.write_fd(fd);

  if (fd != 1) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }

  return r;
}


static int do_get(Objecter* o, const AVolRef& vol, const oid_t& oid,
		  const string& outfile, unsigned op_size)
{
  int fd;
  if (outfile == "-") {
    fd = 1;
  } else {
    fd = TEMP_FAILURE_RETRY(::open(outfile.c_str(), O_WRONLY|O_CREAT|O_TRUNC,
				   0644));
    if (fd < 0) {
      int err = errno;
      cerr << "failed to open file: " << cpp_strerror(err) << endl;
      return -err;
    }
  }

  uint64_t offset = 0;
  int ret;
  while (true) {
    bufferlist outdata;
    ret = o->read(oid, vol, offset, op_size, &outdata);
    if (ret < 0) {
      goto out;
    }
    ret = outdata.write_fd(fd);
    if (ret < 0) {
      cerr << "error writing to file: " << cpp_strerror(ret) << endl;
      goto out;
    }
    if (outdata.length() < op_size)
      break;
    offset += outdata.length();
  }
  ret = 0;

 out:
  if (fd != 1)
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}

static int do_copy(Objecter* o, AVolRef src_vol, const oid_t& src_oid,
		   AVolRef target_vol, const oid_t& target_oid)
{
  bufferlist outdata;
  ObjectOperation read_op(src_vol->op());
  string start_after;

#define COPY_CHUNK_SIZE (4 * 1024 * 1024)
  read_op->read(0, COPY_CHUNK_SIZE, &outdata);

  map<std::string, bufferlist> attrset;
  read_op->getxattrs(attrset);

  bufferlist omap_header;
  read_op->omap_get_header(&omap_header);

#define OMAP_CHUNK 1000
  map<string, bufferlist> omap;
  read_op->omap_get_vals(start_after, string(), OMAP_CHUNK, omap);

  int ret = o->read(src_oid, src_vol, read_op);
  if (ret < 0) {
    return ret;
  }

  ObjectOperation write_op(target_vol->op());

  /* reset dest if exists */
  write_op->create(false);
  write_op->remove();

  write_op->write_full(outdata);
  write_op->omap_set_header(omap_header);

  map<std::string, bufferlist>::iterator iter;
  for (iter = attrset.begin(); iter != attrset.end(); ++iter) {
    write_op->setxattr(iter->first, iter->second);
  }
  if (!omap.empty()) {
    write_op->omap_set(omap);
  }
  ret = o->mutate(target_oid, target_vol, write_op);
  if (ret < 0) {
    return ret;
  }

  uint64_t off = 0;

  while (outdata.length() == COPY_CHUNK_SIZE) {
    off += outdata.length();
    outdata.clear();
    ret = o->read(src_oid, src_vol, off, COPY_CHUNK_SIZE, &outdata);
    if (ret < 0)
      goto err;

    ret = o->write(target_oid, target_vol, off, outdata.length(), outdata);
    if (ret < 0)
      goto err;
  }

  /* iterate through source omap and update target. This is not atomic */
  while (omap.size() == OMAP_CHUNK) {
    /* now start_after should point at the last entry */
    map<string, bufferlist>::iterator iter = omap.end();
    --iter;
    start_after = iter->first;

    omap.clear();
    read_op = src_vol->op();
    read_op->omap_get_vals(start_after, "", OMAP_CHUNK, omap);
    ret = o->read(src_oid, src_vol, read_op);
    if (ret < 0)
      goto err;

    if (omap.empty())
      break;

    write_op = target_vol->op();
    write_op->omap_set(omap);
    ret = o->mutate(target_oid, target_vol, write_op);
    if (ret < 0)
      goto err;
  }

  return 0;

err:
  o->remove(target_oid, target_vol);
  return ret;
}

static int do_put(Objecter* o, AVolRef vol, const oid_t& oid,
		  const string& infile, int op_size)
{
  bufferlist indata;
  bool stdio = false;
  if (infile == "-")
    stdio = true;

  int ret;
  int fd = 0;
  if (!stdio)
    fd = open(infile.c_str(), O_RDONLY);
  if (fd < 0) {
    cerr << "error reading input file " << infile << ": "
	 << cpp_strerror(errno) << endl;
    return 1;
  }
  char *buf = new char[op_size];
  int count = op_size;
  uint64_t offset = 0;
  while (count != 0) {
    count = read(fd, buf, op_size);
    if (count < 0) {
      ret = -errno;
      cerr << "error reading input file " << infile << ": "
	   << cpp_strerror(ret) << endl;
      goto out;
    }
    if (count == 0) {
      if (!offset) {
	ret = o->create(oid, vol, true);
	if (ret < 0) {
	  cerr << "WARNING: could not create object: " << oid << endl;
	  goto out;
	}
      }
      continue;
    }
    indata.append(buf, count);
    if (offset == 0)
      ret = o->write_full(oid, vol, indata);
    else
      ret = o->write(oid, vol, offset, count, indata);
    indata.clear();

    if (ret < 0) {
      goto out;
    }
    offset += count;
  }
  ret = 0;
 out:
  VOID_TEMP_FAILURE_RETRY(close(fd));
  delete[] buf;
  return ret;
}

static const char alphanum_table[]
= "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/* size should be the required string size + 1 */
int gen_rand_alphanumeric(char *dest, int size)
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    cerr << "cannot get random bytes: " << cpp_strerror(ret) << endl;
    return -1;
  }

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_table[pos & 63];
  }
  dest[i] = '\0';

  return 0;
}

struct obj_info {
  oid_t name;
  size_t len;
};

class LoadGen {
  size_t total_sent;
  size_t total_completed;

  Objecter* o;
  AVolRef v;

  map<int, obj_info> objs;

  ceph::mono_time start_time;

public:
  int read_percent;
  int num_objs;
  size_t min_obj_len;
  uint64_t max_obj_len;
  size_t min_op_len;
  size_t max_op_len;
  size_t max_ops;
  size_t max_backlog;
  size_t target_throughput;
  ceph::timespan run_length;

  enum {
    OP_READ,
    OP_WRITE,
  };

  struct LoadGenOp : public bi::slist_base_hook<> {
    int type;
    oid_t oid;
    size_t off;
    size_t len;
    bufferlist bl;
    LoadGen& lg;

    LoadGenOp(LoadGen& _lg) : lg(_lg) {
      int i = get_random(0, lg.objs.size() - 1);
      obj_info& info = lg.objs[i];
      oid = info.name;

      len = get_random(lg.min_op_len, lg.max_op_len);
      if (len > info.len)
	len = info.len;
      off = get_random(0, info.len);

      if (off + len > info.len)
	off = info.len - len;

      i = get_random(1, 100);
      if (i > lg.read_percent)
	type = OP_WRITE;
      else
	type = OP_READ;

      cout << (type == OP_READ ? "READ" : "WRITE") << " : oid="
	   << oid << " off=" << off << " len=" << len << endl;
    }

    LoadGenOp() = delete;
    LoadGenOp(const LoadGenOp&) = delete;
    LoadGenOp(LoadGenOp&&) = delete;
    LoadGenOp& operator =(const LoadGenOp&) = delete;
    LoadGenOp& operator =(LoadGenOp&&) = delete;

    void run() {
      switch (type) {
      case OP_READ:
	lg.o->read(oid, lg.v, off, len, &bl, std::ref(*this));
	break;
      case OP_WRITE:
	bufferptr p = buffer::create(len);
	memset(p.c_str(), 0, len);
	bl.push_back(p);

	lg.o->write(oid, lg.v, off, len, bl, nullptr, std::ref(*this));
	break;
      }
      lg.total_sent += len;
    }

    void operator()(int r) {
      lg.total_completed += len;

      LoadGen::unique_lock l(lg.lock);

      double rate = (double)lg.cur_completed_rate() / (1024 * 1024);
      cout.precision(3);
      cout << "op completed, throughput=" << rate << "MB/sec" << endl;

      lg.pending_ops.erase(lg.pending_ops.iterator_to(*this));
      lg.cond.notify_all();
      l.unlock();

      delete this;
    }
  };

  int max_op;

  bi::slist<LoadGenOp> pending_ops;

  void gen_op(LoadGenOp *op);
  uint64_t gen_next_op();

  uint64_t cur_sent_rate() {
    return total_sent / time_passed();
  }

  uint64_t cur_completed_rate() {
    return total_completed / time_passed();
  }

  uint64_t total_expected() {
    return target_throughput * time_passed();
  }

  float time_passed() {
    std::chrono::duration<double> elapsed = ceph::mono_clock::now() -
      start_time;
    return elapsed.count();
  }

  std::mutex lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  std::condition_variable cond;

  LoadGen(Objecter* _o, AVolRef& _v) : o(_o), v(_v) {
    read_percent = 80;
    min_obj_len = 1024;
    max_obj_len = 5ull * 1024ull * 1024ull * 1024ull;
    min_op_len = 1024;
    target_throughput = 5 * 1024 * 1024; // B/sec
    max_op_len = 2 * 1024 * 1024;
    max_backlog = target_throughput * 2;
    run_length = 60s;

    total_sent = 0;
    total_completed = 0;
    num_objs = 200;
    max_op = 16;
  }
  int bootstrap();
  int run();
  void cleanup();
};

int LoadGen::bootstrap()
{
  char buf[128];
  int i;

  int buf_len = 1;
  bufferptr p = buffer::create(buf_len);
  bufferlist bl;
  memset(p.c_str(), 0, buf_len);
  bl.push_back(p);

  list<CB_Waiter> completions;
  for (i = 0; i < num_objs; i++) {
    obj_info info;
    gen_rand_alphanumeric(buf, 16);
    info.name.name = "oid-";
    info.name.name.append(buf);
    info.len = get_random(min_obj_len, max_obj_len);

    // throttle...
    while (completions.size() > max_ops) {
      CB_Waiter& c = completions.front();
      int ret = c.wait();
      completions.pop_front();
      if (ret < 0) {
	cerr << "aio_write failed" << endl;
	return ret;
      }
    }

    completions.emplace_back();
    CB_Waiter& c = completions.front();
    // generate object
    o->write(info.name, v, info.len - buf_len, buf_len,
	     bl, nullptr, c);
    objs[i] = info;
  }

  for (auto& c : completions) {
    int ret = c.wait();
    if (ret < 0) {
      cerr << "aio_write failed" << endl;
      return ret;
    }
  }
  return 0;
}

uint64_t LoadGen::gen_next_op()
{
  unique_lock l(lock);

  LoadGenOp& op = *(new LoadGenOp(*this));
  pending_ops.push_front(op);
  l.unlock();

  op.run();

  return op.len;
}

int LoadGen::run()
{
  start_time = ceph::mono_clock::now();
  auto end_time = start_time;
  end_time += run_length;
  auto stamp_time = start_time;
  ceph::timespan total_time = 0s;

  unique_lock l(lock, std::defer_lock);
  while (1) {
    l.lock();
    cond.wait_for(l, 1s);
    l.unlock();
    auto now = ceph::mono_clock::now();

    if (now > end_time)
      break;

    uint64_t expected = total_expected();
    l.lock();
    uint64_t sent = total_sent;
    uint64_t completed = total_completed;
    l.unlock();

    if (now - stamp_time >= 1s) {
      double rate = (double)cur_completed_rate() / (1024 * 1024);
      total_time += 1s;
      cout.precision(3);
      cout << std::setw(5) << total_time << ": throughput=" << rate
	   << "MB/sec" << " pending data=" << sent - completed << endl;
      stamp_time = now;
    }

    while (sent < expected &&
	   sent - completed < max_backlog &&
	   pending_ops.size() < max_ops) {
      sent += gen_next_op();
    }
  }

  l.lock();
  cond.wait(l, [this](){ return pending_ops.empty(); });

  return 0;
}

void LoadGen::cleanup()
{
  cout << "cleaning up objects" << endl;
  for (auto& info : objs) {
    int ret = o->remove(info.second.name, v);
    if (ret < 0)
      cerr << "couldn't remove oid: " << info.second.name << " ret=" << ret
	   << endl;
  }
}


class RadosBencher : public ObjBencher {
  class RB_Completion {
    std::mutex lock;
    std::condition_variable cond;
    int r;
    bool done;

    typedef void (*cb_t)(void*, void*);
    cb_t cb;
    void* arg;

  public:

    RB_Completion() : r(0), done(false), cb(nullptr), arg(nullptr) { }
    RB_Completion(const RB_Completion&) = delete;
    RB_Completion(RB_Completion&&) = delete;

    RB_Completion& operator=(const RB_Completion&) = delete;
    RB_Completion& operator=(RB_Completion&&) = delete;

    int wait() {
      std::unique_lock<std::mutex> l(lock);
      cond.wait(l, [this](){ return done; });
      return r;
    }

    bool safe() {
      std::unique_lock<std::mutex> l(lock);
      return done;
    }

    void operator()(int _r) {
      std::unique_lock<std::mutex> l(lock);
      done = true;
      r = _r;
      cond.notify_all();
      l.unlock();
      if (cb)
	cb(static_cast<void*>(this), arg);
    }

    void reset(cb_t _cb, void* _arg) {
      std::unique_lock<std::mutex> l(lock);
      cb = _cb;
      arg = _arg;
      done = false;
      r = 0;
    }

    operator rados::op_callback() {
      return std::ref(*this);
    }
  };

  RB_Completion* completions;
  Objecter* o;
  AVolRef v;
protected:
  int completions_init(int concurrentios) {
    completions = new RB_Completion[concurrentios];
    return 0;
  }
  void completions_done() {
    delete[] completions;
    completions = nullptr;
  }
  int create_completion(int slot, void (*cb)(void *, void*), void *arg) {
    completions[slot].reset(cb, arg);

    return 0;
  }
  void release_completion(int slot) {
    completions[slot].reset(nullptr, nullptr);
  }

  int aio_read(const string& oid, int slot, bufferlist *pbl, size_t len) {
    return o->read(oid_t(oid), v, 0, len, pbl, completions[slot]);
  }

  int aio_write(const string& oid, int slot, bufferlist& bl, size_t len) {
    return o->write(oid_t(oid), v, 0, len, bl, nullptr, completions[slot]);
  }

  int aio_remove(const string& oid, int slot) {
    return o->remove(oid_t(oid), v, nullptr, completions[slot]);
  }

  int sync_read(const string& oid, bufferlist& bl, size_t len) {
    return o->read(oid_t(oid), v, 0, len, &bl);
  }
  int sync_write(const string& oid, bufferlist& bl, size_t len) {
    return o->write_full(oid_t(oid), v, bl);
  }

  int sync_remove(const string& oid) {
    return o->remove(oid_t(oid), v);
  }

  bool completion_is_done(int slot) {
    return completions[slot].safe();
  }

  int completion_wait(int slot) {
    return completions[slot].wait();
  }
  int completion_ret(int slot) {
    return completions[slot].wait();
  }

public:
  RadosBencher(CephContext *cct_, Objecter* _o, AVolRef _v)
    : ObjBencher(cct_), completions(nullptr), o(_o), v(_v) {}
  ~RadosBencher() { }
};

static int do_lock_cmd(std::vector<const char*> &nargs,
		       const std::map < std::string, std::string > &opts,
		       Objecter* o, AVolRef v,
		       const boost::scoped_ptr<Formatter>& formatter)
{
  if (nargs.size() < 3)
    usage_exit();

  string cmd(nargs[1]);
  oid_t oid(nargs[2]);

  string lock_tag;
  string lock_cookie;
  string lock_description;
  int lock_duration = 0;
  ClsLockType lock_type = LOCK_EXCLUSIVE;

  map<string, string>::const_iterator i;
  i = opts.find("lock-tag");
  if (i != opts.end()) {
    lock_tag = i->second;
  }
  i = opts.find("lock-cookie");
  if (i != opts.end()) {
    lock_cookie = i->second;
  }
  i = opts.find("lock-description");
  if (i != opts.end()) {
    lock_description = i->second;
  }
  i = opts.find("lock-duration");
  if (i != opts.end()) {
    lock_duration = strtol(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("lock-type");
  if (i != opts.end()) {
    const string& type_str = i->second;
    if (type_str.compare("exclusive") == 0) {
      lock_type = LOCK_EXCLUSIVE;
    } else if (type_str.compare("shared") == 0) {
      lock_type = LOCK_SHARED;
    } else {
      cerr << "unknown lock type was specified, aborting" << endl;
      return -EINVAL;
    }
  }

  if (cmd.compare("list") == 0) {
    list<string> locks;
    int ret = rados::cls::lock::list_locks(o, v, oid, locks);
    if (ret < 0) {
      cerr << "ERROR: rados_list_locks(): " << cpp_strerror(ret) << endl;
      return ret;
    }

    formatter->open_object_section("object");
    formatter->dump_string("objname", oid.name);
    formatter->open_array_section("locks");
    list<string>::iterator iter;
    for (iter = locks.begin(); iter != locks.end(); ++iter) {
      formatter->open_object_section("lock");
      formatter->dump_string("name", *iter);
      formatter->close_section();
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
    return 0;
  }

  if (nargs.size() < 4)
    usage_exit();

  string lock_name(nargs[3]);

  if (cmd.compare("info") == 0) {
    map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> lockers;
    ClsLockType type = LOCK_NONE;
    string tag;
    int ret = rados::cls::lock::get_lock_info(o, v, oid, lock_name, &lockers,
					      &type, &tag);
    if (ret < 0) {
      cerr << "ERROR: rados_lock_get_lock_info(): " << cpp_strerror(ret) << endl;
      return ret;
    }

    formatter->open_object_section("lock");
    formatter->dump_string("name", lock_name);
    formatter->dump_string("type", cls_lock_type_str(type));
    formatter->dump_string("tag", tag);
    formatter->open_array_section("lockers");
    map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t>::iterator iter;
    for (iter = lockers.begin(); iter != lockers.end(); ++iter) {
      const rados::cls::lock::locker_id_t& id = iter->first;
      const rados::cls::lock::locker_info_t& info = iter->second;
      formatter->open_object_section("locker");
      formatter->dump_stream("name") << id.locker;
      formatter->dump_string("cookie", id.cookie);
      formatter->dump_string("description", info.description);
      formatter->dump_stream("expiration") << info.expiration;
      formatter->dump_stream("addr") << info.addr;
      formatter->close_section();
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

    return ret;
  } else if (cmd.compare("get") == 0) {
    rados::cls::lock::Lock l(lock_name);
    l.set_cookie(lock_cookie);
    l.set_tag(lock_tag);
    l.set_duration(lock_duration * 1s);
    l.set_description(lock_description);
    int ret;
    switch (lock_type) {
    case LOCK_SHARED:
      ret = l.lock_shared(o, v, oid);
      break;
    default:
      ret = l.lock_exclusive(o, v, oid);
    }
    if (ret < 0) {
      cerr << "ERROR: failed locking: " << cpp_strerror(ret) << endl;
      return ret;
    }

    return ret;
  }

  if (nargs.size() < 5)
    usage_exit();

  if (cmd.compare("break") == 0) {
    string locker(nargs[4]);
    rados::cls::lock::Lock l(lock_name);
    l.set_cookie(lock_cookie);
    l.set_tag(lock_tag);
    entity_name_t name;
    if (!name.parse(locker)) {
      cerr << "ERROR: failed to parse locker name (" << locker << ")" << endl;
      return -EINVAL;
    }
    int ret = l.break_lock(o, v, oid, name);
    if (ret < 0) {
      cerr << "ERROR: failed breaking lock: " << cpp_strerror(ret) << endl;
      return ret;
    }
  } else {
    usage_exit();
  }

  return 0;
}

/**********************************************

**********************************************/
static int rados_tool_common(const std::map < std::string, std::string > &opts,
			     std::vector<const char*> &nargs)
{
  int ret;
  string vol_name;
  string target_vol_name;
  int concurrent_ios = 16;
  int op_size = 1 << 22;
  bool cleanup = true;
  std::map<std::string, std::string>::const_iterator i;

  uint64_t min_obj_len = 0;
  uint64_t max_obj_len = 0;
  uint64_t min_op_len = 0;
  uint64_t max_op_len = 0;
  uint64_t max_ops = 0;
  uint64_t max_backlog = 0;
  uint64_t target_throughput = 0;
  int64_t read_percent = -1;
  uint64_t num_objs = 0;
  ceph::timespan run_length = 0ns;

  bool show_time = false;

  string run_name;
  string prefix;

  boost::scoped_ptr<Formatter> formatter;
  bool pretty_format = false;

  i = opts.find("volume");
  if (i != opts.end()) {
    vol_name = i->second;
  }
  i = opts.find("target_volume");
  if (i != opts.end()) {
    target_vol_name = i->second;
  }
  i = opts.find("concurrent-ios");
  if (i != opts.end()) {
    concurrent_ios = strtol(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("run-name");
  if (i != opts.end()) {
    run_name = i->second;
  }
  i = opts.find("prefix");
  if (i != opts.end()) {
    prefix = i->second;
  }
  i = opts.find("block-size");
  if (i != opts.end()) {
    op_size = strtol(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("min-object-size");
  if (i != opts.end()) {
    min_obj_len = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("max-object-size");
  if (i != opts.end()) {
    max_obj_len = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("min-op-len");
  if (i != opts.end()) {
    min_op_len = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("max-op-len");
  if (i != opts.end()) {
    max_op_len = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("max-ops");
  if (i != opts.end()) {
    max_ops = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("max-backlog");
  if (i != opts.end()) {
    max_backlog = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("target-throughput");
  if (i != opts.end()) {
    target_throughput = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("read-percent");
  if (i != opts.end()) {
    read_percent = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("num-objects");
  if (i != opts.end()) {
    num_objs = strtoll(i->second.c_str(), nullptr, 10);
  }
  i = opts.find("run-length");
  if (i != opts.end()) {
    run_length = strtol(i->second.c_str(), nullptr, 10) * 1s;
  }
  i = opts.find("show-time");
  if (i != opts.end()) {
    show_time = true;
  }
  i = opts.find("no-cleanup");
  if (i != opts.end()) {
    cleanup = false;
  }
  i = opts.find("pretty-format");
  if (i != opts.end()) {
    pretty_format = true;
  }
  i = opts.find("format");
  if (i != opts.end()) {
    const string format(i->second);
    if (format == "xml")
      formatter.reset(new XMLFormatter(pretty_format));
    else if (format == "json")
      formatter.reset(new JSONFormatter(pretty_format));
    else {
      cerr << "unrecognized format: " << format << endl;
      return -EINVAL;
    }
  }

  // open rados
  RadosClient rc(cct);

  ret = rc.connect();
  if (ret) {
     cerr << "couldn't connect to cluster! error " << ret << endl;
     return 1;
  }

  Objecter* o = rc.objecter;
  AVolRef v;

  if (!vol_name.empty()) {
    try {
    v = rc.attach_volume(vol_name);
    } catch (std::system_error& e) {
      cerr << "error opening volume " << vol_name << ":" << e.what() << endl;
      return 1;
    }
  }

  assert(!nargs.empty());

  if (strcmp(nargs[0], "stat") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    oid_t oid(nargs[1]);
    uint64_t size;
    ceph::real_time mtime;
    ret = o->stat(oid, v, &size, &mtime);
    if (ret < 0) {
      cerr << " error stat-ing " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << endl;
      return 1;
    } else {
      cout << vol_name << "/" << oid
	   << " mtime " << mtime << ", size " << size << endl;
    }
  }
  else if (strcmp(nargs[0], "get") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();
    ret = do_get(o, v, nargs[1], nargs[2], op_size);
    if (ret < 0) {
      cerr << "error getting " << vol_name << "/" << nargs[1] << ": "
	   << cpp_strerror(ret) << endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "put") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();
    ret = do_put(o, v, nargs[1], nargs[2], op_size);
    if (ret < 0) {
      cerr << "error putting " << vol_name << "/" << nargs[1] << ": "
	   << cpp_strerror(ret) << endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "truncate") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    oid_t oid(nargs[1]);
    long size = atol(nargs[2]);
    if (size < 0) {
      cerr << "error, cannot truncate to negative value" << endl;
      usage_exit();
    }
    ret = o->trunc(oid, v, size);
    if (ret < 0) {
      cerr << "error truncating oid_t "
	   << oid << " to " << size << ": "
	   << cpp_strerror(ret) << endl;
    }
  }
  else if (strcmp(nargs[0], "setxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 4)
      usage_exit();

    oid_t oid(nargs[1]);
    string attr_name(nargs[2]);
    string attr_val(nargs[3]);

    bufferlist bl(attr_val);

    ret = o->setxattr(oid, v, attr_name, bl);
    if (ret < 0) {
      cerr << "error setting xattr " << vol_name << "/" << oid << "/"
	   << attr_name << ": " << cpp_strerror(ret) << endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "getxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    oid_t oid(nargs[1]);
    string attr_name(nargs[2]);

    bufferlist bl;
    ret = o->getxattr(oid, v, attr_name, &bl);
    if (ret < 0) {
      cerr << "error getting xattr " << vol_name << "/" << oid << "/"
	   << attr_name << ": " << cpp_strerror(ret) << endl;
      return 1;
    }
    string s(bl);
    cout << s << endl;
  } else if (strcmp(nargs[0], "rmxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    oid_t oid(nargs[1]);
    string attr_name(nargs[2]);

    ret = o->removexattr(oid, v, attr_name);
    if (ret < 0) {
      cerr << "error removing xattr " << vol_name << "/" << oid << "/"
	   << attr_name << ": " << cpp_strerror(ret) << endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "listxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    oid_t oid(nargs[1]);
    map<std::string, bufferlist> attrset;
    bufferlist bl;
    ret = o->getxattrs(oid, v, attrset);
    if (ret < 0) {
      cerr << "error getting xattr set " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << endl;
      return 1;
    }

    for (map<std::string, bufferlist>::iterator iter = attrset.begin();
	 iter != attrset.end(); ++iter) {
      cout << iter->first << endl;
    }
  } else if (strcmp(nargs[0], "getomapheader") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    oid_t oid(nargs[1]);
    string outfile;
    if (nargs.size() >= 3) {
      outfile = nargs[2];
    }

    bufferlist header;
    ret = o->omap_get_header(oid, v, &header);
    if (ret < 0) {
      cerr << "error getting omap header " << vol_name << "/" << oid
	   << ": " << cpp_strerror(ret) << endl;
      return 1;
    } else {
      if (!outfile.empty()) {
	cerr << "Writing to " << outfile << endl;
	dump_data(outfile, header);
      } else {
	cout << "header (" << header.length() << " bytes) :\n";
	header.hexdump(cout);
	cout << endl;
      }
    }
  } else if (strcmp(nargs[0], "setomapheader") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    oid_t oid(nargs[1]);
    string val(nargs[2]);

    bufferlist bl;
    bl.append(val);

    ret = o->omap_set_header(oid, v, bl);
    if (ret < 0) {
      cerr << "error setting omap value " << vol_name << "/" << oid
	   << ": " << cpp_strerror(ret) << endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "setomapval") == 0) {
    if (vol_name.empty() || nargs.size() < 4)
      usage_exit();

    oid_t oid(nargs[1]);
    string key(nargs[2]);
    string val(nargs[3]);

    map<string, bufferlist> values;
    bufferlist bl;
    bl.append(val);
    values[key] = bl;

    ret = o->omap_set(oid, v, values);
    if (ret < 0) {
      cerr << "error setting omap value " << vol_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "getomapval") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    oid_t oid(nargs[1]);
    string key(nargs[2]);
    std::set<string> keys;
    keys.insert(key);

    std::string outfile;
    if (nargs.size() >= 4) {
      outfile = nargs[3];
    }

    map<string, bufferlist> values;
    ret = o->omap_get_vals_by_keys(oid, v, keys, values);
    if (ret < 0) {
      cerr << "error getting omap value " << vol_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << endl;
      return 1;
    }

    if (values.size() && values.begin()->first == key) {
      cout << " (length " << values.begin()->second.length() << ") : ";
      if (!outfile.empty()) {
	cerr << "Writing to " << outfile << endl;
	dump_data(outfile, values.begin()->second);
      } else {
	values.begin()->second.hexdump(cout);
	cout << endl;
      }
    } else {
      cout << "No such key: " << vol_name << "/" << oid << "/" << key
	   << endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "rmomapkey") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    oid_t oid(nargs[1]);
    string key(nargs[2]);
    std::set<string> keys;
    keys.insert(key);

    ret = o->omap_rm_keys(oid, v, keys);
    if (ret < 0) {
      cerr << "error removing omap key " << vol_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "listomapvals") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    oid_t oid(nargs[1]);
    string last_read = "";
    int MAX_READ = 512;
    do {
      map<string, bufferlist> values;
      ret = o->omap_get_vals(oid, v, last_read, string(), MAX_READ, values);
      if (ret < 0) {
	cerr << "error getting omap keys " << vol_name << "/" << oid << ": "
	     << cpp_strerror(ret) << endl;
	return 1;
      }
      ret = values.size();
      for (map<string, bufferlist>::const_iterator it = values.begin();
	   it != values.end(); ++it) {
	last_read = it->first;
	// dump key in hex if it contains nonprintable characters
	if (std::count_if(it->first.begin(), it->first.end(),
	    (int (*)(int))isprint) < (int)it->first.length()) {
	  cout << "key: (" << it->first.length() << " bytes):\n";
	  bufferlist keybl;
	  keybl.append(it->first);
	  keybl.hexdump(cout);
	} else {
	  cout << it->first;
	}
	cout << endl;
	cout << "value: (" << it->second.length() << " bytes) :\n";
	it->second.hexdump(cout);
	cout << endl;
      }
    } while (ret == MAX_READ);
  }
  else if (strcmp(nargs[0], "cp") == 0) {
    if (vol_name.empty())
      usage_exit();

    if (nargs.size() < 2 || nargs.size() > 3)
      usage_exit();

    string target = target_vol_name;
    if (target.empty())
      target = vol_name;

    oid_t target_obj;
    if (nargs.size() < 3) {
      if (target == vol_name) {
	cerr << "cannot copy object into itself" << endl;
	ret = -1;
	return 1;
      }
      target_obj = nargs[1];
    } else {
      target_obj = nargs[2];
    }

    // open io context.
    AVolRef target_vol;
    try {
      target_vol = rc.attach_volume(target);
    } catch (std::system_error& e) {
      cerr << "error opening target volume " << target << ": "
	   << e.what() << endl;
      return 1;
    }

    ret = do_copy(o, v, nargs[1], target_vol, target_obj);
    if (ret < 0) {
      cerr << "error copying " << vol_name << "/" << nargs[1] << " => "
	   << target << "/" << target_obj << ": " << cpp_strerror(ret)
	   << endl;
      return 1;
    }
  }
   else if (strcmp(nargs[0], "rm") == 0) {
     if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    vector<const char *>::iterator iter = nargs.begin();
    ++iter;
    for (; iter != nargs.end(); ++iter) {
      oid_t oid(*iter);
      ret = o->remove(oid, v);
      if (ret < 0) {
	cerr << "error removing " << vol_name << "/" << oid << ": "
	     << cpp_strerror(ret) << endl;
	return 1;
      }
    }
  }
  else if (strcmp(nargs[0], "create") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    oid_t oid(nargs[1]);
    ret = o->create(oid, v, true);
    if (ret < 0) {
      cerr << "error creating " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << endl;
      return 1;
    }
  }

  else if (strcmp(nargs[0], "bench") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();
    int seconds = atoi(nargs[1]);
    int operation = 0;
    if (strcmp(nargs[2], "write") == 0)
      operation = OP_WRITE;
    else if (strcmp(nargs[2], "seq") == 0)
      operation = OP_SEQ_READ;
    else if (strcmp(nargs[2], "rand") == 0)
      operation = OP_RAND_READ;
    else
      usage_exit();
    RadosBencher bencher(cct, o, v);
    bencher.set_show_time(show_time);
    ret = bencher.aio_bench(operation, seconds, num_objs,
			    concurrent_ios, op_size, cleanup, run_name);
    if (ret != 0)
      cerr << "error during benchmark: " << ret << endl;
  }
  else if (strcmp(nargs[0], "cleanup") == 0) {
    if (vol_name.empty())
      usage_exit();
    RadosBencher bencher(cct, o, v);
    ret = bencher.clean_up(prefix, concurrent_ios, run_name);
    if (ret != 0)
      cerr << "error during cleanup: " << ret << endl;
  } else if (strcmp(nargs[0], "set-alloc-hint") == 0) {
    if (vol_name.empty() || nargs.size() < 4)
      usage_exit();
    string err;
    oid_t oid(nargs[1]);
    uint64_t expected_object_size = strict_strtoll(nargs[2], 10, &err);
    if (!err.empty()) {
      cerr << "couldn't parse expected_object_size: " << err << endl;
      usage_exit();
    }
    uint64_t expected_write_size = strict_strtoll(nargs[3], 10, &err);
    if (!err.empty()) {
      cerr << "couldn't parse expected_write_size: " << err << endl;
      usage_exit();
    }
    ret = o->set_alloc_hint(oid, v, expected_object_size, expected_write_size);
    if (ret < 0) {
      cerr << "error setting alloc-hint " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "load-gen") == 0) {
    if (vol_name.empty()) {
      cerr << "error: must specify volume" << endl;
      usage_exit();
    }
    LoadGen lg(o, v);
    if (min_obj_len)
      lg.min_obj_len = min_obj_len;
    if (max_obj_len)
      lg.max_obj_len = max_obj_len;
    if (min_op_len)
      lg.min_op_len = min_op_len;
    if (max_op_len)
      lg.max_op_len = max_op_len;
    if (max_ops)
      lg.max_ops = max_ops;
    if (max_backlog)
      lg.max_backlog = max_backlog;
    if (target_throughput)
      lg.target_throughput = target_throughput << 20;
    if (read_percent >= 0)
      lg.read_percent = read_percent;
    if (num_objs)
      lg.num_objs = num_objs;
    if (run_length > 0ns)
      lg.run_length = run_length;

    cout << "run length " << run_length << " seconds" << endl;
    cout << "preparing " << lg.num_objs << " objects" << endl;
    ret = lg.bootstrap();
    if (ret < 0) {
      cerr << "load-gen bootstrap failed" << endl;
      exit(1);
    }
    cout << "load-gen will run " << lg.run_length << " seconds" << endl;
    lg.run();
    lg.cleanup();
  } else if (strcmp(nargs[0], "listomapkeys") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    ObjectOperation read(v->op());
    std::set<string> out_keys;
    read->omap_get_keys("", LONG_MAX, &out_keys, &ret);
    oid_t oid(nargs[1]);
    ret = o->read(oid, v, read);
    if (ret < 0) {
      cerr << "error getting omap key set " << vol_name << "/"
	   << nargs[1] << ": "	<< cpp_strerror(ret) << endl;
      return 1;
    }

    for (auto key : out_keys) {
      cout << key << endl;
    }
  } else if (strcmp(nargs[0], "lock") == 0) {
    if (vol_name.empty())
      usage_exit();

    if (!formatter) {
      formatter.reset(new JSONFormatter(pretty_format));
    }
    ret = do_lock_cmd(nargs, opts, o, v, formatter);
  } else {
    cerr << "unrecognized command " << nargs[0] << "; -h or --help for usage" << endl;
    ret = -EINVAL;
    return 1;
  }

  if (ret < 0)
    cerr << "error " << (-ret) << ": " << cpp_strerror(ret) << endl;

  return 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  std::map < std::string, std::string > opts;
  std::vector<const char*>::iterator i;
  std::string val;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, &i, "-h", "--help", (char*)nullptr)) {
      usage(cout);
      exit(0);
    } else if (ceph_argparse_flag(args, &i, "-f", "--force", (char*)nullptr)) {
      opts["force"] = "true";
    } else if (ceph_argparse_flag(args, &i, "-d", "--delete-after", (char*)nullptr)) {
      opts["delete-after"] = "true";
    } else if (ceph_argparse_flag(args, &i, "--pretty-format", (char*)nullptr)) {
      opts["pretty-format"] = "true";
    } else if (ceph_argparse_flag(args, &i, "--show-time", (char*)nullptr)) {
      opts["show-time"] = "true";
    } else if (ceph_argparse_flag(args, &i, "--no-cleanup", (char*)nullptr)) {
      opts["no-cleanup"] = "true";
    } else if (ceph_argparse_witharg(args, i, &val, "--run-name", (char*)nullptr)) {
      opts["run-name"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--prefix", (char*)nullptr)) {
      opts["prefix"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-v", "--volume", (char*)nullptr)) {
      opts["volume"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-volume", (char*)nullptr)) {
      opts["target_volume"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-t", "--concurrent-ios", (char*)nullptr)) {
      opts["concurrent-ios"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)nullptr)) {
      opts["block-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", (char*)nullptr)) {
      opts["block-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-object-size", (char*)nullptr)) {
      opts["min-object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-object-size", (char*)nullptr)) {
      opts["max-object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-op-len", (char*)nullptr)) {
      opts["min-op-len"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-op-len", (char*)nullptr)) {
      opts["max-op-len"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-ops", (char*)nullptr)) {
      opts["max-ops"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-backlog", (char*)nullptr)) {
      opts["max-backlog"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-throughput", (char*)nullptr)) {
      opts["target-throughput"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--read-percent", (char*)nullptr)) {
      opts["read-percent"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--num-objects", (char*)nullptr)) {
      opts["num-objects"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--run-length", (char*)nullptr)) {
      opts["run-length"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--workers", (char*)nullptr)) {
      opts["workers"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)nullptr)) {
      opts["format"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-tag", (char*)nullptr)) {
      opts["lock-tag"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-cookie", (char*)nullptr)) {
      opts["lock-cookie"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-description", (char*)nullptr)) {
      opts["lock-description"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-duration", (char*)nullptr)) {
      opts["lock-duration"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-type", (char*)nullptr)) {
      opts["lock-type"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-N", "--namespace", (char*)nullptr)) {
      opts["namespace"] = val;
    } else {
      if (val[0] == '-')
	usage_exit();
      ++i;
    }
  }

  if (args.empty()) {
    cerr << "rados: you must give an action. Try --help" << endl;
    return 1;
  }
  return rados_tool_common(opts, args);
}
