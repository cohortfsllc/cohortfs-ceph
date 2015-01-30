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

#include <condition_variable>
#include <mutex>

#include "include/types.h"

#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"
using namespace librados;

#include "common/config.h"
#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/obj_bencher.h"
#include "auth/Crypto.h"
#include <iostream>
#include <fstream>

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <locale>

#include "cls/lock/cls_lock_client.h"
#include "include/compat.h"

using std::cout;
using std::cerr;

static CephContext* cct;

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
"   get <obj-name> [outfile]	     fetch object\n"
"   put <obj-name> [infile]	     write object\n"
"   truncate <obj-name> length	     truncate object\n"
"   create <obj-name>		     create object\n"
"   rm <obj-name> ...		     remove object(s)\n"
"   cp <obj-name> [target-obj]	     copy object\n"
"   listxattr <obj-name>\n"
"   getxattr <obj-name> attr\n"
"   setxattr <obj-name> attr val\n"
"   rmxattr <obj-name> attr\n"
"   stat objname		     stat the named object\n"
"   mapext <obj-name>\n"
"   bench <seconds> write|seq|rand [-t concurrent_operations] [--no-cleanup] [--run-name run_name]\n"
"				     default is 16 concurrent IOs and 4 MB ops\n"
"				     default is to clean up after write benchmark\n"
"				     default run-name is 'benchmark_last_metadata'\n"
"   cleanup [--run-name run_name] [--prefix prefix]\n"
"				     clean up a previous benchmark operation\n"
"				     default run-name is 'benchmark_last_metadata'\n"
"   load-gen [options]		     generate load on the cluster\n"
"   listomapkeys <obj-name>	     list the keys in the object map\n"
"   listomapvals <obj-name>	     list the keys and vals in the object map \n"
"   getomapval <obj-name> <key> [file] show the value for the specified key\n"
"				     in the object's object map\n"
"   setomapval <obj-name> <key> <val>\n"
"   rmomapkey <obj-name> <key>\n"
"   getomapheader <obj-name> [file]\n"
"   setomapheader <obj-name> <val>\n"
"   listwatchers <obj-name>	     list the watchers of this object\n"
"   set-alloc-hint <obj-name> <expected-object-size> <expected-write-size>\n"
"				     set allocation hint for an object\n"
"\n"
"ADVISORY LOCKS\n"
"   lock list <obj-name>\n"
"	List all advisory locks on an object\n"
"   lock get <obj-name> <lock-name>\n"
"	Try to acquire a lock\n"
"   lock break <obj-name> <lock-name> <locker-name>\n"
"	Try to break a lock acquired by another client\n"
"   lock info <obj-name> <lock-name>\n"
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
      cerr << "failed to open file: " << cpp_strerror(err) << std::endl;
      return -err;
    }
  }

  int r = data.write_fd(fd);

  if (fd != 1) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }

  return r;
}


static int do_get(IoCtx& io_ctx, const char *objname, const char *outfile, unsigned op_size)
{
  string oid(objname);

  int fd;
  if (strcmp(outfile, "-") == 0) {
    fd = 1;
  } else {
    fd = TEMP_FAILURE_RETRY(::open(outfile, O_WRONLY|O_CREAT|O_TRUNC, 0644));
    if (fd < 0) {
      int err = errno;
      cerr << "failed to open file: " << cpp_strerror(err) << std::endl;
      return -err;
    }
  }

  uint64_t offset = 0;
  int ret;
  while (true) {
    bufferlist outdata;
    ret = io_ctx.read(oid, outdata, op_size, offset);
    if (ret <= 0) {
      goto out;
    }
    ret = outdata.write_fd(fd);
    if (ret < 0) {
      cerr << "error writing to file: " << cpp_strerror(ret) << std::endl;
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

static int do_copy(IoCtx& io_ctx, const string& objname,
		   IoCtx& target_ctx, const string& target_obj)
{
  string oid(objname);
  bufferlist outdata;
  librados::ObjectReadOperation read_op(io_ctx);
  string start_after;

#define COPY_CHUNK_SIZE (4 * 1024 * 1024)
  read_op.read(0, COPY_CHUNK_SIZE, &outdata, NULL);

  map<std::string, bufferlist> attrset;
  read_op.getxattrs(attrset, NULL);

  bufferlist omap_header;
  read_op.omap_get_header(&omap_header, NULL);

#define OMAP_CHUNK 1000
  map<string, bufferlist> omap;
  read_op.omap_get_vals(start_after, OMAP_CHUNK, omap, NULL);

  bufferlist opbl;
  int ret = io_ctx.operate(oid, &read_op, &opbl);
  if (ret < 0) {
    return ret;
  }

  librados::ObjectWriteOperation write_op(io_ctx);
  string target_oid(target_obj);

  /* reset dest if exists */
  write_op.create(false);
  write_op.remove();

  write_op.write_full(outdata);
  write_op.omap_set_header(omap_header);

  map<std::string, bufferlist>::iterator iter;
  for (iter = attrset.begin(); iter != attrset.end(); ++iter) {
    write_op.setxattr(iter->first.c_str(), iter->second);
  }
  if (!omap.empty()) {
    write_op.omap_set(omap);
  }
  ret = target_ctx.operate(target_oid, &write_op);
  if (ret < 0) {
    return ret;
  }

  uint64_t off = 0;

  while (outdata.length() == COPY_CHUNK_SIZE) {
    off += outdata.length();
    outdata.clear();
    ret = io_ctx.read(oid, outdata, COPY_CHUNK_SIZE, off);
    if (ret < 0)
      goto err;

    ret = target_ctx.write(target_oid, outdata, outdata.length(), off);
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
    ret = io_ctx.omap_get_vals(oid, start_after, OMAP_CHUNK, omap);
    if (ret < 0)
      goto err;

    if (omap.empty())
      break;

    ret = target_ctx.omap_set(target_oid, omap);
    if (ret < 0)
      goto err;
  }

  return 0;

err:
  target_ctx.remove(target_oid);
  return ret;
}

static int do_put(IoCtx& io_ctx, const char *objname, const char *infile, int op_size)
{
  string oid(objname);
  bufferlist indata;
  bool stdio = false;
  if (strcmp(infile, "-") == 0)
    stdio = true;

  int ret;
  int fd = 0;
  if (!stdio)
    fd = open(infile, O_RDONLY);
  if (fd < 0) {
    cerr << "error reading input file " << infile << ": " << cpp_strerror(errno) << std::endl;
    return 1;
  }
  char *buf = new char[op_size];
  int count = op_size;
  uint64_t offset = 0;
  while (count != 0) {
    count = read(fd, buf, op_size);
    if (count < 0) {
      ret = -errno;
      cerr << "error reading input file " << infile << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    if (count == 0) {
      if (!offset) {
	ret = io_ctx.create(oid, true);
	if (ret < 0) {
	  cerr << "WARNING: could not create object: " << oid << std::endl;
	  goto out;
	}
      }
      continue;
    }
    indata.append(buf, count);
    if (offset == 0)
      ret = io_ctx.write_full(oid, indata);
    else
      ret = io_ctx.write(oid, indata, count, offset);
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

class RadosWatchCtx : public librados::WatchCtx {
  string name;
public:
  RadosWatchCtx(const char *imgname) : name(imgname) {}
  virtual ~RadosWatchCtx() {}
  virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    string s;
    try {
      bufferlist::iterator iter = bl.begin();
      ::decode(s, iter);
    } catch (buffer::error *err) {
      cout << "could not decode bufferlist, buffer length=" << bl.length() << std::endl;
    }
    cout << name << " got notification opcode=" << (int)opcode << " ver=" << ver << " msg='" << s << "'" << std::endl;
  }
};

static const char alphanum_table[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int gen_rand_alphanumeric(char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    cerr << "cannot get random bytes: " << cpp_strerror(ret) << std::endl;
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
  string name;
  size_t len;
};

class LoadGen {
  size_t total_sent;
  size_t total_completed;

  IoCtx io_ctx;
  Rados *rados;

  map<int, obj_info> objs;

  ceph::mono_time start_time;

  bool going_down;

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

  struct LoadGenOp {
    int id;
    int type;
    string oid;
    size_t off;
    size_t len;
    bufferlist bl;
    LoadGen *lg;
    librados::AioCompletion *completion;

    LoadGenOp() {}
    LoadGenOp(LoadGen *_lg) : lg(_lg), completion(NULL) {}
  };

  int max_op;

  map<int, LoadGenOp *> pending_ops;

  void gen_op(LoadGenOp *op);
  uint64_t gen_next_op();
  void run_op(LoadGenOp *op);

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
    ceph::timespan elapsed = ceph::mono_clock::now() - start_time;
    return ceph::span_to_double(elapsed);
  }

  std::mutex lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  std::condition_variable cond;

  LoadGen(Rados *_rados) : rados(_rados), going_down(false) {
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
  int bootstrap(const string &volume);
  int run();
  void cleanup();

  void io_cb(completion_t c, LoadGenOp *op) {
    total_completed += op->len;

    unique_lock l(lock);

    double rate = (double)cur_completed_rate() / (1024 * 1024);
    cout.precision(3);
    cout << "op " << op->id << " completed, throughput=" << rate  << "MB/sec" << std::endl;

    map<int, LoadGenOp *>::iterator iter = pending_ops.find(op->id);
    if (iter != pending_ops.end())
      pending_ops.erase(iter);

    if (!going_down)
      op->completion->release();

    delete op;

    cond.notify_all();
  }
};

static void _load_gen_cb(completion_t c, void *param)
{
  LoadGen::LoadGenOp *op = (LoadGen::LoadGenOp *)param;
  op->lg->io_cb(c, op);
}

int LoadGen::bootstrap(const string &volume)
{
  char buf[128];
  int i;

  if (volume.empty()) {
    cerr << "ERROR: volume name was not specified" << std::endl;
    return -EINVAL;
  }

  int ret = rados->ioctx_create(volume, io_ctx);
  if (ret < 0) {
    cerr << "error opening volume " << volume << ": "
	 << cpp_strerror(ret) << std::endl;
    return ret;
  }

  int buf_len = 1;
  bufferptr p = buffer::create(buf_len);
  bufferlist bl;
  memset(p.c_str(), 0, buf_len);
  bl.push_back(p);

  list<librados::AioCompletion *> completions;
  for (i = 0; i < num_objs; i++) {
    obj_info info;
    gen_rand_alphanumeric(buf, 16);
    info.name = "obj-";
    info.name.append(buf);
    info.len = get_random(min_obj_len, max_obj_len);

    // throttle...
    while (completions.size() > max_ops) {
      AioCompletion *c = completions.front();
      c->wait_for_complete();
      ret = c->get_return_value();
      c->release();
      completions.pop_front();
      if (ret < 0) {
	cerr << "aio_write failed" << std::endl;
	return ret;
      }
    }

    librados::AioCompletion *c = rados->aio_create_completion(NULL, NULL, NULL);
    completions.push_back(c);
    // generate object
    ret = io_ctx.aio_write(info.name, c, bl, buf_len, info.len - buf_len);
    if (ret < 0) {
      cerr << "couldn't write obj: " << info.name << " ret=" << ret << std::endl;
      return ret;
    }
    objs[i] = info;
  }

  list<librados::AioCompletion *>::iterator iter;
  for (iter = completions.begin(); iter != completions.end(); ++iter) {
    AioCompletion *c = *iter;
    c->wait_for_complete();
    ret = c->get_return_value();
    c->release();
    if (ret < 0) { // yes, we leak.
      cerr << "aio_write failed" << std::endl;
      return ret;
    }
  }
  return 0;
}

void LoadGen::run_op(LoadGenOp *op)
{
  op->completion = rados->aio_create_completion(op, _load_gen_cb, NULL);

  switch (op->type) {
  case OP_READ:
    io_ctx.aio_read(op->oid, op->completion, &op->bl, op->len, op->off);
    break;
  case OP_WRITE:
    bufferptr p = buffer::create(op->len);
    memset(p.c_str(), 0, op->len);
    op->bl.push_back(p);

    io_ctx.aio_write(op->oid, op->completion, op->bl, op->len, op->off);
    break;
  }

  total_sent += op->len;
}

void LoadGen::gen_op(LoadGenOp *op)
{
  int i = get_random(0, objs.size() - 1);
  obj_info& info = objs[i];
  op->oid = info.name;

  size_t len = get_random(min_op_len, max_op_len);
  if (len > info.len)
    len = info.len;
  size_t off = get_random(0, info.len);

  if (off + len > info.len)
    off = info.len - len;

  op->off = off;
  op->len = len;

  i = get_random(1, 100);
  if (i > read_percent)
    op->type = OP_WRITE;
  else
    op->type = OP_READ;

  cout << (op->type == OP_READ ? "READ" : "WRITE") << " : oid=" << op->oid << " off=" << op->off << " len=" << op->len << std::endl;
}

uint64_t LoadGen::gen_next_op()
{
  unique_lock l(lock);

  LoadGenOp *op = new LoadGenOp(this);
  gen_op(op);
  op->id = max_op++;
  pending_ops[op->id] = op;

  l.unlock();

  run_op(op);

  return op->len;
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
	   << "MB/sec" << " pending data=" << sent - completed << std::endl;
      stamp_time = now;
    }

    while (sent < expected &&
	   sent - completed < max_backlog &&
	   pending_ops.size() < max_ops) {
      sent += gen_next_op();
    }
  }

  // get a reference to all pending requests
  vector<librados::AioCompletion *> completions;
  l.lock();
  going_down = true;
  map<int, LoadGenOp *>::iterator iter;
  for (iter = pending_ops.begin(); iter != pending_ops.end(); ++iter) {
    LoadGenOp *op = iter->second;
    completions.push_back(op->completion);
  }
  l.unlock();

  cout << "waiting for all operations to complete" << std::endl;

  // now wait on all the pending requests
  for (vector<librados::AioCompletion *>::iterator citer = completions.begin(); citer != completions.end(); ++citer) {
    librados::AioCompletion *c = *citer;
    c->wait_for_complete();
    c->release();
  }

  return 0;
}

void LoadGen::cleanup()
{
  cout << "cleaning up objects" << std::endl;
  map<int, obj_info>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    obj_info& info = iter->second;
    int ret = io_ctx.remove(info.name);
    if (ret < 0)
      cerr << "couldn't remove obj: " << info.name << " ret=" << ret << std::endl;
  }
}


class RadosBencher : public ObjBencher {
  librados::AioCompletion **completions;
  librados::Rados& rados;
  librados::IoCtx& io_ctx;
protected:
  int completions_init(int concurrentios) {
    completions = new librados::AioCompletion *[concurrentios];
    return 0;
  }
  void completions_done() {
    delete[] completions;
    completions = NULL;
  }
  int create_completion(int slot, void (*cb)(void *, void*), void *arg) {
    completions[slot] = rados.aio_create_completion((void *) arg, 0, cb);

    if (!completions[slot])
      return -EINVAL;

    return 0;
  }
  void release_completion(int slot) {
    completions[slot]->release();
    completions[slot] = 0;
  }

  int aio_read(const std::string& oid, int slot, bufferlist *pbl, size_t len) {
    return io_ctx.aio_read(oid, completions[slot], pbl, len, 0);
  }

  int aio_write(const std::string& oid, int slot, bufferlist& bl, size_t len) {
    return io_ctx.aio_write(oid, completions[slot], bl, len, 0);
  }

  int aio_remove(const std::string& oid, int slot) {
    return io_ctx.aio_remove(oid, completions[slot]);
  }

  int sync_read(const std::string& oid, bufferlist& bl, size_t len) {
    return io_ctx.read(oid, bl, len, 0);
  }
  int sync_write(const std::string& oid, bufferlist& bl, size_t len) {
    return io_ctx.write_full(oid, bl);
  }

  int sync_remove(const std::string& oid) {
    return io_ctx.remove(oid);
  }

  bool completion_is_done(int slot) {
    return completions[slot]->is_safe();
  }

  int completion_wait(int slot) {
    return completions[slot]->wait_for_safe_and_cb();
  }
  int completion_ret(int slot) {
    return completions[slot]->get_return_value();
  }

public:
  RadosBencher(CephContext *cct_, librados::Rados& _r, librados::IoCtx& _i)
    : ObjBencher(cct_), completions(NULL), rados(_r), io_ctx(_i) {}
  ~RadosBencher() { }
};

static int do_lock_cmd(std::vector<const char*> &nargs,
		       const std::map < std::string, std::string > &opts,
		       IoCtx *ioctx,
		       Formatter *formatter)
{
  if (nargs.size() < 3)
    usage_exit();

  string cmd(nargs[1]);
  string oid(nargs[2]);

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
    lock_duration = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("lock-type");
  if (i != opts.end()) {
    const string& type_str = i->second;
    if (type_str.compare("exclusive") == 0) {
      lock_type = LOCK_EXCLUSIVE;
    } else if (type_str.compare("shared") == 0) {
      lock_type = LOCK_SHARED;
    } else {
      cerr << "unknown lock type was specified, aborting" << std::endl;
      return -EINVAL;
    }
  }

  if (cmd.compare("list") == 0) {
    list<string> locks;
    int ret = rados::cls::lock::list_locks(ioctx, oid, &locks);
    if (ret < 0) {
      cerr << "ERROR: rados_list_locks(): " << cpp_strerror(ret) << std::endl;
      return ret;
    }

    formatter->open_object_section("object");
    formatter->dump_string("objname", oid);
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
    int ret = rados::cls::lock::get_lock_info(ioctx, oid, lock_name, &lockers, &type, &tag);
    if (ret < 0) {
      cerr << "ERROR: rados_lock_get_lock_info(): " << cpp_strerror(ret) << std::endl;
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
      ret = l.lock_shared(ioctx, oid);
      break;
    default:
      ret = l.lock_exclusive(ioctx, oid);
    }
    if (ret < 0) {
      cerr << "ERROR: failed locking: " << cpp_strerror(ret) << std::endl;
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
      cerr << "ERROR: failed to parse locker name (" << locker << ")" << std::endl;
      return -EINVAL;
    }
    int ret = l.break_lock(ioctx, oid, name);
    if (ret < 0) {
      cerr << "ERROR: failed breaking lock: " << cpp_strerror(ret) << std::endl;
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

  const char* run_name = NULL;
  const char* prefix = NULL;

  Formatter *formatter = NULL;
  bool pretty_format = false;

  Rados rados;
  IoCtx io_ctx;

  i = opts.find("volume");
  if (i != opts.end()) {
    vol_name = i->second.c_str();
  }
  i = opts.find("target_volume");
  if (i != opts.end()) {
    target_vol_name = i->second.c_str();
  }
  i = opts.find("concurrent-ios");
  if (i != opts.end()) {
    concurrent_ios = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("run-name");
  if (i != opts.end()) {
    run_name = i->second.c_str();
  }
  i = opts.find("prefix");
  if (i != opts.end()) {
    prefix = i->second.c_str();
  }
  i = opts.find("block-size");
  if (i != opts.end()) {
    op_size = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("min-object-size");
  if (i != opts.end()) {
    min_obj_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-object-size");
  if (i != opts.end()) {
    max_obj_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("min-op-len");
  if (i != opts.end()) {
    min_op_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-op-len");
  if (i != opts.end()) {
    max_op_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-ops");
  if (i != opts.end()) {
    max_ops = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-backlog");
  if (i != opts.end()) {
    max_backlog = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("target-throughput");
  if (i != opts.end()) {
    target_throughput = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("read-percent");
  if (i != opts.end()) {
    read_percent = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("num-objects");
  if (i != opts.end()) {
    num_objs = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("run-length");
  if (i != opts.end()) {
    run_length = strtol(i->second.c_str(), NULL, 10) * 1s;
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
    const char *format = i->second.c_str();
    if (strcmp(format, "xml") == 0)
      formatter = new XMLFormatter(pretty_format);
    else if (strcmp(format, "json") == 0)
      formatter = new JSONFormatter(pretty_format);
    else {
      cerr << "unrecognized format: " << format << std::endl;
      return -EINVAL;
    }
  }

  // open rados
  ret = rados.init_with_context(cct);
  if (ret) {
     cerr << "couldn't initialize rados! error " << ret << std::endl;
     ret = -1;
     goto out;
  }

  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster! error " << ret << std::endl;
     ret = -1;
     goto out;
  }

  // open io context.
  if (!vol_name.empty()) {
    ret = rados.ioctx_create(vol_name, io_ctx);
    if (ret < 0) {
      cerr << "error opening volume " << vol_name << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    }
  }

  assert(!nargs.empty());

  if (strcmp(nargs[0], "stat") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    uint64_t size;
    time_t mtime;
    ret = io_ctx.stat(oid, &size, &mtime);
    if (ret < 0) {
      cerr << " error stat-ing " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      cout << vol_name << "/" << oid
	   << " mtime " << mtime << ", size " << size << std::endl;
    }
  }
  else if (strcmp(nargs[0], "get") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();
    ret = do_get(io_ctx, nargs[1], nargs[2], op_size);
    if (ret < 0) {
      cerr << "error getting " << vol_name << "/" << nargs[1] << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "put") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();
    ret = do_put(io_ctx, nargs[1], nargs[2], op_size);
    if (ret < 0) {
      cerr << "error putting " << vol_name << "/" << nargs[1] << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "truncate") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    long size = atol(nargs[2]);
    if (size < 0) {
      cerr << "error, cannot truncate to negative value" << std::endl;
      usage_exit();
    }
    ret = io_ctx.trunc(oid, size);
    if (ret < 0) {
      cerr << "error truncating oid "
	   << oid << " to " << size << ": "
	   << cpp_strerror(ret) << std::endl;
    } else {
      ret = 0;
    }
  }
  else if (strcmp(nargs[0], "setxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 4)
      usage_exit();

    string oid(nargs[1]);
    string attr_name(nargs[2]);
    string attr_val(nargs[3]);

    bufferlist bl;
    bl.append(attr_val.c_str(), attr_val.length());

    ret = io_ctx.setxattr(oid, attr_name.c_str(), bl);
    if (ret < 0) {
      cerr << "error setting xattr " << vol_name << "/" << oid << "/"
	   << attr_name << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    else
      ret = 0;
  }
  else if (strcmp(nargs[0], "getxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string attr_name(nargs[2]);

    bufferlist bl;
    ret = io_ctx.getxattr(oid, attr_name.c_str(), bl);
    if (ret < 0) {
      cerr << "error getting xattr " << vol_name << "/" << oid << "/"
	   << attr_name << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    else
      ret = 0;
    string s(bl.c_str(), bl.length());
    cout << s << std::endl;
  } else if (strcmp(nargs[0], "rmxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string attr_name(nargs[2]);

    ret = io_ctx.rmxattr(oid, attr_name.c_str());
    if (ret < 0) {
      cerr << "error removing xattr " << vol_name << "/" << oid << "/"
	   << attr_name << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
  } else if (strcmp(nargs[0], "listxattr") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    map<std::string, bufferlist> attrset;
    bufferlist bl;
    ret = io_ctx.getxattrs(oid, attrset);
    if (ret < 0) {
      cerr << "error getting xattr set " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    }

    for (map<std::string, bufferlist>::iterator iter = attrset.begin();
	 iter != attrset.end(); ++iter) {
      cout << iter->first << std::endl;
    }
  } else if (strcmp(nargs[0], "getomapheader") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    string outfile;
    if (nargs.size() >= 3) {
      outfile = nargs[2];
    }

    bufferlist header;
    ret = io_ctx.omap_get_header(oid, &header);
    if (ret < 0) {
      cerr << "error getting omap header " << vol_name << "/" << oid
	   << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      if (!outfile.empty()) {
	cerr << "Writing to " << outfile << std::endl;
	dump_data(outfile, header);
      } else {
	cout << "header (" << header.length() << " bytes) :\n";
	header.hexdump(cout);
	cout << std::endl;
      }
      ret = 0;
    }
  } else if (strcmp(nargs[0], "setomapheader") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string val(nargs[2]);

    bufferlist bl;
    bl.append(val);

    ret = io_ctx.omap_set_header(oid, bl);
    if (ret < 0) {
      cerr << "error setting omap value " << vol_name << "/" << oid
	   << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }
  } else if (strcmp(nargs[0], "setomapval") == 0) {
    if (vol_name.empty() || nargs.size() < 4)
      usage_exit();

    string oid(nargs[1]);
    string key(nargs[2]);
    string val(nargs[3]);

    map<string, bufferlist> values;
    bufferlist bl;
    bl.append(val);
    values[key] = bl;

    ret = io_ctx.omap_set(oid, values);
    if (ret < 0) {
      cerr << "error setting omap value " << vol_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }
  } else if (strcmp(nargs[0], "getomapval") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string key(nargs[2]);
    set<string> keys;
    keys.insert(key);

    std::string outfile;
    if (nargs.size() >= 4) {
      outfile = nargs[3];
    }

    map<string, bufferlist> values;
    ret = io_ctx.omap_get_vals_by_keys(oid, keys, values);
    if (ret < 0) {
      cerr << "error getting omap value " << vol_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }

    if (values.size() && values.begin()->first == key) {
      cout << " (length " << values.begin()->second.length() << ") : ";
      if (!outfile.empty()) {
	cerr << "Writing to " << outfile << std::endl;
	dump_data(outfile, values.begin()->second);
      } else {
	values.begin()->second.hexdump(cout);
	cout << std::endl;
      }
      ret = 0;
    } else {
      cout << "No such key: " << vol_name << "/" << oid << "/" << key
	   << std::endl;
      ret = -1;
      goto out;
    }
  } else if (strcmp(nargs[0], "rmomapkey") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string key(nargs[2]);
    set<string> keys;
    keys.insert(key);

    ret = io_ctx.omap_rm_keys(oid, keys);
    if (ret < 0) {
      cerr << "error removing omap key " << vol_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }
  } else if (strcmp(nargs[0], "listomapvals") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    string last_read = "";
    int MAX_READ = 512;
    do {
      map<string, bufferlist> values;
      ret = io_ctx.omap_get_vals(oid, last_read, MAX_READ, values);
      if (ret < 0) {
	cerr << "error getting omap keys " << vol_name << "/" << oid << ": "
	     << cpp_strerror(ret) << std::endl;
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
	cout << std::endl;
	cout << "value: (" << it->second.length() << " bytes) :\n";
	it->second.hexdump(cout);
	cout << std::endl;
      }
    } while (ret == MAX_READ);
    ret = 0;
  }
  else if (strcmp(nargs[0], "cp") == 0) {
    if (vol_name.empty())
      usage_exit();

    if (nargs.size() < 2 || nargs.size() > 3)
      usage_exit();

    string target = target_vol_name;
    if (target.empty())
      target = vol_name;

    string target_obj;
    if (nargs.size() < 3) {
      if (target == vol_name) {
	cerr << "cannot copy object into itself" << std::endl;
	ret = -1;
	goto out;
      }
      target_obj = nargs[1];
    } else {
      target_obj = nargs[2];
    }

    // open io context.
    IoCtx target_ctx;
    ret = rados.ioctx_create(target, target_ctx);
    if (ret < 0) {
      cerr << "error opening target volume " << target << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    }

    ret = do_copy(io_ctx, nargs[1], target_ctx, target_obj);
    if (ret < 0) {
      cerr << "error copying " << vol_name << "/" << nargs[1] << " => "
	   << target << "/" << target_obj << ": " << cpp_strerror(ret)
	   << std::endl;
      goto out;
    }
  }
   else if (strcmp(nargs[0], "rm") == 0) {
     if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    vector<const char *>::iterator iter = nargs.begin();
    ++iter;
    for (; iter != nargs.end(); ++iter) {
      const string & oid = *iter;
      ret = io_ctx.remove(oid);
      if (ret < 0) {
	cerr << "error removing " << vol_name << "/" << oid << ": " << cpp_strerror(ret) << std::endl;
	goto out;
      }
    }
  }
  else if (strcmp(nargs[0], "create") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    ret = io_ctx.create(oid, true);
    if (ret < 0) {
      cerr << "error creating " << vol_name << "/" << oid << ": " << cpp_strerror(ret) << std::endl;
      goto out;
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
    RadosBencher bencher(cct, rados, io_ctx);
    bencher.set_show_time(show_time);
    ret = bencher.aio_bench(operation, seconds, num_objs,
			    concurrent_ios, op_size, cleanup, run_name);
    if (ret != 0)
      cerr << "error during benchmark: " << ret << std::endl;
  }
  else if (strcmp(nargs[0], "cleanup") == 0) {
    if (vol_name.empty())
      usage_exit();
    RadosBencher bencher(cct, rados, io_ctx);
    ret = bencher.clean_up(prefix, concurrent_ios, run_name);
    if (ret != 0)
      cerr << "error during cleanup: " << ret << std::endl;
  }
  else if (strcmp(nargs[0], "watch") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    RadosWatchCtx ctx(oid.c_str());
    uint64_t cookie;
    ret = io_ctx.watch(oid, 0, &cookie, &ctx);
    if (ret != 0)
      cerr << "error calling watch: " << ret << std::endl;
    else {
      cout << "press enter to exit..." << std::endl;
      getchar();
    }
  }
  else if (strcmp(nargs[0], "notify") == 0) {
    if (vol_name.empty() || nargs.size() < 3)
      usage_exit();
    string oid(nargs[1]);
    string msg(nargs[2]);
    bufferlist bl;
    ::encode(msg, bl);
    ret = io_ctx.notify(oid, bl);
    if (ret != 0)
      cerr << "error calling notify: " << ret << std::endl;
  } else if (strcmp(nargs[0], "set-alloc-hint") == 0) {
    if (vol_name.empty() || nargs.size() < 4)
      usage_exit();
    string err;
    string oid(nargs[1]);
    uint64_t expected_object_size = strict_strtoll(nargs[2], 10, &err);
    if (!err.empty()) {
      cerr << "couldn't parse expected_object_size: " << err << std::endl;
      usage_exit();
    }
    uint64_t expected_write_size = strict_strtoll(nargs[3], 10, &err);
    if (!err.empty()) {
      cerr << "couldn't parse expected_write_size: " << err << std::endl;
      usage_exit();
    }
    ret = io_ctx.set_alloc_hint(oid, expected_object_size, expected_write_size);
    if (ret < 0) {
      cerr << "error setting alloc-hint " << vol_name << "/" << oid << ": "
	   << cpp_strerror(ret) << std::endl;
      goto out;
    }
  } else if (strcmp(nargs[0], "load-gen") == 0) {
    if (vol_name.empty()) {
      cerr << "error: must specify volume" << std::endl;
      usage_exit();
    }
    LoadGen lg(&rados);
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

    cout << "run length " << run_length << " seconds" << std::endl;
    cout << "preparing " << lg.num_objs << " objects" << std::endl;
    ret = lg.bootstrap(vol_name);
    if (ret < 0) {
      cerr << "load-gen bootstrap failed" << std::endl;
      exit(1);
    }
    cout << "load-gen will run " << lg.run_length << " seconds" << std::endl;
    lg.run();
    lg.cleanup();
  } else if (strcmp(nargs[0], "listomapkeys") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    librados::ObjectReadOperation read(io_ctx);
    set<string> out_keys;
    read.omap_get_keys("", LONG_MAX, &out_keys, &ret);
    io_ctx.operate(nargs[1], &read, NULL);
    if (ret < 0) {
      cerr << "error getting omap key set " << vol_name << "/"
	   << nargs[1] << ": "	<< cpp_strerror(ret) << std::endl;
      goto out;
    }

    for (set<string>::iterator iter = out_keys.begin();
	 iter != out_keys.end(); ++iter) {
      cout << *iter << std::endl;
    }
  } else if (strcmp(nargs[0], "lock") == 0) {
    if (vol_name.empty())
      usage_exit();

    if (!formatter) {
      formatter = new JSONFormatter(pretty_format);
    }
    ret = do_lock_cmd(nargs, opts, &io_ctx, formatter);
  } else if (strcmp(nargs[0], "listwatchers") == 0) {
    if (vol_name.empty() || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    std::list<obj_watch_t> lw;

    ret = io_ctx.list_watchers(oid, &lw);
    if (ret < 0) {
      cerr << "error listing watchers " << vol_name << "/" << oid << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    else
      ret = 0;

    for (std::list<obj_watch_t>::iterator i = lw.begin(); i != lw.end(); ++i) {
      cout << "watcher=" << i->addr << " client." << i->watcher_id << " cookie=" << i->cookie << std::endl;
    }
  } else {
    cerr << "unrecognized command " << nargs[0] << "; -h or --help for usage" << std::endl;
    ret = -EINVAL;
    goto out;
  }

  if (ret < 0)
    cerr << "error " << (-ret) << ": " << cpp_strerror(ret) << std::endl;

out:
  delete formatter;
  return (ret < 0) ? 1 : 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  std::map < std::string, std::string > opts;
  std::vector<const char*>::iterator i;
  std::string val;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, &i, "-h", "--help", (char*)NULL)) {
      usage(cout);
      exit(0);
    } else if (ceph_argparse_flag(args, &i, "-f", "--force", (char*)NULL)) {
      opts["force"] = "true";
    } else if (ceph_argparse_flag(args, &i, "-d", "--delete-after", (char*)NULL)) {
      opts["delete-after"] = "true";
    } else if (ceph_argparse_flag(args, &i, "--pretty-format", (char*)NULL)) {
      opts["pretty-format"] = "true";
    } else if (ceph_argparse_flag(args, &i, "--show-time", (char*)NULL)) {
      opts["show-time"] = "true";
    } else if (ceph_argparse_flag(args, &i, "--no-cleanup", (char*)NULL)) {
      opts["no-cleanup"] = "true";
    } else if (ceph_argparse_witharg(args, i, &val, "--run-name", (char*)NULL)) {
      opts["run-name"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--prefix", (char*)NULL)) {
      opts["prefix"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-v", "--volume", (char*)NULL)) {
      opts["volume"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-volume", (char*)NULL)) {
      opts["target_volume"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-t", "--concurrent-ios", (char*)NULL)) {
      opts["concurrent-ios"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)NULL)) {
      opts["block-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", (char*)NULL)) {
      opts["block-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-object-size", (char*)NULL)) {
      opts["min-object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-object-size", (char*)NULL)) {
      opts["max-object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-op-len", (char*)NULL)) {
      opts["min-op-len"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-op-len", (char*)NULL)) {
      opts["max-op-len"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-ops", (char*)NULL)) {
      opts["max-ops"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-backlog", (char*)NULL)) {
      opts["max-backlog"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-throughput", (char*)NULL)) {
      opts["target-throughput"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--read-percent", (char*)NULL)) {
      opts["read-percent"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--num-objects", (char*)NULL)) {
      opts["num-objects"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--run-length", (char*)NULL)) {
      opts["run-length"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--workers", (char*)NULL)) {
      opts["workers"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      opts["format"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-tag", (char*)NULL)) {
      opts["lock-tag"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-cookie", (char*)NULL)) {
      opts["lock-cookie"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-description", (char*)NULL)) {
      opts["lock-description"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-duration", (char*)NULL)) {
      opts["lock-duration"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-type", (char*)NULL)) {
      opts["lock-type"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-N", "--namespace", (char*)NULL)) {
      opts["namespace"] = val;
    } else {
      if (val[0] == '-')
	usage_exit();
      ++i;
    }
  }

  if (args.empty()) {
    cerr << "rados: you must give an action. Try --help" << std::endl;
    return 1;
  }
  return rados_tool_common(opts, args);
}
