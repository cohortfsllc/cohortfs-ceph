// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net> and others
 *
 * LGPL2.  See file COPYING.
 *
 */
#include <boost/uuid/string_generator.hpp>
#include "mon/MonClient.h"
#include "mon/MonMap.h"
#include "common/config.h"

#include "auth/KeyRing.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/strtol.h"
#include "global/global_init.h"
#include "common/safe_io.h"
#include "common/secret.h"
#include "include/stringify.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/byteorder.h"

#include "include/intarith.h"

#include "include/compat.h"
#include "common/blkdev.h"

#include <boost/scoped_ptr.hpp>
#include <dirent.h>
#include <errno.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <sys/ioctl.h>

#include "include/rbd_types.h"
#include "common/TextTable.h"
#include "include/util.h"

#include "common/Formatter.h"

#if defined(__linux__)
#include <linux/fs.h>
#endif

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include <blkid/blkid.h>

#define MAX_SECRET_LEN 1000
#define MAX_VOL_NAME_SIZE 128

#define RBD_DIFF_BANNER "rbd diff v1\n"

using std::cout;

#if 0
static string dir_oid = RBD_DIRECTORY;
#endif
static string dir_info_oid = RBD_INFO;

bool udevadm_settle = true;
bool progress = true;
bool resize_allow_shrink = false;

map<string, string> map_options; // -o / --options map

#define dout_subsys ceph_subsys_rbd

void usage()
{
  cout <<
"usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...\n"
"where 'vol' is a rados vol name (default is 'rbd') and 'cmd' is one of:\n"
"  (ls | list) [vol-name]		      list rbd images\n"
"  info <image-name>			       show information about image size,\n"
"					       striping, etc.\n"
"  create --size <MB> <name>		       create an empty image\n"
"  resize --size <MB> <image-name>	       resize (expand or contract) image\n"
"  rm <image-name>			       delete an image\n"
"  export <image-name> <path>		       export image to file\n"
"					       \"-\" for stdout\n"
"  import <path> <image-name>		       import image from file\n"
"					       (dest defaults\n"
"						as the filename part of file)\n"
"					       \"-\" for stdin\n"
"  (cp | copy) <src> <dest>		       copy src image to dest\n"
"  (mv | rename) <src> <dest>		       rename src image to dest\n"
"  watch <image-name>			       watch events on image\n"
"  map <image-name>			       map image to a block device\n"
"					       using the kernel\n"
"  unmap <device>			       unmap a rbd device that was\n"
"					       mapped by the kernel\n"
"  showmapped				       show the rbd images mapped\n"
"					       by the kernel\n"
"  lock list <image-name>		       show locks held on an image\n"
"  lock add <image-name> <id> [--shared <tag>] take a lock called id on an image\n"
"  lock remove <image-name> <id> <locker>      release a lock on an image\n"
"  bench-write <image-name>		       simple write benchmark\n"
"		  --io-size <bytes>		 write size\n"
"		  --io-threads <num>		 ios in flight\n"
"		  --io-total <bytes>		 total bytes to write\n"
"		  --io-pattern <seq|rand>	 write pattern\n"
"\n"
"<image-name> is [vol/]name, or you may specify\n"
"individual pieces of names with -p/--vol and/or --image.\n"
"\n"
"Other input options:\n"
"  --vol <volume>		      source volume name\n"
"  --image <image-name>		      image name\n"
"  --dest <image-name>		      destination [volume and] image name\n"
"  --dest-vol <name>		      destination volume name\n"
"  --path <path-name>		      path name for import/export\n"
"  --size <size in MB>		      size of image for create and resize\n"
"  --id <username>		      rados user (without 'client.'prefix) to\n"
"				      authenticate as\n"
"  --keyfile <path>		      file containing secret key for use with cephx\n"
"  --shared <tag>		      take a shared (rather than exclusive) lock\n"
"  --format <output-format>	      output format (default: plain, json, xml)\n"
"  --pretty-format		      make json or xml output more readable\n"
"  --no-settle			      do not wait for udevadm to settle on map/unmap\n"
"  --no-progress		      do not show progress for long-running commands\n"
"  -o, --options <map-options>	      options to use when mapping an image\n"
"  --read-only			      set device readonly when mapping image\n"
"  --allow-shrink		      allow shrinking of an image when resizing\n";
}

struct MyProgressContext : public librbd::ProgressContext {
  const char *operation;
  int last_pc;

  MyProgressContext(const char *o) : operation(o), last_pc(0) {
  }

  int update_progress(uint64_t offset, uint64_t total) {
    if (progress) {
      int pc = total ? (offset * 100ull / total) : 0;
      if (pc != last_pc) {
	cerr << "\r" << operation << ": "
	  //	   << offset << " / " << total << " "
	     << pc << "% complete...";
	cerr.flush();
	last_pc = pc;
      }
    }
    return 0;
  }
  void finish() {
    if (progress) {
      cerr << "\r" << operation << ": 100% complete...done." << std::endl;
    }
  }
  void fail() {
    if (progress) {
      cerr << "\r" << operation << ": " << last_pc << "% complete...failed."
	   << std::endl;
    }
  }
};

static int get_outfmt(const char *output_format,
		      bool pretty,
		      boost::scoped_ptr<Formatter> *f)
{
  if (!strcmp(output_format, "json")) {
    f->reset(new JSONFormatter(pretty));
  } else if (!strcmp(output_format, "xml")) {
    f->reset(new XMLFormatter(pretty));
  } else if (strcmp(output_format, "plain")) {
    cerr << "rbd: unknown format '" << output_format << "'" << std::endl;
    return -EINVAL;
  }

  return 0;
}

#if 0
static int do_list(librbd::RBD &rbd, librados::IoCtx& io_ctx, bool lflag,
		   Formatter *f)
{
  std::vector<string> names;
  int r = rbd.list(io_ctx, names);
  if (r == -ENOENT)
    r = 0;
  if (r < 0)
    return r;

  if (!lflag) {
    if (f)
      f->open_array_section("images");
    for (std::vector<string>::const_iterator i = names.begin();
       i != names.end(); ++i) {
       if (f)
	 f->dump_string("name", *i);
       else
	 cout << *i << std::endl;
    }
    if (f) {
      f->close_section();
      f->flush(cout);
    }
    return 0;
  }

  TextTable tbl;

  if (f) {
    f->open_array_section("images");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("SIZE", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("FMT", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("PROT", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("LOCK", TextTable::LEFT, TextTable::LEFT);
  }

  string image;

  for (std::vector<string>::const_iterator i = names.begin();
       i != names.end(); ++i) {
    librbd::image_info_t info;
    librbd::Image im;

    r = rbd.open_read_only(io_ctx, im, i->c_str());
    // image might disappear between rbd.list() and rbd.open(); ignore
    // that, warn about other possible errors (EPERM, say, for opening
    // an old-format image, because you need execute permission for the
    // class method)
    if (r < 0) {
      if (r != -ENOENT) {
	cerr << "rbd: error opening " << *i << ": " << cpp_strerror(r)
	     << std::endl;
      }
      // in any event, continue to next image
      continue;
    }

    if (im.stat(info, sizeof(info)) < 0)
      return -EINVAL;

    list<librbd::locker_t> lockers;
    bool exclusive;
    r = im.list_lockers(&lockers, &exclusive, NULL);
    if (r < 0)
      return r;
    string lockstr;
    if (!lockers.empty()) {
      lockstr = (exclusive) ? "excl" : "shr";
    }

    if (f) {
      f->open_object_section("image");
      f->dump_string("image", *i);
      f->dump_unsigned("size", info.size);
      if (!lockers.empty())
	f->dump_string("lock_type", exclusive ? "exclusive" : "shared");
      f->close_section();
    } else {
      tbl << *i
	  << stringify(si_t(info.size))
	  << ""				// protect doesn't apply to images
	  << lockstr
	  << TextTable::endrow;
    }
  }
  if (f) {
    f->close_section();
    f->flush(cout);
  } else if (!names.empty()) {
    cout << tbl;
  }

  return 0;
}
#endif

static int do_create(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname, uint64_t size)
{
  int r;

  r = rbd.create(io_ctx, imgname, size);

  if (r < 0)
    return r;
  return 0;
}

static int do_rename(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname, const char *destname)
{
  int r = rbd.rename(io_ctx, imgname, destname);
  if (r < 0)
    return r;
  return 0;
}

static int do_show_info(const char *imgname, librbd::Image& image,
			Formatter *f)
{
  librbd::image_info_t info;
  int r;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("image");
    f->dump_string("name", imgname);
    f->dump_unsigned("size", info.size);
  } else {
    cout << "rbd image '" << imgname << "':\n"
	 << "\tsize " << prettybyte_t(info.size) << std::endl;
  }

  if (f) {
    f->close_section();
    f->flush(cout);
  }

  return 0;
}

static int do_delete(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname)
{
  MyProgressContext pc("Removing image");
  int r = rbd.remove_with_progress(io_ctx, imgname, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

static int do_resize(librbd::Image& image, uint64_t size)
{
  MyProgressContext pc("Resizing image");
  int r = image.resize_with_progress(size, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

static int do_lock_list(librbd::Image& image, Formatter *f)
{
  list<librbd::locker_t> lockers;
  bool exclusive;
  string tag;
  TextTable tbl;
  int r;

  r = image.list_lockers(&lockers, &exclusive, &tag);
  if (r < 0)
    return r;

  if (f) {
    f->open_object_section("locks");
  } else {
    tbl.define_column("Locker", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("ID", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("Address", TextTable::LEFT, TextTable::LEFT);
  }

  if (lockers.size()) {
    bool one = (lockers.size() == 1);

    if (!f) {
      cout << "There " << (one ? "is " : "are ") << lockers.size()
	   << (exclusive ? " exclusive" : " shared")
	   << " lock" << (one ? "" : "s") << " on this image.\n";
      if (!exclusive)
	cout << "Lock tag: " << tag << "\n";
    }

    for (list<librbd::locker_t>::const_iterator it = lockers.begin();
	 it != lockers.end(); ++it) {
      if (f) {
	f->open_object_section(it->cookie.c_str());
	f->dump_string("locker", it->client);
	f->dump_string("address", it->address);
	f->close_section();
      } else {
	tbl << it->client << it->cookie << it->address << TextTable::endrow;
      }
    }
    if (!f)
      cout << tbl;
  }

  if (f) {
    f->close_section();
    f->flush(cout);
  }
  return 0;
}

static int do_lock_add(librbd::Image& image, const char *cookie,
		       const char *tag)
{
  if (tag)
    return image.lock_shared(cookie, tag);
  else
    return image.lock_exclusive(cookie);
}

static int do_lock_remove(librbd::Image& image, const char *client,
			  const char *cookie)
{
  return image.break_lock(client, cookie);
}

static void rbd_bencher_completion(void *c, void *pc);

struct rbd_bencher;

struct rbd_bencher {
  librbd::Image *image;
  Mutex lock;
  Cond cond;
  int in_flight;

  rbd_bencher(librbd::Image *i)
    : image(i),
      in_flight(0)
  { }

  bool start_write(int max, uint64_t off, uint64_t len, bufferlist& bl)
  {
    {
      Mutex::Locker l(lock);
      if (in_flight >= max)
	return false;
      in_flight++;
    }
    librbd::RBD::AioCompletion *c =
      new librbd::RBD::AioCompletion((void *)this, rbd_bencher_completion);
    image->aio_write(off, len, bl, c);
    //cout << "start " << c << " at " << off << "~" << len << std::endl;
    return true;
  }

  void wait_for(int max) {
    Mutex::Locker l(lock);
    while (in_flight > max) {
      utime_t dur;
      dur.set_from_double(.2);
      cond.WaitInterval(g_ceph_context, lock, dur);
    }
  }

};

void rbd_bencher_completion(void *vc, void *pc)
{
  librbd::RBD::AioCompletion *c = (librbd::RBD::AioCompletion *)vc;
  rbd_bencher *b = static_cast<rbd_bencher *>(pc);
  //cout << "complete " << c << std::endl;
  int ret = c->get_return_value();
  if (ret != 0) {
    cout << "write error: " << cpp_strerror(ret) << std::endl;
    assert(0 == ret);
  }
  b->lock.Lock();
  b->in_flight--;
  b->cond.Signal();
  b->lock.Unlock();
  c->release();
}

static int do_bench_write(librbd::Image& image, uint64_t io_size,
			  uint64_t io_threads, uint64_t io_bytes,
			  string pattern)
{
  rbd_bencher b(&image);

  cout << "bench-write "
       << " io_size " << io_size
       << " io_threads " << io_threads
       << " bytes " << io_bytes
       << " pattern " << pattern
       << std::endl;

  if (pattern != "rand" && pattern != "seq")
    return -EINVAL;

  srand(time(NULL) % (unsigned long) -1);

  bufferptr bp(io_size);
  memset(bp.c_str(), rand() & 0xff, io_size);
  bufferlist bl;
  bl.push_back(bp);

  utime_t start = ceph_clock_now(NULL);
  utime_t last;
  unsigned ios = 0;

  uint64_t size = 0;
  image.size(&size);

  vector<uint64_t> thread_offset;
  uint64_t i;
  uint64_t start_pos;

  // disturb all thread's offset, used by seq write
  for (i = 0; i < io_threads; i++) {
    start_pos = (rand() % (size / io_size)) * io_size;
    thread_offset.push_back(start_pos);
  }

  printf("  SEC	      OPS   OPS/SEC   BYTES/SEC\n");
  uint64_t off;
  for (off = 0; off < io_bytes; off += io_size) {
    b.wait_for(io_threads - 1);
    i = 0;
    while (i < io_threads && off < io_bytes &&
	   b.start_write(io_threads, thread_offset[i], io_size, bl)) {
      ++i;
      ++ios;
      off += io_size;

      if (pattern == "rand") {
	thread_offset[i] = (rand() % (size / io_size)) * io_size;
      } else {
	thread_offset[i] += io_size;
	if (thread_offset[i] + io_size > size)
	  thread_offset[i] = 0;
      }
    }

    utime_t now = ceph_clock_now(NULL);
    utime_t elapsed = now - start;
    if (elapsed.sec() != last.sec()) {
      printf("%5d  %8d	%8.2lf	%8.2lf\n",
	     (int)elapsed,
	     (int)(ios - io_threads),
	     (double)(ios - io_threads) / elapsed,
	     (double)(off - io_threads * io_size) / elapsed);
      last = elapsed;
    }
  }
  b.wait_for(0);
  int r = image.flush();
  if (r < 0) {
    cerr << "Error flushing data at the end: " << cpp_strerror(r) << std::endl;
  }

  utime_t now = ceph_clock_now(NULL);
  double elapsed = now - start;

  printf("elapsed: %5d	ops: %8d  ops/sec: %8.2lf  bytes/sec: %8.2lf\n",
	 (int)elapsed, ios, (double)ios / elapsed, (double)off / elapsed);

  return 0;
}

struct ExportContext {
  librbd::Image *image;
  int fd;
  uint64_t totalsize;
  MyProgressContext pc;

  ExportContext(librbd::Image *i, int f, uint64_t t) :
    image(i),
    fd(f),
    totalsize(t),
    pc("Exporting image")
  {}
};

static int export_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg)
{
  ssize_t ret;
  ExportContext *ec = static_cast<ExportContext *>(arg);
  int fd = ec->fd;
  static char *localbuf = NULL;
  static size_t maplen = 0;

  if (fd == 1) {
    if (!buf) {
      // can't seek stdout; need actual data to write
      if (maplen < len) {
	// never mapped, or need to map larger
	int r;
	if (localbuf != NULL){
	  if ((r = munmap(localbuf, len)) < 0) {
	    cerr << "rbd: error " << r << "munmap'ing buffer" << std::endl;
	    return errno;
	  }
	}

	maplen = len;
	localbuf = (char *)mmap(NULL, maplen, PROT_READ,
				MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (localbuf == MAP_FAILED) {
	  cerr << "rbd: MAP_FAILED mmap'ing buffer for zero writes"
	       << std::endl;
	  return -ENOMEM;
	}
      }
      ret = write(fd, localbuf, len);
    } else {
      ret = write(fd, buf, len);
    }
  } else {		// not stdout
    if (!buf || buf_is_zero(buf, len)) {
      /* a hole */
      return 0;
    }

    ret = lseek64(fd, ofs, SEEK_SET);
    if (ret < 0)
      return -errno;
    ret = write(fd, buf, len);
    ec->pc.update_progress(ofs, ec->totalsize);
  }

  if (ret < 0)
    return -errno;

  return 0;
}

static int do_export(librbd::Image& image, const char *path)
{
  int64_t r;
  librbd::image_info_t info;
  int fd;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  if (strcmp(path, "-") == 0)
    fd = 1;
  else
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0)
    return -errno;

  ExportContext ec(&image, fd, info.size);
  r = image.read_iterate2(0, info.size, export_read_cb, (void *)&ec);
  if (r < 0)
    goto out;

  if (fd != 1)
    r = ftruncate(fd, info.size);
  if (r < 0)
    goto out;

 out:
  close(fd);
  if (r < 0)
    ec.pc.fail();
  else
    ec.pc.finish();
  return r;
}

static const char *imgname_from_path(const char *path)
{
  const char *imgname;

  imgname = strrchr(path, '/');
  if (imgname)
    imgname++;
  else
    imgname = path;

  return imgname;
}

static void set_vol_image_name(const char *orig_vol, const char *orig_img,
			       char **new_vol, char **new_img)
{
  const char *sep;

  if (!orig_img)
    return;

  sep = strchr(orig_img, '/');
  if (!sep) {
    *new_img = strdup(orig_img);
    return;
  }

  *new_vol =  strdup(orig_img);
  sep = strchr(*new_vol, '/');
  assert (sep);

  *(char *)sep = '\0';
  *new_img = strdup(sep + 1);
}

static int do_import(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname, const char *path)
{
  int fd, r;
  struct stat stat_buf;
  MyProgressContext pc("Importing image");
  uint64_t size = 0;

  assert(imgname);

  uint64_t image_pos = 0;
  char *p = new char[io_ctx.op_size()];
  size_t reqlen = io_ctx.op_size(); // amount requested from read
  ssize_t readlen;		// amount received from one read
  size_t blklen = 0;		// amount accumulated from reads to fill blk
  librbd::Image image;

  bool from_stdin = !strcmp(path, "-");
  if (from_stdin) {
    fd = 0;
  } else {
    if ((fd = open(path, O_RDONLY)) < 0) {
      r = -errno;
      cerr << "rbd: error opening " << path << std::endl;
      goto done2;
    }

    if ((fstat(fd, &stat_buf)) < 0) {
      r = -errno;
      cerr << "rbd: stat error " << path << std::endl;
      goto done;
    }
    if (S_ISDIR(stat_buf.st_mode)) {
      r = -EISDIR;
      cerr << "rbd: cannot import a directory" << std::endl;
      goto done;
    }
    if (stat_buf.st_size)
      size = (uint64_t)stat_buf.st_size;

    if (!size) {
      int64_t bdev_size = 0;
      r = get_block_device_size(fd, &bdev_size);
      if (r < 0) {
	cerr << "rbd: unable to get size of file/block device" << std::endl;
	goto done;
      }
      assert(bdev_size >= 0);
      size = (uint64_t) bdev_size;
    }
  }
  r = do_create(rbd, io_ctx, imgname, size);
  if (r < 0) {
    cerr << "rbd: image creation failed" << std::endl;
    goto done;
  }
  r = rbd.open(io_ctx, image, imgname);
  if (r < 0) {
    cerr << "rbd: failed to open image" << std::endl;
    goto done;
  }

  // loop body handles 0 return, as we may have a block to flush
  while ((readlen = ::read(fd, p + blklen, reqlen)) >= 0) {
    blklen += readlen;
    // if read was short, try again to fill the block before writing
    if (readlen && ((size_t)readlen < reqlen)) {
      reqlen -= readlen;
      continue;
    }
    if (!from_stdin)
      pc.update_progress(image_pos, size);

    bufferlist bl(blklen);
    bl.append(p, blklen);
    // resize output image by binary expansion as we go for stdin
    if (from_stdin && (image_pos + (size_t)blklen) > size) {
      size *= 2;
      r = image.resize(size);
      if (r < 0) {
	cerr << "rbd: can't resize image during import" << std::endl;
	goto done;
      }
    }

    // write as much as we got; perhaps less than op_size
    // but skip writing zeros to create sparse images
    if (!bl.is_zero()) {
      r = image.write(image_pos, blklen, bl);
      if (r < 0) {
	cerr << "rbd: error writing to image position " << image_pos
	     << std::endl;
	goto done;
      }
    }
    // done with whole block, whether written or not
    image_pos += blklen;
    // if read had returned 0, we're at EOF and should quit
    if (readlen == 0)
      break;
    blklen = 0;
    reqlen = io_ctx.op_size();
  }
  if (from_stdin) {
    r = image.resize(image_pos);
    if (r < 0) {
      cerr << "rbd: final image resize failed" << std::endl;
      goto done;
    }
  }

  r = 0;

 done:
  if (!from_stdin) {
    if (r < 0)
      pc.fail();
    else
      pc.finish();
    close(fd);
  }
 done2:
  delete[] p;
  return r;
}

static int do_copy(librbd::Image &src, librados::IoCtx& dest_pp,
		   const char *destname)
{
  MyProgressContext pc("Image copy");
  int r = src.copy_with_progress(dest_pp, destname, pc);
  if (r < 0){
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

class RbdWatchCtx : public librados::WatchCtx {
  string name;
public:
  RbdWatchCtx(const char *imgname) : name(imgname) {}
  virtual ~RbdWatchCtx() {}
  virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    cout << name << " got notification opcode=" << (int)opcode << " ver="
	 << ver << " bl.length=" << bl.length() << std::endl;
  }
};

static int do_watch(librados::IoCtx& pp, const char *imgname)
{
  string md_oid, dest_md_oid;
  uint64_t cookie;
  RbdWatchCtx ctx(imgname);

  string header_oid = imgname;
  header_oid += RBD_SUFFIX;

  int r = pp.stat(header_oid, NULL, NULL);
  if (r < 0) {
    return r;
  }

  r = pp.watch(header_oid, 0, &cookie, &ctx);
  if (r < 0) {
    cerr << "rbd: watch failed" << std::endl;
    return r;
  }

  cout << "press enter to exit..." << std::endl;
  getchar();

  return 0;
}

static int do_kernel_add(const char *volname, const char *imgname)
{
  MonMap monmap;
  int r = monmap.build_initial(g_ceph_context, cerr);
  if (r < 0)
    return r;

  map<string, entity_addr_t>::const_iterator it = monmap.mon_addr.begin();
  ostringstream oss;
  for (size_t i = 0; i < monmap.mon_addr.size(); ++i, ++it) {
    oss << it->second.addr;
    if (i + 1 < monmap.mon_addr.size())
      oss << ",";
  }

  const char *user = g_conf->name.get_id().c_str();
  oss << " name=" << user;

  char key_name[strlen(user) + strlen("client.") + 1];
  snprintf(key_name, sizeof(key_name), "client.%s", user);

  KeyRing keyring;
  r = keyring.from_ceph_context(g_ceph_context);
  if (r == -ENOENT && !(g_conf->keyfile.length() ||
			g_conf->key.length()))
    r = 0;
  if (r < 0) {
    cerr << "rbd: failed to get secret: " << cpp_strerror(r) << std::endl;
    return r;
  }
  CryptoKey secret;
  if (keyring.get_secret(g_conf->name, secret)) {
    string secret_str;
    secret.encode_base64(secret_str);

    r = set_kernel_secret(secret_str.c_str(), key_name);
    if (r >= 0) {
      if (r == 0)
	cerr << "rbd: warning: secret has length 0" << std::endl;
      oss << ",key=" << key_name;
    } else if (r == -ENODEV || r == -ENOSYS) {
      /* running against older kernel; fall back to secret= in options */
      oss << ",secret=" << secret_str;
    } else {
      cerr << "rbd: failed to add ceph secret key '" << key_name
	   << "' to kernel: " << cpp_strerror(r) << std::endl;
      return r;
    }
  } else if (is_kernel_secret(key_name)) {
    oss << ",key=" << key_name;
  }

  for (map<string, string>::const_iterator it = map_options.begin();
       it != map_options.end();
       ++it) {
    // for compatibility with < 3.7 kernels, assume that rw is on by
    // default and omit it even if it was specified by the user
    // (see ceph.git commit fb0f1986449b)
    if (it->first == "rw" && it->second == "rw")
      continue;

    oss << "," << it->second;
  }

  oss << " " << volname << " " << imgname;

  // modprobe the rbd module if /sys/bus/rbd doesn't exist
  struct stat sb;
  if ((stat("/sys/bus/rbd", &sb) < 0) || (!S_ISDIR(sb.st_mode))) {
    // turn on single-major device number allocation scheme if the
    // kernel supports it
    const char *cmd = "/sbin/modprobe rbd";
    r = system("/sbin/modinfo -F parm rbd | /bin/grep -q ^single_major:");
    if (r == 0) {
      cmd = "/sbin/modprobe rbd single_major=Y";
    } else if (r < 0) {
      cerr << "rbd: error executing modinfo as shell command!" << std::endl;
    }

    r = system(cmd);
    if (r) {
      if (r < 0)
	cerr << "rbd: error executing modprobe as shell command!" << std::endl;
      else
	cerr << "rbd: modprobe rbd failed! (" << r << ")" <<std::endl;
      return r;
    }
  }

  // 'add' interface is deprecated, use 'add_single_major' if it's
  // available
  //
  // ('add' and 'add_single_major' interfaces are identical, except
  // that if rbd kernel module is new enough and is configured to use
  // single-major scheme, 'add' is disabled in order to prevent old
  // userspace from doing weird things at unmap time)
  int fd = open("/sys/bus/rbd/add_single_major", O_WRONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      fd = open("/sys/bus/rbd/add", O_WRONLY);
      if (fd < 0) {
	r = -errno;
	if (r == -ENOENT) {
	  cerr << "rbd: /sys/bus/rbd/add does not exist!" << std::endl
	       << "Did you run 'modprobe rbd' or is your rbd module too old?"
	       << std::endl;
	}
	return r;
      }
    } else {
      return -errno;
    }
  }

  string add = oss.str();
  r = safe_write(fd, add.c_str(), add.size());
  close(fd);

  // let udevadm do its job before we return
  if (udevadm_settle) {
    int r = system("/sbin/udevadm settle");
    if (r) {
      if (r < 0)
	cerr << "rbd: error executing udevadm as shell command!" << std::endl;
      else
	cerr << "rbd: '/sbin/udevadm settle' failed! (" << r << ")"
	     << std::endl;
      return r;
    }
  }

  return r;
}

static int read_file(const char *filename, char *buf, size_t bufsize)
{
    int fd = open(filename, O_RDONLY);
    if (fd < 0)
      return -errno;

    int r = safe_read(fd, buf, bufsize);
    if (r < 0) {
      cerr << "rbd: could not read " << filename << ": "
	   << cpp_strerror(-r) << std::endl;
      close(fd);
      return r;
    }

    char *end = buf;
    while (end < buf + bufsize && *end && *end != '\n') {
      end++;
    }
    *end = '\0';

    close(fd);
    return r;
}

void do_closedir(DIR *dp)
{
  if (dp)
    closedir(dp);
}

static int do_kernel_showmapped(Formatter *f)
{
  int r;
  bool have_output = false;
  TextTable tbl;

  const char *devices_path = "/sys/bus/rbd/devices";
  std::shared_ptr<DIR> device_dir(opendir(devices_path), do_closedir);
  if (!device_dir.get()) {
    r = -errno;
    cerr << "rbd: could not open " << devices_path << ": "
	 << cpp_strerror(-r) << std::endl;
    return r;
  }

  struct dirent *dent;
  dent = readdir(device_dir.get());
  if (!dent) {
    r = -errno;
    cerr << "rbd: error reading " << devices_path << ": "
	 << cpp_strerror(-r) << std::endl;
    return r;
  }

  if (f) {
    f->open_object_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("vol", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
  }

  do {
    if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
      continue;

    char fn[PATH_MAX];

    char dev[PATH_MAX];
    snprintf(dev, sizeof(dev), "/dev/rbd%s", dent->d_name);

    char name[RBD_MAX_IMAGE_NAME_SIZE];
    snprintf(fn, sizeof(fn), "%s/%s/name", devices_path, dent->d_name);
    r = read_file(fn, name, sizeof(name));
    if (r < 0) {
      cerr << "rbd: could not read name from " << fn << ": "
	   << cpp_strerror(-r) << std::endl;
      continue;
    }

    char vol[4096];
    snprintf(fn, sizeof(fn), "%s/%s/vol", devices_path, dent->d_name);
    r = read_file(fn, vol, sizeof(vol));
    if (r < 0) {
      cerr << "rbd: could not read name from " << fn << ": "
	   << cpp_strerror(-r) << std::endl;
      continue;
    }

    if (f) {
      f->open_object_section(dent->d_name);
      f->dump_string("ivol", vol);
      f->dump_string("name", name);
      f->dump_string("device", dev);
      f->close_section();
    } else {
      tbl << dent->d_name << vol << name << dev << TextTable::endrow;
    }
    have_output = true;

  } while ((dent = readdir(device_dir.get())));

  if (f) {
    f->close_section();
    f->flush(cout);
  } else {
    if (have_output)
      cout << tbl;
  }

  return 0;
}

static int get_rbd_seq(dev_t devno, string &seq)
{
  // convert devno, which might be a partition major:minor pair, into
  // a whole disk major:minor pair
  dev_t wholediskno;
  int r = blkid_devno_to_wholedisk(devno, NULL, 0, &wholediskno);
  if (r) {
    cerr << "rbd: could not compute wholediskno: " << r << std::endl;
    // ignore the error: devno == wholediskno most of the time, and if
    // it turns out it's not we will fail with -ENOENT later anyway
    wholediskno = devno;
  }

  const char *devices_path = "/sys/bus/rbd/devices";
  DIR *device_dir = opendir(devices_path);
  if (!device_dir) {
    r = -errno;
    cerr << "rbd: could not open " << devices_path << ": " << cpp_strerror(-r)
	 << std::endl;
    return r;
  }

  struct dirent *dent;
  dent = readdir(device_dir);
  if (!dent) {
    r = -errno;
    cerr << "Error reading " << devices_path << ": " << cpp_strerror(-r)
	 << std::endl;
    closedir(device_dir);
    return r;
  }

  int match_minor = -1;
  do {
    char fn[strlen(devices_path) + strlen(dent->d_name) + strlen("//major") + 1];
    char buf[32];

    if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
      continue;

    snprintf(fn, sizeof(fn), "%s/%s/major", devices_path, dent->d_name);
    r = read_file(fn, buf, sizeof(buf));
    if (r < 0) {
      cerr << "rbd: could not read major number from " << fn << ": "
	   << cpp_strerror(-r) << std::endl;
      continue;
    }
    string err;
    int cur_major = strict_strtol(buf, 10, &err);
    if (!err.empty()) {
      cerr << err << std::endl;
      cerr << "rbd: could not parse major number read from " << fn << ": "
	   << cpp_strerror(-r) << std::endl;
      continue;
    }
    if (cur_major != (int)major(wholediskno))
      continue;

    if (match_minor == -1) {
      // matching minors in addition to majors is not necessary unless
      // single-major scheme is turned on, but, if the kernel supports
      // it, do it anyway (blkid stuff above ensures that we always have
      // the correct minor to match with)
      struct stat sbuf;
      snprintf(fn, sizeof(fn), "%s/%s/minor", devices_path, dent->d_name);
      match_minor = (stat(fn, &sbuf) == 0);
    }

    if (match_minor == 1) {
      snprintf(fn, sizeof(fn), "%s/%s/minor", devices_path, dent->d_name);
      r = read_file(fn, buf, sizeof(buf));
      if (r < 0) {
	cerr << "rbd: could not read minor number from " << fn << ": "
	     << cpp_strerror(-r) << std::endl;
	continue;
      }
      int cur_minor = strict_strtol(buf, 10, &err);
      if (!err.empty()) {
	cerr << err << std::endl;
	cerr << "rbd: could not parse minor number read from " << fn << ": "
	     << cpp_strerror(-r) << std::endl;
	continue;
      }
      if (cur_minor != (int)minor(wholediskno))
	continue;
    } else {
      assert(match_minor == 0);
    }

    seq = string(dent->d_name);
    closedir(device_dir);
    return 0;
  } while ((dent = readdir(device_dir)));

  closedir(device_dir);
  return -ENOENT;
}

static int do_kernel_rm(const char *dev)
{
  struct stat sbuf;
  if (stat(dev, &sbuf) || !S_ISBLK(sbuf.st_mode)) {
    cerr << "rbd: " << dev << " is not a block device" << std::endl;
    return -EINVAL;
  }

  string seq_num;
  int r = get_rbd_seq(sbuf.st_rdev, seq_num);
  if (r == -ENOENT) {
    cerr << "rbd: " << dev << " is not an rbd device" << std::endl;
    return -EINVAL;
  }
  if (r < 0)
    return r;

  // let udevadm do its job *before* we try to unmap
  if (udevadm_settle) {
    int r = system("/sbin/udevadm settle");
    if (r) {
      if (r < 0)
	cerr << "rbd: error executing udevadm as shell command!" << std::endl;
      else
	cerr << "rbd: '/sbin/udevadm settle' failed! (" << r << ")" <<std::endl;
      // ignore the error, though.
    }
  }

  // see comment in do_kernel_add(), same goes for 'remove' vs
  // 'remove_single_major'
  int fd = open("/sys/bus/rbd/remove_single_major", O_WRONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      fd = open("/sys/bus/rbd/remove", O_WRONLY);
      if (fd < 0)
	return -errno;
    } else {
      return -errno;
    }
  }

  r = safe_write(fd, seq_num.c_str(), seq_num.size());
  if (r < 0) {
    cerr << "rbd: failed to remove rbd device" << ": " << cpp_strerror(-r)
	 << std::endl;
    close(fd);
    return r;
  }

  r = close(fd);

  // let udevadm finish, if present
  if (udevadm_settle){
    int r = system("/sbin/udevadm settle");
    if (r) {
      if (r < 0)
	cerr << "rbd: error executing udevadm as shell command!" << std::endl;
      else
	cerr << "rbd: '/sbin/udevadm settle' failed! (" << r << ")" <<std::endl;
      return r;
    }
  }

  if (r < 0)
    r = -errno;
  return r;
}

static string map_option_uuid_cb(const char *value_char)
{
  boost::uuids::string_generator parse;
  boost::uuids::uuid u;
  try {
    u = parse(value_char);
  } catch (std::runtime_error& e) {
    return "";
  }

  ostringstream oss;
  oss << u;
  return oss.str();
}

static string map_option_ip_cb(const char *value_char)
{
  entity_addr_t a;
  const char *endptr;
  if (!a.parse(value_char, &endptr) ||
      endptr != value_char + strlen(value_char)) {
    return "";
  }

  ostringstream oss;
  oss << a.addr;
  return oss.str();
}

static string map_option_int_cb(const char *value_char)
{
  string err;
  int d = strict_strtol(value_char, 10, &err);
  if (!err.empty() || d < 0)
    return "";

  ostringstream oss;
  oss << d;
  return oss.str();
}

static void put_map_option(const string key, string val)
{
  map<string, string>::const_iterator it = map_options.find(key);
  if (it != map_options.end()) {
    cerr << "rbd: warning: redefining map option " << key << ": '"
	 << it->second << "' -> '" << val << "'" << std::endl;
  }
  map_options[key] = val;
}

static int put_map_option_value(const string opt, const char *value_char,
				string (*parse_cb)(const char *))
{
  if (!value_char || *value_char == '\0') {
    cerr << "rbd: " << opt << " option requires a value" << std::endl;
    return 1;
  }

  string value = parse_cb(value_char);
  if (value.empty()) {
    cerr << "rbd: invalid " << opt << " value '" << value_char << "'"
	 << std::endl;
    return 1;
  }

  put_map_option(opt, opt + "=" + value);
  return 0;
}

static int parse_map_options(char *options)
{
  for (char *this_char = strtok(options, ", ");
       this_char != NULL;
       this_char = strtok(NULL, ",")) {
    char *value_char;

    if ((value_char = strchr(this_char, '=')) != NULL)
      *value_char++ = '\0';

    if (!strcmp(this_char, "fsid")) {
      if (put_map_option_value("fsid", value_char, map_option_uuid_cb))
	return 1;
    } else if (!strcmp(this_char, "ip")) {
      if (put_map_option_value("ip", value_char, map_option_ip_cb))
	return 1;
    } else if (!strcmp(this_char, "share") || !strcmp(this_char, "noshare")) {
      put_map_option("share", this_char);
    } else if (!strcmp(this_char, "crc") || !strcmp(this_char, "nocrc")) {
      put_map_option("crc", this_char);
    } else if (!strcmp(this_char, "mount_timeout")) {
      if (put_map_option_value("mount_timeout", value_char, map_option_int_cb))
	return 1;
    } else if (!strcmp(this_char, "osdkeepalive")) {
      if (put_map_option_value("osdkeepalive", value_char, map_option_int_cb))
	return 1;
    } else if (!strcmp(this_char, "osd_idle_ttl")) {
      if (put_map_option_value("osd_idle_ttl", value_char, map_option_int_cb))
	return 1;
    } else if (!strcmp(this_char, "rw") || !strcmp(this_char, "ro")) {
      put_map_option("rw", this_char);
    } else {
      cerr << "rbd: unknown map option '" << this_char << "'" << std::endl;
      return 1;
    }
  }

  return 0;
}

enum {
  OPT_NO_CMD = 0,
  OPT_LIST,
  OPT_INFO,
  OPT_CREATE,
  OPT_RESIZE,
  OPT_RM,
  OPT_EXPORT,
  OPT_EXPORT_DIFF,
  OPT_DIFF,
  OPT_IMPORT,
  OPT_IMPORT_DIFF,
  OPT_COPY,
  OPT_RENAME,
  OPT_WATCH,
  OPT_MAP,
  OPT_UNMAP,
  OPT_SHOWMAPPED,
  OPT_LOCK_LIST,
  OPT_LOCK_ADD,
  OPT_LOCK_REMOVE,
  OPT_BENCH_WRITE,
};

static int get_cmd(const char *cmd, bool lockcmd)
{
  if (!lockcmd) {
    if (strcmp(cmd, "ls") == 0 ||
	strcmp(cmd, "list") == 0)
      return OPT_LIST;
    if (strcmp(cmd, "info") == 0)
      return OPT_INFO;
    if (strcmp(cmd, "create") == 0)
      return OPT_CREATE;
    if (strcmp(cmd, "resize") == 0)
      return OPT_RESIZE;
    if (strcmp(cmd, "rm") == 0)
      return OPT_RM;
    if (strcmp(cmd, "export") == 0)
      return OPT_EXPORT;
    if (strcmp(cmd, "import") == 0)
      return OPT_IMPORT;
    if (strcmp(cmd, "copy") == 0 ||
	strcmp(cmd, "cp") == 0)
      return OPT_COPY;
    if (strcmp(cmd, "rename") == 0 ||
	strcmp(cmd, "mv") == 0)
      return OPT_RENAME;
    if (strcmp(cmd, "watch") == 0)
      return OPT_WATCH;
    if (strcmp(cmd, "map") == 0)
      return OPT_MAP;
    if (strcmp(cmd, "showmapped") == 0)
      return OPT_SHOWMAPPED;
    if (strcmp(cmd, "unmap") == 0)
      return OPT_UNMAP;
    if (strcmp(cmd, "bench-write") == 0)
      return OPT_BENCH_WRITE;
  } else {
    if (strcmp(cmd, "ls") == 0 ||
	strcmp(cmd, "list") == 0)
      return OPT_LOCK_LIST;
    if (strcmp(cmd, "add") == 0)
      return OPT_LOCK_ADD;
    if (strcmp(cmd, "remove") == 0 ||
	strcmp(cmd, "rm") == 0)
      return OPT_LOCK_REMOVE;
  }

  return OPT_NO_CMD;
}

/*
 * Called 1-N times depending on how many args the command needs.  If
 * the positional varN is already set, set the next one; this handles
 * both --args above and unadorned args below.	Calling with all args
 * filled is an error.
 */
static bool set_conf_param(const char *param, const char **var1,
			   const char **var2, const char **var3)
{
  if (!*var1)
    *var1 = param;
  else if (var2 && !*var2)
    *var2 = param;
  else if (var3 && !*var3)
    *var3 = param;
  else
    return false;
  return true;
}

bool size_set;

int main(int argc, const char **argv)
{
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx, dest_io_ctx;
  librbd::Image image;

  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  int opt_cmd = OPT_NO_CMD;
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  const char *volname = NULL;
  uint64_t size = 0;  // in bytes
  bool output_format_specified = false;
  const char *imgname = NULL, *destname = NULL,
    *dest_volname = NULL, *path = NULL,
    *devpath = NULL, *lock_cookie = NULL, *lock_client = NULL,
    *lock_tag = NULL, *output_format = "plain";
#if 0
  bool lflag = false;
#endif
  int pretty_format = 0;
  long long bench_io_size = 4096, bench_io_threads = 16, bench_bytes = 1 << 30;
  string bench_pattern = "seq";

  std::string val;
  std::ostringstream err;
  long long sizell = 0;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--secret", (char*)NULL)) {
      int r = g_conf->set_val("keyfile", val.c_str());
      assert(r == 0);
    } else if (ceph_argparse_flag(args, &i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "--vol", (char*)NULL)) {
      volname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      output_format = strdup(val.c_str());
      output_format_specified = true;
    } else if (ceph_argparse_flag(args, &i, &val, "--pretty-format", (char*)NULL)) {
      pretty_format = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-vol", (char*)NULL)) {
      dest_volname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--image", (char*)NULL)) {
      imgname = strdup(val.c_str());
    } else if (ceph_argparse_withlonglong(args, i, &sizell, &err, "-s", "--size", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << "rbd: " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      if (sizell < 0) {
	cerr << "rbd: size must be >= 0" << std::endl;
	return EXIT_FAILURE;
      }
      size = sizell << 20;   // bytes to MB
      size_set = true;
#if 0
    } else if (ceph_argparse_flag(args, &i, "-l", "--long", (char*)NULL)) {
      lflag = true;
#endif
    } else if (ceph_argparse_withlonglong(args, i, &bench_io_size, &err, "--io-size", (char*)NULL)) {
    } else if (ceph_argparse_withlonglong(args, i, &bench_io_threads, &err, "--io-threads", (char*)NULL)) {
    } else if (ceph_argparse_withlonglong(args, i, &bench_bytes, &err, "--io-total", (char*)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &bench_pattern, &err, "--io-pattern", (char*)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &val, "--path", (char*)NULL)) {
      path = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--dest", (char*)NULL)) {
      destname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--shared", (char *)NULL)) {
      lock_tag = strdup(val.c_str());
    } else if (ceph_argparse_flag(args, &i, "--no-settle", (char *)NULL)) {
      udevadm_settle = false;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--options", (char*)NULL)) {
      char *map_options = strdup(val.c_str());
      if (parse_map_options(map_options)) {
	cerr << "rbd: couldn't parse map options" << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_flag(args, &i, "--read-only", (char *)NULL)) {
      // --read-only is equivalent to -o ro
      put_map_option("rw", "ro");
    } else if (ceph_argparse_flag(args, &i, "--no-progress", (char *)NULL)) {
      progress = false;
    } else if (ceph_argparse_flag(args, &i , "--allow-shrink", (char *)NULL)) {
      resize_allow_shrink = true;
    } else if (ceph_argparse_binary_flag(args, i, &pretty_format, NULL, "--pretty-format", (char*)NULL)) {
    } else {
      ++i;
    }
  }

  common_init_finish(g_ceph_context);

  i = args.begin();
  if (i == args.end()) {
    cerr << "rbd: you must specify a command." << std::endl;
    return EXIT_FAILURE;
  } else if (strcmp(*i, "lock") == 0) {
    i = args.erase(i);
    if (i == args.end()) {
      cerr << "rbd: which lock command do you want?" << std::endl;
      return EXIT_FAILURE;
    }
    opt_cmd = get_cmd(*i, true);
  } else {
    opt_cmd = get_cmd(*i, false);
  }
  if (opt_cmd == OPT_NO_CMD) {
    cerr << "rbd: error parsing command '" << *i << "'; -h or --help for usage" << std::endl;
    return EXIT_FAILURE;
  }

  // loop across all remaining arguments; by command, accumulate any
  // that are still missing into the appropriate variables, one at a
  // time (i.e. SET_CONF_PARAM will be called N times for N remaining
  // arguments).

#define SET_CONF_PARAM(v, p1, p2, p3) \
if (!set_conf_param(v, p1, p2, p3)) { \
  cerr << "rbd: extraneous parameter " << v << std::endl; \
  return EXIT_FAILURE; \
}

  for (i = args.erase(i); i != args.end(); ++i) {
    const char *v = *i;
    switch (opt_cmd) {
      case OPT_LIST:
	SET_CONF_PARAM(v, &volname, NULL, NULL);
	break;
      case OPT_INFO:
      case OPT_CREATE:
      case OPT_RESIZE:
      case OPT_RM:
      case OPT_WATCH:
      case OPT_MAP:
      case OPT_BENCH_WRITE:
      case OPT_LOCK_LIST:
      case OPT_DIFF:
	SET_CONF_PARAM(v, &imgname, NULL, NULL);
	break;
      case OPT_UNMAP:
	SET_CONF_PARAM(v, &devpath, NULL, NULL);
	break;
      case OPT_EXPORT:
      case OPT_EXPORT_DIFF:
	SET_CONF_PARAM(v, &imgname, &path, NULL);
	break;
      case OPT_IMPORT:
      case OPT_IMPORT_DIFF:
	SET_CONF_PARAM(v, &path, &imgname, NULL);
	break;
      case OPT_COPY:
      case OPT_RENAME:
	SET_CONF_PARAM(v, &imgname, &destname, NULL);
	break;
      case OPT_SHOWMAPPED:
	cerr << "rbd: showmapped takes no parameters" << std::endl;
	return EXIT_FAILURE;
      case OPT_LOCK_ADD:
	SET_CONF_PARAM(v, &imgname, &lock_cookie, NULL);
	break;
      case OPT_LOCK_REMOVE:
	SET_CONF_PARAM(v, &imgname, &lock_client, &lock_cookie);
	break;
    default:
	assert(0);
	break;
    }
  }

  if (pretty_format && !strcmp(output_format, "plain")) {
    cerr << "rbd: --pretty-format only works when --format is json or xml"
	 << std::endl;
    return EXIT_FAILURE;
  }

  boost::scoped_ptr<Formatter> formatter;
  if (output_format_specified && opt_cmd != OPT_SHOWMAPPED &&
      opt_cmd != OPT_INFO && opt_cmd != OPT_LIST &&
      opt_cmd != OPT_LOCK_LIST) {
    cerr << "rbd: command doesn't use output formatting"
	 << std::endl;
    return EXIT_FAILURE;
  } else if (get_outfmt(output_format, pretty_format, &formatter) < 0) {
    return EXIT_FAILURE;
  }

  if (opt_cmd == OPT_EXPORT && !imgname) {
    cerr << "rbd: image name was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if ((opt_cmd == OPT_IMPORT || opt_cmd == OPT_IMPORT_DIFF) && !path) {
    cerr << "rbd: path was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if (opt_cmd == OPT_IMPORT && !destname) {
    destname = imgname;
    if (!destname)
      destname = imgname_from_path(path);
    imgname = NULL;
  }

  if (opt_cmd != OPT_LOCK_ADD && lock_tag) {
    cerr << "rbd: only the lock add command uses the --shared option"
	 << std::endl;
    return EXIT_FAILURE;
  }

  if ((opt_cmd == OPT_LOCK_ADD || opt_cmd == OPT_LOCK_REMOVE) &&
      !lock_cookie) {
    cerr << "rbd: lock id was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if (opt_cmd != OPT_LIST &&
      opt_cmd != OPT_IMPORT &&
      opt_cmd != OPT_IMPORT_DIFF &&
      opt_cmd != OPT_UNMAP &&
      opt_cmd != OPT_SHOWMAPPED && !imgname) {
    cerr << "rbd: image name was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if (opt_cmd == OPT_UNMAP && !devpath) {
    cerr << "rbd: device path was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  // do this unconditionally so we can parse vol/image into
  // the relevant parts
  set_vol_image_name(volname, imgname, (char **)&volname,
		     (char **)&imgname);
  set_vol_image_name(dest_volname, destname, (char **)&dest_volname,
		     (char **)&destname);

  if (opt_cmd == OPT_IMPORT) {
    if (volname && dest_volname) {
      cerr << "rbd: source and destination vol both specified" << std::endl;
      return EXIT_FAILURE;
    }
    if (imgname && destname) {
      cerr << "rbd: source and destination image both specified" << std::endl;
      return EXIT_FAILURE;
    }
    if (volname)
      dest_volname = volname;
  }

  if (!volname)
    volname = "rbd";

  if (!dest_volname)
    dest_volname = "rbd";

  if (opt_cmd == OPT_EXPORT && !path)
    path = imgname;

  if ((opt_cmd == OPT_COPY || opt_cmd == OPT_RENAME) &&
      !destname ) {
    cerr << "rbd: destination image name was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if ((opt_cmd == OPT_RENAME) && (strcmp(volname, dest_volname) != 0)) {
    cerr << "rbd: mv/rename across vols not supported" << std::endl;
    cerr << "source vol: " << volname << " dest vol: " << dest_volname
      << std::endl;
    return EXIT_FAILURE;
  }

  bool talk_to_cluster = (opt_cmd != OPT_MAP &&
			  opt_cmd != OPT_UNMAP &&
			  opt_cmd != OPT_SHOWMAPPED);
  if (talk_to_cluster && rados.init_with_context(g_ceph_context) < 0) {
    cerr << "rbd: couldn't initialize rados!" << std::endl;
    return EXIT_FAILURE;
  }

  if (talk_to_cluster && rados.connect() < 0) {
    cerr << "rbd: couldn't connect to the cluster!" << std::endl;
    return EXIT_FAILURE;
  }

  int r;
  if (talk_to_cluster && opt_cmd != OPT_IMPORT) {
    r = rados.ioctx_create(volname, io_ctx);
    if (r < 0) {
      cerr << "rbd: error opening vol " << volname << ": "
	   << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (imgname && talk_to_cluster &&
      (opt_cmd == OPT_RESIZE || opt_cmd == OPT_WATCH ||
       opt_cmd == OPT_LOCK_ADD || opt_cmd == OPT_LOCK_REMOVE ||
       opt_cmd == OPT_BENCH_WRITE || opt_cmd == OPT_INFO ||
       opt_cmd == OPT_EXPORT || opt_cmd == OPT_COPY ||
       opt_cmd == OPT_DIFF || opt_cmd == OPT_LOCK_LIST)) {
    if (opt_cmd == OPT_INFO || opt_cmd == OPT_EXPORT ||
	opt_cmd == OPT_EXPORT || opt_cmd == OPT_COPY ||
	opt_cmd == OPT_LOCK_LIST) {
      r = rbd.open_read_only(io_ctx, image, imgname);
    } else {
      r = rbd.open(io_ctx, image, imgname);
    }
    if (r < 0) {
      cerr << "rbd: error opening image " << imgname << ": "
	   << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_COPY || opt_cmd == OPT_IMPORT) {
    r = rados.ioctx_create(dest_volname, dest_io_ctx);
    if (r < 0) {
      cerr << "rbd: error opening vol " << dest_volname << ": "
	   << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_CREATE || opt_cmd == OPT_RESIZE) {
    if (!size_set) {
      cerr << "rbd: must specify --size <MB>" << std::endl;
      return EINVAL;
    }
  }

  switch (opt_cmd) {
#if 0
  case OPT_LIST:
    r = do_list(rbd, io_ctx, lflag, formatter.get());
    if (r < 0) {
      cerr << "rbd: list: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
#endif
    cerr << "rbd: list: unsupported, currently." << std::endl;
    return -1;
    break;

  case OPT_CREATE:
    r = do_create(rbd, io_ctx, imgname, size);
    if (r < 0) {
      cerr << "rbd: create error: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_RENAME:
    r = do_rename(rbd, io_ctx, imgname, destname);
    if (r < 0) {
      cerr << "rbd: rename error: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_INFO:
    r = do_show_info(imgname, image, formatter.get());
    if (r < 0) {
      cerr << "rbd: info: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_RM:
    r = do_delete(rbd, io_ctx, imgname);
    if (r < 0) {
      if (r == -EBUSY) {
	cerr << "rbd: error: image still has watchers"
	     << std::endl
	     << "This means the image is still open or the client using "
	     << "it crashed. Try again after closing/unmapping it or "
	     << "waiting 30s for the crashed client to timeout."
	     << std::endl;
      } else {
	cerr << "rbd: delete error: " << cpp_strerror(-r) << std::endl;
      }
      return -r ;
    }
    break;

  case OPT_RESIZE:
    librbd::image_info_t info;
    r = image.stat(info, sizeof(info));
    if (r < 0) {
      cerr << "rbd: resize error: " << cpp_strerror(-r) << std::endl;
      return -r;
    }

    if (info.size > size && !resize_allow_shrink) {
      cerr << "rbd: shrinking an image is only allowed with the --allow-shrink flag" << std::endl;
      return EINVAL;
    }

    r = do_resize(image, size);
    if (r < 0) {
      cerr << "rbd: resize error: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_EXPORT:
    if (!path) {
      cerr << "rbd: export requires pathname" << std::endl;
      return EINVAL;
    }
    r = do_export(image, path);
    if (r < 0) {
      cerr << "rbd: export error: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_IMPORT:
    if (!path) {
      cerr << "rbd: import requires pathname" << std::endl;
      return EINVAL;
    }
    r = do_import(rbd, dest_io_ctx, destname, path);
    if (r < 0) {
      cerr << "rbd: import failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_COPY:
    r = do_copy(image, dest_io_ctx, destname);
    if (r < 0) {
      cerr << "rbd: copy failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_WATCH:
    r = do_watch(io_ctx, imgname);
    if (r < 0) {
      cerr << "rbd: watch failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_MAP:
    r = do_kernel_add(volname, imgname);
    if (r < 0) {
      cerr << "rbd: add failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_UNMAP:
    r = do_kernel_rm(devpath);
    if (r < 0) {
      cerr << "rbd: remove failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_SHOWMAPPED:
    r = do_kernel_showmapped(formatter.get());
    if (r < 0) {
      cerr << "rbd: showmapped failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;

  case OPT_LOCK_LIST:
    r = do_lock_list(image, formatter.get());
    if (r < 0) {
      cerr << "rbd: listing locks failed: " << cpp_strerror(r) << std::endl;
      return -r;
    }
    break;

  case OPT_LOCK_ADD:
    r = do_lock_add(image, lock_cookie, lock_tag);
    if (r < 0) {
      if (r == -EBUSY || r == -EEXIST) {
	if (lock_tag) {
	  cerr << "rbd: lock is alrady held by someone else"
	       << " with a different tag" << std::endl;
	} else {
	  cerr << "rbd: lock is already held by someone else" << std::endl;
	}
      } else {
	cerr << "rbd: taking lock failed: " << cpp_strerror(r) << std::endl;
      }
      return -r;
    }
    break;

  case OPT_LOCK_REMOVE:
    r = do_lock_remove(image, lock_cookie, lock_client);
    if (r < 0) {
      cerr << "rbd: releasing lock failed: " << cpp_strerror(r) << std::endl;
      return -r;
    }
    break;

  case OPT_BENCH_WRITE:
    r = do_bench_write(image, bench_io_size, bench_io_threads, bench_bytes, bench_pattern);
    if (r < 0) {
      cerr << "bench-write failed: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    break;
  }

  return 0;
}
