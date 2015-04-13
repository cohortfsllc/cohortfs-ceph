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
#include <boost/scope_exit.hpp>
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
#include "include/byteorder.h"

#include "include/intarith.h"

#include "include/compat.h"
#include "common/blkdev.h"
#include "librbd/Image.h"

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

#include "osdc/RadosClient.h"

#define MAX_SECRET_LEN 1000
#define MAX_VOL_NAME_SIZE 128

#define RBD_DIFF_BANNER "rbd diff v1\n"

using namespace std::placeholders;
using std::cout;
using std::shared_ptr;
using librbd::Image;

bool udevadm_settle = true;
bool progress = true;

map<string, string> map_options; // -o / --options map

#define dout_subsys ceph_subsys_rbd

static CephContext* cct;

void usage()
{
  cout <<
"usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...\n"
"where 'vol' is a rados vol name (default is 'rbd') and 'cmd' is one of:\n"
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
"  --vol <volume>	      source volume name\n"
"  --image <image-name>	      image name\n"
"  --dest <image-name>	      destination [volume and] image name\n"
"  --dest-vol <name>	      destination volume name\n"
"  --path <path-name>	      path name for import/export\n"
"  --size <size in MB>	      size of image for create and resize\n"
"  --id <username>	      rados user (without 'client.'prefix) to\n"
"			      authenticate as\n"
    "  --keyfile <path>	      file containing secret key for use with cephx\n";
}

struct rbd_bencher {
  Image *image;
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  std::condition_variable cond;
  int in_flight;

  void callback(int r) {
    if (r != 0) {
      cout << "write error: " << cpp_strerror(r) << std::endl;
      abort();
    }
    unique_lock bl(lock);
    in_flight--;
    cond.notify_all();
    bl.unlock();
  }

  rbd_bencher(Image *i)
    : image(i),
      in_flight(0) { }

  bool start_write(int max, uint64_t off, uint64_t len, bufferlist& bl)
  {
    {
      lock_guard l(lock);
      if (in_flight >= max)
	return false;
      in_flight++;
    }
    image->write(off, len, bl,
		 std::bind(&rbd_bencher::callback, this, _1));
    return true;
  }

  void wait_for(int max) {
    unique_lock l(lock);
    while (in_flight > max) {
      cond.wait_for(l, 200ms);
    }
  }

};

static int do_bench_write(Image& image, uint64_t io_size,
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

  ceph::mono_time start = ceph::mono_clock::now();
  ceph::timespan last = 0ns;
  unsigned ios = 0;

  uint64_t size = image.get_size();

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

    ceph::mono_time now = ceph::mono_clock::now();
    std::chrono::duration<double> elapsed = now - start;
    if ((elapsed > last ? elapsed - last : last - elapsed) > 1s) {
      printf("%5lf  %8d	%8.2lf	%8.2lf\n",
	     elapsed.count(),
	     (int)(ios - io_threads),
	     (double)(ios - io_threads) / elapsed.count(),
	     (double)(off - io_threads * io_size) /
	     elapsed.count());
      last = std::chrono::duration_cast<ceph::timespan>(elapsed);
    }
  }
  b.wait_for(0);
  try {
    image.flush();
  } catch (std::error_condition &e) {
    cerr << "Error flushing data at the end: " << e.message() << std::endl;
  }

  std::chrono::duration<double> elapsed = ceph::mono_clock::now() - start;

  printf("elapsed: %5lld\tops: %8d  ops/sec: %8.2lf  bytes/sec: %8.2lf\n",
	 std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(),
	 ios, (double)ios / elapsed.count(),
	 (double)off / elapsed.count());

  return 0;
}

static void export_read_cb(int fd, uint64_t off, size_t len,
			   const bufferlist& buf)
{
  ssize_t ret;
  if (buf.is_zero()) {
    /* a hole */
  }

  ret = lseek64(fd, off, SEEK_SET);
  if (ret < 0)
    throw std::system_error(errno, std::system_category());
  ret = buf.write_fd(fd);
  if (ret < 0)
    throw std::system_error(errno, std::system_category());
}

static void do_export(librbd::Image& image, const string& path)
{
  int fd;

  if ((fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644)) < 0) {
    cerr << "rbd: error opening " << path << std::endl;
    throw std::error_condition(errno, std::generic_category());
  }
  BOOST_SCOPE_EXIT(fd, &path) {
    int r = ::close(fd);
    if (r < 0) {
      cerr << "rbd: error closing " << path << std::endl;
      throw std::error_condition(errno, std::generic_category());
    }
  } BOOST_SCOPE_EXIT_END;

  image.read_iterate(std::bind(export_read_cb, fd, _1, _2, _3));

  if (ftruncate(fd, image.get_size()) < 0) {
    cerr << "rbd: error truncating " << path << std::endl;
    throw std::error_condition(errno, std::generic_category());
  }
}

static string imgname_from_path(const string& path)
{
  size_t slash = path.rfind('/');
  if (slash == string::npos)
    return path;

  return string(path.cbegin() + slash + 1, path.cend());
}

static void set_vol_image_name(const string& orig_vol, const string& orig_img,
			       string& new_vol, string& new_img)
{
  auto sep = orig_img.find('/');
  if (sep == string::npos) {
    new_img = orig_img;
    new_vol = orig_vol;
    return;
  }

  // the C++ string library departing from the iterator pattern to
  // bound subsets like every other part of the STL is stupid.

  // It should be ashamed of itself.

  // Fortunately...

  string t(orig_img); // Prevent aliasing bug
  new_vol = string(t.begin(), t.begin() + sep);
  new_img = string(t.begin() + sep + 1, t.end());
}

static void do_import(rados::RadosClient& rc,
		      const shared_ptr<const Volume>& v, const string& imgname,
		      const string& path)
{
  int fd;
  if ((fd = ::open(path.c_str(), O_RDONLY)) < 0) {
    cerr << "rbd: error opening " << path << std::endl;
    throw std::error_condition(errno, std::generic_category());
  }
  BOOST_SCOPE_EXIT(fd) {
    // This is a read open, so none of the reasons for checking the
    // return value of close apply.
    ::close(fd);
  } BOOST_SCOPE_EXIT_END;

  uint64_t size = 0;
  struct stat stat_buf;
  if ((fstat(fd, &stat_buf)) < 0) {
    cerr << "rbd: stat error " << path << std::endl;
    throw std::error_condition(errno, std::generic_category());
  }

  if (S_ISDIR(stat_buf.st_mode)) {
    cerr << "rbd: cannot import a directory" << std::endl;
    throw std::make_error_condition(std::errc::is_a_directory);
  }

  if (stat_buf.st_size)
    size = (uint64_t)stat_buf.st_size;
  if (!size) {
    int64_t bdev_size = 0;
    int r = get_block_device_size(fd, &bdev_size);
    if (r < 0) {
      cerr << "rbd: unable to get size of file/block device" << std::endl;
      throw std::error_condition(-r, std::generic_category());
    }
    assert(bdev_size >= 0);
    size = (uint64_t) bdev_size;
  }

  Image image;
  try {
    Image::create(&rc, v, imgname, size);
    image = Image(&rc, v, imgname);
  } catch (std::error_condition &e) {
    cerr << "rbd:: image creation failed: " << e.message() << std::endl;
  }

  ssize_t readlen;
  uint64_t image_pos = 0;
  bufferlist bl;
  try {
    while ((readlen = bl.read_fd(fd, v->op_size())) > 0) {
      // write as much as we got; perhaps less than op_size
      // but skip writing zeros to create sparse images
      if (!bl.is_zero()) {
	image.write(image_pos, bl.length(), bl);
      }
      // done with whole block, whether written or not
      image_pos += bl.length();
      bl.clear();
    }
    image.flush();
  } catch (std::error_condition& e) {
    cerr << "rbd: error writing to image position " << image_pos
	 << ": " << e.message() << std::endl;
    throw;
  }
}

enum {
  OPT_NO_CMD = 0,
  OPT_INFO,
  OPT_CREATE,
  OPT_RESIZE,
  OPT_RM,
  OPT_EXPORT,
  OPT_IMPORT,
  OPT_COPY,
  OPT_RENAME,
  OPT_BENCH_WRITE,
};

static int get_cmd(const char *cmd)
{
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
  if (strcmp(cmd, "bench-write") == 0)
    return OPT_BENCH_WRITE;

  return OPT_NO_CMD;
}

/*
 * Called 1-N times depending on how many args the command needs.  If
 * the positional varN is already set, set the next one; this handles
 * both --args above and unadorned args below.	Calling with all args
 * filled is an error.
 */
static bool set_conf_param(const char* param, string& var1,
			   string& var2)
{
  if (var1.empty())
    var1 = string(param);
  else if (var2.empty())
    var2 = string(param);
  else
    return false;
  return true;
}

bool size_set;

int main(int argc, const char **argv)
{
  librbd::Image image;

  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  int opt_cmd = OPT_NO_CMD;
  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY, 0);

  rados::RadosClient rc(cct);

  std::string volname, dest_volname, imgname, destname, path;
  uint64_t size = 0;  // in bytes
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
      int r = cct->_conf->set_val("keyfile", val.c_str());
      assert(r == 0);
    } else if (ceph_argparse_flag(args, &i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "--vol", (char*)NULL)) {
      volname = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-vol",
				     (char*)NULL)) {
      dest_volname = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--image",
				     (char*)NULL)) {
      imgname = val;
    } else if (ceph_argparse_withlonglong(args, i, &sizell, &err,
					  "-s", "--size", (char*)NULL)) {
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
    } else if (ceph_argparse_withlonglong(args, i, &bench_io_size, &err,
					  "--io-size", (char*)NULL)) {
    } else if (ceph_argparse_withlonglong(args, i, &bench_io_threads, &err,
					  "--io-threads", (char*)NULL)) {
    } else if (ceph_argparse_withlonglong(args, i, &bench_bytes, &err,
					  "--io-total", (char*)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &bench_pattern, &err,
				     "--io-pattern", (char*)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &val, "--path", (char*)NULL)) {
      path = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest", (char*)NULL)) {
      destname = val;
    } else {
      ++i;
    }
  }

  common_init_finish(cct);

  i = args.begin();
  if (i == args.end()) {
    cerr << "rbd: you must specify a command." << std::endl;
    return EXIT_FAILURE;
  } else {
    opt_cmd = get_cmd(*i);
  }
  if (opt_cmd == OPT_NO_CMD) {
    cerr << "rbd: error parsing command '" << *i << "'; -h or --help for usage"
	 << std::endl;
    return EXIT_FAILURE;
  }

  // loop across all remaining arguments; by command, accumulate any
  // that are still missing into the appropriate variables, one at a
  // time (i.e. SET_CONF_PARAM will be called N times for N remaining
  // arguments).

#define SET_CONF_PARAM(v, p1, p2) \
if (!set_conf_param(v, p1, p2)) { \
  cerr << "rbd: extraneous parameter " << v << std::endl; \
  return EXIT_FAILURE; \
}

  for (i = args.erase(i); i != args.end(); ++i) {
    string nil;
    const char *v = *i;
    switch (opt_cmd) {
    case OPT_CREATE:
    case OPT_RESIZE:
    case OPT_RM:
    case OPT_BENCH_WRITE:
      SET_CONF_PARAM(v, imgname, nil);
      break;
    case OPT_EXPORT:
      SET_CONF_PARAM(v, imgname, path);
      break;
    case OPT_IMPORT:
      SET_CONF_PARAM(v, path, imgname);
      break;
    case OPT_COPY:
    case OPT_RENAME:
      SET_CONF_PARAM(v, imgname, destname);
      break;
    default:
      abort();
      break;
    }
  }

  if (opt_cmd == OPT_EXPORT && imgname.empty()) {
    cerr << "rbd: image name was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if ((opt_cmd == OPT_IMPORT) && path.empty()) {
    cerr << "rbd: path was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if (opt_cmd == OPT_IMPORT && destname.empty()) {
    destname = imgname;
    if (destname.empty())
      destname = imgname_from_path(path);
    imgname.clear();
  }

  if (opt_cmd == OPT_IMPORT && dest_volname.empty()) {
    dest_volname = volname;
    volname.clear();
    if (dest_volname.empty()) {
      cerr << "rbd: volume was not specified" << std::endl;
      return EXIT_FAILURE;
    }
  }

  if (opt_cmd != OPT_IMPORT && imgname.empty()) {
    cerr << "rbd: image name was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if (rc.connect() < 0) {
    cerr << "rbd: couldn't connect to the cluster!" << std::endl;
    return EXIT_FAILURE;
  }


  // do this unconditionally so we can parse vol/image into
  // the relevant parts
  set_vol_image_name(volname, imgname, volname, imgname);
  set_vol_image_name(dest_volname, destname, dest_volname, destname);

  shared_ptr<const Volume> vol, destvol;
  if (!volname.empty()) {
    vol = rc.lookup_volume(volname);
    if (!vol) {
      cerr << "rbd: volume " << volname << " does not exist." << std::endl;
      exit(EXIT_FAILURE);
    }
    if (vol->attach(cct) < 0) {
      cerr << "rbd: unable to attach volume " << volname << "." << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  if (!dest_volname.empty()) {
    destvol = rc.lookup_volume(dest_volname);
    if (!destvol) {
      cerr << "rbd: volume " << volname << " does not exist." << std::endl;
      exit(EXIT_FAILURE);
    }
    if (destvol->attach(cct) < 0) {
      cerr << "rbd: unable to attach volume " << dest_volname << "."
	   << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  if (opt_cmd == OPT_IMPORT) {
    if (!volname.empty() && !dest_volname.empty()) {
      cerr << "rbd: source and destination vol both specified" << std::endl;
      return EXIT_FAILURE;
    }
    if (!imgname.empty() && !destname.empty()) {
      cerr << "rbd: source and destination image both specified" << std::endl;
      return EXIT_FAILURE;
    }
    if (!volname.empty())
      dest_volname = volname;
  }

  if (volname.empty())
    volname = "rbd";

  if (dest_volname.empty())
    dest_volname = "rbd";

  if (opt_cmd == OPT_EXPORT && path.empty())
    path = imgname;

  if ((opt_cmd == OPT_COPY || opt_cmd == OPT_RENAME) &&
      destname.empty()) {
    cerr << "rbd: destination image name was not specified" << std::endl;
    return EXIT_FAILURE;
  }

  if ((opt_cmd == OPT_RENAME) && (volname != dest_volname)) {
    cerr << "rbd: mv/rename across vols not supported" << std::endl;
    cerr << "source vol: " << volname << " dest vol: " << dest_volname
      << std::endl;
    return EXIT_FAILURE;
  }

  try {
    if (!imgname.empty() &&
	(opt_cmd == OPT_RESIZE || opt_cmd == OPT_BENCH_WRITE ||
	 opt_cmd == OPT_EXPORT || opt_cmd == OPT_COPY)) {
      if (opt_cmd == OPT_EXPORT || opt_cmd == OPT_EXPORT ||
	  opt_cmd == OPT_COPY) {
	image = Image(&rc, vol, imgname, librbd::read_only);
      } else {
	image = Image(&rc, vol, imgname);
      }
    }
  } catch (std::error_condition& e) {
	cerr << "rbd: error opening image " << imgname << ": "
	     << e.message() << std::endl;
	exit(EXIT_FAILURE);
  }


  if (opt_cmd == OPT_CREATE || opt_cmd == OPT_RESIZE) {
    if (!size_set) {
      cerr << "rbd: must specify --size <MB>" << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  if (path.empty() &&
      ((opt_cmd == OPT_IMPORT) || (opt_cmd == OPT_EXPORT))) {
    cerr << "rbd: "
	 << (opt_cmd == OPT_IMPORT ? "import" : "export")
	 << " requires pathname" << std::endl;
    exit(EXIT_FAILURE);
  }

  string errstr;
  try {
    switch (opt_cmd) {
    case OPT_CREATE:
      errstr = "create of image " + vol->name + "/" + imgname;
      Image::create(&rc, vol, imgname, size);
      break;

    case OPT_RENAME:
      errstr = "rename of image " + vol->name + "/" + imgname + " to " +
	vol->name + "/" + destname;
      Image::rename(&rc, vol, imgname, destname);
      break;

    case OPT_RM:
      errstr = "delete of image " + vol->name + "/" + imgname;
      Image::remove(&rc, vol, imgname);
      break;

    case OPT_RESIZE:
      errstr = "resize of image " + vol->name + "/" + imgname;
      image.resize(size);
      break;

    case OPT_EXPORT:
      errstr = "export of image " + vol->name + "/" + imgname + " to " +
	"path";
      do_export(image, path);
      break;

    case OPT_IMPORT:
      errstr = "import of image " + destvol->name + "/" + imgname + " from " +
	"path";
      do_import(rc, destvol, destname, path);
      break;

    case OPT_COPY:
    {
      errstr = "copy of image " + vol->name + "/" + imgname + " to " +
	destvol->name + "/" + destname;
      Image::create(&rc, destvol, destname, image.get_size());
      Image dest(&rc, destvol, destname);
      image.copy(image, dest);
    }
    break;

    case OPT_BENCH_WRITE:
      errstr = "bench write to " + vol->name + "/" + imgname;
      do_bench_write(image, bench_io_size, bench_io_threads, bench_bytes,
		     bench_pattern);
      break;
    }
  } catch (std::error_condition& e) {
    cerr << "rbd: unable to " << errstr << ": "
	 << e.message() << std::endl;
    exit(EXIT_FAILURE);
  }

  exit(EXIT_SUCCESS);
}
