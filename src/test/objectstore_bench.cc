/*
 *  Ceph ObjectStore benchmark
 */

#include <chrono>
#include <cassert>

#include "os/ObjectStore.h"

#include "global/global_init.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"

#ifdef HAVE_CDS
#include <cds/init.h>  //cds::Initialize и cds::Terminate
#include <cds/gc/hp.h> //cds::gc::HP (Hazard Pointer)
#include <cds/intrusive/skip_list_hp.h> //cds intrusive skip lists
#endif


#define dout_subsys ceph_subsys_filestore

static void usage()
{
  derr << "usage: objectstore-bench [flags]\n"
      "	 --size\n"
      "	       total size in bytes\n"
      "	 --block-size\n"
      "	       block size in bytes for each write\n"
      "	 --repeats\n"
      "	       number of times to repeat the write cycle\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n"
      "	 --multi-object\n"
      "	       have each thread write to a separate object\n" << dendl;
  generic_server_usage();
}

// helper class for bytes with units
struct byte_units {
  size_t v;
  byte_units(size_t v) : v(v) {}

  bool parse(const std::string &val);

  operator size_t() const { return v; }
};

bool byte_units::parse(const string &val)
{
  char *endptr;
  errno = 0;
  unsigned long long ret = strtoull(val.c_str(), &endptr, 10);
  if (errno == ERANGE && ret == ULLONG_MAX)
    return false;
  if (errno && ret == 0)
    return false;
  if (endptr == val.c_str())
    return false;

  // interpret units
  int lshift = 0;
  switch (*endptr) {
    case 't':
    case 'T':
      lshift += 10;
      // cases fall through
    case 'g':
    case 'G':
      lshift += 10;
    case 'm':
    case 'M':
      lshift += 10;
    case 'k':
    case 'K':
      lshift += 10;
      if (*++endptr)
	return false;
    case 0:
      break;

    default:
      return false;
  }

  // test for overflow
  typedef std::numeric_limits<unsigned long long> limits;
  if (ret & ~((1 << (limits::digits - lshift))-1))
    return false;

  v = ret << lshift;
  return true;
}

std::ostream& operator<<(std::ostream &out, const byte_units &amount)
{
  static const char* units[] = { "B", "KB", "MB", "GB", "TB" };
  static const int max_units = sizeof(units)/sizeof(*units);

  int unit = 0;
  auto v = amount.v;
  while (v >= 1024 && unit < max_units) {
    // preserve significant bytes
    if (v < 1048576 && (v % 1024 != 0))
      break;
    v >>= 10;
    unit++;
  }
  return out << v << ' ' << units[unit];
}

byte_units size = 1048576;
byte_units block_size = 4096;
int repeats = 1;
int n_threads = 1;
bool multi_object = false;
ObjectStore *fs;

class OBS_Worker : public Thread
{
  object_t oid;
  ObjectStore::CollectionHandle ch;

 public:
  OBS_Worker() { }

  void set_oid(const object_t &_oid) { oid = _oid; }
  void set_coll(ObjectStore::CollectionHandle _ch) { ch = _ch; }

  void *entry() {
    bufferlist data;
    data.append(buffer::create(block_size));

    dout(0) << "Writing " << size << " in blocks of " << block_size
	<< dendl;

    // use a sequencer for each thread so they don't serialize each other
    ObjectStore::Sequencer seq("osbench worker");

    for (int ix = 0; ix < repeats; ++ix) {
      uint64_t offset = 0;
      size_t len = size;

      list<ObjectStore::Transaction*> tls;

      std::cout << "Write cycle " << ix << std::endl;
      while (len) {
	size_t count = len < block_size ? len : (size_t)block_size;

	ObjectStore::Transaction *t = new ObjectStore::Transaction;
	t->push_col(ch);
	t->push_oid(oid);

	// use the internal offsets for col/cid, obj/oid for brevity
	t->write(offset, count, data);
	tls.push_back(t);

	offset += count;
	len -= count;
      }

      // set up the finisher
      Mutex lock;
      Cond cond;
      bool done = false;

      fs->queue_transactions(&seq, tls, NULL,
			     new C_SafeCond(&lock, &cond, &done));

      lock.Lock();
      while (!done)
	cond.Wait(lock);
      lock.Unlock();

      while (!tls.empty()) {
	ObjectStore::Transaction *t = tls.front();
	tls.pop_front();
	delete t;
      }
    }

    // return ObjectStore handles
    fs->close_collection(ch);

    return 0;
  }
};

int main(int argc, const char *argv[])
{
  int ret;

  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
	      CODE_ENVIRONMENT_UTILITY, 0);

  string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--size", (char*)NULL)) {
      if (!size.parse(val)) {
	derr << "error parsing size: It must be an int." << dendl;
	usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)NULL)) {
      if (!block_size.parse(val)) {
	derr << "error parsing block-size: It must be an int." << dendl;
	usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--repeats", (char*)NULL)) {
      repeats = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)NULL)) {
      n_threads = atoi(val.c_str());
    } else if (ceph_argparse_flag(args, &i, "--multi-object", (char*)NULL)) {
      multi_object = true;
    } else {
      derr << "Error: can't understand argument: " << *i <<
	  "\n" << dendl;
      usage();
    }
  }

  common_init_finish(g_ceph_context);

  // create object store
  dout(0) << "objectstore " << g_conf->osd_objectstore << dendl;
  dout(0) << "data " << g_conf->osd_data << dendl;
  dout(0) << "journal " << g_conf->osd_journal << dendl;
  dout(0) << "size " << size << dendl;
  dout(0) << "block-size " << block_size << dendl;
  dout(0) << "repeats " << repeats << dendl;

  fs = ObjectStore::create(g_ceph_context,
			   g_conf->osd_objectstore,
			   g_conf->osd_data,
			   g_conf->osd_journal);
  if (fs == NULL) {
    derr << "bad objectstore type " << g_conf->osd_objectstore << dendl;
    return 1;
  }
  if (fs->mkfs() < 0) {
    derr << "mkfs failed" << dendl;
    return 1;
  }
  if (fs->mount() < 0) {
    derr << "mount failed" << dendl;
    return 1;
  }

  dout(10) << "created objectstore " << fs << dendl;

  coll_t cid;
  ObjectStore::Transaction ft;
  ret = ft.create_collection(cid);
  fs->apply_transaction(ft);
  if (ret) {
      derr << "objectstore_bench: error while creating collection " << cid
	   << " apply_transaction returned " << ret << dendl;
      return 1;
  }

  CollectionHandle ch = fs->open_collection(cid);
  if (! ch) {
    derr << "objectstore_bench: error opening collection " << cid << dendl;
    return 1;
  }

  std::vector<object_t> oids;
  if (multi_object) {
    oids.resize(n_threads);
    for (int i = 0; i < n_threads; i++) {
      stringstream oss;
      oss << "osbench-thread-" << i;
      oids[i].name = oss.str();
    }
  } else {
    oids.push_back(object_t("osbench"));
  }

  std::vector<OBS_Worker> workers(n_threads);
  auto t1 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < n_threads; i++) {
    workers[i].set_coll(ch);
    if (multi_object)
      workers[i].set_oid(oids[i]);
    else
      workers[i].set_oid(oids[0]);
    workers[i].create();
  }
  for (auto &worker : workers)
    worker.join();
  auto t2 = std::chrono::high_resolution_clock::now();
  workers.clear();

  // remove the object
  ObjectStore::Transaction t;
  uint16_t c_ix, o_ix;
  c_ix = t.push_col(ch);
  for (vector<object_t>::iterator i = oids.begin(); i != oids.end(); ++i) {
      o_ix = t.push_oid(hobject_t(*i));
      t.remove(c_ix, o_ix);
  }
  fs->apply_transaction(t);

  fs->close_collection(ch);
  fs->umount();
  delete fs;

  using std::chrono::duration_cast;
  using std::chrono::microseconds;
  auto duration = duration_cast<microseconds>(t2 - t1);
  byte_units total = size * repeats * n_threads;
  byte_units rate = (1000000LL * total) / duration.count();
  size_t iops = (1000000LL * total / block_size) / duration.count();
  dout(0) << "Wrote " << total << " in "
      << duration.count() << "us, at a rate of " << rate << "/s and "
      << iops << " iops" << dendl;

  return 0;
}
