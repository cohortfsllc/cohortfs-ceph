// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <random>
#include <thread>
#include <vector>

#include "mds/ceph_mds.h"

#include "global/global_init.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_mds
static CephContext* cct;

static void usage()
{
  derr << "usage: bench_libmds [flags]\n"
      "	 --files\n"
      "	       total number of files in the directory\n"
      "	 --lookups\n"
      "	       total number of lookups in the directory\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n" << dendl;
  generic_server_usage();
}

typedef std::mt19937 rng_t;

int create_thread(libmds *mds, const libmds_fileid_t *dir, int start, int end)
{
  for (int i = start; i < end; i++) {
    char name[16];
    snprintf(name, sizeof(name), "file.%d", i);

    int r = libmds_create(mds, dir, name);
    if (r) {
      derr << "libmds_create(\"" << name << "\") failed with " << r << dendl;
      return r;
    }
  }
  return 0;
}

int bench_create(libmds *mds, const libmds_fileid_t *dir,
                 int n_files, int n_threads)
{
  if (n_files % n_threads) {
    derr << "files=" << n_files << " must be divisible by threads= "
        << n_threads << dendl;
    return -EINVAL;
  }
  const int per_thread = n_files / n_threads;

  dout(0) << "Starting " << n_files << " creates in "
      << n_threads << " threads..." << dendl;

  std::vector<int> results(n_threads);
  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  using namespace std::chrono;
  auto t1 = high_resolution_clock::now();

  // start threads
  for (int i = 0; i < n_threads; i++) {
    auto fn = [=, &results]() {
      results[i] = create_thread(mds, dir, i * per_thread,
                                 (i + 1) * per_thread);
    };
    threads.emplace_back(fn);
  }
  // join threads
  for (auto &t : threads)
    t.join();

  auto t2 = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(t2 - t1);

  // check for failures
  for (auto &r : results)
    if (r)
      return r;

  auto rate = (1000000LL * n_files) / duration.count();
  dout(0) << "Created " << n_files << " files in " << duration.count()
      << "us, at a rate of " << rate << "/s" << dendl;

  return 0;
}

int lookup_thread(libmds *mds, const libmds_fileid_t *dir,
                  int n_files, int n_lookups, rng_t &rng)
{
  std::uniform_int_distribution<> dist(0, n_files - 1);

  for (int i = 0; i < n_lookups; i++) {
    char name[16];
    snprintf(name, sizeof(name), "file.%d", dist(rng));

    libmds_ino_t ino;
    int r = libmds_lookup(mds, dir, name, &ino);
    if (r) {
      derr << "libmds_lookup(\"" << name << "\") failed with " << r << dendl;
      return r;
    }
  }
  return 0;
}

int bench_lookup(libmds *mds, const libmds_fileid_t *dir,
                 int n_files, int n_lookups, int n_threads, rng_t &rng)
{
  dout(0) << "Starting " << n_lookups << " lookups in "
      << n_threads << " threads..." << dendl;

  std::vector<int> results(n_threads);
  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  using namespace std::chrono;
  auto t1 = high_resolution_clock::now();

  // start threads
  for (int i = 0; i < n_threads; i++) {
    auto fn = [=, &results, &rng]() {
      results[i] = lookup_thread(mds, dir, n_files, n_lookups, rng);
    };
    threads.emplace_back(fn);
  }
  // join threads
  for (auto &t : threads)
    t.join();

  auto t2 = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(t2 - t1);

  // check for failures
  for (auto &r : results)
    if (r)
      return r;

  auto rate = (1000000LL * n_lookups) / duration.count();
  dout(0) << n_lookups << " lookups in " << duration.count()
      << "us, at a rate of " << rate << "/s" << dendl;

  return 0;
}

int unlink_thread(libmds *mds, const libmds_fileid_t *dir, int start, int end)
{
  for (int i = start; i < end; i++) {
    char name[16];
    snprintf(name, sizeof(name), "file.%d", i);

    int r = libmds_unlink(mds, dir, name);
    if (r) {
      derr << "libmds_unlink(\"" << name << "\") failed with " << r << dendl;
      return r;
    }
  }
  return 0;
}

int bench_unlink(libmds *mds, const libmds_fileid_t *dir,
                 int n_files, int n_threads)
{
  if (n_files % n_threads) {
    derr << "files=" << n_files << " must be divisible by threads= "
        << n_threads << dendl;
    return -EINVAL;
  }
  const int per_thread = n_files / n_threads;

  dout(0) << "Starting " << n_files << " unlinks in "
      << n_threads << " threads..." << dendl;

  std::vector<int> results(n_threads);
  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  using namespace std::chrono;
  auto t1 = high_resolution_clock::now();

  // start threads
  for (int i = 0; i < n_threads; i++) {
    auto fn = [=, &results]() {
      results[i] = unlink_thread(mds, dir, i * per_thread,
                                 (i + 1) * per_thread);
    };
    threads.emplace_back(fn);
  }
  // join threads
  for (auto &t : threads)
    t.join();

  auto t2 = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(t2 - t1);

  // check for failures
  for (auto &r : results)
    if (r)
      return r;

  auto rate = (1000000LL * n_files) / duration.count();
  dout(0) << "Unlinked " << n_files << " files in " << duration.count()
      << "us, at a rate of " << rate << "/s" << dendl;

  return 0;
}

int run_benchmarks(libmds *mds, int n_files, int n_lookups, int n_threads,
                   rng_t &rng)
{
  const uint8_t volume[] = "abcdefghijklmnop";
  const libmds_fileid_t root { volume, 1 };

  int r = bench_create(mds, &root, n_files, n_threads);
  if (r) {
    derr << "bench_create() failed with " << r << dendl;
    return r;
  }

  r = bench_lookup(mds, &root, n_files, n_lookups, n_threads, rng);
  if (r) {
    derr << "bench_lookup() failed with " << r << dendl;
    return r;
  }

  r = bench_unlink(mds, &root, n_files, n_threads);
  if (r) {
    derr << "bench_unlink() failed with " << r << dendl;
    return r;
  }
  return 0;
}

int main(int argc, const char *argv[])
{
  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
		    CODE_ENVIRONMENT_UTILITY, 0);

  int n_files = 100;
  int n_lookups = 100;
  int n_threads = 1;

  string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--files", (char*)NULL)) {
      n_files = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--lookups", (char*)NULL)) {
      n_lookups = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)NULL)) {
      n_threads = atoi(val.c_str());
    } else {
      derr << "Error: can't understand argument: " << *i << dendl;
      usage();
    }
  }

  common_init_finish(cct);

  std::random_device rd;
  std::mt19937 rng(rd());

  // create object store
  dout(0) << "files " << n_files << dendl;
  dout(0) << "lookups " << n_lookups << dendl;
  dout(0) << "threads " << n_threads << dendl;
  dout(0) << "mds_cache_highwater " << cct->_conf->mds_cache_highwater << dendl;
  dout(0) << "mds_cache_lowwater " << cct->_conf->mds_cache_lowwater << dendl;

  struct libmds_init_args initargs = {0};
  libmds *mds = libmds_init(&initargs);
  if (mds == nullptr)
    return 1;

  run_benchmarks(mds, n_files, n_lookups, n_threads, rng);

  libmds_shutdown(mds);
  libmds_join(mds);
  libmds_cleanup(mds);
  return 0;
}
