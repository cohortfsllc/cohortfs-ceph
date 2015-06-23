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

static void usage()
{
  std::cerr << "usage: bench_libmds [flags]\n"
      "	 --files\n"
      "	       total number of files in the directory\n"
      "	 --lookups\n"
      "	       total number of lookups in the directory\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n" << std::endl;
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
      std::cerr << "libmds_create(\"" << name << "\") failed with "
          << r << std::endl;
      return r;
    }
  }
  return 0;
}

int bench_create(libmds *mds, const libmds_fileid_t *dir,
                 int n_files, int n_threads)
{
  if (n_files % n_threads) {
    std::cerr << "files=" << n_files << " must be divisible by threads= "
        << n_threads << std::endl;
    return -EINVAL;
  }
  const int per_thread = n_files / n_threads;

  std::cout << "Starting " << n_files << " creates in "
      << n_threads << " threads..." << std::endl;

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
  std::cout << "Created " << n_files << " files in " << duration.count()
      << "us, at a rate of " << rate << "/s" << std::endl;

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
      std::cerr << "libmds_lookup(\"" << name << "\") failed with "
          << r << std::endl;
      return r;
    }
  }
  return 0;
}

int bench_lookup(libmds *mds, const libmds_fileid_t *dir,
                 int n_files, int n_lookups, int n_threads, rng_t &rng)
{
  std::cout << "Starting " << n_lookups << " lookups in "
      << n_threads << " threads..." << std::endl;

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
  std::cout << n_lookups << " lookups in " << duration.count()
      << "us, at a rate of " << rate << "/s" << std::endl;

  return 0;
}

int unlink_thread(libmds *mds, const libmds_fileid_t *dir, int start, int end)
{
  for (int i = start; i < end; i++) {
    char name[16];
    snprintf(name, sizeof(name), "file.%d", i);

    int r = libmds_unlink(mds, dir, name);
    if (r) {
      std::cerr << "libmds_unlink(\"" << name << "\") failed with "
          << r << std::endl;
      return r;
    }
  }
  return 0;
}

int bench_unlink(libmds *mds, const libmds_fileid_t *dir,
                 int n_files, int n_threads)
{
  if (n_files % n_threads) {
    std::cerr << "files=" << n_files << " must be divisible by threads= "
        << n_threads << std::endl;
    return -EINVAL;
  }
  const int per_thread = n_files / n_threads;

  std::cout << "Starting " << n_files << " unlinks in "
      << n_threads << " threads..." << std::endl;

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
  std::cout << "Unlinked " << n_files << " files in " << duration.count()
      << "us, at a rate of " << rate << "/s" << std::endl;

  return 0;
}

int run_benchmarks(libmds *mds, int n_files, int n_lookups, int n_threads,
                   rng_t &rng)
{
  const uint8_t volume[] = "abcdefghijklmnop";
  const libmds_fileid_t root { volume, 1 };

  int r = bench_create(mds, &root, n_files, n_threads);
  if (r) {
    std::cerr << "bench_create() failed with " << r << std::endl;
    return r;
  }

  r = bench_lookup(mds, &root, n_files, n_lookups, n_threads, rng);
  if (r) {
    std::cerr << "bench_lookup() failed with " << r << std::endl;
    return r;
  }

  r = bench_unlink(mds, &root, n_files, n_threads);
  if (r) {
    std::cerr << "bench_unlink() failed with " << r << std::endl;
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
      ++i;
    }
  }

  std::random_device rd;
  std::mt19937 rng(rd());

  // create object store
  std::cout << "files " << n_files << std::endl;
  std::cout << "lookups " << n_lookups << std::endl;
  std::cout << "threads " << n_threads << std::endl;

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
