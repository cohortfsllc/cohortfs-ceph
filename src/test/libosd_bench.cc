// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "libosd/ceph_osd.h"
#include "common/ceph_argparse.h"

#define dout_subsys ceph_subsys_osd

static void usage()
{
  std::cerr << "usage: libosd_bench [flags]\n"
      "	 --volume\n"
      "	       name of the volume\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n" << std::endl;
  generic_server_usage();
}

int main(int argc, const char *argv[])
{
  const struct libosd_init_args init_args {
    .id = 0,
    .config = nullptr,
    .cluster = nullptr,
    .callbacks = nullptr,
    .argv = argv,
    .argc = argc,
    .user = nullptr,
  };

  // default values
  int threads = 1;
  std::string volume;

  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  string val;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)NULL)) {
      threads = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--volume", (char*)NULL)) {
      volume = val;
    } else {
      std::cerr << "Error: can't understand argument: " << *i << std::endl;
      usage();
    }
  }

  if (threads < 1 || threads > 256) {
    std::cerr << "Invalid thread count " << threads << std::endl;
    return EXIT_FAILURE;
  }
  if (volume.empty()) {
    std::cerr << "Missing argument --volume" << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "threads " << threads << std::endl;
  std::cout << "volume " << volume << std::endl;

  struct libosd* osd = libosd_init(&init_args);
  if (osd == nullptr) {
    std::cerr << "libosd_init() failed" << std::endl;
    return EXIT_FAILURE;
  }

  uint8_t uuid[16];
  int r = osd->get_volume(volume.c_str(), uuid);
  if (r != 0) {
    std::cerr << "libosd_get_volume() failed with " << r << std::endl;
    return EXIT_FAILURE;
  }

  osd->shutdown();
  osd->join();
  libosd_cleanup(osd);
  std::cout << "libosd_cleanup() finished" << std::endl;
  return EXIT_SUCCESS;
}
