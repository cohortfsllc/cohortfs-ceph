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
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"

#include "common/errno.h"
#include "mon/MonMap.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "osd/PlaceSystem.h"
#include "osd/OSDMap.h"

void usage()
{
  cout
    << " usage: [--print] [--createsimple <numosd> [--clobber] ] <mapfilename>"
    << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  const char *me = argv[0];

  std::string fn;
  bool print = false;
  bool print_json = false;
  bool tree = false;
  bool createsimple = false;
  bool create_from_conf = false;
  int num_osd = 0;
  bool clobber = false;
  bool modified = false;
  int range_first = -1;
  int range_last = -1;

  std::string val;
  std::ostringstream err;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
    } else if (ceph_argparse_flag(args, i, "-p", "--print", (char*)NULL)) {
      print = true;
    } else if (ceph_argparse_flag(args, i, "--dump-json", (char*)NULL)) {
      print_json = true;
    } else if (ceph_argparse_flag(args, i, "--tree", (char*)NULL)) {
      tree = true;
    } else if (ceph_argparse_withint(args, i, &num_osd, &err, "--createsimple", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      createsimple = true;
    } else if (ceph_argparse_flag(args, i, "--create-from-conf", (char*)NULL)) {
      create_from_conf = true;
    } else if (ceph_argparse_flag(args, i, "--clobber", (char*)NULL)) {
      clobber = true;
    } else if (ceph_argparse_withint(args, i, &range_first, &err, "--range_first", (char*)NULL)) {
    } else if (ceph_argparse_withint(args, i, &range_last, &err, "--range_last", (char*)NULL)) {
    } else {
      ++i;
    }
  }
  if (args.empty()) {
    cerr << me << ": must specify osdmap filename" << std::endl;
    usage();
  }
  else if (args.size() > 1) {
    cerr << me << ": too many arguments" << std::endl;
    usage();
  }
  fn = args[0];

  if (range_first >= 0 && range_last >= 0) {
    set<OSDMap*> maps;
    OSDMap *prev = NULL;
    for (int i=range_first; i <= range_last; i++) {
      ostringstream f;
      f << fn << "/" << i;
      bufferlist bl;
      string error, s = f.str();
      int r = bl.read_file(s.c_str(), &error);
      if (r < 0) {
	cerr << "unable to read " << s << ": " << cpp_strerror(r) << std::endl;
	exit(1);
      }
      cout << s << " got " << bl.length() << " bytes" << std::endl;
      OSDMap *o = OSDMapPlaceSystem::getSystem().newOSDMap();
      o->decode(bl);
      maps.insert(o);
      if (prev)
	OSDMap::dedup(prev, o);
      prev = o;
    }
    exit(0);
  }

  OSDMapRef osdmap;
  bufferlist bl;

  cout << me << ": osdmap file '" << fn << "'" << std::endl;

  int r = 0;
  struct stat st;
  if (!createsimple && !create_from_conf && !clobber) {
    std::string error;
    r = bl.read_file(fn.c_str(), &error);
    if (r == 0) {
      try {
	osdmap->decode(bl);
      }
      catch (const buffer::error &e) {
	cerr << me << ": error decoding osdmap '" << fn << "'" << std::endl;
	return -1;
      }
    }
    else {
      cerr << me << ": couldn't open " << fn << ": " << error << std::endl;
      return -1;
    }
  }
  else if ((createsimple || create_from_conf) && !clobber && ::stat(fn.c_str(), &st) == 0) {
    cerr << me << ": " << fn << " exists, --clobber to overwrite" << std::endl;
    return -1;
  }

  if (createsimple) {
    if (num_osd < 1) {
      cerr << me << ": osd count must be > 0" << std::endl;
      exit(1);
    }
    uuid_d fsid;
    memset(&fsid, 0, sizeof(uuid_d));
    osdmap->build_simple(g_ceph_context, 0, fsid, num_osd);
    modified = true;
  }
  if (create_from_conf) {
    uuid_d fsid;
    memset(&fsid, 0, sizeof(uuid_d));
    int r = osdmap->build_simple_from_conf(g_ceph_context, 0, fsid);
    if (r < 0)
      return -1;
    modified = true;
  }

  if (!print && !print_json && !tree && !modified) {
    cerr << me << ": no action specified?" << std::endl;
    usage();
  }

  if (modified)
    osdmap->inc_epoch();

  if (print)
    osdmap->print(cout);
  if (print_json)
    osdmap->dump_json(cout);

  if (modified) {
    bl.clear();
    osdmap->encode(bl);

    // write it out
    cout << me << ": writing epoch " << osdmap->get_epoch()
	 << " to " << fn
	 << std::endl;
    int r = bl.write_file(fn.c_str());
    if (r) {
      cerr << "osdmaptool: error writing to '" << fn << "': "
	   << cpp_strerror(r) << std::endl;
      return 1;
    }
  }
  

  return 0;
}
