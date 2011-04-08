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

#include "include/types.h"
#include "include/rados/librados.hpp"

using namespace librados;

#include <iostream>

#include <stdlib.h>
#include <time.h>

void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  str[0] = '\0';
  for (int i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

class C_Watch : public WatchCtx {
public:
  C_Watch() {}
  void notify(uint8_t opcode, uint64_t ver) {
    cout << "C_Watch::notify() opcode=" << (int)opcode << " ver=" << ver << std::endl;
  }
};

void testradospp_milestone(void)
{
  cout << "*** press enter to continue ***" << std::endl;
  getchar();
}

int main(int argc, const char **argv) 
{
  Rados rados;
  if (rados.init(NULL) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  if (rados.conf_read_file(NULL)) {
     cerr << "couldn't read configuration file." << std::endl;
     exit(1);
  }

  if (!rados.conf_set("config option that doesn't exist",
                     "some random value")) {
    printf("error: succeeded in setting nonexistent config option\n");
    exit(1);
  }
  if (rados.conf_set("log to stderr", "2")) {
    printf("error: error setting log_to_stderr\n");
    exit(1);
  }
  rados.reopen_log();
  std::string tmp;
  if (rados.conf_get("log to stderr", tmp)) {
    printf("error: failed to read log_to_stderr from config\n");
    exit(1);
  }
  if (tmp[0] != '2') {
    printf("error: new setting for log_to_stderr failed to take effect.\n");
    exit(1);
  }

  if (rados.connect()) {
    printf("error connecting\n");
    exit(1);
  }

  cout << "rados_initialize completed" << std::endl;
  testradospp_milestone();

  time_t tm;
  bufferlist bl, bl2, blf;
  char buf[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  bl.append(buf, strlen(buf));
  blf.append(buf, 16);

  const char *oid = "bar";

  IoCtx io_ctx;
  int r = rados.ioctx_create("data", io_ctx);
  cout << "ioctx_create result = " << r << std::endl;

  r = io_ctx.write(oid, bl, bl.length(), 0);
  uint64_t objver = io_ctx.get_last_version();
  cout << "io_ctx.write returned " << r << " last_ver=" << objver << std::endl;

  uint64_t stat_size;
  time_t stat_mtime;
  r = io_ctx.stat(oid, &stat_size, &stat_mtime);
  cout << "io_ctx.stat size = " << stat_size << " mtime = " << stat_mtime << std::endl;

  r = io_ctx.stat(oid, NULL, NULL);
  cout << "io_ctx.stat(does_not_exist) = " << r;

  uint64_t handle;
  C_Watch wc;
  r = io_ctx.watch(oid, objver, &handle, &wc);
  cout << "io_ctx.watch returned " << r << std::endl;

  testradospp_milestone();
  io_ctx.set_notify_timeout(7);
  r = io_ctx.notify(oid, objver);
  cout << "io_ctx.notify returned " << r << std::endl;
  testradospp_milestone();

  r = io_ctx.notify(oid, objver);
  cout << "io_ctx.notify returned " << r << std::endl;
  testradospp_milestone();

  r = io_ctx.unwatch(oid, handle);
  cout << "io_ctx.unwatch returned " << r << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  testradospp_milestone();

  r = io_ctx.notify(oid, objver);
  cout << "io_ctx.notify returned " << r << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  testradospp_milestone();
  io_ctx.set_assert_version(objver);

  r = io_ctx.write(oid, bl, bl.length() - 1, 0);
  cout << "io_ctx.write returned " << r << std::endl;

  r = io_ctx.write(oid, bl, bl.length() - 2, 0);
  cout << "io_ctx.write returned " << r << std::endl;
  r = io_ctx.write(oid, bl, bl.length() - 3, 0);
  cout << "rados.write returned " << r << std::endl;
  r = io_ctx.append(oid, bl, bl.length());
  cout << "rados.write returned " << r << std::endl;
  r = io_ctx.write_full(oid, blf);
  cout << "rados.write_full returned " << r << std::endl;
  r = io_ctx.read(oid, bl, bl.length(), 0);
  cout << "rados.read returned " << r << std::endl;
  r = io_ctx.trunc(oid, 8);
  cout << "rados.trunc returned " << r << std::endl;
  r = io_ctx.read(oid, bl, bl.length(), 0);
  cout << "rados.read returned " << r << std::endl;
  r = io_ctx.exec(oid, "crypto", "md5", bl, bl2);
  cout << "exec returned " << r <<  " buf size=" << bl2.length() << std::endl;
  const unsigned char *md5 = (const unsigned char *)bl2.c_str();
  char md5_str[bl2.length()*2 + 1];
  buf_to_hex(md5, bl2.length(), md5_str);
  cout << "md5 result=" << md5_str << std::endl;

  r = io_ctx.exec(oid, "crypto", "sha1", bl, bl2);
  cout << "exec returned " << r << std::endl;
  const unsigned char *sha1 = (const unsigned char *)bl2.c_str();
  char sha1_str[bl2.length()*2 + 1];
  buf_to_hex(sha1, bl2.length(), sha1_str);
  cout << "sha1 result=" << sha1_str << std::endl;

  r = io_ctx.exec(oid, "acl", "set", bl, bl2);
  r = io_ctx.exec(oid, "acl", "get", bl, bl2);
  cout << "exec returned " << r << std::endl;
  if (bl2.length() > 0) {
    cout << "attr=" << bl2.c_str() << std::endl;
  }

  int size = io_ctx.read(oid, bl2, 128, 0);
  cout << "read result=" << bl2.c_str() << std::endl;
  cout << "size=" << size << std::endl;

  const char *oid2 = "jjj10.rbd";
  r = io_ctx.exec(oid2, "rbd", "snap_list", bl, bl2);
  cout << "snap_list result=" << r << std::endl;
  r = io_ctx.exec(oid2, "rbd", "snap_add", bl, bl2);
  cout << "snap_add result=" << r << std::endl;

  if (r > 0) {
    char *s = bl2.c_str();
    for (int i=0; i<r; i++, s += strlen(s) + 1)
      cout << s << std::endl;
  }

  cout << "iterating over objects..." << std::endl;
  int num_objs = 0;
  for (ObjectIterator iter = io_ctx.objects_begin();
       iter != io_ctx.objects_end(); ++iter) {
    num_objs++;
    cout << "'" << *iter << "'" << std::endl;
  }
  cout << "iterated over " << num_objs << " objects." << std::endl;
  map<string, bufferlist> attrset;
  io_ctx.getxattrs(oid, attrset);

  map<string, bufferlist>::iterator it;
  for (it = attrset.begin(); it != attrset.end(); ++it) {
    cout << "xattr: " << it->first << std::endl;
  }

  r = io_ctx.remove(oid);
  cout << "remove result=" << r << std::endl;
  rados.shutdown();

  return 0;
}

