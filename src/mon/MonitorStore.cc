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

#include "MonitorStore.h"
#include "common/Clock.h"

#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(dir)
static ostream& _prefix(const string& dir) {
  return *_dout << dbeginl << "store(" << dir << ") ";
}


#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/file.h>

int MonitorStore::mount()
{
  char t[1024];

  dout(1) << "mount" << dendl;
  // verify dir exists
  DIR *d = ::opendir(dir.c_str());
  if (!d) {
    derr(1) << "basedir " << dir << " dne" << dendl;
    return -ENOENT;
  }
  ::closedir(d);

  // open lockfile
  snprintf(t, sizeof(t), "%s/lock", dir.c_str());
  lock_fd = ::open(t, O_CREAT|O_RDWR, 0600);
  if (lock_fd < 0)
    return -errno;
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(lock_fd, F_SETLK, &l);
  if (r < 0) {
    derr(0) << "failed to lock " << t << ", is another cmon still running?" << dendl;
    return -errno;
  }

  if (g_conf.chdir && g_conf.chdir[0] && dir[0] != '/') {
    // combine it with the cwd, in case fuse screws things up (i.e. fakefuse)
    string old = dir;
    char cwd[1024];
    getcwd(cwd, sizeof(cwd));
    dir = cwd;
    dir += "/";
    dir += old;
  }
  return 0;
}

int MonitorStore::umount()
{
  ::close(lock_fd);
  return 0;
}

int MonitorStore::mkfs()
{
  dout(1) << "mkfs" << dendl;

  char cmd[1024];
  snprintf(cmd, sizeof(cmd), "test -d %s && /bin/rm -rf %s ; mkdir -p %s", dir.c_str(), dir.c_str(), dir.c_str());
  dout(1) << cmd << dendl;
  int r = system(cmd);
  return r;
}

void MonitorStore::sync()
{
  dout(10) << "sync" << dendl;
  ::sync();
}

version_t MonitorStore::get_int(const char *a, const char *b)
{
  char fn[1024];
  if (b)
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  else
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  
  FILE *f = ::fopen(fn, "r");
  if (!f) 
    return 0;
  
  char buf[20];
  ::fgets(buf, 20, f);
  ::fclose(f);
  
  version_t val = atoi(buf);
  
  if (b) {
    dout(15) << "get_int " << a << "/" << b << " = " << val << dendl;
  } else {
    dout(15) << "get_int " << a << " = " << val << dendl;
  }
  return val;
}


void MonitorStore::put_int(version_t val, const char *a, const char *b, bool sync)
{
  char fn[1024];
  snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "set_int " << a << "/" << b << " = " << val << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "set_int " << a << " = " << val << dendl;
  }
  
  char vs[30];
  snprintf(vs, sizeof(vs), "%lld\n", (unsigned long long)val);

  char tfn[1024];
  snprintf(tfn, sizeof(tfn), "%s.new", fn);

  int fd = ::open(tfn, O_WRONLY|O_CREAT, 0644);
  assert(fd >= 0);
  ::write(fd, vs, strlen(vs));
  if (sync)
    ::fsync(fd);
  ::close(fd);
  ::rename(tfn, fn);
}


// ----------------------------------------
// buffers

bool MonitorStore::exists_bl_ss(const char *a, const char *b)
{
  char fn[1024];
  if (b) {
    dout(15) << "exists_bl " << a << "/" << b << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "exists_bl " << a << dendl;
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  }
  
  struct stat st;
  int r = ::stat(fn, &st);
  //char buf[80];
  //dout(15) << "exists_bl stat " << fn << " r=" << r << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
  return r == 0;
}

int MonitorStore::erase_ss(const char *a, const char *b)
{
  char fn[1024];
  if (b) {
    dout(15) << "erase_ss " << a << "/" << b << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "erase_ss " << a << dendl;
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  }
  return ::unlink(fn);
}

int MonitorStore::get_bl_ss(bufferlist& bl, const char *a, const char *b)
{
  char fn[1024];
  if (b) {
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  }
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    char buf[80];
    if (b) {
      dout(15) << "get_bl " << a << "/" << b << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    } else {
      dout(15) << "get_bl " << a << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    }
    return -errno;
  }

  // get size
  struct stat st;
  int rc = ::fstat(fd, &st);
  assert(rc == 0);
  __int32_t len = st.st_size;
 
  // read buffer
  bl.clear();
  bufferptr bp(len);
  int off = 0;
  while (off < len) {
    dout(20) << "reading at off " << off << " of " << len << dendl;
    int r = ::read(fd, bp.c_str()+off, len-off);
    if (r < 0) {
      char buf[80];
      derr(0) << "errno on read " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    }
    assert(r>0);
    off += r;
  }
  bl.append(bp);
  ::close(fd);

  if (b) {
    dout(15) << "get_bl " << a << "/" << b << " = " << bl.length() << " bytes" << dendl;
  } else {
    dout(15) << "get_bl " << a << " = " << bl.length() << " bytes" << dendl;
  }

  return len;
}

int MonitorStore::write_bl_ss(bufferlist& bl, const char *a, const char *b, bool append, bool sync)
{
  char fn[1024];
  snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "put_bl " << a << "/" << b << " = " << bl.length() << " bytes" << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "put_bl " << a << " = " << bl.length() << " bytes" << dendl;
  }
  
  char tfn[1024];
  int err = 0;
  int fd;
  if (append) {
    fd = ::open(fn, O_WRONLY|O_CREAT|O_APPEND, 0644);
  } else {
    snprintf(tfn, sizeof(tfn), "%s.new", fn);
    fd = ::open(tfn, O_WRONLY|O_CREAT, 0644);
  }
  assert(fd >= 0);
  
  err = bl.write_fd(fd);

  if (sync && !err)
    ::fsync(fd);
  ::close(fd);
  if (!append && !err) {
    ::rename(tfn, fn);
  }

  assert(!err);  // for now

  return err;
}

