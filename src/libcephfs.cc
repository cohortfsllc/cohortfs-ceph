// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <fcntl.h>
#include <iostream>
#include <string.h>
#include <string>

#include "auth/Crypto.h"
#include "client/Client.h"
#include "include/cephfs/libcephfs.h"
#include "common/Mutex.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/version.h"
#include "mon/MonClient.h"
#include "include/str_list.h"
#include "messages/MMonMap.h"
#include "msg/Messenger.h"
#include "include/assert.h"

class ceph_mount_info
{
public:
  ceph_mount_info(uint64_t msgr_nonce_, CephContext *cct_)
    : msgr_nonce(msgr_nonce_),
      mounted(false),
      inited(false),
      client(NULL),
      monclient(NULL),
      messenger(NULL),
      cct(cct_)
  {
  }

  ~ceph_mount_info()
  {
    try {
      shutdown();
      if (cct) {
	cct->put();
	cct = NULL;
      }
    }
    catch (const std::exception& e) {
      // we shouldn't get here, but if we do, we want to know about it.
      lderr(cct) << "ceph_mount_info::~ceph_mount_info: caught exception: "
	         << e.what() << dendl;
    }
    catch (...) {
      // ignore
    }
  }

  int mount(const std::string &mount_root)
  {
    int ret;
    
    if (mounted)
      return -EISCONN;

    common_init_finish(cct);

    //monmap
    monclient = new MonClient(cct);
    ret = -1000;
    if (monclient->build_initial_monmap() < 0)
      goto fail;

    //network connection
    messenger = Messenger::create(cct, entity_name_t::CLIENT(), "client", msgr_nonce);

    //at last the client
    ret = -1002;
    client = new Client(messenger, monclient);
    if (!client)
      goto fail;

    ret = -1003;
    if (messenger->start() != 0)
      goto fail;

    ret = client->init();
    if (ret)
      goto fail;

    inited = true;

    ret = client->mount(mount_root);
    if (ret)
      goto fail;

    mounted = true;
    return 0;

  fail:
    shutdown();
    return ret;
  }

  int unmount()
  {
    if (!mounted)
      return -ENOTCONN;
    shutdown();
    return 0;
  }

  void shutdown()
  {
    if (mounted) {
      client->unmount();
      mounted = false;
    }
    if (inited) {
      client->shutdown();
      inited = false;
    }
    if (messenger) {
      messenger->shutdown();
      messenger->wait();
      delete messenger;
      messenger = NULL;
    }
    if (monclient) {
      delete monclient;
      monclient = NULL;
    }
    if (client) {
      delete client;
      client = NULL;
    }
  }

  bool is_mounted()
  {
    return mounted;
  }

  int conf_read_file(const char *path_list)
  {
    std::deque<std::string> parse_errors;
    int ret = cct->_conf->parse_config_files(path_list, &parse_errors, NULL, 0);
    if (ret)
      return ret;
    cct->_conf->apply_changes(NULL);
    complain_about_parse_errors(cct, &parse_errors);
    return 0;
  }

  int conf_parse_argv(int argc, const char **argv)
  {
    int ret;
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    ret = cct->_conf->parse_argv(args);
    if (ret)
	return ret;
    cct->_conf->apply_changes(NULL);
    return 0;
  }

  int conf_parse_env(const char *name)
  {
    md_config_t *conf = cct->_conf;
    vector<const char*> args;
    env_to_vec(args, name);
    int ret = conf->parse_argv(args);
    if (ret)
      return ret;
    conf->apply_changes(NULL);
    return 0;
  }

  int conf_set(const char *option, const char *value)
  {
    int ret = cct->_conf->set_val(option, value);
    if (ret)
      return ret;
    cct->_conf->apply_changes(NULL);
    return 0;
  }

  int conf_get(const char *option, char *buf, size_t len)
  {
    char *tmp = buf;
    return cct->_conf->get_val(option, &tmp, len);
  }

  Client *get_client()
  {
    return client;
  }

  const char *get_cwd()
  {
    client->getcwd(cwd);
    return cwd.c_str();
  }

  CephContext *get_ceph_context() const {
    return cct;
  }

private:
  uint64_t msgr_nonce;
  bool mounted;
  bool inited;
  Client *client;
  MonClient *monclient;
  Messenger *messenger;
  CephContext *cct;
  std::string cwd;
};

extern "C" const char *ceph_version(int *pmajor, int *pminor, int *ppatch)
{
  int major, minor, patch;
  const char *v = ceph_version_to_str();

  int n = sscanf(v, "%d.%d.%d", &major, &minor, &patch);
  if (pmajor)
    *pmajor = (n >= 1) ? major : 0;
  if (pminor)
    *pminor = (n >= 2) ? minor : 0;
  if (ppatch)
    *ppatch = (n >= 3) ? patch : 0;
  return VERSION;
}

extern "C" int ceph_create_with_context(class ceph_mount_info **cmount, CephContext *cct)
{
  uint64_t nonce = 0;

  // 6 bytes of random and 2 bytes of pid
  get_random_bytes((char*)&nonce, sizeof(nonce));
  nonce &= ~0xffff;
  nonce |= (uint64_t)getpid();

  *cmount = new class ceph_mount_info(nonce, cct);
  return 0;
}

extern "C" int ceph_create(class ceph_mount_info **cmount, const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf->parse_env(); // environment variables coverride
  cct->_conf->apply_changes(NULL);
  return ceph_create_with_context(cmount, cct);
}

extern "C" int ceph_unmount(class ceph_mount_info *cmount)
{
  return cmount->unmount();
}

extern "C" int ceph_release(class ceph_mount_info *cmount)
{
  if (cmount->is_mounted())
    return -EISCONN;
  delete cmount;
  return 0;
}

extern "C" void ceph_shutdown(class ceph_mount_info *cmount)
{
  cmount->shutdown();
  delete cmount;
}

extern "C" int ceph_conf_read_file(class ceph_mount_info *cmount, const char *path)
{
  return cmount->conf_read_file(path);
}

extern "C" int ceph_conf_parse_argv(class ceph_mount_info *cmount, int argc,
				     const char **argv)
{
  return cmount->conf_parse_argv(argc, argv);
}

extern "C" int ceph_conf_parse_env(class ceph_mount_info *cmount, const char *name)
{
  return cmount->conf_parse_env(name);
}

extern "C" int ceph_conf_set(class ceph_mount_info *cmount, const char *option,
			     const char *value)
{
  return cmount->conf_set(option, value);
}

extern "C" int ceph_conf_get(class ceph_mount_info *cmount, const char *option,
			     char *buf, size_t len)
{
  if (buf == NULL) {
    return -EINVAL;
  }
  return cmount->conf_get(option, buf, len);
}

extern "C" int ceph_mount(class ceph_mount_info *cmount, const char *root)
{
  std::string mount_root;
  if (root)
    mount_root = root;
  return cmount->mount(mount_root);
}

extern "C" int ceph_is_mounted(class ceph_mount_info *cmount)
{
  return cmount->is_mounted() ? 1 : 0;
}

extern "C" int ceph_statfs(class ceph_mount_info *cmount, const char *path,
			   struct statvfs *stbuf)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->statfs(path, stbuf);
}

extern "C" int ceph_get_local_osd(class ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->get_local_osd();
}

extern "C" const char* ceph_getcwd(class ceph_mount_info *cmount)
{
  return cmount->get_cwd();
}

extern "C" int ceph_chdir (class ceph_mount_info *cmount, const char *s)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->chdir(s);
}

extern "C" int ceph_opendir(class ceph_mount_info *cmount,
			    const char *name, struct ceph_dir_result **dirpp)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->opendir(name, (dir_result_t **)dirpp);
}

extern "C" int ceph_closedir(class ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->closedir((dir_result_t*)dirp);
}

extern "C" struct dirent * ceph_readdir(class ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted()) {
    /* Client::readdir also sets errno to signal errors. */
    errno = -ENOTCONN;
    return NULL;
  }
  return cmount->get_client()->readdir((dir_result_t*)dirp);
}

extern "C" int ceph_readdir_r(class ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->readdir_r((dir_result_t*)dirp, de);
}

extern "C" int ceph_readdirplus_r(class ceph_mount_info *cmount, struct ceph_dir_result *dirp,
				  struct dirent *de, struct stat *st, int *stmask)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->readdirplus_r((dir_result_t*)dirp, de, st, stmask);
}

extern "C" int ceph_getdents(class ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			     char *buf, int buflen)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->getdents((dir_result_t*)dirp, buf, buflen);
}

extern "C" int ceph_getdnames(class ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			      char *buf, int buflen)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->getdnames((dir_result_t*)dirp, buf, buflen);
}

extern "C" void ceph_rewinddir(class ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted())
    return;
  cmount->get_client()->rewinddir((dir_result_t*)dirp);
}

extern "C" loff_t ceph_telldir(class ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->telldir((dir_result_t*)dirp);
}

extern "C" void ceph_seekdir(class ceph_mount_info *cmount, struct ceph_dir_result *dirp, loff_t offset)
{
  if (!cmount->is_mounted())
    return;
  cmount->get_client()->seekdir((dir_result_t*)dirp, offset);
}

extern "C" int ceph_link (class ceph_mount_info *cmount, const char *existing,
			  const char *newname)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->link(existing, newname);
}

extern "C" int ceph_unlink(class ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->unlink(path);
}

extern "C" int ceph_rename(class ceph_mount_info *cmount, const char *from,
			   const char *to)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->rename(from, to);
}

// dirs
extern "C" int ceph_mkdir(class ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->mkdir(path, mode);
}

extern "C" int ceph_mkdirs(class ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->mkdirs(path, mode);
}

extern "C" int ceph_rmdir(class ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->rmdir(path);
}

// symlinks
extern "C" int ceph_readlink(class ceph_mount_info *cmount, const char *path,
			     char *buf, loff_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->readlink(path, buf, size);
}

extern "C" int ceph_symlink(class ceph_mount_info *cmount, const char *existing,
			    const char *newname)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->symlink(existing, newname);
}

// inode stuff
extern "C" int ceph_stat(class ceph_mount_info *cmount, const char *path,
			 struct stat *stbuf)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->stat(path, stbuf);
}

extern "C" int ceph_lstat(class ceph_mount_info *cmount, const char *path,
			  struct stat *stbuf)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->lstat(path, stbuf);
}

extern "C" int ceph_setattr(class ceph_mount_info *cmount, const char *relpath,
			    struct stat *attr, int mask)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->setattr(relpath, attr, mask);
}

// *xattr() calls supporting samba/vfs
extern "C" int ceph_getxattr(class ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->getxattr(path, name, value, size);
}

extern "C" int ceph_lgetxattr(class ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->lgetxattr(path, name, value, size);
}

extern "C" int ceph_listxattr(class ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->listxattr(path, list, size);
}

extern "C" int ceph_llistxattr(class ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->llistxattr(path, list, size);
}

extern "C" int ceph_removexattr(class ceph_mount_info *cmount, const char *path, const char *name)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->removexattr(path, name);
}

extern "C" int ceph_lremovexattr(class ceph_mount_info *cmount, const char *path, const char *name)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->lremovexattr(path, name);
}

extern "C" int ceph_setxattr(class ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->setxattr(path, name, value, size, flags);
}

extern "C" int ceph_lsetxattr(class ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->lsetxattr(path, name, value, size, flags);
}
/* end xattr support */

extern "C" int ceph_chmod(class ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->chmod(path, mode);
}
extern "C" int ceph_fchmod(class ceph_mount_info *cmount, int fd, mode_t mode)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->fchmod(fd, mode);
}
extern "C" int ceph_chown(class ceph_mount_info *cmount, const char *path,
			  int uid, int gid)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->chown(path, uid, gid);
}
extern "C" int ceph_fchown(class ceph_mount_info *cmount, int fd,
			   int uid, int gid)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->fchown(fd, uid, gid);
}
extern "C" int ceph_lchown(class ceph_mount_info *cmount, const char *path,
			   int uid, int gid)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->lchown(path, uid, gid);
}


extern "C" int ceph_utime(class ceph_mount_info *cmount, const char *path,
			  struct utimbuf *buf)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->utime(path, buf);
}

extern "C" int ceph_truncate(class ceph_mount_info *cmount, const char *path,
			     loff_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->truncate(path, size);
}

// file ops
extern "C" int ceph_mknod(class ceph_mount_info *cmount, const char *path,
			  mode_t mode, dev_t rdev)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->mknod(path, mode, rdev);
}

extern "C" int ceph_open(class ceph_mount_info *cmount, const char *path,
			 int flags, mode_t mode)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->open(path, flags, mode);
}

extern "C" int ceph_open_layout(class ceph_mount_info *cmount, const char *path, int flags,
    mode_t mode, int stripe_unit, int stripe_count, int object_size, const char *data_pool)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->open(path, flags, mode, stripe_unit,
      stripe_count, object_size, data_pool);
}

extern "C" int ceph_close(class ceph_mount_info *cmount, int fd)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->close(fd);
}

extern "C" loff_t ceph_lseek(class ceph_mount_info *cmount, int fd,
			     loff_t offset, int whence)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->lseek(fd, offset, whence);
}

extern "C" int ceph_read(class ceph_mount_info *cmount, int fd, char *buf,
			 loff_t size, loff_t offset)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->read(fd, buf, size, offset);
}

extern "C" int ceph_write(class ceph_mount_info *cmount, int fd, const char *buf,
			  loff_t size, loff_t offset)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->write(fd, buf, size, offset);
}

extern "C" int ceph_ftruncate(class ceph_mount_info *cmount, int fd, loff_t size)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->ftruncate(fd, size);
}

extern "C" int ceph_fsync(class ceph_mount_info *cmount, int fd, int syncdataonly)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->fsync(fd, syncdataonly);
}

extern "C" int ceph_fallocate(class ceph_mount_info *cmount, int fd, int mode,
	                      loff_t offset, loff_t length)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->fallocate(fd, mode, offset, length);
}

extern "C" int ceph_fstat(class ceph_mount_info *cmount, int fd, struct stat *stbuf)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->fstat(fd, stbuf);
}

extern "C" int ceph_sync_fs(class ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->sync_fs();
}


extern "C" int ceph_get_file_stripe_unit(class ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_stripe_unit;
}

extern "C" int ceph_get_path_stripe_unit(class ceph_mount_info *cmount, const char *path)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  return l.fl_stripe_unit;
}

extern "C" int ceph_get_file_stripe_count(class ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_stripe_count;
}

extern "C" int ceph_get_path_stripe_count(class ceph_mount_info *cmount, const char *path)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  return l.fl_stripe_count;
}

extern "C" int ceph_get_file_object_size(class ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_object_size;
}

extern "C" int ceph_get_path_object_size(class ceph_mount_info *cmount, const char *path)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  return l.fl_object_size;
}

extern "C" int ceph_get_file_pool(class ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_pg_pool;
}

extern "C" int ceph_get_path_pool(class ceph_mount_info *cmount, const char *path)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  return l.fl_pg_pool;
}

extern "C" int ceph_get_file_pool_name(class ceph_mount_info *cmount, int fh, char *buf, size_t len)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  string name = cmount->get_client()->get_pool_name(l.fl_pg_pool);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length();
}

extern "C" int ceph_get_pool_name(class ceph_mount_info *cmount, int pool, char *buf, size_t len)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  string name = cmount->get_client()->get_pool_name(pool);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length();
}

extern "C" int ceph_get_path_pool_name(class ceph_mount_info *cmount, const char *path, char *buf, size_t len)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  string name = cmount->get_client()->get_pool_name(l.fl_pg_pool);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length();
}

extern "C" int ceph_get_file_layout(class ceph_mount_info *cmount, int fh, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  if (stripe_unit)
    *stripe_unit = l.fl_stripe_unit;
  if (stripe_count)
    *stripe_count = l.fl_stripe_count;
  if (object_size)
    *object_size = l.fl_object_size;
  if (pg_pool)
    *pg_pool = l.fl_pg_pool;
  return 0;
}

extern "C" int ceph_get_path_layout(class ceph_mount_info *cmount, const char *path, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  if (stripe_unit)
    *stripe_unit = l.fl_stripe_unit;
  if (stripe_count)
    *stripe_count = l.fl_stripe_count;
  if (object_size)
    *object_size = l.fl_object_size;
  if (pg_pool)
    *pg_pool = l.fl_pg_pool;
  return 0;
}

extern "C" int ceph_get_file_replication(class ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  int rep = cmount->get_client()->get_pool_replication(l.fl_pg_pool);
  return rep;
}

extern "C" int ceph_get_path_replication(class ceph_mount_info *cmount, const char *path)
{
  struct ceph_file_layout l;
  int r;

  if (!cmount->is_mounted())
    return -ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l);
  if (r < 0)
    return r;
  int rep = cmount->get_client()->get_pool_replication(l.fl_pg_pool);
  return rep;
}

extern "C" int ceph_set_default_file_stripe_unit(class ceph_mount_info *cmount,
						 int stripe)
{
  // this option no longer exists
  return -EOPNOTSUPP;
}

extern "C" int ceph_set_default_file_stripe_count(class ceph_mount_info *cmount,
						  int count)
{
  // this option no longer exists
  return -EOPNOTSUPP;
}

extern "C" int ceph_set_default_object_size(class ceph_mount_info *cmount, int size)
{
  // this option no longer exists
  return -EOPNOTSUPP;
}

extern "C" int ceph_set_default_file_replication(class ceph_mount_info *cmount,
						 int replication)
{
  // this option no longer exists
  return -EOPNOTSUPP;
}

extern "C" int ceph_set_default_preferred_pg(class ceph_mount_info *cmount, int osd)
{
  // this option no longer exists
  return -EOPNOTSUPP;
}

extern "C" int ceph_get_file_extent_osds(class ceph_mount_info *cmount, int fh,
    loff_t offset, loff_t *length, int *osds, int nosds)
{
  if (nosds < 0)
    return -EINVAL;

  if (!cmount->is_mounted())
    return -ENOTCONN;

  vector<int> vosds;
  int ret = cmount->get_client()->get_file_extent_osds(fh, offset, length, vosds);
  if (ret < 0)
    return ret;

  if (!nosds)
    return vosds.size();

  if ((int)vosds.size() > nosds)
    return -ERANGE;

  for (int i = 0; i < (int)vosds.size(); i++)
    osds[i] = vosds[i];

  return vosds.size();
}

extern "C" int ceph_get_osd_crush_location(class ceph_mount_info *cmount,
    int osd, char *path, size_t len)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;

  if (!path && len)
    return -EINVAL;

  vector<pair<string, string> > loc;
  int ret = cmount->get_client()->get_osd_crush_location(osd, loc);
  if (ret)
    return ret;

  size_t needed = 0;
  size_t cur = 0;
  vector<pair<string, string> >::iterator it;
  for (it = loc.begin(); it != loc.end(); ++it) {
    string& type = it->first;
    string& name = it->second;
    needed += type.size() + name.size() + 2;
    if (needed <= len) {
      strcpy(path + cur, type.c_str());
      cur += type.size() + 1;
      strcpy(path + cur, name.c_str());
      cur += name.size() + 1;
    }
  }

  if (len == 0)
    return needed;

  if (needed > len)
    return -ERANGE;

  return needed;
}

extern "C" int ceph_get_osd_addr(class ceph_mount_info *cmount, int osd,
    struct sockaddr_storage *addr)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;

  if (!addr)
    return -EINVAL;

  entity_addr_t address;
  int ret = cmount->get_client()->get_osd_addr(osd, address);
  if (ret < 0)
    return ret;

  memcpy(addr, &address.ss_addr(), sizeof(*addr));

  return 0;
}

extern "C" int ceph_get_file_stripe_address(class ceph_mount_info *cmount, int fh,
					    loff_t offset, struct sockaddr_storage *addr, int naddr)
{
  vector<entity_addr_t> address;
  unsigned i;
  int r;

  if (naddr < 0)
    return -EINVAL;

  if (!cmount->is_mounted())
    return -ENOTCONN;

  r = cmount->get_client()->get_file_stripe_address(fh, offset, address);
  if (r < 0)
    return r;

  for (i = 0; i < (unsigned)naddr && i < address.size(); i++)
    memcpy(&addr[i], &address[i].ss_addr(), sizeof(*addr));

  /* naddr == 0: drop through and return actual size */
  if (naddr && (address.size() > (unsigned)naddr))
    return -ERANGE;

  return address.size();
}

extern "C" int ceph_localize_reads(class ceph_mount_info *cmount, int val)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  if (!val)
    cmount->get_client()->clear_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  else
    cmount->get_client()->set_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  return 0;
}

extern "C" CephContext *ceph_get_mount_context(class ceph_mount_info *cmount)
{
  return cmount->get_ceph_context();
}

extern "C" int ceph_debug_get_fd_caps(class ceph_mount_info *cmount, int fd)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->get_caps_issued(fd);
}

extern "C" int ceph_debug_get_file_caps(class ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->get_caps_issued(path);
}

extern "C" int ceph_get_stripe_unit_granularity(class ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return CEPH_MIN_STRIPE_UNIT;
}

extern "C" int ceph_get_pool_id(class ceph_mount_info *cmount, const char *pool_name)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;

  if (!pool_name || !pool_name[0])
    return -EINVAL;

  /* negative range reserved for errors */
  int64_t pool_id = cmount->get_client()->get_pool_id(pool_name);
  if (pool_id > 0x7fffffff)
    return -ERANGE;

  /* get_pool_id error codes fit in int */
  return (int)pool_id;
}

extern "C" int ceph_get_pool_replication(class ceph_mount_info *cmount,
					 int pool_id)
{
  if (!cmount->is_mounted())
    return -ENOTCONN;
  return cmount->get_client()->get_pool_replication(pool_id);
}
/* Low-level exports */

extern "C" int ceph_ll_lookup_root(struct ceph_mount_info *cmount,
                  Inode **parent)
{
  *parent = cmount->get_client()->get_root();
  if (*parent)
    return 0;
  return EFAULT;
}

extern "C" struct Inode *ceph_ll_get_inode(class ceph_mount_info *cmount,
					   vinodeno_t vino)
{
  return (cmount->get_client())->ll_get_inode(vino);
}

extern "C" int ceph_ll_lookup(class ceph_mount_info *cmount,
			      struct Inode *parent, const char *name,
			      struct stat *attr, Inode **out,
			      int uid, int gid)
{
  return (cmount->get_client())->ll_lookup(parent, name, attr, out, uid, gid);
}

extern "C" int ceph_ll_put(class ceph_mount_info *cmount, Inode *in)
{
  return (cmount->get_client()->ll_put(in));
}

extern "C" int ceph_ll_forget(class ceph_mount_info *cmount, Inode *in,
			      int count)
{
  return (cmount->get_client()->ll_forget(in, count));
}

extern "C" int ceph_ll_walk(class ceph_mount_info *cmount, const char *name,
			    struct Inode **i,
			    struct stat *attr)
{
  return(cmount->get_client()->ll_walk(name, i, attr));
}

extern "C" int ceph_ll_getattr(class ceph_mount_info *cmount,
			       Inode *in, struct stat *attr,
			       int uid, int gid)
{
  return (cmount->get_client()->ll_getattr(in, attr, uid, gid));
}

extern "C" int ceph_ll_setattr(class ceph_mount_info *cmount,
			       Inode *in, struct stat *st,
			       int mask, int uid, int gid)
{
  return (cmount->get_client()->ll_setattr(in, st, mask, uid, gid));
}

extern "C" int ceph_ll_open(class ceph_mount_info *cmount, Inode *in,
			    int flags, Fh **fh, int uid, int gid)
{
  return (cmount->get_client()->ll_open(in, flags, fh, uid, gid));
}

extern "C" int ceph_ll_read(class ceph_mount_info *cmount, Fh* filehandle,
			    int64_t off, uint64_t len, char* buf)
{
  bufferlist bl;
  int r = 0;

  r = cmount->get_client()->ll_read(filehandle, off, len, &bl);
  if (r >= 0)
    {
      bl.copy(0, bl.length(), buf);
      r = bl.length();
    }
  return r;
}

extern "C" int ceph_ll_read_block(class ceph_mount_info *cmount,
				  Inode *in, uint64_t blockid,
				  char* buf, uint64_t offset,
				  uint64_t length,
				  struct ceph_file_layout* layout)
{

  return (cmount->get_client()->ll_read_block(in, blockid, buf, offset,
					      length, layout));
}

extern "C" int ceph_ll_write_block(class ceph_mount_info *cmount,
				   Inode *in, uint64_t blockid,
				   char *buf, uint64_t offset,
				   uint64_t length,
				   struct ceph_file_layout *layout,
				   uint64_t snapseq, uint32_t sync)
{
  return (cmount->get_client()->ll_write_block(in, blockid, buf, offset,
					       length, layout, snapseq, sync));
}

extern "C" int ceph_ll_commit_blocks(class ceph_mount_info *cmount,
				     Inode *in, uint64_t offset,
				     uint64_t range)
{
  return (cmount->get_client()->ll_commit_blocks(in, offset, range));
}

extern "C" int ceph_ll_fsync(class ceph_mount_info *cmount,
			     Fh *fh, int syncdataonly)
{
  return (cmount->get_client()->ll_fsync(fh, syncdataonly));
}

extern "C" loff_t ceph_ll_lseek(class ceph_mount_info *cmount,
				Fh *fh, loff_t offset, int whence)
{
  return (cmount->get_client()->ll_lseek(fh, offset, whence));
}

extern "C" int ceph_ll_write(class ceph_mount_info *cmount,
			     Fh *fh, int64_t off, uint64_t len,
			     const char *data)
{
  return (cmount->get_client()->ll_write(fh, off, len, data));
}

extern "C" int64_t ceph_ll_readv(class ceph_mount_info *cmount,
				 struct Fh *fh, const struct iovec *iov,
				 int iovcnt, int64_t off)
{
  return -1; // TODO:  implement
}

extern "C" int64_t ceph_ll_writev(class ceph_mount_info *cmount,
				  struct Fh *fh, const struct iovec *iov,
				  int iovcnt, int64_t off)
{
  return -1; // TODO:  implement
}

extern "C" int ceph_ll_close(class ceph_mount_info *cmount, Fh* fh)
{
  return (cmount->get_client()->ll_release(fh));
}

extern "C" int ceph_ll_create(class ceph_mount_info *cmount,
			      struct Inode *parent, const char *name,
			      mode_t mode, int flags, struct stat *attr,
			      struct Inode **out, Fh **fhp, int uid, int gid)
{
  return (cmount->get_client())->ll_create(parent, name, mode, flags,
					   attr, out, fhp, uid, gid);
}

extern "C" int ceph_ll_mkdir(class ceph_mount_info *cmount,
			     Inode *parent, const char *name,
			     mode_t mode, struct stat *attr, Inode **out,
			     int uid, int gid)
{
  return (cmount->get_client()->ll_mkdir(parent, name, mode, attr, out, uid,
					 gid));
}

extern "C" int ceph_ll_link(class ceph_mount_info *cmount,
			    Inode *in, Inode *newparent,
			    const char *name, struct stat *attr, int uid,
			    int gid)
{
  return (cmount->get_client()->ll_link(in, newparent, name, attr, uid,
					gid));
}

extern "C" int ceph_ll_truncate(class ceph_mount_info *cmount,
				Inode *in, uint64_t length, int uid,
				int gid)
{
  struct stat st;
  st.st_size=length;

  return(cmount->get_client()->ll_setattr(in, &st, CEPH_SETATTR_SIZE, uid,
					  gid));
}

extern "C" int ceph_ll_opendir(class ceph_mount_info *cmount,
			       Inode *in,
			       struct ceph_dir_result **dirpp,
			       int uid, int gid)
{
  return (cmount->get_client()->ll_opendir(in, (dir_result_t**) dirpp, uid,
					   gid));
}

extern "C" int ceph_ll_releasedir(class ceph_mount_info *cmount,
				  ceph_dir_result *dir)
{
  (void) cmount->get_client()->ll_releasedir((dir_result_t*) dir);
  return (0);
}

extern "C" int ceph_ll_rename(class ceph_mount_info *cmount,
			      Inode *parent, const char *name,
			      Inode *newparent, const char *newname,
			      int uid, int gid)
{
  return (cmount->get_client()->ll_rename(parent, name, newparent, newname,
					uid, gid));
}

extern "C" int ceph_ll_unlink(class ceph_mount_info *cmount,
			      Inode *in, const char *name,
			      int uid, int gid)
{
  return (cmount->get_client()->ll_unlink(in, name, uid, gid));
}

extern "C" int ceph_ll_statfs(class ceph_mount_info *cmount,
			      Inode *in, struct statvfs *stbuf)
{
  return (cmount->get_client()->ll_statfs(in, stbuf));
}

extern "C" int ceph_ll_readlink(class ceph_mount_info *cmount,
				Inode *in, char *buf, size_t bufsiz, int uid,
				int gid)
{
  return (cmount->get_client()->ll_readlink(in, buf, bufsiz, uid, gid));
}

extern "C" int ceph_ll_symlink(class ceph_mount_info *cmount,
			       Inode *in, const char *name,
			       const char *value, struct stat *attr,
			       Inode **out, int uid, int gid)
{
  return (cmount->get_client()->ll_symlink(in, name, value, attr, out, uid,
					   gid));
}

extern "C" int ceph_ll_rmdir(class ceph_mount_info *cmount,
			     Inode *in, const char *name,
			     int uid, int gid)
{
  return (cmount->get_client()->ll_rmdir(in, name, uid, gid));
}

extern "C" int ceph_ll_getxattr(class ceph_mount_info *cmount,
				Inode *in, const char *name, void *value,
				size_t size, int uid, int gid)
{
  return (cmount->get_client()->ll_getxattr(in, name, value, size, uid, gid));
}
extern "C" int ceph_ll_listxattr(struct ceph_mount_info *cmount,
                              Inode *in, char *list,
                              size_t buf_size, size_t *list_size, int uid, int gid)
{
  int res = cmount->get_client()->ll_listxattr(in, list, buf_size, uid, gid);
  if (res >= 0) {
    *list_size = (size_t)res;
    return 0;
  }
  return res;
}

extern "C" int ceph_ll_setxattr(class ceph_mount_info *cmount,
				Inode *in, const char *name,
				const void *value, size_t size,
				int flags, int uid, int gid)
{
  return (cmount->get_client()->ll_setxattr(in, name, value, size, flags, uid,
					    gid));
}

extern "C" int ceph_ll_removexattr(class ceph_mount_info *cmount,
				   Inode *in, const char *name,
				   int uid, int gid)
{
  return (cmount->get_client()->ll_removexattr(in, name, uid, gid));
}

extern "C" uint32_t ceph_ll_stripe_unit(class ceph_mount_info *cmount,
					Inode *in)
{
  return (cmount->get_client()->ll_stripe_unit(in));
}

extern "C" uint32_t ceph_ll_file_layout(class ceph_mount_info *cmount,
					Inode *in,
					struct ceph_file_layout *layout)
{
  return (cmount->get_client()->ll_file_layout(in, layout));
}

uint64_t ceph_ll_snap_seq(class ceph_mount_info *cmount, Inode *in)
{
  return (cmount->get_client()->ll_snap_seq(in));
}

extern "C" int ceph_ll_get_stripe_osd(class ceph_mount_info *cmount,
				      Inode *in, uint64_t blockno,
				      struct ceph_file_layout* layout)
{
  return (cmount->get_client()->ll_get_stripe_osd(in, blockno, layout));
}

extern "C" int ceph_ll_num_osds(class ceph_mount_info *cmount)
{
  return (cmount->get_client()->ll_num_osds());
}

extern "C" int ceph_ll_osdaddr(class ceph_mount_info *cmount,
			       int osd, uint32_t *addr)
{
  return (cmount->get_client()->ll_osdaddr(osd, addr));
}

extern "C" uint64_t ceph_ll_get_internal_offset(class ceph_mount_info *cmount,
						Inode *in,
						uint64_t blockno)
{
  return (cmount->get_client()->ll_get_internal_offset(in, blockno));
}
