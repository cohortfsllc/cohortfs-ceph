#include "client/libceph.h"

#include <string.h>
#include <fcntl.h>
#include <iostream>

#include "common/Mutex.h"
#include "messages/MMonMap.h"
#include "common/common_init.h"
#include "msg/SimpleMessenger.h"
#include "client/Client.h"

/* ************* ************* ************* *************
 * C interface
 */

extern "C" const char *ceph_version(int *pmajor, int *pminor, int *ppatch)
{
  int major, minor, patch;

  sscanf(VERSION, "%d.%d.%d", &major, &minor, &patch);
  if (pmajor)
    *pmajor = major;
  if (pminor)
    *pminor = minor;
  if (ppatch)
    *ppatch = patch;
  return VERSION;
}

static Mutex ceph_client_mutex("ceph_client");
static int client_initialized = 0;
static int client_mount = 0;
static Client *client = NULL;
static MonClient *monclient = NULL;
static SimpleMessenger *messenger = NULL;

/* This is replicated functionality from the Client class, but the
   Client class has it as protected.  Likely shouldn't be in here at
   all */

interval_set<int> free_fd_set;  // unused fds
hash_map<int, Fh*> fd_map;
int get_fd() {
  int fd = free_fd_set.start();
  free_fd_set.erase(fd, 1);
  return fd;
}
void put_fd(int fd) {
  free_fd_set.insert(fd, 1);
}

extern "C" int ceph_initialize(int argc, const char **argv)
{
  if (free_fd_set.empty())
    free_fd_set.insert(10, 1<<30);
  ceph_client_mutex.Lock();
  if (!client_initialized) {
    //create everything to start a client
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    common_set_defaults(false);
    common_init(args, "libceph", true);
    if (g_conf.clock_tare) g_clock.tare();
    //monmap
    monclient = new MonClient();
    if (monclient->build_initial_monmap() < 0) {
      delete monclient;
      return -1; //error!
    }
    //network connection
    messenger = new SimpleMessenger();
    messenger->register_entity(entity_name_t::CLIENT());

    //at last the client
    client = new Client(messenger, monclient);

    messenger->start();

    client->init();
  }
  ++client_initialized;
  ceph_client_mutex.Unlock();
  return 0;
}

extern "C" void ceph_deinitialize()
{
  ceph_client_mutex.Lock();
  --client_initialized;
  if(!client_initialized) {
    if(client_mount) {
      client_mount = 0;
      client->unmount();
    }
    client->shutdown();
    delete client;
    messenger->wait();
    messenger->destroy();
    delete monclient;
  }
  ceph_client_mutex.Unlock();
}

extern "C" int ceph_mount()
{
  int ret;
  Mutex::Locker lock(ceph_client_mutex);
  if(!client_mount) {
     ret = client->mount();
     if (ret!=0)
       return ret;
  }
  ++client_mount;
  return 0;
}

extern "C" int ceph_umount()
{
  Mutex::Locker lock(ceph_client_mutex);
  --client_mount;
  if (!client_mount)
    return client->unmount();
  return 0;
}

extern "C" int ceph_statfs(const char *path, struct statvfs *stbuf)
{
  return client->statfs(path, stbuf);
}

extern "C" int ceph_get_local_osd()
{
  return client->get_local_osd();
}

extern "C" int ceph_getcwd(char *buf, int buflen)
{
  string cwd;
  client->getcwd(cwd);
  int size = cwd.size()+1; //need space for null character
  if (size > buflen) {
    if (buflen == 0) return size;
    else return -ERANGE;
  }
  size = cwd.copy(buf, size);
  buf[size] = '\0'; //fill in null character
  return 0;
}

extern "C" int ceph_chdir (const char *s)
{
  return client->chdir(s);
}

/*if we want to extern C this, we need to convert it to const char*,
which will mean storing it somewhere or else making the caller
responsible for delete-ing a c-string they didn't create*/
void ceph_getcwd(string& cwd)
{
  client->getcwd(cwd);
}

extern "C" int ceph_opendir(const char *name, DIR **dirpp)
{
  return client->opendir(name, dirpp);
}

extern "C" int ceph_closedir(DIR *dirp)
{
  return client->closedir(dirp);
}

extern "C" int ceph_readdir_r(DIR *dirp, struct dirent *de)
{
  return client->readdir_r(dirp, de);
}

extern "C" int ceph_readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask)
{
  return client->readdirplus_r(dirp, de, st, stmask);
}

extern "C" int ceph_getdents(DIR *dirp, char *buf, int buflen)
{
  return client->getdents(dirp, buf, buflen);
}

extern "C" int ceph_getdnames(DIR *dirp, char *buf, int buflen)
{
  return client->getdnames(dirp, buf, buflen);
}

extern "C" void ceph_rewinddir(DIR *dirp)
{
  client->rewinddir(dirp);
}

extern "C" loff_t ceph_telldir(DIR *dirp)
{
  return client->telldir(dirp);
}

extern "C" void ceph_seekdir(DIR *dirp, loff_t offset)
{
  client->seekdir(dirp, offset);
}

extern "C" int ceph_link (const char *existing, const char *newname)
{
  return client->link(existing, newname);
}

extern "C" int ceph_unlink (const char *path)
{
  return client->unlink(path);
}

extern "C" int ceph_rename(const char *from, const char *to)
{
  return client->rename(from, to);
}

// dirs
extern "C" int ceph_mkdir(const char *path, mode_t mode)
{
  return client->mkdir(path, mode);
}

extern "C" int ceph_mkdirs(const char *path, mode_t mode)
{
  return client->mkdirs(path, mode);
}

extern "C" int ceph_rmdir(const char *path)
{
  return client->rmdir(path);
}

// symlinks
extern "C" int ceph_readlink(const char *path, char *buf, loff_t size)
{
  return client->readlink(path, buf, size);
}

extern "C" int ceph_symlink(const char *existing, const char *newname)
{
  return client->symlink(existing, newname);
}

// inode stuff
extern "C" int ceph_lstat(const char *path, struct stat *stbuf)
{
  return client->lstat(path, stbuf);
}

extern "C" int ceph_lstat_precise(const char *path, stat_precise *stbuf)
{
  return client->lstat_precise(path, (Client::stat_precise*)stbuf);
}

extern "C" int ceph_setattr(const char *relpath, struct stat *attr, int mask)
{
  Client::stat_precise p_attr = Client::stat_precise(*attr);
  return client->setattr(relpath, &p_attr, mask);
}

extern "C" int ceph_setattr_precise(const char *relpath,
				    struct stat_precise *attr, int mask) {
  return client->setattr(relpath, (Client::stat_precise*)attr, mask);
}

extern "C" int ceph_chmod(const char *path, mode_t mode)
{
  return client->chmod(path, mode);
}
extern "C" int ceph_chown(const char *path, uid_t uid, gid_t gid)
{
  return client->chown(path, uid, gid);
}

extern "C" int ceph_utime(const char *path, struct utimbuf *buf)
{
  return client->utime(path, buf);
}

extern "C" int ceph_truncate(const char *path, loff_t size)
{
  return client->truncate(path, size);
}

// file ops
extern "C" int ceph_mknod(const char *path, mode_t mode, dev_t rdev)
{
  return client->mknod(path, mode, rdev);
}

extern "C" int ceph_open(const char *path, int flags, mode_t mode)
{
  return client->open(path, flags, mode);
}

extern "C" int ceph_close(int fd)
{
  return client->close(fd);
}

extern "C" loff_t ceph_lseek(int fd, loff_t offset, int whence)
{
  return client->lseek(fd, offset, whence);
}

extern "C" int ceph_read(int fd, char *buf, loff_t size, loff_t offset)
{
  return client->read(fd, buf, size, offset);
}

extern "C" int ceph_write(int fd, const char *buf, loff_t size, loff_t offset)
{
  return client->write(fd, buf, size, offset);
}

extern "C" int ceph_ftruncate(int fd, loff_t size)
{
  return client->ftruncate(fd, size);
}

extern "C" int ceph_fsync(int fd, bool syncdataonly)
{
  return client->fsync(fd, syncdataonly);
}

extern "C" int ceph_fstat(int fd, struct stat *stbuf)
{
  return client->fstat(fd, stbuf);
}

extern "C" int ceph_sync_fs()
{
  return client->sync_fs();
}

extern "C" int ceph_get_file_stripe_unit(int fh)
{
  return client->get_file_stripe_unit(fh);
}

extern "C" int ceph_get_file_replication(const char *path) {
  int fd = client->open(path, O_RDONLY);
  int rep = client->get_file_replication(fd);
  client->close(fd);
  return rep;
}

extern "C" int ceph_get_default_preferred_pg(int fd)
{
  return client->get_default_preferred_pg(fd);
}

extern "C" int ceph_set_default_file_stripe_unit(int stripe)
{
  client->set_default_file_stripe_unit(stripe);
  return 0;
}

extern "C" int ceph_set_default_file_stripe_count(int count)
{
  client->set_default_file_stripe_unit(count);
  return 0;
}

extern "C" int ceph_set_default_object_size(int size)
{
  client->set_default_object_size(size);
  return 0;
}

extern "C" int ceph_set_default_file_replication(int replication)
{
  client->set_default_file_replication(replication);
  return 0;
}

extern "C" int ceph_set_default_preferred_pg(int pg)
{
  client->set_default_preferred_pg(pg);
  return 0;
}

extern "C" int ceph_get_file_stripe_address(int fh, loff_t offset, char *buf, int buflen)
{
  string address;
  int r = client->get_file_stripe_address(fh, offset, address);
  if (r != 0) return r; //at time of writing, method ONLY returns
  // 0 or -EINVAL if there are no known osds
  int len = address.size()+1;
  if (len > buflen) {
    if (buflen == 0) return len;
    else return -ERANGE;
  }
  len = address.copy(buf, len, 0);
  buf[len] = '\0'; // write a null char to terminate c-style string
  return 0;
}

/* Low-level exports */

extern "C" int ceph_ll_lookup(vinodeno_t parent, const char *name,
			      struct stat *attr, int uid, int gid)
{
  
  return (client->ll_lookup(parent, name, attr, uid, gid));
}

bool ceph_ll_forget(vinodeno_t vino, int count)
{
  return (client->ll_forget(vino, count));
}

extern "C" int ceph_ll_walk(const char *name, struct stat *attr)
{
  return(client->ll_walk(name, attr));
}
    
  
extern "C" int ceph_ll_getattr(vinodeno_t vi, struct stat *attr,
			       int uid, int gid)
{
  return (client->ll_getattr(vi, attr, uid, gid));
}

extern "C" int ceph_ll_setattr(vinodeno_t vi, struct stat *st,
			       int mask, int uid, int gid)
{
  return (client->ll_setattr(vi, st, mask, uid, gid));
}
  
extern "C" int ceph_ll_open(vinodeno_t vi, int flags, int uid,
			    int gid)
{
  Mutex::Locker lock(ceph_client_mutex);
  int ret;
  Fh *filehandle=NULL;
  ret=client->ll_open(vi, flags, &filehandle, uid, gid);
  assert(filehandle);
  if (ret != 0) {
    return ret;
  } else {
    ret = get_fd();
    fd_map[ret] = filehandle;
  }
  return ret;
}

extern "C" int ceph_ll_read(int fd, int64_t off, uint64_t len, char* buf)
{
  Mutex::Locker lock(ceph_client_mutex);
  Fh *filehandle=fd_map[fd];
  bufferlist bl;
  int r=client->ll_read(filehandle, off, len, &bl);
  if (r >= 0)
    {
      bl.copy(0, bl.length(), buf);
      r = bl.length();
    }
  return r;
}

extern "C" int ceph_ll_write(int fd, int64_t off, uint64_t len,
			     const char *data)
{
  Mutex::Locker lock(ceph_client_mutex);
  Fh *filehandle=fd_map[fd];
  return (client->ll_write(filehandle, off, len, data));
}

extern "C" int ceph_ll_close(int fd)
{
  Mutex::Locker lock(ceph_client_mutex);
  Fh *filehandle=fd_map[fd];
  if (!filehandle)
      return 0;
  int rc=client->ll_release(filehandle);
  fd_map.erase(fd);
  put_fd(fd);
  return rc;
}

extern "C" int ceph_ll_create(vinodeno_t parent, const char* name,
			      mode_t mode, int flags,
			      struct stat *attr, int uid,
			      int gid)
{
  Mutex::Locker lock(ceph_client_mutex);
  int ret;
  Fh *filehandle=NULL;

  ret=client->ll_create(parent, name, mode, flags, attr, &filehandle,
			uid, gid);
  if (ret == 0) {
    ret = get_fd();
    fd_map[ret] = filehandle;
  }
  return ret;
}

extern "C" int ceph_ll_mkdir(vinodeno_t parent, const char *name,
			     mode_t mode, struct stat *attr,
			     int uid, int gid)
{
  return (client->ll_mkdir(parent, name, mode, attr, uid, gid));
}

extern "C" int ceph_ll_link(vinodeno_t obj, vinodeno_t newparrent,
			    const char *name, struct stat *attr,
			    int uid, int gid)
{
  return (client->ll_link(obj, newparrent, name, attr, uid, gid));
}

extern "C" int ceph_ll_truncate(vinodeno_t obj, uint64_t length, int uid,
				int gid)
{
  Mutex::Locker lock(ceph_client_mutex);
  struct stat st;
  st.st_size=length;
  
  return(client->ll_setattr(obj, &st, CEPH_SETATTR_SIZE, uid, gid));
}
  
extern "C" int ceph_ll_opendir(vinodeno_t vino, void **dirpp,
			       int uid, int gid)
{
  return(client->ll_opendir(vino, dirpp, uid, gid));
}

extern "C" void ceph_ll_releasedir(DIR* dir)
{
  client->ll_releasedir((void*)dir);
}

extern "C" int ceph_ll_rename(vinodeno_t parent, const char *name,
			  vinodeno_t newparent, const char *newname,
			  int uid, int gid)
{
  return (client->ll_rename(parent, name, newparent, newname, uid,
			    gid)); 
}

extern "C" int ceph_ll_unlink(vinodeno_t vino, const char *name,
			      int uid, int gid) 
{
  return (client->ll_unlink(vino, name, uid, gid));
}

extern "C" int ceph_ll_statfs(vinodeno_t vino, struct statvfs *stbuf)
{
  return (client->ll_statfs(vino, stbuf));
}

extern "C" int ceph_ll_readlink(vinodeno_t vino, const char **value, int uid, int gid)
{
  return (client->ll_readlink(vino, value, uid, gid));
}


extern "C" int ceph_ll_symlink(vinodeno_t parent, const char *name, const char *value, struct stat *attr, int uid, int gid)
{
  return (client->ll_symlink(parent, name, value, attr, uid, gid));
}

extern "C" int ceph_ll_rmdir(vinodeno_t vino, const char *name,
			     int uid = -1, int gid = -1)
{
  return (client->ll_rmdir(vino, name, uid, gid));
}
