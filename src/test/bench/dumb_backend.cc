// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "acconfig.h"

#include <unistd.h>
#include "dumb_backend.h"

string DumbBackend::get_full_path(const string &oid_t)
{
	return path + "/" + oid_t;
}

void DumbBackend::_write(
  const string &oid_t,
  uint64_t offset,
  const bufferlist &bl,
  std::function<void(int)>&& on_applied,
  std::function<void(int)>&& on_commit)
{
  string full_path(get_full_path(oid_t));
  int fd = ::open(
    full_path.c_str(), O_CREAT|O_WRONLY, 0777);
  if (fd < 0) {
    std::cout << full_path << ": errno is " << errno << std::endl;
    assert(0);
  }

  int r =  ::lseek(fd, offset, SEEK_SET);
  if (r < 0) {
    r = errno;
    std::cout << "lseek failed, errno is: " << r << std::endl;
    ::close(fd);
    return;
  }
  bl.write_fd(fd);
  on_applied(0);
  if (do_fsync)
    ::fsync(fd);
#ifdef HAVE_SYNC_FILE_RANGE
  if (do_sync_file_range)
    ::sync_file_range(fd, offset, bl.length(),
		      SYNC_FILE_RANGE_WAIT_AFTER);
#else
# warning "sync_file_range not supported!"
#endif
#ifdef HAVE_POSIX_FADVISE
  if (do_fadvise) {
    int fa_r = ::posix_fadvise(fd, offset, bl.length(), POSIX_FADV_DONTNEED);
    if (fa_r) {
	std::cout << "posix_fadvise failed, errno is: " << fa_r << std::endl;
    }
  }
#else
# warning "posix_fadvise not supported!"
#endif
  ::close(fd);
  {
    lock_guard l(pending_commit_mutex);
    pending_commits.emplace_back(std::move(on_commit));
  }
  sem.Put();
}

void DumbBackend::read(
  const string &oid_t,
  uint64_t offset,
  uint64_t length,
  bufferlist *bl,
  std::function<void(int)>&& on_complete)
{
  string full_path(get_full_path(oid_t));
  int fd = ::open(
    full_path.c_str(), 0, O_RDONLY);
  if (fd < 0) return;

  int r = ::lseek(fd, offset, SEEK_SET);
  if (r < 0) {
    r = errno;
    std::cout << "lseek failed, errno is: " << r << std::endl;
    ::close(fd);
    return;
  }

  bl->read_fd(fd, length);
  ::close(fd);
  on_complete(0);
}

void DumbBackend::sync_loop()
{
  while (1) {
    sleep(sync_interval);
    {
      lock_guard l(sync_loop_mutex);
      if (sync_loop_stop != 0) {
	sync_loop_stop = 2;
	sync_loop_cond.notify_all();
	break;
      }
    }
    tp.pause();
#ifdef HAVE_SYS_SYNCFS
    ::syncfs(sync_fd);
#else
    ::sync();
#endif
    {
      lock_guard l(pending_commit_mutex);
      for (auto i = pending_commits.begin();
	   i != pending_commits.end();
	   pending_commits.erase(i++)) {
	(*i)(0);
      }
    }
    tp.unpause();
  }
}
