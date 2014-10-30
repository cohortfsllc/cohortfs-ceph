// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>

#include "libosd/ceph_osd.h"


static int test_sync_write(struct libosd *osd, uuid_t volume)
{
  char buf[64] = {};
  int r;

  // flags=0 -> EINVAL
  r = libosd_write(osd, "sync-inval0", volume, 0, sizeof(buf), buf,
		   0, NULL, NULL);
  if (r != -EINVAL) {
    fprintf(stderr, "libosd_write(flags=0) returned %d, expected -EINVAL\n", r);
    return -1;
  }

  // flags=UNSTABLE|STABLE -> EINVAL
  r = libosd_write(osd, "sync-inval2", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE,
		   NULL, NULL);
  if (r != -EINVAL) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE|STABLE) returned %d, "
	"expected -EINVAL\n", r);
    return -1;
  }

  // flags=UNSTABLE
  r = libosd_write(osd, "sync-unstable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE, NULL, NULL);
  if (r < 0) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE) failed with %d\n", r);
    return r;
  }
  if (r != sizeof(buf)) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE) wrote %d bytes, "
       " expected %ld\n", r, sizeof(buf));
    return -1;
  }

  // flags=STABLE
  r = libosd_write(osd, "sync-stable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_STABLE, NULL, NULL);
  if (r < 0) {
    fprintf(stderr, "libosd_write(flags=STABLE) failed with %d\n", r);
    return r;
  }
  if (r != sizeof(buf)) {
    fprintf(stderr, "libosd_write(flags=STABLE) wrote %d bytes, "
       " expected %ld\n", r, sizeof(buf));
    return -1;
  }
  return r;
}

struct io_completion {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int result;
  uint64_t length;
  int done;
};

static void write_completion(int result, uint64_t length, int flags, void *user)
{
  struct io_completion *io = (struct io_completion*)user;
  printf("write_completion result %d flags %d\n", result, flags);

  pthread_mutex_lock(&io->mutex);
  io->result = result;
  io->length = length;
  io->done++;
  pthread_cond_signal(&io->cond);
  pthread_mutex_unlock(&io->mutex);
}

static int wait_for_completion(struct io_completion *io, int count)
{
  pthread_mutex_lock(&io->mutex);
  while (io->done < count && io->result == 0)
    pthread_cond_wait(&io->cond, &io->mutex);
  pthread_mutex_unlock(&io->mutex);
  return io->result;
}

static int test_async_write(struct libosd *osd, uuid_t volume)
{
  char buf[64] = {};
  struct io_completion io1 = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  struct io_completion io2 = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  struct io_completion io3 = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .done = 0
  };
  int r;

  // flags=0 -> EINVAL
  r = libosd_write(osd, "async-inval0", volume, 0, sizeof(buf), buf,
		   0, write_completion, NULL);
  if (r != -EINVAL) {
    fprintf(stderr, "libosd_write(flags=0) returned %d, expected -EINVAL\n", r);
    return -1;
  }

  // flags=UNSTABLE
  r = libosd_write(osd, "async-unstable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE, write_completion, &io1);
  if (r != 0) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE) failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io1, 1);
  if (r != 0) {
    fprintf(stderr, "write_completion(flags=UNSTABLE) got result %d\n", r);
    return r;
  }
  if (io1.length != sizeof(buf)) {
    fprintf(stderr, "write_completion(flags=UNSTABLE) got length %ld, "
	"expected %ld\n", io1.length, sizeof(buf));
    return r;
  }

  // flags=STABLE
  r = libosd_write(osd, "async-stable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_STABLE, write_completion, &io2);
  if (r != 0) {
    fprintf(stderr, "libosd_write(flags=STABLE) failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io2, 1);
  if (r != 0) {
    fprintf(stderr, "write_completion(flags=STABLE) got result %d\n", r);
    return r;
  }
  if (io2.length != sizeof(buf)) {
    fprintf(stderr, "write_completion(flags=STABLE) got length %ld, "
	"expected %ld\n", io2.length, sizeof(buf));
    return r;
  }

  // flags=UNSTABLE|STABLE
  r = libosd_write(osd, "async-unstable-stable", volume, 0, sizeof(buf), buf,
		   LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE,
		   write_completion, &io3);
  if (r != 0) {
    fprintf(stderr, "libosd_write(flags=UNSTABLE|STABLE) failed with %d\n", r);
    return r;
  }
  r = wait_for_completion(&io3, 2); // wait for both callbacks
  if (r != 0) {
    fprintf(stderr, "write_completion(flags=UNSTABLE|STABLE) got result %d\n", r);
    return r;
  }
  if (io3.length != sizeof(buf)) {
    fprintf(stderr, "write_completion(flags=UNSTABLE) got length %ld, "
	"expected %ld\n", io3.length, sizeof(buf));
    return r;
  }
  return 0;
}

static int run_tests(struct libosd *osd, uuid_t volume)
{
  int r = test_sync_write(osd, volume);
  if (r != 0) {
    fprintf(stderr, "test_sync_write() failed with %d\n", r);
    return r;
  }

  r = test_async_write(osd, volume);
  if (r != 0) {
    fprintf(stderr, "test_async_write() failed with %d\n", r);
    return r;
  }
  return 0;
}

int main(int argc, const char *argv[])
{
  uuid_t volume;
  int r = 0;
  struct libosd_init_args args = {
    .id = 0,
    .config = NULL,
  };
  struct libosd *osd = libosd_init(&args);
  if (osd == NULL) {
    fputs("osd init failed\n", stderr);
    return -1;
  }
  signal(SIGINT, libosd_signal);

  r = libosd_get_volume(osd, "rbd", volume);
  if (r != 0) {
    fprintf(stderr, "libosd_get_volume() failed with %d\n", r);
  } else {
    r = run_tests(osd, volume);
  }

  libosd_shutdown(osd);
  fputs("libosd shutting down\n", stderr);

  libosd_join(osd);
  fputs("libosd_join returned\n", stderr);

  libosd_cleanup(osd);
  fputs("libosd_cleanup finished\n", stderr);
  return r;
}
