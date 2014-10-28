// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>
#include <stdio.h>
#include <pthread.h>

#include "libosd/ceph_osd.h"


static int test_sync()
{
  struct libosd_init_args args = {
    .id = 0,
    .config = "/etc/ceph/ceph.conf",
  };
  struct libosd *osd = libosd_init(&args);
  if (osd == NULL) {
    fputs("osd init failed\n", stderr);
    return 1;
  }

  uuid_t volume;
  int r = libosd_get_volume(osd, "rbd", volume);
  if (r != 0) {
    fprintf(stderr, "libosd_get_volume() failed with %d\n", r);
  } else {
    char buf[64] = {};
    r = libosd_write(osd, "obj", volume, 0, sizeof(buf), buf,
		     LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE,
		     NULL, NULL);
    fprintf(stderr, "libosd_write() returned %d\n", r);
  }

  libosd_shutdown(osd);
  fputs("libosd shutting down\n", stderr);

  libosd_join(osd);
  fputs("libosd_join returned\n", stderr);

  libosd_cleanup(osd);
  fputs("libosd_cleanup finished\n", stderr);
  return 0;
}

struct io_completion {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int result;
  int done;
};

static void write_completion(int result, uint64_t length, int flags, void *user)
{
  struct io_completion *io = (struct io_completion*)user;
  printf("write_completion result %d flags %d\n", result, flags);

  /* wait for STABLE */
  if (result == 0 && (flags & LIBOSD_WRITE_CB_STABLE) == 0)
    return;

  pthread_mutex_lock(&io->mutex);
  io->result = result;
  io->done = 1;
  pthread_cond_signal(&io->cond);
  pthread_mutex_unlock(&io->mutex);
}

static int test_async()
{
  struct libosd_init_args args = {
    .id = 0,
    .config = "/etc/ceph/ceph.conf",
  };
  struct libosd *osd = libosd_init(&args);
  if (osd == NULL) {
    fputs("osd init failed\n", stderr);
    return 1;
  }

  uuid_t volume;
  int r = libosd_get_volume(osd, "rbd", volume);
  if (r != 0) {
    fprintf(stderr, "libosd_get_volume() failed with %d\n", r);
  } else {
    struct io_completion io = {
      .mutex = PTHREAD_MUTEX_INITIALIZER,
      .cond = PTHREAD_COND_INITIALIZER,
      .done = 0
    };
    char buf[64] = {};
    r = libosd_write(osd, "obj", volume, 0, sizeof(buf), buf,
		     LIBOSD_WRITE_CB_UNSTABLE | LIBOSD_WRITE_CB_STABLE,
		     write_completion, &io);
    fprintf(stderr, "libosd_write() returned %d\n", r);

    pthread_mutex_lock(&io.mutex);
    while (!io.done)
      pthread_cond_wait(&io.cond, &io.mutex);
    pthread_mutex_unlock(&io.mutex);
    fprintf(stderr, "write_callback() got result %d\n", io.result);
  }

  fputs("libosd shutting down\n", stderr);
  libosd_shutdown(osd);

  libosd_join(osd);
  fputs("libosd_join returned\n", stderr);

  libosd_cleanup(osd);
  fputs("libosd_cleanup finished\n", stderr);
  return 0;
}

static int test_double()
{
  struct libosd *osd1, *osd2;
  struct libosd_init_args args1 = {
    .id = 0,
    .config = "/etc/ceph/ceph.conf",
  };
  struct libosd_init_args args2 = {
    .id = 1,
    .config = "/etc/ceph/ceph.conf",
  };

  /* start osds */
  osd1 = libosd_init(&args1);
  if (osd1 == NULL) {
    fputs("osd1 init failed\n", stderr);
    return 1;
  }
  printf("osd1 created %p\n", osd1);

  osd2 = libosd_init(&args2);
  if (osd2 == NULL) {
    fputs("osd2 init failed\n", stderr);
    return 1;
  }
  printf("osd2 created %p\n", osd2);

  fputs("libosds shutting down\n", stderr);
  libosd_shutdown(osd1);
  libosd_shutdown(osd2);

  /* join osds */
  printf("waiting on osd1 %p\n", osd1);
  libosd_join(osd1);
  printf("waiting on osd2 %p\n", osd2);
  libosd_join(osd2);

  /* clean up */
  libosd_cleanup(osd1);
  libosd_cleanup(osd2);
  puts("finished");
  return 0;
}


int main(int argc, const char *argv[])
{
  int r = 0;

  signal(SIGINT, libosd_signal);

  r = test_sync();
  if (r != 0) {
    fprintf(stderr, "test_sync() failed with %d\n", r);
    return r;
  }

  r = test_async();
  if (r != 0) {
    fprintf(stderr, "test_async() failed with %d\n", r);
    return r;
  }

  r = test_double();
  if (r != 0) {
    fprintf(stderr, "test_double() failed with %d\n", r);
    return r;
  }
  return 0;
}
