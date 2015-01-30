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
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_Sem_Posix__H
#define CEPH_Sem_Posix__H

#include "acconfig.h"

#ifdef HAVE_SEMAPHORE_H

#include <semaphore.h>

class Semaphore
{
  sem_t sem;
public:
  Semaphore(unsigned int initial = 0) {
    sem_init(&sem, 0, initial);
  }
  ~Semaphore() {
    sem_destroy(&sem);
  }
  void Put() {
    sem_post(&sem);
  }
  void Get() {
    sem_wait(&sem);
  }
};

#else // !HAVE_SEMAPHORE_H

#include <condition_variable>
#include <mutex>

class Semaphore
{
  std::mutex m;
  typedef std::unique_lock<std::mutex> unique_lock;
  std::condition_variable c;
  unsigned int count;

  public:

  Semaphore(unsigned int initial = 0)
    : count(initial)
  {
  }

  void Put()
  {
    unique_lock l(m);
    count++;
    c.notify_all();
    l.unlock();
  }

  void Get()
  {
    unique_lock l(m);
    while(count == 0) {
      c.wait(l);
    }
    count--;
    l.unlock();
  }
;

#endif // !HAVE_SEMAPHORE_H

#endif // !CEPH_Sem_Posix__H
