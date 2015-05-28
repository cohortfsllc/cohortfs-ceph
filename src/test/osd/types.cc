// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"
#include "osd/osd_types.h"
#include "osd/ObjectContext.h"
#include "osd/OSDMap.h"
#include "gtest/gtest.h"
#include "common/Thread.h"

#include <sstream>

using std::cout;

class ObjectContextTest : public ::testing::Test {
protected:

  static const useconds_t DELAY_MAX = 20 * 1000 * 1000;

  class Thread_read_lock : public Thread {
  public:
    ObjectContext &obc;

    Thread_read_lock(ObjectContext& _obc) :
      obc(_obc)
    {
    }

    virtual void *entry() {
      obc.ondisk_read_lock();
      return NULL;
    }
  };

  class Thread_write_lock : public Thread {
  public:
    ObjectContext &obc;

    Thread_write_lock(ObjectContext& _obc) :
      obc(_obc)
    {
    }

    virtual void *entry() {
      obc.ondisk_write_lock();
      return NULL;
    }
  };

};

TEST_F(ObjectContextTest, read_write_lock)
{
  {
    hoid_t oid;
    ObjectContext obc(oid, NULL);

    //
    // write_lock
    // write_lock
    // write_unlock
    // write_unlock
    //
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_write_lock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_lock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(2, obc.unstable_writes);

    obc.ondisk_write_unlock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_unlock();

    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);
  }

  useconds_t delay = 0;

  {
    hoid_t oid;
    ObjectContext obc(oid, NULL);

    //
    // write_lock
    // read_lock => wait
    // write_unlock => signal
    // read_unlock
    //
    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_write_lock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    Thread_read_lock t(obc);
    t.create();

    do {
      cout << "Trying (1) with delay " << delay << "us\n";
      usleep(delay);
    } while (obc.readers_waiting == 0 &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);

    EXPECT_EQ(1, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_unlock();

    do {
      cout << "Trying (2) with delay " << delay << "us\n";
      usleep(delay);
    } while ((obc.readers == 0 || obc.readers_waiting == 1) &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);
    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(1, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_read_unlock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    t.join();
  }

  {
    hoid_t oid;
    ObjectContext obc(oid, NULL);

    //
    // read_lock
    // write_lock => wait
    // read_unlock => signal
    // write_unlock
    //
    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_read_lock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(1, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    Thread_write_lock t(obc);
    t.create();

    do {
      cout << "Trying (3) with delay " << delay << "us\n";
      usleep(delay);
    } while ((obc.writers_waiting == 0) &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(1, obc.readers);
    EXPECT_EQ(1, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    obc.ondisk_read_unlock();

    do {
      cout << "Trying (4) with delay " << delay << "us\n";
      usleep(delay);
    } while ((obc.unstable_writes == 0 || obc.writers_waiting == 1) &&
	     ( delay = delay * 2 + 1) < DELAY_MAX);

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(1, obc.unstable_writes);

    obc.ondisk_write_unlock();

    EXPECT_EQ(0, obc.readers_waiting);
    EXPECT_EQ(0, obc.readers);
    EXPECT_EQ(0, obc.writers_waiting);
    EXPECT_EQ(0, obc.unstable_writes);

    t.join();
  }

}

/*
 * Local Variables:
 * compile-command: "cd .. ;
 *   make unittest_osd_types ;
 *   ./unittest_osd_types # --gtest_filter=pg_missing_t.constructor
 * "
 * End:
 */
