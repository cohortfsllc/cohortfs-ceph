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

#include <condition_variable>
#include <iostream>
#include <mutex>

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/mount.h>
#include "os/ObjectStore.h"
#include "os/file/FileStore.h"
#include "os/kv/KeyValueStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/errno.h"
#include <unordered_map>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

typedef boost::mt11213b gen_type;
static CephContext* cct;

#if GTEST_HAS_PARAM_TEST

typedef std::lock_guard<std::mutex> lock_guard;
typedef std::unique_lock<std::mutex> unique_lock;


class StoreTest : public ::testing::TestWithParam<const char*> {
public:
  boost::scoped_ptr<ObjectStore> store;

  StoreTest() : store(0) {}
  virtual void SetUp() {
    int r = ::mkdir("store_test_temp_dir", 0777);
    if (r < 0 && errno != EEXIST) {
      r = -errno;
      cerr << __func__ << ": unable to create store_test_temp_dir" << ": "
	   << cpp_strerror(r) << std::endl;
      return;
    }

    ObjectStore *store_ =
      ObjectStore::create(cct,
			  string(GetParam()),
			  string("store_test_temp_dir"),
			  string("store_test_temp_journal"));
    store.reset(store_);
    EXPECT_EQ(store->mkfs(), 0);
    EXPECT_EQ(store->mount(), 0);
  }

  virtual void TearDown() {
    store->umount();
  }
};

bool sorted(const vector<hoid_t> &in) {
  hoid_t start;
  for (vector<hoid_t>::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (start > *i) return false;
    start = *i;
  }
  return true;
}
#if 0
TEST_P(StoreTest, SimpleColTest) {
  coll_t cid = coll_t("initial");
  int r = 0;
  {
    Transaction t;
    t.create_collection(cid);
    cerr << "create collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    Transaction t;
    t.create_collection(cid);
    cerr << "add collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleObjectTest) {
  int r;
  coll_t cid = coll_t("coll");
  {
    Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  hoid_t hoid("Object 1");
  {
    Transaction t;
    (void) t.push_cid(cid);
    (void) t.push_oid(hoid);
    t.touch();
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    Transaction t;
    (void) t.push_cid(cid);
    (void) t.push_oid(hoid);
    t.remove();
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, SimpleObjectLongnameTest) {
  int r;
  coll_t cid = coll_t("coll");
  {
    Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  hoid_t hoid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaObjectaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1");
  {
    Transaction t;
    (void) t.push_cid(cid);
    (void) t.push_oid(hoid);
    t.touch();
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    Transaction t;
    (void) t.push_cid(cid);
    (void) t.push_oid(hoid);
    t.remove();
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

#if 0 /* XXXX Matt */
TEST_P(StoreTest, ManyObjectTest) {
  int NUM_OBJS = 10;
  int r = 0;
  coll_t cid("blah");
  string base(200, 'a'); // XXX: was 500, can't handle name > MAX_PATH
  set<hoid_t> created;
  {
    Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  for (int i = 0; i < NUM_OBJS; ++i) {
    if (!(i % 5)) {
      cerr << "Object " << i << std::endl;
    }
    Transaction t;
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    hoid_t oid(oid_t(string(buf) + base));
    (void) t.push_cid(cid);
    (void) t.push_oid(oid);
    t.touch();
    created.insert(oid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  CollectionHandle ch = store->open_collection(cid);
  for (set<hoid_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    struct stat buf;
    ObjectHandle oh = store->get_object(ch, *i);
    ASSERT_TRUE(!store->stat(ch, oh, &buf));
    store->put_object(oh);
  }
#if 0
  set<hoid_t> listed;
  vector<hoid_t> objects;
  r = store->collection_list(ch, objects);
  ASSERT_EQ(r, 0);

  cerr << "objects.size() is " << objects.size() << std::endl;
  for (vector<hoid_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    listed.insert(*i);
    ASSERT_TRUE(created.count(*i));
  }
  ASSERT_TRUE(listed.size() == created.size());

  hoid_t start, next;
  objects.clear();
  r = store->collection_list_partial(
    ch,
    start,
    50,
    60,
    &objects,
    &next);
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(objects.empty());

  objects.clear();
  listed.clear();
  while (1) {
    r = store->collection_list_partial(ch,
				       start,
				       50,
				       60,
				       &objects,
				       &next);
    ASSERT_TRUE(sorted(objects));
    ASSERT_EQ(r, 0);
    listed.insert(objects.begin(), objects.end());
    objects.clear();
    start = next;
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  for (set<hoid_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }
#endif
  for (set<hoid_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    Transaction t;
    (void) t.push_col(ch);
    (void) t.push_oid(*i);
    t.remove();
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  cerr << "cleaning up" << std::endl;
  store->close_collection(ch);
  {
    Transaction t;
    t.remove_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}
#endif /* Matt */
#endif /* GTEST_HAS_PARAM_TEST */

class ObjectGenerator {
public:
  virtual hoid_t create_object(gen_type *gen) = 0;
  virtual ~ObjectGenerator() {}
};

class MixedGenerator : public ObjectGenerator {
public:
  unsigned seq;
  MixedGenerator() : seq(0) {}
  hoid_t create_object(gen_type *gen) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%u", seq);

    boost::uniform_int<> true_false(0, 1);
    string name(buf);
    if (true_false(*gen)) {
      // long
      name.append(200, 'a'); // XXX: was 500, can't handle name > MAX_PATH
    } else if (true_false(*gen)) {
      name = "DIR_" + name;
    }

    // hash
    //boost::binomial_distribution<uint32_t> bin(0xFFFFFF, 0.5);
    ++seq;
    return hoid_t(oid_t(name, chunktype::data, rand() & 0xFF));
  }
};

class SyntheticWorkloadState {
public:
  static const unsigned max_in_flight = 16;
  static const unsigned max_objects = 1000;
  static const unsigned max_object_len = 1024 * 20;
  CollectionHandle ch;
  unsigned in_flight;
  map<hoid_t, bufferlist> contents;
  set<hoid_t> available_objects;
  set<hoid_t> in_flight_objects;
  ObjectGenerator *object_gen;
  gen_type *rng;
  ObjectStore *store;

  std::mutex lock;
  std::condition_variable cond;

  class C_SyntheticOnReadable : public Context {
  public:
    SyntheticWorkloadState *state;
    Transaction *t;
    hoid_t hoid;
    C_SyntheticOnReadable(SyntheticWorkloadState *state,
			  Transaction *t, hoid_t _oid)
      : state(state), t(t), hoid(_oid) {}

    void finish(int r) {
      lock_guard locker(state->lock);
      ASSERT_TRUE(state->in_flight_objects.count(hoid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(hoid);
      if (state->contents.count(hoid))
	state->available_objects.insert(hoid);
      --(state->in_flight);
      std::cout << "DEC state->in_flight finish (" << hoid << ") "
		<< "in_flight " << state->in_flight << std::endl;
      state->cond.notify_all();
      delete t;
    }
  };

  static void filled_byte_array(bufferlist& bl, size_t size)
  {
    static const char alphanum[] = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

    bufferptr bp(size);
    for (unsigned int i = 0; i < size - 1; i++) {
      bp[i] = alphanum[rand() % sizeof(alphanum)];
    }
    bp[size - 1] = '\0';

    bl.append(bp);
  }

  SyntheticWorkloadState(ObjectStore *store,
			 ObjectGenerator *gen,
			 gen_type *rng)
    : ch(NULL), in_flight(0), object_gen(gen), rng(rng), store(store) {}
  ~SyntheticWorkloadState() {
    if (ch) {
      store->sync_and_flush();
      coll_t cid = ch->get_cid();
      store->close_collection(ch);
      {
	Transaction t;
	t.remove_collection(cid);
	cerr << "remove collection" << std::endl;
	int r = store->apply_transaction(t);
      }
    }
  }

  int init(const coll_t &cid) {
    Transaction t;
    t.create_collection(cid);
    int r = store->apply_transaction(t);
    if (r == 0) {
      ch = store->open_collection(cid);
      if (ch == NULL)
        r = -ENOENT;
    }
    return r;
  }

  hoid_t get_uniform_random_object(unique_lock& l) {
    while (in_flight >= max_in_flight || available_objects.empty())
      cond.wait(l);
    boost::uniform_int<> choose(0, available_objects.size() - 1);
    int index = choose(*rng);
    set<hoid_t>::iterator i = available_objects.begin();
    for ( ; index > 0; --index, ++i) ;
    hoid_t ret = *i;
    return ret;
  }

  void wait_for_ready(unique_lock& l) {
    while (in_flight >= max_in_flight)
      cond.wait(l);
  }

  void wait_for_done() {
    unique_lock l(lock);
    while (in_flight)
      cond.wait(l);
  }

  bool can_create() {
    return (available_objects.size() + in_flight_objects.size()) < max_objects;
  }

  bool can_unlink() {
    return (available_objects.size() + in_flight_objects.size()) > 0;
  }

  int touch() {
    unique_lock l(lock);
    if (!can_create())
      return -ENOSPC;
    wait_for_ready(l);
    hoid_t new_obj = object_gen->create_object(rng);
    available_objects.erase(new_obj);
    Transaction *t = new Transaction;
    (void) t->push_col(ch);
    (void) t->push_oid(new_obj);
    t->touch();
    ++in_flight;
    std::cout << "TOUCH oid (" << new_obj << ") in_flight "
	      << in_flight << std::endl;
    in_flight_objects.insert(new_obj);
    if (!contents.count(new_obj))
      contents[new_obj] = bufferlist();
    return store->queue_transaction(
      t, new C_SyntheticOnReadable(this, t, new_obj));
  }

  int write() {
    unique_lock l(lock);
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready(l);

    hoid_t new_obj = get_uniform_random_object(l);
    available_objects.erase(new_obj);
    Transaction *t = new Transaction;

    boost::uniform_int<> u1(0, max_object_len/2);
    boost::uniform_int<> u2(0, max_object_len);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    bufferlist bl;
    if (offset > len)
      std::swap(offset, len);

    filled_byte_array(bl, len);

    if (contents[new_obj].length() <= offset) {
      contents[new_obj].append_zero(offset-contents[new_obj].length());
      contents[new_obj].append(bl);
    } else {
      bufferlist value;
      contents[new_obj].copy(0, offset, value);
      value.append(bl);
      if (value.length() < contents[new_obj].length())
	contents[new_obj].copy(
	  value.length(), contents[new_obj].length()-value.length(), value);
      value.swap(contents[new_obj]);
    }
    (void) t->push_col(ch);
    (void) t->push_oid(new_obj);
    t->write(offset, len, bl);
    ++in_flight;
    std::cout << "WRITE oid (" << new_obj << ") in_flight "
	      << in_flight << std::endl;
    in_flight_objects.insert(new_obj);
    return store->queue_transaction(
      t, new C_SyntheticOnReadable(this, t, new_obj));
  }

  void read() {
    boost::uniform_int<> u1(0, max_object_len/2);
    boost::uniform_int<> u2(0, max_object_len);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    if (offset > len)
      std::swap(offset, len);

    hoid_t oid;
    int r;
    {
      unique_lock l(lock);
      if (!can_unlink())
	return ;
      wait_for_ready(l);

      oid = hoid_t(get_uniform_random_object(l));
    }
    bufferlist bl, result;
    ObjectHandle oh = store->get_object(ch, oid);
    ASSERT_TRUE(oh);

    std::cout << "READ oid (" << oid << ") (doesn't change in_flight) "
      "in_flight " << in_flight << std::endl;

    r = store->read(ch, oh, offset, len, result);
    if (offset >= contents[oid].length()) {
      ASSERT_EQ(r, 0);
    } else {
      size_t max_len = contents[oid].length() - offset;
      if (len > max_len)
	len = max_len;
      ASSERT_EQ(len, result.length());
      contents[oid].copy(offset, len, bl);
      ASSERT_EQ(r, (int)len);
      ASSERT_TRUE(result.contents_equal(bl));
    }
    store->put_object(oh);
  }

  int truncate() {
    unique_lock l(lock);
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready(l);

    hoid_t oid = get_uniform_random_object(l);
    available_objects.erase(oid);
    Transaction *t = new Transaction;

    boost::uniform_int<> choose(0, max_object_len);
    size_t len = choose(*rng);
    bufferlist bl;

    t->push_col(ch);
    t->push_oid(oid);
    t->truncate(len);
    ++in_flight;
    std::cout << "TRUNC oid (" << oid << ") in_flight "
	      << in_flight << std::endl;
    in_flight_objects.insert(oid);
    if (contents[oid].length() <= len)
      contents[oid].append_zero(len - contents[oid].length());
    else {
      contents[oid].copy(0, len, bl);
      bl.swap(contents[oid]);
    }

    return store->queue_transaction(
      t, new C_SyntheticOnReadable(this, t, oid));
  }

  void scan() {
    unique_lock l(lock);
    cond.wait(l, [&](){ return in_flight; });
    vector<hoid_t> objects;
    set<hoid_t> objects_set, objects_set2;
    hoid_t next, current;
    while (1) {
      cerr << "scanning..." << std::endl;
      int r = store->collection_list_partial(ch, current, 50, 100,
					     &objects, &next);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects));
      objects_set.insert(objects.begin(), objects.end());
      objects.clear();
      if (objects.empty()) break;
      current = next;
    }
    ASSERT_EQ(objects_set.size(), available_objects.size());
    for (set<hoid_t>::iterator i = objects_set.begin();
	 i != objects_set.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }

    int r = store->collection_list(ch, objects);
    ASSERT_EQ(r, 0);
    objects_set2.insert(objects.begin(), objects.end());
    ASSERT_EQ(objects_set2.size(), available_objects.size());
    for (set<hoid_t>::iterator i = objects_set2.begin();
	 i != objects_set2.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }
  }

  void stat() {
    hoid_t oid;
    {
      unique_lock l(lock);
      if (!can_unlink())
	return ;
      oid = get_uniform_random_object(l);
      in_flight_objects.insert(oid);
      available_objects.erase(oid);
      ++in_flight;
    }

    std::cout << "STAT oid (" << oid << ") in_flight "
	      << in_flight << std::endl;

    struct stat buf;
    ObjectHandle oh = store->get_object(ch, oid);
    ASSERT_TRUE(oh);
    int r = store->stat(ch, oh, &buf);
    ASSERT_EQ(0, r);
    std::cout << "STAT oid IFASSERT EQ(" << buf.st_size
	      << ", " << contents[oid].length() << " oid "
	      << oid << std::endl;
    //ASSERT_TRUE(buf.st_size == contents[oid].length());
    {
      lock_guard locker(lock);
      std::cout << "DEC in_flight STAT (" << oid << ")" << std::endl;
      --in_flight;
      cond.notify_all();
      in_flight_objects.erase(oid);
      available_objects.insert(oid);
    }
    store->put_object(oh);
    ASSERT_TRUE(buf.st_size == contents[oid].length());
  }

  int unlink() {
    unique_lock l(lock);
    if (!can_unlink())
      return -ENOENT;
    hoid_t to_remove = hoid_t(get_uniform_random_object(l));
    Transaction *t = new Transaction;
    (void) t->push_col(ch);
    (void) t->push_oid(to_remove);
    t->remove();
    ++in_flight;

    std::cout << "UNLINK oid (" << to_remove << ") in_flight "
	      << in_flight << std::endl;

    available_objects.erase(to_remove);
    in_flight_objects.insert(to_remove);
    contents.erase(to_remove);
    return store->queue_transaction(
      t, new C_SyntheticOnReadable(this, t, to_remove));
  }

  void print_internal_state() {
    lock_guard locker(lock);
    cerr << "available_objects: " << available_objects.size()
	 << " in_flight_objects: " << in_flight_objects.size()
	 << " total objects: "
	 << in_flight_objects.size() + available_objects.size()
	 << " in_flight " << in_flight << std::endl;
  }
};

TEST_P(StoreTest, Synthetic) {
  MixedGenerator gen;
  gen_type rng(time(NULL));
  coll_t cid("synthetic_1");

  std::cout << "Synthetic test synthetic_1" << std::endl;

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng);
  ASSERT_EQ(test_obj.init(cid), 0);
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) {
      std::cout << "GEN Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 90) {
      std::cout << "STAT Op " << i << std::endl;
      test_obj.stat();
    } else if (val > 85) {
      std::cout << "UNLINK Op " << i << std::endl;
      test_obj.unlink();
    } else if (val > 50) {
      std::cout << "WRITE Op " << i << std::endl;
      test_obj.write();
    } else if (val > 10) {
      std::cout << "READ Op " << i << std::endl;
      test_obj.read();
    } else {
      std::cout << "TRUNC Op " << i << std::endl;
      test_obj.truncate();
    }
  }
  test_obj.wait_for_done();
}

TEST_P(StoreTest, OMapTest) {
  coll_t cid("blah");
  oid_t hoid("tesomap", chunktype::entirety);
  int r;
  {
    Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  CollectionHandle ch = store->open_collection(cid);
  if (ch == NULL)
    return;

  map<string, bufferlist> attrs;
  {
    Transaction t;
    (void) t.push_col(ch);
    (void) t.push_oid(hoid);
    t.touch();
    t.omap_clear();
    map<string, bufferlist> start_set;
    t.omap_setkeys(start_set);
    store->apply_transaction(t);
  }

  ObjectHandle oh = store->get_object(ch, hoid);

  for (int i = 0; i < 100; i++) {
    if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    }
    Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, oh, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
	 j != attrs.end();
	 ++j) {
      bool correct = cur_attrs.count(j->first) &&
	string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
	std::cout << j->first << " is present in cur_attrs "
		  << cur_attrs.count(j->first) << " times " << std::endl;
	if (cur_attrs.count(j->first) > 0) {
	  std::cout << j->second.c_str() << " : "
		    << cur_attrs[j->first].c_str() << std::endl;
	}
      }
      ASSERT_EQ(correct, true);
    }
    ASSERT_EQ(attrs.size(), cur_attrs.size());

    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    bl.clear();
    bufferptr bp(buf, strlen(buf) + 1);
    bl.append(bp);
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    attrs.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    (void) t.push_col(ch);
    (void) t.push_obj(oh);
    t.omap_setkeys(to_add);
    store->apply_transaction(t);
  }

  int i = 0;
  while (attrs.size()) {
    if (!(i%5)) {
      std::cout << "removal: On iteration " << i << std::endl;
    }
    Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, oh, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
	 j != attrs.end();
	 ++j) {
      bool correct = cur_attrs.count(j->first) &&
	string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
	std::cout << j->first << " is present in cur_attrs "
		  << cur_attrs.count(j->first) << " times " << std::endl;
	if (cur_attrs.count(j->first) > 0) {
	  std::cout << j->second.c_str() << " : "
		    << cur_attrs[j->first].c_str() << std::endl;
	}
      }
      ASSERT_EQ(correct, true);
    }

    string to_remove = attrs.begin()->first;
    set<string> keys_to_remove;
    keys_to_remove.insert(to_remove);
    t.push_col(ch);
    t.push_obj(oh);
    t.omap_rmkeys(keys_to_remove);
    store->apply_transaction(t);

    attrs.erase(to_remove);

    ++i;
  }

  {
    bufferlist bl1;
    bl1.append("omap_header");
    Transaction t;
    (void) t.push_col(ch);
    (void) t.push_obj(oh);
    t.omap_setheader(bl1);
    store->apply_transaction(t);

    {
      bufferlist bl2;
      bl2.append("value");
      Transaction t;
      map<string, bufferlist> to_add;
      to_add.insert(pair<string, bufferlist>("key", bl2));
      (void) t.push_col(ch);
      (void) t.push_obj(oh);
      t.omap_setkeys(to_add);
      store->apply_transaction(t);

      bufferlist bl3;
      map<string, bufferlist> cur_attrs;
      r = store->omap_get(ch, oh, &bl3, &cur_attrs);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(cur_attrs.size(), size_t(1));
      ASSERT_TRUE(bl3.contents_equal(bl1));
    }
  }

  store->put_object(oh);
  store->close_collection(ch);

  Transaction t;
  (void) t.push_cid(cid);
  (void) t.push_oid(hoid);
  t.remove();
  t.remove_collection(cid);
  store->apply_transaction(t);
}

TEST_P(StoreTest, XattrTest) {
  coll_t cid("blah");
  oid_t hoid("tesomap", chunktype::entirety);
  bufferlist big;
  for (unsigned i = 0; i < 10000; ++i) {
    big.append('\0');
  }
  bufferlist small;
  for (unsigned i = 0; i < 10; ++i) {
    small.append('\0');
  }
  int r;
  {
    Transaction t;
    t.create_collection(cid); // pushes slot
    t.push_oid(hoid);
    t.touch();
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    Transaction t;
    t.push_cid(cid);
    t.push_oid(hoid);
    t.setattr("attr1", small);
    attrs["attr1"] = small;
    t.setattr("attr2", big);
    attrs["attr2"] = big;
    t.setattr("attr3", small);
    attrs["attr3"] = small;
    t.setattr("attr1", small);
    attrs["attr1"] = small;
    t.setattr("attr4", big);
    attrs["attr4"] = big;
    t.setattr("attr3", big);
    attrs["attr3"] = big;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  CollectionHandle ch = store->open_collection(cid);
  ObjectHandle oh = store->get_object(ch, hoid);

  map<string, bufferptr> aset;
  store->getattrs(ch, oh, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  {
    Transaction t;
    t.push_col(ch);
    t.push_obj(oh);
    t.rmattr("attr2");
    attrs.erase("attr2");
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  aset.clear();
  store->getattrs(ch, oh, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  bufferptr bp;
  r = store->getattr(ch, oh, "attr2", bp);
  ASSERT_EQ(r, -ENODATA);

  r = store->getattr(ch, oh, "attr3", bp);
  ASSERT_GE(r, 0);
  bufferlist bl2;
  bl2.push_back(bp);
  ASSERT_TRUE(bl2 == attrs["attr3"]);

  store->put_object(oh);
  store->close_collection(ch);

  {
    Transaction t;
    (void)  t.remove_collection(cid);
    store->apply_transaction(t);
  }
} /* TEST_P(StoreTest, OMapTest) */

void colsplittest(
  ObjectStore *store,
  unsigned num_objects,
  unsigned common_suffix_size
  ) {
  coll_t cid("from");
  coll_t tid("to");
  int r = 0;
  {
    Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    Transaction t;
    t.push_cid(cid);
    for (uint32_t i = 0; i < 2*num_objects; ++i) {
      stringstream objname;
      objname << "obj" << i;
      t.push_oid(hoid_t(oid_t(objname.str(), chunktype::data,
			      i << common_suffix_size)));
      t.touch();
    }
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  Transaction t;
  vector<hoid_t> objects;

  t.remove_collection(cid);
  t.remove_collection(tid);
  r = store->apply_transaction(t);
  ASSERT_EQ(r, 0);
}

TEST_P(StoreTest, ColSplitTest1) {
  colsplittest(store.get(), 10000, 11);
}
TEST_P(StoreTest, ColSplitTest2) {
  colsplittest(store.get(), 100, 7);
}

#if 0
TEST_P(StoreTest, ColSplitTest3) {
  colsplittest(store.get(), 100000, 25);
}

TEST_P(StoreTest, MoveRename) {
  coll_t temp_cid("mytemp");
  oid_t temp_oid("tmp_obj", chunktype::entirety);
  coll_t cid("dest");
  oid_t oid("dest_obj", chunktype::entirety);
  int r;
  {
    Transaction t;
    t.create_collection(cid);
    t.push_oid(hoid_t(oid));
    t.touch();
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  CollectionHandle ch = store->open_collection(cid);

  ASSERT_TRUE(store->exists(ch, oid));
  bufferlist data, attr;
  map<string, bufferlist> omap;
  data.append("data payload");
  attr.append("attr value");
  omap["omap_key"].append("omap value");
  {
    Transaction t;
    t.create_collection(temp_cid);
    t.push_oid(hoid_t(temp_oid));
    t.touch();
    t.write(0, data.length(), data);
    t.setattr("attr", attr);
    t.omap_setkeys(omap);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  CollectionHandle tch = store->open_collection(temp_cid);
  ASSERT_TRUE(store->exists(tch, temp_oid));
  {
    Transaction t;
    uint16_t c1_ix, c2_ix, o1_ix, o2_ix;
    c1_ix = t.push_col(ch);
    o1_ix = t.push_oid(hoid_t(oid));
    t.remove(c1_ix, o1_ix);
    c2_ix = t.push_col(tch);
    o2_ix = t.push_oid(hoid_t(temp_oid));
    // not usual order of arguments, wrt "old" and "new"
    t.collection_move_rename(c2_ix, o2_ix, c1_ix, o1_ix);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, oid));
  ASSERT_FALSE(store->exists(tch, temp_oid));
  ObjectHandle oh = store->get_object(ch, hoid_t(oid));
  {
    bufferlist newdata;
    r = store->read(ch, oh, 0, 1000, newdata);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(newdata.contents_equal(data));
    bufferlist newattr;
    r = store->getattr(ch, oh, "attr", newattr);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(newattr.contents_equal(attr));
    set<string> keys;
    keys.insert("omap_key");
    map<string, bufferlist> newomap;
    r = store->omap_get_values(ch, oh, keys, &newomap);
    ASSERT_GE(r, 0);
    ASSERT_EQ(1u, newomap.size());
    ASSERT_TRUE(newomap.count("omap_key"));
    ASSERT_TRUE(newomap["omap_key"].contents_equal(omap["omap_key"]));
  }
  store->put_object(oh);
  store->close_collection(ch);
  store->close_collection(tch);
}
#endif

INSTANTIATE_TEST_CASE_P(
  ObjectStore,
  StoreTest,
  ::testing::Values("filestore", "keyvaluestore-dev"));

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif


//
// support tests for qa/workunits/filestore/filestore.sh
//
TEST(EXT4StoreTest, _detect_fs) {
  if (::getenv("DISK") == NULL || ::getenv("MOUNTPOINT") == NULL) {
    cerr << "SKIP because DISK and MOUNTPOINT environment variables are not set. It is meant to run from qa/workunits/filestore/filestore.sh " << std::endl;
    return;
  }
  const string disk(::getenv("DISK"));
  EXPECT_LT((unsigned)0, disk.size());
  const string mnt(::getenv("MOUNTPOINT"));
  EXPECT_LT((unsigned)0, mnt.size());
  ::umount(mnt.c_str());

  const string dir("store_test_temp_dir");
  const string journal("store_test_temp_journal");

  //
  // without user_xattr, ext4 fails
  //
  {
    cct->_conf->set_val("filestore_xattr_use_omap", "true");
    EXPECT_EQ(::system((string("mount -o loop,nouser_xattr ") + disk + " " + mnt).c_str()), 0);
    EXPECT_EQ(::chdir(mnt.c_str()), 0);
    EXPECT_EQ(::mkdir(dir.c_str(), 0755), 0);
    FileStore store(cct, dir, journal);
    EXPECT_EQ(store._detect_fs(), -ENOTSUP);
    EXPECT_EQ(::chdir(".."), 0);
    EXPECT_EQ(::umount(mnt.c_str()), 0);
  }
  //
  // mounted with user_xattr, ext4 fails if filestore_xattr_use_omap is false
  //
  {
    cct->_conf->set_val("filestore_xattr_use_omap", "false");
    EXPECT_EQ(::system((string("mount -o loop,user_xattr ") + disk + " " + mnt).c_str()), 0);
    EXPECT_EQ(::chdir(mnt.c_str()), 0);
    FileStore store(cct, dir, journal);
    EXPECT_EQ(store._detect_fs(), -ENOTSUP);
    EXPECT_EQ(::chdir(".."), 0);
    EXPECT_EQ(::umount(mnt.c_str()), 0);
  }
  //
  // mounted with user_xattr, ext4 succeeds if filestore_xattr_use_omap is true
  //
  {
    cct->_conf->set_val("filestore_xattr_use_omap", "true");
    EXPECT_EQ(::system((string("mount -o loop,user_xattr ") + disk + " " + mnt).c_str()), 0);
    EXPECT_EQ(::chdir(mnt.c_str()), 0);
    FileStore store(cct, dir, journal);
    EXPECT_EQ(store._detect_fs(), 0);
    EXPECT_EQ(::chdir(".."), 0);
    EXPECT_EQ(::umount(mnt.c_str()), 0);
  }
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);
  cct->_conf->set_val("osd_journal_size", "400");
  cct->_conf->set_val("filestore_index_retry_probability", "0.5");
  cct->_conf->set_val("filestore_op_thread_timeout", "1000");
  cct->_conf->set_val("filestore_op_thread_suicide_timeout", "10000");
  cct->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Local Variables:
// compile-command: "cd ../.. ; make ceph_test_objectstore ; ./ceph_test_objectstore --gtest_filter=StoreTest.* --log-to-stderr=true --debug-filestore=20"
// End:
