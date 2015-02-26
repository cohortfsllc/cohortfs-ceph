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
#include "os/FileStore.h"
#include "os/KeyValueStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/errno.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

#include <unordered_map>
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
      cerr << __func__ << ": unable to create store_test_temp_dir" << ": " << cpp_strerror(r) << std::endl;
      return;
    }

    ObjectStore *store_ = ObjectStore::create(cct,
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

bool sorted(const vector<oid_t> &in) {
  oid_t start;
  for (vector<oid_t>::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (start > *i) return false;
    start = *i;
  }
  return true;
}

TEST_P(StoreTest, SimpleColTest) {
  coll_t cid = coll_t("initial");
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "create collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "add collection" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
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
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  oid_t hoid("Object 1");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
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
    ObjectStore::Transaction t;
    t.create_collection(cid);
    cerr << "Creating collection " << cid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  oid_t hoid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaObjectaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}

TEST_P(StoreTest, ManyObjectTest) {
  int NUM_OBJS = 2000;
  int r = 0;
  coll_t cid("blah");
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<oid_t> created;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  for (int i = 0; i < NUM_OBJS; ++i) {
    if (!(i % 5)) {
      cerr << "Object " << i << std::endl;
    }
    ObjectStore::Transaction t;
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    oid_t hoid(string(buf) + base);
    t.touch(cid, hoid);
    created.insert(hoid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  for (set<oid_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    struct stat buf;
    ASSERT_TRUE(!store->stat(cid, *i, &buf));
  }

  set<oid_t> listed;
  vector<oid_t> objects;
  r = store->collection_list(cid, objects);
  ASSERT_EQ(r, 0);

  cerr << "objects.size() is " << objects.size() << std::endl;
  for (vector<oid_t> ::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    listed.insert(*i);
    ASSERT_TRUE(created.count(*i));
  }
  ASSERT_TRUE(listed.size() == created.size());

  oid_t start, next;
  objects.clear();
  r = store->collection_list_partial(
    cid,
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
    r = store->collection_list_partial(cid,
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
  for (set<oid_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<oid_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  cerr << "cleaning up" << std::endl;
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
}


class ObjectGenerator {
public:
  virtual oid_t create_object(gen_type *gen) = 0;
  virtual ~ObjectGenerator() {}
};

class MixedGenerator : public ObjectGenerator {
public:
  unsigned seq;
  MixedGenerator() : seq(0) {}
  oid_t create_object(gen_type *gen) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%u", seq);

    boost::uniform_int<> true_false(0, 1);
    string name(buf);
    if (true_false(*gen)) {
      // long
      for (int i = 0; i < 100; ++i) name.append("aaaaa");
    } else if (true_false(*gen)) {
      name = "DIR_" + name;
    }

    // hash
    //boost::binomial_distribution<uint32_t> bin(0xFFFFFF, 0.5);
    ++seq;
    return oid_t(name, chunktype::data, rand() & 0xFF);
  }
};

class SyntheticWorkloadState {
public:
  static const unsigned max_in_flight = 16;
  static const unsigned max_objects = 3000;
  static const unsigned max_object_len = 1024 * 20;
  coll_t cid;
  unsigned in_flight;
  map<oid_t, bufferlist> contents;
  set<oid_t> available_objects;
  set<oid_t> in_flight_objects;
  ObjectGenerator *object_gen;
  gen_type *rng;
  ObjectStore *store;
  ObjectStore::Sequencer *osr;

  std::mutex lock;
  std::condition_variable cond;

  class C_SyntheticOnReadable : public Context {
  public:
    SyntheticWorkloadState *state;
    ObjectStore::Transaction *t;
    oid_t hoid;
    C_SyntheticOnReadable(SyntheticWorkloadState *state,
			  ObjectStore::Transaction *t, oid_t hoid)
      : state(state), t(t), hoid(hoid) {}

    void finish(int r) {
      lock_guard locker(state->lock);
      ASSERT_TRUE(state->in_flight_objects.count(hoid));
      ASSERT_EQ(r, 0);
      state->in_flight_objects.erase(hoid);
      if (state->contents.count(hoid))
	state->available_objects.insert(hoid);
      --(state->in_flight);
      state->cond.notify_all();
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
			 gen_type *rng,
			 ObjectStore::Sequencer *osr,
			 coll_t cid)
    : cid(cid), in_flight(0), object_gen(gen), rng(rng), store(store),
      osr(osr) {}

  int init() {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    return store->apply_transaction(t);
  }

  oid_t get_uniform_random_object(unique_lock& l) {
    while (in_flight >= max_in_flight || available_objects.empty())
      cond.wait(l);
    boost::uniform_int<> choose(0, available_objects.size() - 1);
    int index = choose(*rng);
    set<oid_t>::iterator i = available_objects.begin();
    for ( ; index > 0; --index, ++i) ;
    oid_t ret = *i;
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
    oid_t new_obj = object_gen->create_object(rng);
    available_objects.erase(new_obj);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->touch(cid, new_obj);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    if (!contents.count(new_obj))
      contents[new_obj] = bufferlist();
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, new_obj));
  }

  int write() {
    unique_lock l(lock);
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready(l);

    oid_t new_obj = get_uniform_random_object(l);
    available_objects.erase(new_obj);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;

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
	contents[new_obj].copy(value.length(), contents[new_obj].length()-value.length(), value);
      value.swap(contents[new_obj]);
    }

    t->write(cid, new_obj, offset, len, bl);
    ++in_flight;
    in_flight_objects.insert(new_obj);
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, new_obj));
  }

  void read() {
    boost::uniform_int<> u1(0, max_object_len/2);
    boost::uniform_int<> u2(0, max_object_len);
    uint64_t offset = u1(*rng);
    uint64_t len = u2(*rng);
    if (offset > len)
      std::swap(offset, len);

    oid_t oid;
    int r;
    {
      unique_lock l(lock);
      if (!can_unlink())
	return ;
      wait_for_ready(l);

      oid = get_uniform_random_object(l);
    }
    bufferlist bl, result;
    r = store->read(cid, oid, offset, len, result);
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
  }

  int truncate() {
    unique_lock l(lock);
    if (!can_unlink())
      return -ENOENT;
    wait_for_ready(l);

    oid_t oid = get_uniform_random_object(l);
    available_objects.erase(oid);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;

    boost::uniform_int<> choose(0, max_object_len);
    size_t len = choose(*rng);
    bufferlist bl;

    t->truncate(cid, oid, len);
    ++in_flight;
    in_flight_objects.insert(oid);
    if (contents[oid].length() <= len)
      contents[oid].append_zero(len - contents[oid].length());
    else {
      contents[oid].copy(0, len, bl);
      bl.swap(contents[oid]);
    }

    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(this, t, oid));
  }

  void scan() {
    unique_lock l(lock);
    cond.wait(l, [&](){ return in_flight; });
    vector<oid_t> objects;
    set<oid_t> objects_set, objects_set2;
    oid_t next, current;
    while (1) {
      cerr << "scanning..." << std::endl;
      int r = store->collection_list_partial(cid, current, 50, 100,
					     &objects, &next);
      ASSERT_EQ(r, 0);
      ASSERT_TRUE(sorted(objects));
      objects_set.insert(objects.begin(), objects.end());
      objects.clear();
      if (objects.empty()) break;
      current = next;
    }
    ASSERT_EQ(objects_set.size(), available_objects.size());
    for (set<oid_t>::iterator i = objects_set.begin();
	 i != objects_set.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }

    int r = store->collection_list(cid, objects);
    ASSERT_EQ(r, 0);
    objects_set2.insert(objects.begin(), objects.end());
    ASSERT_EQ(objects_set2.size(), available_objects.size());
    for (set<oid_t>::iterator i = objects_set2.begin();
	 i != objects_set2.end();
	 ++i) {
      ASSERT_GT(available_objects.count(*i), (unsigned)0);
    }
  }

  void stat() {
    oid_t hoid;
    {
      unique_lock l(lock);
      if (!can_unlink())
	return ;
      hoid = get_uniform_random_object(l);
      in_flight_objects.insert(hoid);
      available_objects.erase(hoid);
      ++in_flight;
    }
    struct stat buf;
    int r = store->stat(cid, hoid, &buf);
    ASSERT_EQ(0, r);
    ASSERT_TRUE(buf.st_size == contents[hoid].length());
    {
      lock_guard locker(lock);
      --in_flight;
      cond.notify_all();
      in_flight_objects.erase(hoid);
      available_objects.insert(hoid);
    }
  }

  int unlink() {
    unique_lock l(lock);
    if (!can_unlink())
      return -ENOENT;
    oid_t to_remove = get_uniform_random_object(l);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->remove(cid, to_remove);
    ++in_flight;
    available_objects.erase(to_remove);
    in_flight_objects.insert(to_remove);
    contents.erase(to_remove);
    return store->queue_transaction(osr, t, new C_SyntheticOnReadable(
				      this, t, to_remove));
  }

  void print_internal_state() {
    lock_guard locker(lock);
    cerr << "available_objects: " << available_objects.size()
	 << " in_flight_objects: " << in_flight_objects.size()
	 << " total objects: " << in_flight_objects.size()
      + available_objects.size()
	 << " in_flight " << in_flight << std::endl;
  }
};

TEST_P(StoreTest, Synthetic) {
  ObjectStore::Sequencer osr("test");
  MixedGenerator gen;
  gen_type rng(time(NULL));
  coll_t cid("synthetic_1");

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, &osr, cid);
  test_obj.init();
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 97) {
      test_obj.scan();
    } else if (val > 90) {
      test_obj.stat();
    } else if (val > 85) {
      test_obj.unlink();
    } else if (val > 50) {
      test_obj.write();
    } else if (val > 10) {
      test_obj.read();
    } else {
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
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    store->apply_transaction(t);
  }

  for (int i = 0; i < 100; i++) {
    if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
	 j != attrs.end();
	 ++j) {
      bool correct = cur_attrs.count(j->first) && string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
	std::cout << j->first << " is present in cur_attrs " << cur_attrs.count(j->first) << " times " << std::endl;
	if (cur_attrs.count(j->first) > 0) {
	  std::cout << j->second.c_str() << " : " << cur_attrs[j->first].c_str() << std::endl;
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
    t.omap_setkeys(cid, hoid, to_add);
    store->apply_transaction(t);
  }

  int i = 0;
  while (attrs.size()) {
    if (!(i%5)) {
      std::cout << "removal: On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
	 j != attrs.end();
	 ++j) {
      bool correct = cur_attrs.count(j->first) && string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
	std::cout << j->first << " is present in cur_attrs " << cur_attrs.count(j->first) << " times " << std::endl;
	if (cur_attrs.count(j->first) > 0) {
	  std::cout << j->second.c_str() << " : " << cur_attrs[j->first].c_str() << std::endl;
	}
      }
      ASSERT_EQ(correct, true);
    }

    string to_remove = attrs.begin()->first;
    set<string> keys_to_remove;
    keys_to_remove.insert(to_remove);
    t.omap_rmkeys(cid, hoid, keys_to_remove);
    store->apply_transaction(t);

    attrs.erase(to_remove);

    ++i;
  }

  {
    bufferlist bl1;
    bl1.append("omap_header");
    ObjectStore::Transaction t;
    t.omap_setheader(cid, hoid, bl1);
    store->apply_transaction(t);

    bufferlist bl2;
    bl2.append("value");
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key", bl2));
    t.omap_setkeys(cid, hoid, to_add);
    store->apply_transaction(t);

    bufferlist bl3;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(cid, hoid, &bl3, &cur_attrs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(cur_attrs.size(), size_t(1));
    ASSERT_TRUE(bl3.contents_equal(bl1));
  }

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
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
    ObjectStore::Transaction t;
    t.create_collection(cid);
    t.touch(cid, hoid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.setattr(cid, hoid, "attr1", small);
    attrs["attr1"] = small;
    t.setattr(cid, hoid, "attr2", big);
    attrs["attr2"] = big;
    t.setattr(cid, hoid, "attr3", small);
    attrs["attr3"] = small;
    t.setattr(cid, hoid, "attr1", small);
    attrs["attr1"] = small;
    t.setattr(cid, hoid, "attr4", big);
    attrs["attr4"] = big;
    t.setattr(cid, hoid, "attr3", big);
    attrs["attr3"] = big;
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  map<string, bufferptr> aset;
  store->getattrs(cid, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  {
    ObjectStore::Transaction t;
    t.rmattr(cid, hoid, "attr2");
    attrs.erase("attr2");
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  aset.clear();
  store->getattrs(cid, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  bufferptr bp;
  r = store->getattr(cid, hoid, "attr2", bp);
  ASSERT_EQ(r, -ENODATA);

  r = store->getattr(cid, hoid, "attr3", bp);
  ASSERT_GE(r, 0);
  bufferlist bl2;
  bl2.push_back(bp);
  ASSERT_TRUE(bl2 == attrs["attr3"]);
}

void colsplittest(
  ObjectStore *store,
  unsigned num_objects,
  unsigned common_suffix_size
  ) {
  coll_t cid("from");
  coll_t tid("to");
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < 2*num_objects; ++i) {
      stringstream objname;
      objname << "oid" << i;
      t.touch(cid, oid_t(objname.str(), chunktype::data,
		       i << common_suffix_size));
    }
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }

  ObjectStore::Transaction t;
  vector<oid_t> objects;

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
#endif

TEST_P(StoreTest, MoveRename) {
  coll_t temp_cid("mytemp");
  oid_t temp_obj("tmp_obj", chunktype::entirety);
  coll_t cid("dest");
  oid_t oid("dest_obj", chunktype::entirety);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid);
    t.touch(cid, oid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, oid));
  bufferlist data, attr;
  map<string, bufferlist> omap;
  data.append("data payload");
  attr.append("attr value");
  omap["omap_key"].append("omap value");
  {
    ObjectStore::Transaction t;
    t.create_collection(temp_cid);
    t.touch(temp_cid, temp_obj);
    t.write(temp_cid, temp_obj, 0, data.length(), data);
    t.setattr(temp_cid, temp_obj, "attr", attr);
    t.omap_setkeys(temp_cid, temp_obj, omap);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(temp_cid, temp_obj));
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.collection_move_rename(temp_cid, temp_obj, cid, oid);
    r = store->apply_transaction(t);
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(cid, oid));
  ASSERT_FALSE(store->exists(temp_cid, temp_obj));
  {
    bufferlist newdata;
    r = store->read(cid, oid, 0, 1000, newdata);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(newdata.contents_equal(data));
    bufferlist newattr;
    r = store->getattr(cid, oid, "attr", newattr);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(newattr.contents_equal(attr));
    set<string> keys;
    keys.insert("omap_key");
    map<string, bufferlist> newomap;
    r = store->omap_get_values(cid, oid, keys, &newomap);
    ASSERT_GE(r, 0);
    ASSERT_EQ(1u, newomap.size());
    ASSERT_TRUE(newomap.count("omap_key"));
    ASSERT_TRUE(newomap["omap_key"].contents_equal(omap["omap_key"]));
  }
}

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
