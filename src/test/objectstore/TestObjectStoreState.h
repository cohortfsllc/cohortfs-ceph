// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 New Dream Network
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef TEST_OBJECTSTORE_STATE_H_
#define TEST_OBJECTSTORE_STATE_H_

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include "os/ObjectStore.h"

extern CephContext* cct;

typedef boost::mt11213b rngen_t;

class TestObjectStoreState {
public:
  struct coll_entry_t {
    int m_id;
    coll_t m_coll;
    ObjectStore* os;
    oid_t m_meta_obj;
    map<int, oid_t*> m_objects;
    int m_next_object_id;

    coll_entry_t(int i, char *coll_buf, char *meta_obj_buf)
      : m_id(i), m_coll(coll_buf),
	m_meta_obj(oid_t(meta_obj_buf)),
	m_next_object_id(0)
      {}

    ~coll_entry_t();

    oid_t *touch_obj(int id);
    bool check_for_obj(int id);
    oid_t *get_obj(int id);
    oid_t *remove_obj(int id);
    oid_t *get_obj_at(int pos, int *key = NULL);
    oid_t *remove_obj_at(int pos, int *key = NULL);
    oid_t *replace_obj(int id, oid_t *oid);
    int get_random_obj_id(rngen_t& gen);

   private:
    oid_t *get_obj(int id, bool remove);
    oid_t *get_obj_at(int pos, bool remove, int *key = NULL);
  };

  /* kept in upper case for consistency with coll_t's */
  static const coll_t META_COLL;
  static const coll_t TEMP_COLL;

 protected:
  std::shared_ptr<ObjectStore> m_store;
  map<int, coll_entry_t*> m_collections;
  vector<int> m_collections_ids;
  int m_next_coll_nr;
  int m_num_objs_per_coll;
  int m_num_objects;

  int m_max_in_flight;
  std::atomic<int> m_in_flight;
  std::mutex m_finished_lock;
  std::condition_variable m_finished_cond;

  void wait_for_ready() {
    std::unique_lock<std::mutex> mfl(m_finished_lock);
    while ((m_max_in_flight > 0) && (m_in_flight >= m_max_in_flight))
      m_finished_cond.wait(mfl);
  }

  void wait_for_done() {
    std::unique_lock<std::mutex> mfl(m_finished_lock);
    while (m_in_flight)
      m_finished_cond.wait(mfl);
  }

  void set_max_in_flight(int max) {
    m_max_in_flight = max;
  }
  void set_num_objs_per_coll(int val) {
    m_num_objs_per_coll = val;
  }

  coll_entry_t *get_coll(int key, bool erase = false);
  coll_entry_t *get_coll_at(int pos, bool erase = false);

 private:
  static const int m_default_num_colls = 30;

 public:
  TestObjectStoreState(ObjectStore *store) :
    m_next_coll_nr(0), m_num_objs_per_coll(10), m_num_objects(0),
    m_max_in_flight(0) {
    m_in_flight = 0;
    m_store.reset(store);
  }
  ~TestObjectStoreState() {
    map<int, coll_entry_t*>::iterator it = m_collections.begin();
    while (it != m_collections.end()) {
      if (it->second)
	delete it->second;
      m_collections.erase(it++);
    }
  }

  void init(int colls, int objs);
  void init() {
    init(m_default_num_colls, 0);
  }

  int inc_in_flight() {
    return ++m_in_flight;
  }

  int dec_in_flight() {
    return --m_in_flight;
  }

  coll_entry_t *coll_create(int id);

  class C_OnFinished: public Context {
   protected:
    TestObjectStoreState *m_state;
    ObjectStore::Transaction *m_tx;

   public:
    C_OnFinished(TestObjectStoreState *state,
	ObjectStore::Transaction *t) : m_state(state), m_tx(t) { }

    void finish(int r) {
      std::lock_guard<std::mutex> locker(m_state->m_finished_lock);
      m_state->dec_in_flight();
      m_state->m_finished_cond.notify_all();

      delete m_tx;
    }
  };
};

#endif /* TEST_OBJECTSTORE_STATE_H_ */
