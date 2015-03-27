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
#include <cassert>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include <sstream>
#include "os/ObjectStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>

#include "DeterministicOpSequence.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "deterministic_seq "

DeterministicOpSequence::DeterministicOpSequence(ObjectStore *store,
						 std::string status)
  : TestObjectStoreState(store), txn(0)
{
  txn_coll = coll_t("meta");
  txn_object = oid_t("txn");

  if (!status.empty())
    m_status.open(status.c_str());
}

DeterministicOpSequence::~DeterministicOpSequence()
{
  // TODO Auto-generated destructor stub
}

bool DeterministicOpSequence::run_one_op(int op, rngen_t& gen)
{
  bool ok = false;
  switch (op) {
  case DSOP_TOUCH:
    ok = do_touch(gen);
    break;
  case DSOP_WRITE:
    ok = do_write(gen);
    break;
  case DSOP_CLONE:
    ok = do_clone(gen);
    break;
  case DSOP_CLONE_RANGE:
    ok = do_clone_range(gen);
    break;
  case DSOP_OBJ_REMOVE:
    ok = do_remove(gen);
    break;
  case DSOP_COLL_ADD:
    ok = do_coll_add(gen);
    break;
  case DSOP_COLL_RENAME:
    //do_coll_rename(gen);
    break;
  case DSOP_SET_ATTRS:
    ok = do_set_attrs(gen);
    break;
  default:
    assert(0 == "bad op");
  }
  return ok;
}

void DeterministicOpSequence::generate(int seed, int num_txs)
{
  std::ostringstream ss;
  ss << "generate run " << num_txs << " --seed " << seed;

  if (m_status.is_open()) {
    m_status << ss.str() << std::endl;
    m_status.flush();
  }

  dout(0) << ss.str() << dendl;

  rngen_t gen(seed);
  boost::uniform_int<> op_rng(DSOP_FIRST, DSOP_LAST);

  for (txn = 1; txn <= num_txs; ) {
    int op = op_rng(gen);
    _print_status(txn, op);
    dout(0) << "generate seq " << txn << " op " << op << dendl;
    if (run_one_op(op, gen))
      txn++;
  }
}

void DeterministicOpSequence::_print_status(int seq, int op)
{
  if (!m_status.is_open())
    return;
  m_status << seq << " " << op << std::endl;
  m_status.flush();
}

int DeterministicOpSequence::_gen_coll_id(rngen_t& gen)
{
  boost::uniform_int<> coll_rng(0, m_collections_ids.size()-1);
  return coll_rng(gen);
}

int DeterministicOpSequence::_gen_obj_id(rngen_t& gen)
{
  boost::uniform_int<> obj_rng(0, m_num_objects - 1);
  return obj_rng(gen);
}

void DeterministicOpSequence::note_txn(Transaction *t)
{
  bufferlist bl;
  ::encode(txn, bl);
  (void) t->push_cid(txn_coll);
  (void) t->push_oid(txn_object);
  t->truncate(0);
  t->write(0, bl.length(), bl);
  dout(10) << __func__ << " " << txn << dendl;
}

bool DeterministicOpSequence::do_touch(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);
  int obj_id = _gen_obj_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  assert(entry != NULL);

  // Don't care about other collections if already exists
  if (!entry->check_for_obj(obj_id)) {
    bool other_found = false;
    map<int, coll_entry_t*>::iterator it = m_collections.begin();
    for (; it != m_collections.end(); ++it) {
      if (it->second->check_for_obj(obj_id)) {
	assert(it->first != coll_id);
	other_found = true;
      }
    }
    if (other_found) {
      dout(0) << "do_touch new object in collection and exists in another"
	      << dendl;
      return false;
    }
  }
  oid_t *oid = entry->touch_obj(obj_id);

  dout(0) << "do_touch " << entry->m_coll.to_str() << "/" << oid->name
	  << dendl;

  _do_touch(entry->m_coll, *oid);
  return true;
}

bool DeterministicOpSequence::do_remove(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  assert(entry != NULL);

  if (entry->m_objects.size() == 0) {
    dout(0) << "do_remove no objects in collection" << dendl;
    return false;
  }
  int obj_id = entry->get_random_obj_id(gen);
  oid_t *oid = entry->touch_obj(obj_id);
  assert(oid);

  dout(0) << "do_remove " << entry->m_coll.to_str() << "/" << oid->name << dendl;

  _do_remove(entry->m_coll, *oid);
  oid_t *rmobj = entry->remove_obj(obj_id);
  assert(rmobj);
  delete rmobj;
  return true;
}

static void _gen_random(rngen_t& gen,
			size_t size, bufferlist& bl) {

  static const char alphanum[] = "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

  boost::uniform_int<> char_rng(0, sizeof(alphanum));
  bufferptr bp(size);
  for (unsigned int i = 0; i < size - 1; i++) {
    bp[i] = alphanum[char_rng(gen)];
  }
  bp[size - 1] = '\0';
  bl.append(bp);
}

static void gen_attrs(rngen_t &gen,
		      map<string, bufferlist> *out) {
  boost::uniform_int<> num_rng(10, 30);
  boost::uniform_int<> key_size_rng(5, 10);
  boost::uniform_int<> val_size_rng(100, 1000);
  size_t num_attrs = static_cast<size_t>(num_rng(gen));
  for (size_t i = 0; i < num_attrs; ++i) {
    size_t key_size = static_cast<size_t>(num_rng(gen));
    size_t val_size = static_cast<size_t>(num_rng(gen));
    bufferlist keybl;
    _gen_random(gen, key_size, keybl);
    string key(keybl.c_str(), keybl.length());
    _gen_random(gen, val_size, (*out)[key]);
  }
}

bool DeterministicOpSequence::do_set_attrs(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  assert(entry != NULL);

  if (entry->m_objects.size() == 0) {
    dout(0) << "do_set_attrs no objects in collection" << dendl;
    return false;
  }
  int obj_id = entry->get_random_obj_id(gen);
  oid_t *oid = entry->touch_obj(obj_id);
  assert(oid);

  map<string, bufferlist> out;
  gen_attrs(gen, &out);

  dout(0) << "do_set_attrs " << out.size() << " entries" << dendl;
  _do_set_attrs(entry->m_coll, *oid, out);
  return true;
}

bool DeterministicOpSequence::do_write(rngen_t& gen)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  assert(entry != NULL);

  if (entry->m_objects.size() == 0) {
    dout(0) << "do_write no objects in collection" << dendl;
    return false;
  }
  int obj_id = entry->get_random_obj_id(gen);
  oid_t *oid = entry->touch_obj(obj_id);
  assert(oid);

  boost::uniform_int<> size_rng(100, (2 << 19));
  size_t size = (size_t) size_rng(gen);
  bufferlist bl;
  _gen_random(gen, size, bl);

  dout(0) << "do_write " << entry->m_coll.to_str() << "/" << oid->name
	  << " 0~" << size << dendl;

  _do_write(entry->m_coll, *oid, 0, bl.length(), bl);
  return true;
}

bool DeterministicOpSequence::_prepare_clone(rngen_t& gen,
					     coll_t& coll_ret,
					     oid_t& orig_obj_ret,
					     oid_t& new_obj_ret)
{
  int coll_id = _gen_coll_id(gen);

  coll_entry_t *entry = get_coll_at(coll_id);
  assert(entry != NULL);

  if (entry->m_objects.size() >= 2) {
    dout(0) << "_prepare_clone coll " << entry->m_coll.to_str()
	    << " doesn't have 2 or more objects" << dendl;
    return false;
  }

  int orig_obj_id = entry->get_random_obj_id(gen);
  oid_t *orig_obj = entry->touch_obj(orig_obj_id);
  assert(orig_obj);

  int id;
  do {
    id = entry->get_random_obj_id(gen);
  } while (id == orig_obj_id);
  oid_t *new_obj = entry->touch_obj(id);
  assert(new_obj);

  coll_ret = entry->m_coll;
  orig_obj_ret = *orig_obj;
  new_obj_ret = *new_obj;

  return true;
}

bool DeterministicOpSequence::do_clone(rngen_t& gen)
{
  coll_t coll;
  oid_t orig_obj, new_obj;
  if (!_prepare_clone(gen, coll, orig_obj, new_obj)) {
    return false;
  }

  dout(0) << "do_clone " << coll.to_str() << "/" << orig_obj.name
      << " => " << coll.to_str() << "/" << new_obj.name << dendl;

  _do_clone(coll, orig_obj, new_obj);
  return true;
}

bool DeterministicOpSequence::do_clone_range(rngen_t& gen)
{
  coll_t coll;
  oid_t orig_obj, new_obj;
  if (!_prepare_clone(gen, coll, orig_obj, new_obj)) {
    return false;
  }

  /* Whenever we have to make a clone_range() operation, just write to the
   * object first, so we know we have something to clone in the said range.
   * This may not be the best solution ever, but currently we're not keeping
   * track of the written-to objects, and until we know for sure we really
   * need to, let's just focus on the task at hand.
   */

  boost::uniform_int<> write_size_rng(100, (2 << 19));
  size_t size = (size_t) write_size_rng(gen);
  bufferlist bl;
  _gen_random(gen, size, bl);

  boost::uniform_int<> clone_len(1, bl.length());
  size = (size_t) clone_len(gen);

  dout(0) << "do_clone_range " << coll.to_str() << "/" << orig_obj.name
      << " (0~" << size << ")"
      << " => " << coll.to_str() << "/" << new_obj.name
      << " (0)" << dendl;
  _do_write_and_clone_range(coll, orig_obj, new_obj, 0, size, 0, bl);
  return true;
}

bool DeterministicOpSequence::_prepare_colls(rngen_t& gen,
					     coll_entry_t* &orig_coll,
					     coll_entry_t* &new_coll)
{
  assert(m_collections_ids.size() > 1);
  int orig_coll_id = _gen_coll_id(gen);
  int new_coll_id;
  do {
    new_coll_id = _gen_coll_id(gen);
  } while (new_coll_id == orig_coll_id);

  dout(0) << "_prepare_colls from coll id " << orig_coll_id
      << " to coll id " << new_coll_id << dendl;

  orig_coll = get_coll_at(orig_coll_id);
  assert(orig_coll != NULL);
  new_coll = get_coll_at(new_coll_id);
  assert(new_coll != NULL);

  if (!orig_coll->m_objects.size()) {
    dout(0) << "_prepare_colls coll " << orig_coll->m_coll.to_str()
	<< " has no objects to use" << dendl;
    return false;
  }

  return true;
}

bool DeterministicOpSequence::do_coll_rename(rngen_t& gen)
{
  int coll_pos = _gen_coll_id(gen);
  dout(0) << "do_coll_rename coll pos #" << coll_pos << dendl;

  coll_entry_t *coll_entry = get_coll_at(coll_pos);
  if (!coll_entry) {
    dout(0) << "do_coll_rename no collection at pos #" << coll_pos << dendl;
    return false;
  }

  coll_t orig_coll = coll_entry->m_coll;
  char buf[100];
  memset(buf, 0, 100);
  snprintf(buf, 100, "0.%d_head", m_next_coll_nr++);
  coll_t new_coll(buf);
  coll_entry->m_coll = new_coll;

  dout(0) << "do_coll_rename " << orig_coll.to_str()
      << " => " << new_coll.to_str() << dendl;
  _do_coll_rename(orig_coll, new_coll);
  return true;
}

bool DeterministicOpSequence::do_coll_add(rngen_t& gen)
{
  coll_entry_t *orig_coll = NULL, *new_coll = NULL;
  if (!_prepare_colls(gen, orig_coll, new_coll))
    return false;

  assert(orig_coll && new_coll);

  boost::uniform_int<> obj_rng(0, orig_coll->m_objects.size()-1);
  int obj_pos = obj_rng(gen);
  int obj_key = -1;
  oid_t *oid = orig_coll->get_obj_at(obj_pos, &obj_key);
  if (!oid) {
    dout(0) << "do_coll_add coll " << orig_coll->m_coll.to_str()
	    << " has no object as pos #" << obj_pos << " (key " << obj_key
	    << ")" << dendl;
    return false;
  }
  if (new_coll->check_for_obj(obj_key)) {
    dout(0) << "do_coll_add coll " << orig_coll->m_coll.to_str()
	<< " already has object as pos #" << obj_pos << " (key " << obj_key
	    << ")" << dendl;
    return false;
  }
  dout(0) << "do_coll_add " << orig_coll->m_coll.to_str() << "/" << oid->name
	<< " => " << new_coll->m_coll.to_str() << "/" << oid->name << dendl;
  new_coll->touch_obj(obj_key);

  _do_coll_add(orig_coll->m_coll, new_coll->m_coll, *oid);
  return true;
}

void DeterministicOpSequence::_do_touch(coll_t coll, oid_t& oid)
{
  Transaction t;
  note_txn(&t);
  uint16_t c_ix = t.push_cid(coll);
  uint16_t o_ix =  t.push_oid(hoid_t(oid));
  t.touch(c_ix, o_ix);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_remove(coll_t coll, oid_t& oid)
{
  Transaction t;
  note_txn(&t);
  (void) t.push_cid(coll);
  (void) t.push_oid(oid);
  t.remove();
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_set_attrs(
  coll_t coll,
  oid_t& oid,
  const map<string, bufferlist>& attrs)
{
  Transaction t;
  note_txn(&t);
  (void) t.push_cid(coll);
  (void) t.push_oid(hoid_t(oid));
  t.omap_setkeys(attrs);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_write(coll_t coll, oid_t& oid,
					uint64_t off, uint64_t len,
					const bufferlist& data)
{
  Transaction t;
  note_txn(&t);
  (void) t.push_cid(coll);
  (void) t.push_oid(hoid_t(oid));
  t.write(off, len, data);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_clone(coll_t coll, oid_t& orig_obj,
					oid_t& new_obj)
{
  Transaction t;
  note_txn(&t);
  uint16_t c_ix = t.push_cid(coll);
  uint16_t o1_ix = t.push_oid(orig_obj);
  uint16_t o2_ix = t.push_oid(new_obj);
  t.clone(c_ix, o1_ix, o2_ix);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_clone_range(coll_t coll,
					      oid_t& orig_oid,
					      oid_t& new_oid,
					      uint64_t srcoff,
					      uint64_t srclen,
					      uint64_t dstoff)
{
  Transaction t;
  note_txn(&t);
  uint16_t c_ix = t.push_cid(coll);
  uint16_t o1_ix = t.push_oid(hoid_t(orig_oid));
  uint16_t o2_ix = t.push_oid(hoid_t(new_oid));
  t.clone_range(c_ix, o1_ix, o2_ix, srcoff, srclen, dstoff);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_write_and_clone_range(coll_t coll,
							oid_t& orig_oid,
							oid_t& new_oid,
							uint64_t srcoff,
							uint64_t srclen,
							uint64_t dstoff,
							bufferlist& bl)
{
  Transaction t;
  note_txn(&t);
  uint16_t c_ix = t.push_cid(coll);
  uint16_t o1_ix = t.push_oid(hoid_t(orig_oid));
  uint16_t o2_ix = t.push_oid(hoid_t(new_oid));
  t.write(c_ix, o1_ix, srcoff, bl.length(), bl);
  t.clone_range(c_ix, o1_ix, o2_ix, srcoff, srclen, dstoff);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_coll_add(coll_t orig_coll,
					   coll_t new_coll,
					   oid_t& oid)
{
  Transaction t;
  note_txn(&t);
  uint16_t c1_ix = t.push_cid(orig_coll);
  uint16_t o1_ix = t.push_oid(hoid_t(oid));
  uint16_t c2_ix = t.push_cid(new_coll);
  t.remove(c2_ix, o1_ix);
  t.collection_add(c2_ix, c1_ix, o1_ix);
  m_store->apply_transaction(t);
}

void DeterministicOpSequence::_do_coll_rename(coll_t orig_coll,
					      coll_t new_coll)
{
  Transaction t;
  note_txn(&t);
  uint16_t c1_ix = t.push_cid(orig_coll);
  uint16_t c2_ix = t.push_cid(new_coll);
  t.collection_rename(c1_ix, c2_ix);
  m_store->apply_transaction(t);
}
