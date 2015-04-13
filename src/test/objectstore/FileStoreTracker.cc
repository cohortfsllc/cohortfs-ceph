// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <mutex>
#include "FileStoreTracker.h"
#include <stdlib.h>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include "include/Context.h"

using std::get;

class OnApplied : public Context {
  FileStoreTracker *tracker;
  FileStoreTracker::trans_list in_flight;
  Transaction *t;
public:
  OnApplied(FileStoreTracker *tracker,
	    FileStoreTracker::trans_list& in_flight,
	    Transaction *t)
    : tracker(tracker), in_flight(in_flight), t(t) {}

  void finish(int r) {
    for (FileStoreTracker::trans_list::iterator i =
	   in_flight.begin();
	 i != in_flight.end();
	 ++i) {
      tracker->applied(i->first, i->second);
    }
    delete t;
  }
};

class OnCommitted : public Context {
  FileStoreTracker *tracker;
  FileStoreTracker::trans_list in_flight;
public:
  OnCommitted(FileStoreTracker *tracker,
	      FileStoreTracker::trans_list& in_flight)
    : tracker(tracker), in_flight(in_flight) {}

  void finish(int r) {
    for (FileStoreTracker::trans_list::iterator i =
	   in_flight.begin();
	 i != in_flight.end();
	 ++i) {
      tracker->committed(i->first, i->second);
    }
  }
};

int FileStoreTracker::init()
{
  set<string> to_get;
  to_get.insert("STATUS");
  map<string, bufferlist> got;
  db->get("STATUS", to_get, &got);
  restart_seq = 0;
  if (!got.empty()) {
    bufferlist::iterator bp = got.begin()->second.begin();
    ::decode(restart_seq, bp);
  }
  ++restart_seq;
  KeyValueDB::Transaction t = db->get_transaction();
  got.clear();
  ::encode(restart_seq, got["STATUS"]);
  t->set("STATUS", got);
  db->submit_transaction(t);
  return 0;
}

void FileStoreTracker::submit_transaction(Transaction &t)
{
  FileStoreTracker::trans_list in_flight;
  OutTransaction out;
  out.t = new ::Transaction;
  out.in_flight = &in_flight;
  for (list<Transaction::Op*>::iterator i = t.ops.begin();
       i != t.ops.end();
       ++i) {
    (**i)(this, &out);
  }
  store->queue_transaction(
    out.t,
    new OnApplied(this, in_flight, out.t),
    new OnCommitted(this, in_flight));
}

void FileStoreTracker::write(const obj_desc_t &obj,
			     OutTransaction *out)
{
  lock_guard l(lock);
  std::cerr << "Writing " << obj << std::endl;
  ObjectContents contents = get_current_content(obj);

  uint64_t offset = rand() % (SIZE/2);
  uint64_t len = rand() % (SIZE/2);
  if (!len) len = 10;
  contents.write(rand(), offset, len);

  bufferlist to_write;
  ObjectContents::Iterator iter = contents.get_iterator();
  iter.seek_to(offset);
  for (uint64_t i = offset;
       i < offset + len;
       ++i, ++iter) {
    assert(iter.valid());
    to_write.append(*iter);
  }
  (void) out->t->push_col(get<0>(obj));
  (void) out->t->push_oid(hoid_t(get<1>(obj)));
  out->t->write(offset, len, to_write);
  out->in_flight->push_back(make_pair(obj,
				      set_content(obj, contents)));
}

void FileStoreTracker::remove(const obj_desc_t &obj,
			      OutTransaction *out)
{
  std::cerr << "Deleting " << obj << std::endl;
  lock_guard l(lock);
  ObjectContents old_contents = get_current_content(obj);
  if (!old_contents.exists())
    return;
  (void) out->t->push_col(get<0>(obj));
  (void) out->t->push_oid(hoid_t(get<1>(obj)));
  out->t->remove();
  ObjectContents contents;
  out->in_flight->push_back(trans_elt(obj,
				      set_content(obj, contents)));
}

void FileStoreTracker::clone_range(const obj_desc_t &from,
				   const obj_desc_t &to,
				   OutTransaction *out) {
  lock_guard l(lock);
  std::cerr << "CloningRange " << from << " to " << to << std::endl;
  assert(get<0>(from) == get<0>(to));
  ObjectContents from_contents = get_current_content(from);
  ObjectContents to_contents = get_current_content(to);
  if (!from_contents.exists()) {
    return;
  }
  if (get<1>(from) == get<1>(to)) {
    return;
  }

  uint64_t new_size = from_contents.size();
  interval_set<uint64_t> interval_to_clone;
  uint64_t offset = rand() % (new_size/2);
  uint64_t len = rand() % (new_size/2);
  if (!len) len = 10;
  interval_to_clone.insert(offset, len);
  to_contents.clone_range(from_contents, interval_to_clone);
  uint16_t c_fix, o_fix;
  uint16_t o_tix;
  c_fix = out->t->push_col(get<0>(from));
  o_fix = out->t->push_oid(hoid_t(get<1>(from)));
  o_tix = out->t->push_oid(hoid_t(get<1>(to)));
  out->t->clone_range(c_fix, o_fix, o_tix, offset, len, offset);
  out->in_flight->push_back(trans_elt(to,
				      set_content(to, to_contents)));
}

void FileStoreTracker::clone(const obj_desc_t &from,
			     const obj_desc_t &to,
			     OutTransaction *out) {
  lock_guard l(lock);
  std::cerr << "Cloning " << from << " to " << to << std::endl;
  assert(get<0>(from) == get<0>(to));
  if (get<1>(from) == get<1>(to)) {
    return;
  }
  ObjectContents from_contents = get_current_content(from);
  ObjectContents to_contents = get_current_content(to);
  if (!from_contents.exists()) {
    return;
  }

  uint16_t c_fix, o_fix;
  uint16_t c_tix, o_tix = 0;
  if (to_contents.exists()) {
    c_tix = out->t->push_col(get<0>(to));
    o_tix = out->t->push_oid(hoid_t(get<1>(to)));
    out->t->remove(c_tix, o_tix);
  }
  c_fix = out->t->push_col(get<0>(from));
  o_fix = out->t->push_oid(hoid_t(get<1>(from)));
  out->t->clone(c_fix, o_fix, o_tix);
  out->in_flight->push_back(trans_elt(to, set_content(to, from_contents)));
}

string obj_to_prefix(const FileStoreTracker::obj_desc_t &obj) {
  string sep;
  sep.push_back('^');
  return get<0>(obj)->get_cid().to_str() + sep + get<1>(obj) + "_CONTENTS_";
}

string obj_to_meta_prefix(const FileStoreTracker::obj_desc_t &obj) {
  string sep;
  sep.push_back('^');
  return get<0>(obj)->get_cid().to_str() + sep + get<1>(obj);
}

string seq_to_key(uint64_t seq) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%*llu", 20, (unsigned long long int)seq);
  return string(buf);
}

struct ObjStatus {
  uint64_t last_applied;
  uint64_t last_committed;
  uint64_t restart_seq;
  ObjStatus() : last_applied(0), last_committed(0), restart_seq(0) {}

  uint64_t get_last_applied(uint64_t seq) const {
    if (seq > restart_seq)
      return last_committed;
    else
      return last_applied;
  }
  void set_last_applied(uint64_t _last_applied, uint64_t seq) {
    last_applied = _last_applied;
    restart_seq = seq;
  }
  uint64_t trim_to() const {
    return last_applied < last_committed ?
      last_applied : last_committed;
  }
};

void encode(const ObjStatus &obj, bufferlist &bl) {
  ::encode(obj.last_applied, bl);
  ::encode(obj.last_committed, bl);
  ::encode(obj.restart_seq, bl);
}

void decode(ObjStatus &obj, bufferlist::iterator &bl) {
  ::decode(obj.last_applied, bl);
  ::decode(obj.last_committed, bl);
  ::decode(obj.restart_seq, bl);
}

ObjStatus get_obj_status(const FileStoreTracker::obj_desc_t &obj,
			 KeyValueDB *db)
{
  set<string> to_get;
  to_get.insert("META");
  map<string, bufferlist> got;
  db->get(obj_to_meta_prefix(obj), to_get, &got);
  ObjStatus retval;
  if (!got.empty()) {
    bufferlist::iterator bp = got.begin()->second.begin();
    ::decode(retval, bp);
  }
  return retval;
}

void set_obj_status(const FileStoreTracker::obj_desc_t &obj,
		    const ObjStatus &status,
		    KeyValueDB::Transaction t)
{
  map<string, bufferlist> to_set;
  ::encode(status, to_set["META"]);
  t->set(obj_to_meta_prefix(obj), to_set);
}

void _clean_forward(const FileStoreTracker::obj_desc_t &obj,
		    uint64_t last_valid,
		    KeyValueDB *db)
{
  KeyValueDB::Transaction t = db->get_transaction();
  KeyValueDB::Iterator i = db->get_iterator(obj_to_prefix(obj));
  set<string> to_remove;
  i->upper_bound(seq_to_key(last_valid));
  for (; i->valid(); i->next()) {
    to_remove.insert(i->key());
  }
  t->rmkeys(obj_to_prefix(obj), to_remove);
  db->submit_transaction(t);
}

void FileStoreTracker::verify(const obj_desc_t &obj, bool on_start) {
  lock_guard l(lock);
  std::cerr << "Verifying " << obj << std::endl;

  pair<uint64_t, uint64_t> valid_reads = get_valid_reads(obj);
  std::cerr << "valid_reads is " << valid_reads << std::endl;
  bufferlist contents;

  CollectionHandle ch = get<0>(obj);
  ObjectHandle oh = store->get_object(ch, hoid_t(get<1>(obj)));
  int r = store->read(ch, oh, 0, 2*SIZE, contents);
  store->put_object(oh);

  std::cerr << "exists: " << r << std::endl;

  for (uint64_t i = valid_reads.first;
       i < valid_reads.second;
       ++i) {
    ObjectContents old_contents = get_content(obj, i);

    std::cerr << "old_contents exists " << old_contents.exists() << std::endl;
    if (!old_contents.exists() && (r == -ENOENT))
      return;

    if (old_contents.exists() && (r == -ENOENT))
      continue;

    if (!old_contents.exists() && (r != -ENOENT))
      continue;

    if (contents.length() != old_contents.size()) {
      std::cerr << "old_contents.size() is "
		<< old_contents.size() << std::endl;
      continue;
    }

    bufferlist::iterator bp = contents.begin();
    ObjectContents::Iterator iter = old_contents.get_iterator();
    iter.seek_to_first();
    bool matches = true;
    uint64_t pos = 0;
    for (; !bp.end() && iter.valid();
	 ++iter, ++bp, ++pos) {
      if (*iter != *bp) {
	std::cerr << "does not match at pos " << pos << std::endl;
	matches = false;
	break;
      }
    }
    if (matches) {
      if (on_start)
	_clean_forward(obj, i, db);
      return;
    }
  }
  std::cerr << "Verifying " << obj << " failed " << std::endl;
  assert(0);
}

ObjectContents FileStoreTracker::get_current_content(
  const obj_desc_t &obj)
{
  KeyValueDB::Iterator iter = db->get_iterator(
    obj_to_prefix(obj));
  iter->seek_to_last();
  if (iter->valid()) {
    bufferlist bl = iter->value();
    bufferlist::iterator bp = bl.begin();
    pair<uint64_t, bufferlist> val;
    ::decode(val, bp);
    assert(seq_to_key(val.first) == iter->key());
    bp = val.second.begin();
    return ObjectContents(bp);
  }
  return ObjectContents();
}

ObjectContents FileStoreTracker::get_content(
  const obj_desc_t &obj, uint64_t version)
{
  set<string> to_get;
  map<string, bufferlist> got;
  to_get.insert(seq_to_key(version));
  db->get(obj_to_prefix(obj), to_get, &got);
  if (got.empty())
    return ObjectContents();
  pair<uint64_t, bufferlist> val;
  bufferlist::iterator bp = got.begin()->second.begin();
  ::decode(val, bp);
  bp = val.second.begin();
  assert(val.first == version);
  return ObjectContents(bp);
}

pair<uint64_t, uint64_t> FileStoreTracker::get_valid_reads(
  const obj_desc_t &obj)
{
  pair<uint64_t, uint64_t> bounds = make_pair(0,1);
  KeyValueDB::Iterator iter = db->get_iterator(
    obj_to_prefix(obj));
  iter->seek_to_last();
  if (iter->valid()) {
    pair<uint64_t, bufferlist> val;
    bufferlist bl = iter->value();
    bufferlist::iterator bp = bl.begin();
    ::decode(val, bp);
    bounds.second = val.first + 1;
  }

  ObjStatus obj_status = get_obj_status(obj, db);
  bounds.first = obj_status.get_last_applied(restart_seq);
  return bounds;
}

void clear_obsolete(const FileStoreTracker::obj_desc_t &obj,
		    const ObjStatus &status,
		    KeyValueDB *db,
		    KeyValueDB::Transaction t)
{
  KeyValueDB::Iterator iter = db->get_iterator(obj_to_prefix(obj));
  set<string> to_remove;
  iter->seek_to_first();
  for (; iter->valid() && iter->key() < seq_to_key(status.trim_to());
       iter->next())
    to_remove.insert(iter->key());
  t->rmkeys(obj_to_prefix(obj), to_remove);
}

void FileStoreTracker::committed(const obj_desc_t &obj,
				 uint64_t seq) {
  lock_guard l(lock);
  ObjStatus status = get_obj_status(obj, db);
  assert(status.last_committed < seq);
  status.last_committed = seq;
  KeyValueDB::Transaction t = db->get_transaction();
  clear_obsolete(obj, status, db, t);
  set_obj_status(obj, status, t);
  db->submit_transaction(t);
}

void FileStoreTracker::applied(const obj_desc_t &obj,
			       uint64_t seq) {
  lock_guard l(lock);
  std::cerr << "Applied " << obj << " version " << seq << std::endl;
  ObjStatus status = get_obj_status(obj, db);
  assert(status.last_applied < seq);
  status.set_last_applied(seq, restart_seq);
  KeyValueDB::Transaction t = db->get_transaction();
  clear_obsolete(obj, status, db, t);
  set_obj_status(obj, status, t);
  db->submit_transaction(t);
}

uint64_t FileStoreTracker::set_content(const obj_desc_t &obj,
				       ObjectContents &content) {
  KeyValueDB::Transaction t = db->get_transaction();
  KeyValueDB::Iterator iter = db->get_iterator(
    obj_to_prefix(obj));
  iter->seek_to_last();
  uint64_t most_recent = 0;
  if (iter->valid()) {
    pair<uint64_t, bufferlist> val;
    bufferlist bl = iter->value();
    bufferlist::iterator bp = bl.begin();
    ::decode(val, bp);
    most_recent = val.first;
  }
  bufferlist buf_content;
  content.encode(buf_content);
  map<string, bufferlist> to_set;
  ::encode(make_pair(most_recent + 1, buf_content),
	   to_set[seq_to_key(most_recent + 1)]);
  t->set(obj_to_prefix(obj), to_set);
  db->submit_transaction(t);
  return most_recent + 1;
}
