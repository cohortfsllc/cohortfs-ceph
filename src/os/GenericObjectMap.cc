// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "include/buffer.h"

#include <cassert>
#include <iostream>
#include <set>
#include <map>
#include <string>
#include <vector>

#include <errno.h>

#include "GenericObjectMap.h"
#include "common/debug.h"
#include "common/config.h"

#define dout_subsys ceph_subsys_keyvaluestore
const string GenericObjectMap::GLOBAL_STATE_KEY = "HEADER";

const string GenericObjectMap::USER_PREFIX = "_SEQ_";
const string GenericObjectMap::INTERN_PREFIX = "_INTERN_";
const string GenericObjectMap::COMPLETE_PREFIX = "_COMPLETE_";
const string GenericObjectMap::HOBJECT_TO_SEQ_PREFIX = "_HOBJTOSEQ_";
const string GenericObjectMap::PARENT_KEY = "_PARENT_HEADER_";

// In order to make right ordering for leveldb matching with hobject_t,
// so use "!" to separated
const string GenericObjectMap::HOBJECT_KEY_SEP_S = "!";
const char GenericObjectMap::HOBJECT_KEY_SEP_C = '!';

// ============== GenericObjectMap Key Function =================

static void append_escaped(string &out, const string &in)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out.push_back('%');
      out.push_back('p');
    } else if (*i == '.') {
      out.push_back('%');
      out.push_back('e');
    } else if (*i == GenericObjectMap::HOBJECT_KEY_SEP_C) {
      out.push_back('%');
      out.push_back('u');
    } else if (*i == '!') {
      out.push_back('%');
      out.push_back('s');
    } else {
      out.push_back(*i);
    }
  }
}

static bool append_unescaped(string &out,
			     const char *begin,
			     const char *end)
{
  for (const char *i = begin; i < end; ++i) {
    if (*i == '%') {
      ++i;
      if (*i == 'p')
	out.push_back('%');
      else if (*i == 'e')
	out.push_back('.');
      else if (*i == 'u')
	out.push_back(GenericObjectMap::HOBJECT_KEY_SEP_C);
      else if (*i == 's')
	out.push_back('!');
      else
	return false;
    } else {
      out.push_back(*i);
    }
  }
  return true;
}

string GenericObjectMap::header_key(const coll_t &cid)
{
  string full_name;

  append_escaped(full_name, cid.to_str());
  full_name.append(HOBJECT_KEY_SEP_S);
  return full_name;
}

string GenericObjectMap::header_key(const coll_t &cid, const hobject_t &oid)
{
  string full_name;

  append_escaped(full_name, cid.to_str());
  full_name.push_back(HOBJECT_KEY_SEP_C);

  oid.append_str(full_name, HOBJECT_KEY_SEP_C, append_escaped);
  full_name.push_back(HOBJECT_KEY_SEP_C);

  return full_name;
}

bool GenericObjectMap::parse_header_key(const string &long_name,
					coll_t *coll, hobject_t *out)
{
  const char *current = long_name.c_str();
  const char *bound = strchr(current, HOBJECT_KEY_SEP_C);

  if (!bound)
    return false;

  if (coll) {
    string cs;
    if (!append_unescaped(cs, current, bound)) {
      return false;
    }
    *coll = coll_t(cs);
  }

  current = bound + 1;
  bound = long_name.c_str() + long_name.length() - 1;
  if (*bound != HOBJECT_KEY_SEP_C)
    return false;

  try {
    if (out) {
      *out = hobject_t::parse_c_str(current, HOBJECT_KEY_SEP_C,
				    append_unescaped, bound);
    }
  } catch (std::invalid_argument &e) {
    return false;
  }

  return true;
}


// ============== GenericObjectMap Prefix =================

string GenericObjectMap::user_prefix(Header header, const string &prefix)
{
  return USER_PREFIX + seq_key(header->seq) + prefix;
}

string GenericObjectMap::complete_prefix(Header header)
{
  return INTERN_PREFIX + seq_key(header->seq) + COMPLETE_PREFIX;
}

string GenericObjectMap::parent_seq_prefix(uint64_t seq)
{
  return INTERN_PREFIX + seq_key(seq) + PARENT_KEY;
}


// ============== GenericObjectMapIteratorImpl =================

int GenericObjectMap::GenericObjectMapIteratorImpl::init()
{
  invalid = false;
  if (ready) {
    return 0;
  }

  assert(!parent_iter);
  if (header->parent) {
    Header parent = map->lookup_parent(header);
    if (!parent) {
      assert(0);
      return -EINVAL;
    }
    parent_iter.reset(new GenericObjectMapIteratorImpl(map, parent, prefix));
  }

  key_iter = map->db->get_iterator(map->user_prefix(header, prefix));
  assert(key_iter);
  complete_iter = map->db->get_iterator(map->complete_prefix(header));
  assert(complete_iter);
  cur_iter = key_iter;
  assert(cur_iter);
  ready = true;
  return 0;
}

ObjectMap::ObjectMapIterator GenericObjectMap::get_iterator(
    const coll_t &cid, const hobject_t &oid, const string &prefix)
{
  Header header = lookup_header(cid, oid);
  if (!header)
    return ObjectMap::ObjectMapIterator(new EmptyIteratorImpl());
  return _get_iterator(header, prefix);
}

int GenericObjectMap::GenericObjectMapIteratorImpl::seek_to_first()
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->seek_to_first();
    if (r < 0)
      return r;
  }
  r = key_iter->seek_to_first();
  if (r < 0)
    return r;
  return adjust();
}

int GenericObjectMap::GenericObjectMapIteratorImpl::seek_to_last()
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->seek_to_last();
    if (r < 0)
      return r;
    if (parent_iter->valid())
      r = parent_iter->next();
    if (r < 0)
      return r;
  }
  r = key_iter->seek_to_last();
  if (r < 0)
    return r;
  if (key_iter->valid())
    r = key_iter->next();
  if (r < 0)
    return r;
  return adjust();
}

int GenericObjectMap::GenericObjectMapIteratorImpl::lower_bound(const string &to)
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->lower_bound(to);
    if (r < 0)
      return r;
  }
  r = key_iter->lower_bound(to);
  if (r < 0)
    return r;
  return adjust();
}

int GenericObjectMap::GenericObjectMapIteratorImpl::upper_bound(const string &after)
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->upper_bound(after);
    if (r < 0)
      return r;
  }
  r = key_iter->upper_bound(after);
  if (r < 0)
    return r;
  return adjust();
}

bool GenericObjectMap::GenericObjectMapIteratorImpl::valid()
{
  bool valid = !invalid && ready;
  assert(!valid || cur_iter->valid());
  return valid;
}

bool GenericObjectMap::GenericObjectMapIteratorImpl::valid_parent()
{
  if (parent_iter && parent_iter->valid() &&
      (!key_iter->valid() || key_iter->key() > parent_iter->key()))
    return true;
  return false;
}

int GenericObjectMap::GenericObjectMapIteratorImpl::next()
{
  assert(cur_iter->valid());
  assert(valid());
  cur_iter->next();
  return adjust();
}

int GenericObjectMap::GenericObjectMapIteratorImpl::next_parent()
{
  if (!parent_iter || !parent_iter->valid()) {
    invalid = true;
    return 0;
  }
  r = next();
  if (r < 0)
    return r;
  if (!valid() || on_parent() || !parent_iter->valid())
    return 0;

  return lower_bound(parent_iter->key());
}

int GenericObjectMap::GenericObjectMapIteratorImpl::in_complete_region(
    const string &to_test, string *begin, string *end)
{
  complete_iter->upper_bound(to_test);
  if (complete_iter->valid())
    complete_iter->prev();
  else
    complete_iter->seek_to_last();

  if (!complete_iter->valid())
    return false;

  string _end;
  if (begin)
    *begin = complete_iter->key();
  _end = string(complete_iter->value().c_str());
  if (end)
    *end = _end;
  return (to_test >= complete_iter->key()) && (!_end.size() || _end > to_test);
}

/**
 * Moves parent_iter to the next position both out of the complete_region and
 * not equal to key_iter.  Then, we set cur_iter to parent_iter if valid and
 * less than key_iter and key_iter otherwise.
 */
int GenericObjectMap::GenericObjectMapIteratorImpl::adjust()
{
  string begin, end;
  while (parent_iter && parent_iter->valid()) {
    if (in_complete_region(parent_iter->key(), &begin, &end)) {
      if (end.size() == 0) {
	parent_iter->seek_to_last();
	if (parent_iter->valid())
	  parent_iter->next();
      } else {
	parent_iter->lower_bound(end);
      }
    } else if (key_iter->valid() && key_iter->key() == parent_iter->key()) {
      parent_iter->next();
    } else {
      break;
    }
  }
  if (valid_parent()) {
    cur_iter = parent_iter;
  } else if (key_iter->valid()) {
    cur_iter = key_iter;
  } else {
    invalid = true;
  }
  assert(invalid || cur_iter->valid());
  return 0;
}

string GenericObjectMap::GenericObjectMapIteratorImpl::key()
{
  return cur_iter->key();
}

bufferlist GenericObjectMap::GenericObjectMapIteratorImpl::value()
{
  return cur_iter->value();
}

int GenericObjectMap::GenericObjectMapIteratorImpl::status()
{
  return r;
}


// ============== GenericObjectMap Public API =================

void GenericObjectMap::set_keys(const Header header,
				const string &prefix,
				const map<string, bufferlist> &set,
				KeyValueDB::Transaction t)
{
  t->set(user_prefix(header, prefix), set);
}

int GenericObjectMap::clear(const Header header,
			    KeyValueDB::Transaction t)
{
  remove_header(header->cid, header->oid, header, t);
  assert(header->num_children > 0);
  header->num_children--;
  int r = _clear(header, t);
  if (r < 0)
    return r;
  return 0;
}

int GenericObjectMap::rm_keys(const Header header,
			      const string &prefix,
			      const set<string> &to_clear,
			      KeyValueDB::Transaction t)
{
  t->rmkeys(user_prefix(header, prefix), to_clear);
  if (!header->parent) {
    return 0;
  }

  // Copy up keys from parent around to_clear
  int keep_parent;
  {
    GenericObjectMapIterator iter = _get_iterator(header, prefix);
    iter->seek_to_first();
    map<string, string> new_complete;
    map<string, bufferlist> to_write;
    for(set<string>::const_iterator i = to_clear.begin();
	i != to_clear.end(); ) {
      unsigned copied = 0;
      iter->lower_bound(*i);
      ++i;
      if (!iter->valid())
	break;
      string begin = iter->key();
      if (!iter->on_parent())
	iter->next_parent();
      if (new_complete.size() && new_complete.rbegin()->second == begin) {
	begin = new_complete.rbegin()->first;
      }
      while (iter->valid() && copied < 20) {
	if (!to_clear.count(iter->key()))
	  to_write[iter->key()].append(iter->value());
	if (i != to_clear.end() && *i <= iter->key()) {
	  ++i;
	  copied = 0;
	}

	iter->next_parent();
	copied++;
      }
      if (iter->valid()) {
	new_complete[begin] = iter->key();
      } else {
	new_complete[begin] = "";
	break;
      }
    }
    t->set(user_prefix(header, prefix), to_write);
    merge_new_complete(header, new_complete, iter, t);
    keep_parent = need_parent(iter);
    if (keep_parent < 0)
      return keep_parent;
  }

  if (!keep_parent) {
    Header parent = lookup_parent(header);
    if (!parent)
      return -EINVAL;
    parent->num_children--;
    _clear(parent, t);
    header->parent = 0;
    set_header(header->cid, header->oid, *header, t);
    t->rmkeys_by_prefix(complete_prefix(header));
  }

  return 0;
}

int GenericObjectMap::get(const coll_t &cid, const hobject_t &oid,
			  const string &prefix,
			  map<string, bufferlist> *out)
{
  Header header = lookup_header(cid, oid);
  if (!header)
    return -ENOENT;

  ObjectMap::ObjectMapIterator iter = _get_iterator(header, prefix);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    out->insert(make_pair(iter->key(), iter->value()));
  }

  return 0;
}

int GenericObjectMap::get_keys(const coll_t &cid, const hobject_t &oid,
			       const string &prefix,
			       set<string> *keys)
{
  Header header = lookup_header(cid, oid);
  if (!header)
    return -ENOENT;

  ObjectMap::ObjectMapIterator iter = _get_iterator(header, prefix);
  for (; iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    keys->insert(iter->key());
  }
  return 0;
}

int GenericObjectMap::get_values(const coll_t &cid, const hobject_t &oid,
				 const string &prefix,
				 const set<string> &keys,
				 map<string, bufferlist> *out)
{
  Header header = lookup_header(cid, oid);
  if (!header)
    return -ENOENT;
  return scan(header, prefix, keys, 0, out);
}

int GenericObjectMap::check_keys(const coll_t &cid, const hobject_t &oid,
				 const string &prefix,
				 const set<string> &keys,
				 set<string> *out)
{
  Header header = lookup_header(cid, oid);
  if (!header)
    return -ENOENT;
  return scan(header, prefix, keys, out, 0);
}

void GenericObjectMap::clone(const Header parent, const coll_t &cid,
			     const hobject_t &target,
			     KeyValueDB::Transaction t,
			     Header *old_header, Header *new_header)
{
  {
    Header destination = lookup_header(cid, target);
    if (destination) {
      remove_header(cid, target, destination, t);
      destination->num_children--;
      _clear(destination, t);
    }
  }

  Header source = generate_new_header(parent->cid, parent->oid, parent, t);
  Header destination = generate_new_header(cid, target, parent, t);

  destination->data = parent->data;
  source->data = parent->data;

  parent->num_children = 2;
  set_parent_header(parent, t);
  set_header(parent->cid, parent->oid, *source, t);
  set_header(cid, target, *destination, t);

  if (new_header)
    *old_header = source;
  if (new_header)
    *new_header = destination;
}

void GenericObjectMap::rename(const Header old_header, const coll_t &cid,
			      const hobject_t &target,
			      KeyValueDB::Transaction t)
{
  if (old_header->oid == target && old_header->cid == cid)
    return ;

  remove_header(old_header->cid, old_header->oid, old_header, t);
  old_header->cid = cid;
  old_header->oid = target;
  set_header(cid, target, *old_header, t);

  // "in_use" still hold the "seq"
}

int GenericObjectMap::init(bool do_upgrade)
{
  map<string, bufferlist> result;
  set<string> to_get;
  to_get.insert(GLOBAL_STATE_KEY);
  int r = db->get(INTERN_PREFIX, to_get, &result);
  if (r < 0)
    return r;
  if (!result.empty()) {
    bufferlist::iterator bliter = result.begin()->second.begin();
    state.decode(bliter);
    if (state.v < 1) { // Needs upgrade
      if (!do_upgrade) {
	ldout(cct, 1) << "GenericObjbectMap requires an upgrade,"
		<< " set filestore_update_to"
		<< dendl;
	return -ENOTSUP;
      } else {
	r = upgrade();
	if (r < 0)
	  return r;
      }
    }
  } else {
    // New store
    state.v = 1;
    state.seq = 1;
  }
  ldout(cct, 20) << "(init)genericobjectmap: seq is " << state.seq << dendl;
  return 0;
}

int GenericObjectMap::sync(const Header header, KeyValueDB::Transaction t)
{
  write_state(t);
  if (header) {
    set_header(header->cid, header->oid, *header, t);
  }
  return 0;
}

bool GenericObjectMap::check(std::ostream &out)
{
  bool retval = true;
  map<uint64_t, uint64_t> parent_to_num_children;
  map<uint64_t, uint64_t> parent_to_actual_num_children;
  KeyValueDB::Iterator iter = db->get_iterator(HOBJECT_TO_SEQ_PREFIX);

  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    _Header header;
    assert(header.num_children == 1);
    header.num_children = 0; // Hack for leaf node
    bufferlist bl = iter->value();
    while (true) {
      bufferlist::iterator bliter = bl.begin();
      header.decode(bliter);
      if (header.seq != 0)
	parent_to_actual_num_children[header.seq] = header.num_children;
      if (header.parent == 0)
	break;

      if (!parent_to_num_children.count(header.parent))
	parent_to_num_children[header.parent] = 0;
      parent_to_num_children[header.parent]++;
      if (parent_to_actual_num_children.count(header.parent))
	break;

      set<string> to_get;
      map<string, bufferlist> got;
      to_get.insert(PARENT_KEY);
      db->get(parent_seq_prefix(header.parent), to_get, &got);
      if (got.empty()) {
	out << "Missing: seq " << header.parent << std::endl;
	retval = false;
	break;
      } else {
	bl = got.begin()->second;
      }
    }
  }

  for (map<uint64_t, uint64_t>::iterator i = parent_to_num_children.begin();
       i != parent_to_num_children.end();
       parent_to_num_children.erase(i++)) {
    if (!parent_to_actual_num_children.count(i->first))
      continue;
    if (parent_to_actual_num_children[i->first] != i->second) {
      out << "Invalid: seq " << i->first << " recorded children: "
	  << parent_to_actual_num_children[i->first] << " found: "
	  << i->second << std::endl;
      retval = false;
    }
    parent_to_actual_num_children.erase(i->first);
  }
  return retval;
}


// ============== GenericObjectMap Intern Implementation =================

int GenericObjectMap::scan(Header header,
			   const string &prefix,
			   const set<string> &in_keys,
			   set<string> *out_keys,
			   map<string, bufferlist> *out_values)
{
  ObjectMap::ObjectMapIterator db_iter = _get_iterator(header, prefix);
  for (set<string>::const_iterator key_iter = in_keys.begin();
       key_iter != in_keys.end();
       ++key_iter) {
    db_iter->lower_bound(*key_iter);
    if (db_iter->status())
      return db_iter->status();

    if (db_iter->valid() && db_iter->key() == *key_iter) {
      if (out_keys)
	out_keys->insert(*key_iter);
      if (out_values)
	out_values->insert(make_pair(db_iter->key(), db_iter->value()));
    }
  }
  return 0;
}

int GenericObjectMap::_clear(Header header, KeyValueDB::Transaction t)
{
  while (1) {
    if (header->num_children) {
      set_parent_header(header, t);
      break;
    }

    clear_header(header, t);
    if (!header->parent)
      break;

    Header parent = lookup_parent(header);
    if (!parent) {
      return -EINVAL;
    }
    assert(parent->num_children > 0);
    parent->num_children--;
    header.swap(parent);
  }
  return 0;
}

int GenericObjectMap::merge_new_complete(
    Header header, const map<string, string> &new_complete,
    GenericObjectMapIterator iter, KeyValueDB::Transaction t)
{
  KeyValueDB::Iterator complete_iter = db->get_iterator(
    complete_prefix(header));
  map<string, string>::const_iterator i = new_complete.begin();
  set<string> to_remove;
  map<string, bufferlist> to_add;

  string begin, end;
  while (i != new_complete.end()) {
    string new_begin = i->first;
    string new_end = i->second;
    int r = iter->in_complete_region(new_begin, &begin, &end);
    if (r < 0)
      return r;
    if (r) {
      to_remove.insert(begin);
      new_begin = begin;
    }
    ++i;
    while (i != new_complete.end()) {
      if (!new_end.size() || i->first <= new_end) {
	if (!new_end.size() && i->second > new_end) {
	  new_end = i->second;
	}
	++i;
	continue;
      }

      r = iter->in_complete_region(new_end, &begin, &end);
      if (r < 0)
	return r;
      if (r) {
	to_remove.insert(begin);
	new_end = end;
	continue;
      }
      break;
    }
    bufferlist bl;
    bl.append(bufferptr(new_end.c_str(), new_end.size() + 1));
    to_add.insert(make_pair(new_begin, bl));
  }
  t->rmkeys(complete_prefix(header), to_remove);
  t->set(complete_prefix(header), to_add);
  return 0;
}

int GenericObjectMap::need_parent(GenericObjectMapIterator iter)
{
  int r = iter->seek_to_first();
  if (r < 0)
    return r;

  if (!iter->valid())
    return 0;

  string begin, end;
  if (iter->in_complete_region(iter->key(), &begin, &end) && end == "") {
    return 0;
  }
  return 1;
}

int GenericObjectMap::write_state(KeyValueDB::Transaction t)
{
  ldout(cct, 20) << __func__ << " seq is " << state.seq << dendl;
  bufferlist bl;
  state.encode(bl);
  map<string, bufferlist> to_write;
  to_write[GLOBAL_STATE_KEY] = bl;
  t->set(INTERN_PREFIX, to_write);
  return 0;
}

// NOTE(haomai): It may occur dead lock if thread A hold header A try to header
// B and thread hold header B try to get header A
GenericObjectMap::Header GenericObjectMap::_lookup_header(
    const coll_t &cid, const hobject_t &oid)
{
  set<string> to_get;
  to_get.insert(header_key(cid, oid));
  _Header header;

  while (1) {
    map<string, bufferlist> out;
    bool try_again = false;

    int r = db->get(HOBJECT_TO_SEQ_PREFIX, to_get, &out);
    if (r < 0)
      return Header();
    if (out.empty())
      return Header();

    bufferlist::iterator iter = out.begin()->second.begin();
    header.decode(iter);

    while (in_use.count(header.seq)) {
      header_cond.Wait(header_lock);

      // Another thread is hold this header, wait for it.
      // Because the seq of this object may change, such as clone
      // and rename operation, here need to look up "seq" again
      try_again = true;
    }

    if (!try_again) {
      break;
    }
  }

  Header ret = Header(new _Header(header), RemoveOnDelete(this));
  in_use.insert(ret->seq);
  return ret;
}

GenericObjectMap::Header GenericObjectMap::_generate_new_header(
    const coll_t &cid, const hobject_t &oid, Header parent,
    KeyValueDB::Transaction t)
{
  Header header = Header(new _Header(), RemoveOnDelete(this));
  header->seq = state.seq++;
  if (parent) {
    header->parent = parent->seq;
  }
  header->num_children = 1;
  header->oid = oid;
  header->cid = cid;
  assert(!in_use.count(header->seq));
  in_use.insert(header->seq);

  write_state(t);
  return header;
}

GenericObjectMap::Header GenericObjectMap::lookup_parent(Header input)
{
  Mutex::Locker l(header_lock);
  while (in_use.count(input->parent))
    header_cond.Wait(header_lock);
  map<string, bufferlist> out;
  set<string> keys;
  keys.insert(PARENT_KEY);

  ldout(cct, 20) << "lookup_parent: parent " << input->parent
       << " for seq " << input->seq << dendl;

  int r = db->get(parent_seq_prefix(input->parent), keys, &out);
  if (r < 0) {
    assert(0);
    return Header();
  }
  if (out.empty()) {
    assert(0);
    return Header();
  }

  Header header = Header(new _Header(), RemoveOnDelete(this));
  header->seq = input->parent;
  bufferlist::iterator iter = out.begin()->second.begin();
  header->decode(iter);
  ldout(cct, 20) << "lookup_parent: parent seq is " << header->seq << " with parent "
	   << header->parent << dendl;
  in_use.insert(header->seq);
  return header;
}

GenericObjectMap::Header GenericObjectMap::lookup_create_header(
    const coll_t &cid, const hobject_t &oid, KeyValueDB::Transaction t)
{
  Mutex::Locker l(header_lock);
  Header header = _lookup_header(cid, oid);
  if (!header) {
    header = _generate_new_header(cid, oid, Header(), t);
    set_header(cid, oid, *header, t);
  }
  return header;
}

void GenericObjectMap::set_parent_header(Header header, KeyValueDB::Transaction t)
{
  ldout(cct, 20) << __func__ << " setting seq " << header->seq << dendl;
  map<string, bufferlist> to_write;
  header->encode(to_write[PARENT_KEY]);
  t->set(parent_seq_prefix(header->seq), to_write);
}

void GenericObjectMap::clear_header(Header header, KeyValueDB::Transaction t)
{
  ldout(cct, 20) << __func__ << " clearing seq " << header->seq << dendl;
  t->rmkeys_by_prefix(user_prefix(header, string()));
  t->rmkeys_by_prefix(complete_prefix(header));
  set<string> keys;
  keys.insert(PARENT_KEY);
  t->rmkeys(parent_seq_prefix(header->seq), keys);
}

// only remove HOBJECT_TO_SEQ
void GenericObjectMap::remove_header(const coll_t &cid,
				     const hobject_t &oid, Header header,
				     KeyValueDB::Transaction t)
{
  ldout(cct, 20) << __func__ << " removing " << header->seq
	   << " cid " << cid << " oid " << oid << dendl;
  set<string> to_remove;
  to_remove.insert(header_key(cid, oid));
  t->rmkeys(HOBJECT_TO_SEQ_PREFIX, to_remove);
}

void GenericObjectMap::set_header(const coll_t &cid, const hobject_t &oid,
				  _Header header, KeyValueDB::Transaction t)
{
  ldout(cct, 20) << __func__ << " setting " << header.seq
	   << " cid " << cid << " oid " << oid << " parent seq "
	   << header.parent << dendl;
  map<string, bufferlist> to_set;
  header.encode(to_set[header_key(cid, oid)]);
  t->set(HOBJECT_TO_SEQ_PREFIX, to_set);
}

int GenericObjectMap::list_objects(const coll_t &cid, hobject_t start, int max,
				   vector<hobject_t> *out, hobject_t *next)
{
  // FIXME
  Mutex::Locker l(header_lock);

  int size = 0;
  KeyValueDB::Iterator iter = db->get_iterator(HOBJECT_TO_SEQ_PREFIX);
  for (iter->lower_bound(header_key(cid, start)); iter->valid(); iter->next()) {
    bufferlist bl = iter->value();
    bufferlist::iterator bliter = bl.begin();
    _Header header;
    header.decode(bliter);

    if (header.cid != cid) {
      break;
    }

    if (max && size >= max) {
      if (next)
	*next = header.oid;
      break;
    }

    assert(start <= header.oid);


    size++;
    if (out)
      out->push_back(header.oid);
    start = header.oid;
  }

  if (out->size())
    ldout(cct, 20) << "objects: " << *out << dendl;

  return 0;
}
