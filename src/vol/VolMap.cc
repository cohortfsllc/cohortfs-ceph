// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com> All rights
 * reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 *
 */


#include "VolMap.h"

#include <sstream>

#ifdef USING_UNICODE
#include <unicode/uchar.h>
#define L_IS_WHITESPACE(c) (u_isUWhiteSpace(c))
#define L_IS_PRINTABLE(c) (u_hasBinaryProperty((c), UCHAR_POSIX_PRINT))
#else
#include <ctype.h>
#define L_IS_WHITESPACE(c) (isspace(c))
#define L_IS_PRINTABLE(c) (isprint(c))
#endif


#define dout_subsys ceph_subsys_mon


using std::stringstream;

const string VolMap::EMPTY_STRING = "";

void VolMap::encode(bufferlist& bl, uint64_t features) const {
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(epoch, bl);
  ::encode(version, bl);
  ::encode(vol_info_by_uuid, bl);
}


void VolMap::decode(bufferlist::iterator& p) {
  __u8 v;
  ::decode(v, p);
  ::decode(epoch, p);
  ::decode(version, p);
  ::decode(vol_info_by_uuid, p);
  vol_info_by_name.clear();

  // build name map from uuid map (only uuid map is encoded)
  for(map<uuid_d,vol_info_t>::const_iterator i = vol_info_by_uuid.begin();
      i != vol_info_by_uuid.end();
      ++i) {
    vol_info_by_name[i->second.name] = i->second;
  }
}


/*
 * Generates a UUID for the new volume and tries to add it to the
 * DB. Returns the uuid generated in the uuid_out parameter.
 */
int VolMap::create_volume(const string& name,
			  uuid_d& uuid_out) {
  uuid_out.generate_random();
  return add_volume(uuid_out, name);
}


int VolMap::add_volume(uuid_d uuid, string name) {
  string error_message;
  if (!is_valid_volume_name(name, error_message)) {
    dout(0) << "attempt to add volume with invalid (" << error_message
	    << ") name \"" << name << "\"" << dendl;
    return -EINVAL;
  }

  if (vol_info_by_uuid.count(uuid) > 0) {
    dout(0) << "attempt to add volume with existing uuid " << uuid << dendl;
    return -EEXIST;
  }

  if (vol_info_by_name.count(name) > 0) {
    dout(0) << "attempt to add volume with existing name \"" << name
	    << "\"" << dendl;
    return -EEXIST;
  }

  vol_info_t vi(uuid, name);
  vol_info_by_uuid[uuid] = vi;
  vol_info_by_name[name] = vi;
  return 0;
}


int VolMap::rename_volume(uuid_d uuid,
			  const string& name,
			  vol_info_t& vinfo) {
 if (vol_info_by_uuid.count(uuid) <= 0) {
    dout(0) << "attempt to update volume with non-existing uuid "
	    << uuid << dendl;
    return -ENOENT;
  }

  vinfo = vol_info_by_uuid[uuid];

  if (vinfo.name != name) {
    string error_message;
    if (!is_valid_volume_name(name, error_message)) {
      dout(0) << "attempt to rename volume using invalid ("
	      << error_message << ") name" << dendl;
      return -EINVAL;
    } if (vol_info_by_name.count(name) > 0) {
      dout(0) << "attempt to update volume " << uuid << " with new name of \""
	      << name << "\", however that volume name is already used"
	      << dendl;
      return -EEXIST;
    }

    vol_info_by_name.erase(vinfo.name);
    vinfo.name = name;
    vol_info_by_uuid[uuid] = vinfo;
    vol_info_by_name[name] = vinfo;

    dout(10) << "updated volume " << vinfo << dendl;
  } else {
    dout(10) << "no changes provided to update name of volume "
	     << uuid << dendl;
  }

  return 0;
}


/*
 * Starting with the uuid as the invariant, find the entry in each map
 * that is associated with that uuid and update (if necessary) the
 * name.
 */
int VolMap::update_volume(uuid_d uuid, string name, vol_info_t& vinfo) {
  int result = rename_volume(uuid, name, vinfo);
  return result;
}


int VolMap::remove_volume(uuid_d uuid, const string& name_verifier) {
  if (vol_info_by_uuid.count(uuid) <= 0) {
    dout(0) << "attempt to remove volume with non-existing uuid " << uuid << dendl;
    return -ENOENT;
  }

  vol_info_t vinfo = vol_info_by_uuid[uuid];

  if (!name_verifier.empty() && name_verifier != vinfo.name) {
    dout(0) << "attempt to remove volume " << uuid
	    << " with non-matching volume name verifier (\"" << name_verifier
	    << "\" instead of \"" << vinfo.name << "\"" << dendl;
    return -EINVAL;
  }

  vol_info_by_name.erase(vinfo.name);
  vol_info_by_uuid.erase(uuid);

  return 0;
}


vector<VolMap::vol_info_t> VolMap::search_vol_info(const string& searchKey,
						   size_t max)
  const
{
  size_t count = 0;
  vector<VolMap::vol_info_t> result;

  try {
    uuid_d uuid = uuid_d::parse(searchKey);

  // TODO : if searchKey could be a *partial* uuid, search for all
  // volumes w/ uuids that begin with that partial.

    const map<uuid_d,vol_info_t>::const_iterator i = find(uuid);
    if (i != end_u()) {
      result.push_back(i->second);
      ++count;
    }
  } catch (const std::invalid_argument &ia) {
  }

  if (count < max) {
    map<string,vol_info_t>::const_iterator i = find(searchKey);
    if (i != end_n()) {
      result.push_back(i->second);
      ++count;
    }
  }

  return result;
}


bool VolMap::get_vol_uuid(const string& volspec, uuid_d& uuid_out) const
{
  // vector<VolMap::vol_info_t> vols_found = search_vol_info(volspec, 2);
  vector<vol_info_t> vols_found = search_vol_info(volspec, 2);
  if (vols_found.size() == 1) {
    uuid_out = vols_found[0].uuid;
    return true;
  } else {
    return false;
  }
}


void VolMap::vol_info_t::dump(Formatter *f) const
{
  char uuid_buf[uuid_d::uuid_d::char_rep_buf_size];
  uuid.print(uuid_buf);
  string uuid_str(uuid_buf);
  f->dump_string("uuid", uuid_str);

  f->dump_string("name", name);
}

void VolMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);

  f->open_array_section("volumes");
  for(map<uuid_d,vol_info_t>::const_iterator i = vol_info_by_uuid.begin();
      i != vol_info_by_uuid.end();
      ++i) {

    f->open_object_section(string(i->first).c_str());
    i->second.dump(f);
    f->close_section();

  }
  f->close_section();
}

void VolMap::print(ostream& out) const
{
    out << "epoch\t" << epoch << "\n";
    for(map<uuid_d,vol_info_t>::const_iterator i = vol_info_by_uuid.begin();
	i != vol_info_by_uuid.end();
	++i) {
      out << i->first << ":\t"
	  << "'" << i->second.name << "\n";
    }
} // VolMap::print


/* TODO: if the volume map is very big, perhaps the summary should
   simply list how many entries there are or somesuch. */
void VolMap::print_summary(ostream& out) const
{
  out << "e" << epoch << ": ";

  bool first = true;
  for(map<uuid_d,vol_info_t>::const_iterator i = vol_info_by_uuid.begin();
      i != vol_info_by_uuid.end();
      ++i) {
    if (!first) {
      out << ", ";
    } else {
      first = false;
    }
    out << "'" << i->second.name << "' "
	<< "(" << i->first << ") ";
  }
} // VolMap::print_summary


/* TODO: consider doing something different in dump and print_summary;
   see comment on print_summary. */
void VolMap::dump(ostream& out) const
{
  print_summary(out);
}


void VolMap::vol_info_t::encode(bufferlist& bl) const {
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(uuid, bl);
  ::encode(name, bl);
}


void VolMap::vol_info_t::decode(bufferlist::iterator& bl) {
  __u8 v;
  ::decode(v, bl);
  ::decode(uuid, bl);
  ::decode(name, bl);
}

void VolMap::vol_info_t::decode(bufferlist& bl) {
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::inc_add::encode(bufferlist& bl,
					  uint64_t features) const {
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(sequence, bl);
  ::encode(vol_info, bl);
}


void VolMap::Incremental::inc_add::decode(bufferlist::iterator& bl) {
  __u8 v;
  ::decode(v, bl);
  ::decode(sequence, bl);
  ::decode(vol_info, bl);
}


void VolMap::Incremental::inc_add::decode(bufferlist& bl) {
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::inc_remove::encode(bufferlist& bl,
					     uint64_t features) const {
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(sequence, bl);
  ::encode(uuid, bl);
}


void VolMap::Incremental::inc_remove::decode(bufferlist::iterator& bl) {
  __u8 v;
  ::decode(v, bl);
  ::decode(sequence, bl);
  ::decode(uuid, bl);
}


void VolMap::Incremental::inc_remove::decode(bufferlist& bl) {
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::encode(bufferlist& bl, uint64_t features) const {
  ::encode(version, bl);
  ::encode(next_sequence, bl);
  ::encode(additions, bl, features);
  ::encode(removals, bl);
  ::encode(updates, bl);
}


void VolMap::Incremental::decode(bufferlist::iterator& bl) {
  ::decode(version, bl);
  ::decode(next_sequence, bl);
  ::decode(additions, bl);
  ::decode(removals, bl);
  ::decode(updates, bl);
}


void VolMap::Incremental::decode(bufferlist& bl) {
  bufferlist::iterator p = bl.begin();
  decode(p);
}


/*
 * Apply an Incremental to a VolMap. Apply each increment in the order
 * it was given in case they interact. So we use three cursors going
 * through the three vectors of the different types of increments and
 * find the next one to apply.
 */
void VolMap::apply_incremental(CephContext *cct,
			       const VolMap::Incremental& inc) {
  uint16_t sequence = 0; // current sequence number we're trying to apply
  vector<Incremental::inc_add>::const_iterator add_cursor = inc.additions.begin();
  vector<Incremental::inc_remove>::const_iterator rem_cursor = inc.removals.begin();
  vector<Incremental::inc_update>::const_iterator upd_cursor = inc.updates.begin();

  while (add_cursor != inc.additions.end()
	 || rem_cursor != inc.removals.end()
	 || upd_cursor != inc.updates.end()) {
    if (add_cursor != inc.additions.end() && add_cursor->sequence == sequence) {
      add_volume(add_cursor->vol_info.uuid,
		 add_cursor->vol_info.name);
      ++add_cursor;
    } else if (rem_cursor != inc.removals.end() && rem_cursor->sequence == sequence) {
      remove_volume(rem_cursor->uuid);
      ++rem_cursor;
    } else if (upd_cursor != inc.updates.end() && upd_cursor->sequence == sequence) {
      vol_info_t out_vinfo;
      update_volume(upd_cursor->vol_info.uuid,
		    upd_cursor->vol_info.name,
		    out_vinfo);
      ++upd_cursor;
    } else {
      assert(0 == "couldn't find next update in sequence");
    }
    ++sequence;
  }

  version = inc.version;
}


/*
 * Returns true if the volume name provided is valid. Volume names
 * cannot be empty, begin or end with whitespace, or be parseable as a
 * UUID.
 */
bool VolMap::is_valid_volume_name(const string& name, string& error) {
  if (name.empty()) {
    error = "volume name may not be empty";
    return false;
  }

  if (L_IS_WHITESPACE(*name.begin())) {
    error = "volume name may not begin with space characters";
    return false;
  }

  if (L_IS_WHITESPACE(*name.rbegin())) {
    error = "volume name may not end with space characters";
    return false;
  }

  for (string::const_iterator c = name.begin(); c != name.end(); ++c) {
    if (!L_IS_PRINTABLE(*c)) {
      error = "volume name can only contain printable characters";
      return false;
    }
  }

  try {
    uuid_d::parse(name);
    error = "volume name cannot match the form of UUIDs";
    return false;
  } catch (const std::invalid_argument &ia) {
    return true;
  }

  return true;
}
