// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 * 
 */


#include "VolMap.h"

#include <sstream>


#define dout_subsys ceph_subsys_mon


using std::stringstream;


void VolMap::encode(bufferlist& bl) const {
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
int VolMap::create_volume(string name, uint16_t crush_map_entry, uuid_d& uuid_out) {
  uuid_out.generate_random();
  return add_volume(uuid_out, name, crush_map_entry);
}


int VolMap::add_volume(uuid_d uuid, string name, uint16_t crush_map_entry) {
  if (vol_info_by_uuid.count(uuid) > 0) {
    dout(0) << "attempt to add volume with existing uuid " << uuid << dendl;
    return -EEXIST;
  } else if (vol_info_by_name.count(name) > 0) {
    dout(0) << "attempt to add volume with existing name \"" << name << "\"" << dendl;
    return -EEXIST;
  }
    
  vol_info_t vi(uuid, name, crush_map_entry);
  vol_info_by_uuid[uuid] = vi;
  vol_info_by_name[name] = vi;
  return 0;
}
  

/*
 * Starting with the uuid as the invariant, find the entry in each map
 * that is associated with that uuid and update (if necessary) the
 * name and crush_map_entry.
 */
int VolMap::update_volume(uuid_d uuid, string name, uint16_t crush_map_entry) {
  if (vol_info_by_uuid.count(uuid) <= 0) {
    dout(0) << "attempt to update volume with non-existing uuid " << uuid << dendl;
    return -ENOENT;
  }

  vol_info_t vinfo = vol_info_by_uuid[uuid];
  bool modified = false;

  if (vinfo.crush_map_entry != crush_map_entry) {
    vinfo.crush_map_entry = crush_map_entry;
    modified = true;
  }

  if (vinfo.name != name) {
    if (vol_info_by_name.count(name) > 0) {
      dout(0) << "attempt to update volume " << uuid << " with new name of \""
	      << name << "\", however that volume name is already used" << dendl;
      return -EEXIST;
    }

    vol_info_by_name.erase(vinfo.name);
    vinfo.name = name;
    modified = true;
  }

  if (modified) {
    vol_info_by_uuid[uuid] = vinfo;
    vol_info_by_name[name] = vinfo;
    dout(10) << "updated volume " << vinfo << dendl;
  } else {
    dout(10) << "updated volume " << uuid << " without changes" << dendl;
  }

  return 0;
}
  

int VolMap::remove_volume(uuid_d uuid) {
  if (vol_info_by_uuid.count(uuid) <= 0) {
    dout(0) << "attempt to remove volume with non-existing uuid " << uuid << dendl;
    return -ENOENT;
  }

  vol_info_t vinfo = vol_info_by_uuid[uuid];

  vol_info_by_name.erase(vinfo.name);
  vol_info_by_uuid.erase(uuid);

  return 0;
}


vector<VolMap::vol_info_t> VolMap::search_vol_info(const string& searchKey, size_t max) {
  size_t count = 0;
  vector<VolMap::vol_info_t> result;

  uuid_d uuid;
  const bool canBeUuid = uuid.parse(searchKey.c_str());

  // TODO : if searchKey could be a *partial* uuid, search for all
  // volumes w/ uuids that begin with that partial.

  if (canBeUuid) {
    VolMap::vol_info_t vol_info;
    const bool found = get_vol_info_uuid(uuid, vol_info);
    if (found) {
      result.push_back(vol_info);
      ++count;
    }
  }

  if (count < max) {
    VolMap::vol_info_t vol_info;
    const bool found = get_vol_info_name(searchKey, vol_info);
    if (found) {
      result.push_back(vol_info);
      ++count;
    }
  }

  return result;
}



void VolMap::vol_info_t::dump(Formatter *f) const
{
  char uuid_buf[uuid_d::uuid_d::char_rep_buf_size];
  uuid.print(uuid_buf);
  string uuid_str(uuid_buf);
  f->dump_string("uuid", uuid_str);

  f->dump_string("name", name);
  f->dump_int("crush_map_entry", (int64_t) crush_map_entry);
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
	  << "'" << i->second.name << "' "
	  << i->second.crush_map_entry
	  << "\n";
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
	<< "(" << i->first << ") " 
	<< i->second.crush_map_entry;
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
  ::encode(crush_map_entry, bl);
}


void VolMap::vol_info_t::decode(bufferlist::iterator& bl) {
  __u8 v;
  ::decode(v, bl);
  ::decode(uuid, bl);
  ::decode(name, bl);
  ::decode(crush_map_entry, bl);
}

void VolMap::vol_info_t::decode(bufferlist& bl) {
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::inc_add::encode(bufferlist& bl) const {
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


void VolMap::Incremental::inc_remove::encode(bufferlist& bl) const {
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


void VolMap::Incremental::encode(bufferlist& bl) const {
  ::encode(version, bl);
  ::encode(next_sequence, bl);
  ::encode(additions, bl);
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


void VolMap::apply_incremental(const VolMap::Incremental& inc) {
  uint16_t sequence = 0;
  vector<Incremental::inc_add>::const_iterator add_cursor = inc.additions.begin();
  vector<Incremental::inc_remove>::const_iterator rem_cursor = inc.removals.begin();
  vector<Incremental::inc_update>::const_iterator upd_cursor = inc.updates.begin();

  while (add_cursor != inc.additions.end()
	 || rem_cursor != inc.removals.end()
	 || upd_cursor != inc.updates.end()) {
    if (add_cursor != inc.additions.end() && add_cursor->sequence == sequence) {
      add_volume(add_cursor->vol_info.uuid,
		 add_cursor->vol_info.name,
		 add_cursor->vol_info.crush_map_entry);
      ++add_cursor;
    } else if (rem_cursor != inc.removals.end() && rem_cursor->sequence == sequence) {
      remove_volume(rem_cursor->uuid);
      ++rem_cursor;
    } else if (upd_cursor != inc.updates.end() && upd_cursor->sequence == sequence) {
      update_volume(upd_cursor->vol_info.uuid,
		 upd_cursor->vol_info.name,
		 upd_cursor->vol_info.crush_map_entry);
      ++upd_cursor;
    } else {
      assert(0 == "couldn't find next update in sequence");
    }
    ++sequence;
  }

  version = inc.version;
}
