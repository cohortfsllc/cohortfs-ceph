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

const std::string Volume::typestrings[] = {
  "VolFS", "VolBlock", "VolDeDupFS", "VolDeDupBlock","NotAVolType"};

bool Volume::valid_name(const string &name, string &error)
{
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

}

bool Volume::valid(string& error)
{
  if (!valid_name(name, error)) {
    return false;
  }

  if (uuid == 0) {
    error = "UUID may not be 0.";
    return false;
  }

  return true;
}

int Volume::update(std::tr1::shared_ptr<const Volume> v)
{
#warning To be done.
}

void VolMap::encode(bufferlist& bl, uint64_t features) const {
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(epoch, bl);
  ::encode(version, bl);
  ::encode(vol_by_uuid, bl);
}

void VolMap::decode(bufferlist::iterator& p) {
  __u8 v;
  ::decode(v, p);
  ::decode(epoch, p);
  ::decode(version, p);
#warning Address this.
//  ::decode(vol_by_uuid, p);
  vol_by_name.clear();

  // build name map from uuid map (only uuid map is encoded)
  for(map<uuid_d,VolumeRef>::const_iterator i = vol_by_uuid.begin();
      i != vol_by_uuid.end();
      ++i) {
    vol_by_name[i->second->name] = i->second;
  }
}


/*
 * Generates a UUID for the new volume and tries to add it to the
 * DB. Returns the uuid generated in the uuid_out parameter.
 */
int VolMap::create_volume(VolumeRef vol, uuid_d& out)
{
  vol->uuid = uuid_d::generate_random();
  vol->last_update = epoch + 1;
  out = vol->uuid;
  return add_volume(vol);
}


int VolMap::add_volume(VolumeRef vol) {
  string error_message;
  if (!vol->valid(error_message)) {
    dout(0) << "attempt to add invalid volume: " << error_message
	    << dendl;
    return -EINVAL;
  }

  if (vol_by_uuid.count(vol->uuid) > 0) {
    dout(0) << "attempt to add volume with existing uuid "
	    << vol->uuid << dendl;
    return -EEXIST;
  }

  if (vol_by_name.count(vol->name) > 0) {
    dout(0) << "attempt to add volume with existing name \""
	    << vol->name << "\"" << dendl;
    return -EEXIST;
  }

  vol_by_uuid[vol->uuid] = vol;
  vol_by_name[vol->name] = vol;
  return 0;
}

int VolMap::rename_volume(VolumeRef v,
			  const string& name)
{
  if (v->name != name) {
    string error_message;
    if (!Volume::valid_name(name, error_message)) {
      dout(0) << "attempt to rename volume using invalid ("
	      << error_message << ") name" << dendl;
      return -EINVAL;
    } if (vol_by_name.count(name) > 0) {
      dout(0) << "attempt to update volume " << v->uuid
	      << " with new name of \""
	      << name << "\", however that volume name is already used"
	      << dendl;
      return -EEXIST;
    }

    vol_by_name.erase(v->name);
    v->name = name;
    vol_by_name[name] = v;

    dout(10) << "updated volume " << *v << dendl;
  } else {
    dout(10) << "no changes provided to update name of volume "
	     << v->uuid << dendl;
  }

  return 0;
}

int VolMap::rename_volume(uuid_d uuid,
			  const string& name)
{
  map<uuid_d,VolumeRef>::iterator i = vol_by_uuid.find(uuid);
  if (i != end_u()) {
    dout(0) << "attempt to update volume with non-existing uuid "
	    << uuid << dendl;
    return -ENOENT;
  }

  VolumeRef v = i->second;

  return rename_volume(v, name);
}

/*
 * Starting with the uuid as the invariant, find the entry in each map
 * that is associated with that uuid and update (if necessary) the
 * name.
 */
int VolMap::update_volume(uuid_d uuid, VolumeRef nv)
{
  string error;
  map<uuid_d,VolumeRef>::iterator i = vol_by_uuid.find(uuid);

  if (i != end_u()) {
    dout(0) << "attempt to update volume with non-existing uuid "
	    << uuid << dendl;
    return -ENOENT;
  }

  VolumeRef v = i->second;
  int result = rename_volume(v, nv->name);

  /* Come back and look at this later, it should be all or nothing. */
  if (result != 0) {
    return result;
  }

  nv->uuid = v->uuid;
  nv->last_update = epoch + 1;

  if (!nv->valid(error)) {
    dout(0) << "Volume update invalid: " << error << dendl;
    return -EINVAL;
  }

  return v->update(nv);
}


int VolMap::remove_volume(uuid_d uuid, const string& name_verifier)
{
  map<uuid_d,VolumeRef>::iterator i = vol_by_uuid.find(uuid);

  if (i != end_u()) {
    dout(0) << "attempt to remove volume with non-existing uuid "
	    << uuid << dendl;
    return -ENOENT;
  }

  VolumeRef v = i->second;

  if (!name_verifier.empty() && name_verifier != v->name) {
    dout(0) << "attempt to remove volume " << uuid
	    << " with non-matching volume name verifier (\"" << name_verifier
	    << "\" instead of \"" << v->name << "\"" << dendl;
    return -EINVAL;
  }

  vol_by_name.erase(v->name);
  vol_by_uuid.erase(uuid);

  return 0;
}


vector<VolumeCRef> VolMap::search_vol(const string& searchKey,
				      size_t max) const
{
  size_t count = 0;
  vector<VolumeCRef> result;

  try {
    uuid_d uuid = uuid_d::parse(searchKey);

  // TODO : if searchKey could be a *partial* uuid, search for all
  // volumes w/ uuids that begin with that partial.

    const map<uuid_d,VolumeRef>::const_iterator i = find(uuid);
    if (i != end_u()) {
      result.push_back(i->second);
      ++count;
    }
  } catch (const std::invalid_argument &ia) {
  }

  if (count < max) {
    map<string,VolumeRef>::const_iterator i = vol_by_name.find(searchKey);
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
  vector<VolumeCRef> vols_found = search_vol(volspec, 2);
  if (vols_found.size() == 1) {
    uuid_out = vols_found[0]->uuid;
    return true;
  } else {
    return false;
  }
}

const string& Volume::type_string(vol_type type)
{
  if ((type < 0) || (type >= NotAVolType)) {
    return typestrings[NotAVolType];
  } else {
    return typestrings[type];
  }
}

void Volume::dump(Formatter *f) const
{
  char uuid_buf[uuid_d::uuid_d::char_rep_buf_size];
  uuid.print(uuid_buf);
  string uuid_str(uuid_buf);
  f->dump_string("uuid", uuid_str);
  f->dump_string("name", name);
  f->dump_string("type", type_string(type));
  /* We don't yet dump the placement text.  It's doubtful that there's
     actually any reason to bother. */
}

void VolMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);

  f->open_array_section("volumes");
  for(map<uuid_d,VolumeRef>::const_iterator i = vol_by_uuid.begin();
      i != vol_by_uuid.end();
      ++i) {

    f->open_object_section(string(i->first).c_str());
    i->second->dump(f);
    f->close_section();

  }
  f->close_section();
}

void VolMap::print(ostream& out) const
{
    out << "epoch\t" << epoch << "\n";
    for(map<uuid_d,VolumeRef>::const_iterator i = vol_by_uuid.begin();
	i != vol_by_uuid.end();
	++i) {
      out << i->first << ":\t"
	  << "'" << i->second->name << "\n";
    }
}

/* TODO: if the volume map is very big, perhaps the summary should
   simply list how many entries there are or somesuch. */
void VolMap::print_summary(ostream& out) const
{
  out << "e" << epoch << ": ";

  bool first = true;
  for(map<uuid_d,VolumeRef>::const_iterator i = vol_by_uuid.begin();
      i != vol_by_uuid.end();
      ++i) {
    if (!first) {
      out << ", ";
    } else {
      first = false;
    }
    out << "'" << i->second->name << "' "
	<< "(" << i->first << ") ";
  }
}


/* TODO: consider doing something different in dump and print_summary;
   see comment on print_summary. */
void VolMap::dump(ostream& out) const
{
  print_summary(out);
}

void Volume::encode(bufferlist& bl) const
{
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(type, bl);
  ::encode(uuid, bl);
  ::encode(name, bl);
  ::encode(last_update, bl);
  ::encode(place_text, bl);
}

void Volume::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(type, bl);
  ::decode(uuid, bl);
  ::decode(name, bl);
  ::decode(last_update, bl);
  ::decode(place_text, bl);
}

void Volume::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::inc_add::encode(bufferlist& bl,
					  uint64_t features) const {
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(sequence, bl);
  vol->encode(bl);
}


void VolMap::Incremental::inc_add::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(sequence, bl);
#warning Address this or crash.
  vol->decode(bl);
}


void VolMap::Incremental::inc_add::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::inc_remove::encode(bufferlist& bl,
					     uint64_t features) const
{
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(sequence, bl);
  ::encode(uuid, bl);
}


void VolMap::Incremental::inc_remove::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(sequence, bl);
  ::decode(uuid, bl);
}


void VolMap::Incremental::inc_remove::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void VolMap::Incremental::encode(bufferlist& bl, uint64_t features) const
{
  ::encode(version, bl);
  ::encode(next_sequence, bl);
  ::encode(additions, bl, features);
  ::encode(removals, bl);
  ::encode(updates, bl);
}


void VolMap::Incremental::decode(bufferlist::iterator& bl)
{
  ::decode(version, bl);
  ::decode(next_sequence, bl);
  ::decode(additions, bl);
  ::decode(removals, bl);
  ::decode(updates, bl);
}


void VolMap::Incremental::decode(bufferlist& bl)
{
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
      add_volume(add_cursor->vol);
      ++add_cursor;
    } else if (rem_cursor != inc.removals.end() && rem_cursor->sequence == sequence) {
      remove_volume(rem_cursor->uuid);
      ++rem_cursor;
    } else if (upd_cursor != inc.updates.end() && upd_cursor->sequence == sequence) {
      update_volume(upd_cursor->vol->uuid,
		    upd_cursor->vol);
      ++upd_cursor;
    } else {
      assert(0 == "couldn't find next update in sequence");
    }
    ++sequence;
  }

  version = inc.version;
}
