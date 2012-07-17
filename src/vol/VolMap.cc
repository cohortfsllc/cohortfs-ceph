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
using std::stringstream;


int VolMap::create_volume(string name, uint16_t crush_map_entry) {
  uuid_d uuid;
  uuid.generate_random();
  return add_volume(uuid, name, crush_map_entry);
}


int VolMap::add_volume(uuid_d uuid, string name, uint16_t crush_map_entry) {
  if (vol_info_by_uuid.count(uuid) > 0 ||
      vol_info_by_name.count(name) > 0) {
    return -EINVAL;
  }
    
  vol_info_t vi(uuid, name, crush_map_entry);
  vol_info_by_uuid[uuid] = vi;
  vol_info_by_name[name] = vi;
  return 0;
}
  

int VolMap::remove_volume(uuid_d uuid) {
  return -ENOENT;
}


int VolMap::remove_volume(string name) {
  return -ENOENT;
}


void VolMap::vol_info_t::dump(Formatter *f) const
{
  char uuid_buf[uuid_d::uuid_d::char_rep_buf_size];
  uuid.print(uuid_buf);
  string uuid_str(uuid_buf);
  f->dump_string("uuid", uuid_str);

  f->dump_string("name", name);
  f->dump_int("crush_map_entry", (int64_t)crush_map_entry);
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

void VolMap::print(ostream& out) 
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


void VolMap::print_summary(ostream& out) 
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

