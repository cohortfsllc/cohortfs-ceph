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
 */


#ifndef CEPH_PLACESYSTEM_H
#define CEPH_PLACESYSTEM_H


#include <string>
#include <map>

#include "include/inttypes.h"

#include "OSD.h"
#include "OSDMap.h"
#include "mon/OSDMonitor.h"


template<class T>
class PlaceSystemBase {
private:
  static std::map<std::string,T*> nameMap;
  static std::map<__u16,T*> identifierMap;
  const std::string name;
  const __u16 identifier;

protected:

  PlaceSystemBase(const std::string& name, const __u16 identifier) :
    name(name),
    identifier(identifier)
  {
    T* sub_this = dynamic_cast<T*>(this);
    assert(sub_this);
    nameMap[name] = sub_this;
    identifierMap[identifier] = sub_this;
  }

public:

  virtual ~PlaceSystemBase() {
    nameMap.erase(name);
    identifierMap.erase(identifier);
  }

  std::string getSystemName() const { return name; }
  __u16 getSystemIdentifier() const { return identifier; }

  static const T& getSystem() {
    const string& name = g_conf->osd_placement_system;
    return getSystem(name);
  }

  static const T& getSystem(const std::string& name) {
    assert(nameMap.count(name));
    return *nameMap[name];
  }

  static const T& getSystem(const __u16 identifier) {
    assert(identifierMap.count(identifier));
    return *identifierMap[identifier];
  }
};


class OSDPlaceSystem : public PlaceSystemBase<OSDPlaceSystem> {
public:
  OSDPlaceSystem(const std::string& name, const __u16 id) :
    PlaceSystemBase(name, id)
  { }

  virtual OSD* newOSD(int id, Messenger *internal, Messenger *external,
		      Messenger *hb_client, Messenger *hb_front_server,
		      Messenger *hb_back_server, MonClient *mc,
		      const std::string &dev,
		      const std::string &jdev) const = 0;
};


class OSDMapPlaceSystem : public PlaceSystemBase<OSDMapPlaceSystem> {
public:
  
  OSDMapPlaceSystem(const std::string& name, const __u16 identifier) :
    PlaceSystemBase(name, identifier)
  {
    // emtpy
  }

  // creates a new Map; caller must deallocate
  virtual OSDMap* newOSDMap() const = 0;

  // creates a new MapIncremental; caller must deallocate
  virtual OSDMap::Incremental* newOSDMapIncremental() const = 0;
};


class OSDMonitorPlaceSystem  : public PlaceSystemBase<OSDMonitorPlaceSystem> {
public:
  OSDMonitorPlaceSystem(const std::string& name, const __u16 id) :
    PlaceSystemBase(name, id)
  { }

  virtual OSDMonitor* newOSDMonitor(Monitor* mon, Paxos* p,
				    const string& service_name) const = 0;
};


#endif // CEPH_PLACESYSTEN_H
