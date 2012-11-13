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

#include "OSDMap.h"
#include "mon/OSDMonitor.h"


class PlaceSystem {
  static std::map<std::string,PlaceSystem*> nameMap;
  static std::map<__u16,PlaceSystem*> identifierMap;

  const std::string name;
  const __u16 identifier;

protected:

  PlaceSystem(const std::string& name, const __u16 identifier);

public:

  static PlaceSystem* getSystem(const std::string& name);
  static PlaceSystem* getSystem(const __u16 identifier);

  virtual ~PlaceSystem() {}

  std::string getSystemName() const { return name; }
  __u16 getSystemIdentifier() const { return identifier; }

  // creates a new Map; caller must deallocate
  virtual OSDMap* newOSDMap() const = 0;

  // creates a new MapIncremental; caller must deallocate
  virtual OSDMap::Incremental* newOSDMapIncremental() const = 0;

  // 
  virtual OSDMonitor* newOSDMonitor(Monitor* mon, Paxos* p) const = 0;
};

#endif // CEPH_PLACESYSTEN_H
