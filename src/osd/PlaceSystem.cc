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


#include "PlaceSystem.h"


std::map<std::string,OSDMapPlaceSystem*> OSDMapPlaceSystem::nameMap;
std::map<__u16,OSDMapPlaceSystem*> OSDMapPlaceSystem::identifierMap;


const OSDMapPlaceSystem& OSDMapPlaceSystem::getSystem() {
  return getSystem(g_conf->osd_placement_system);
}


const OSDMapPlaceSystem& OSDMapPlaceSystem::getSystem(const std::string& name) {
  assert(nameMap.count(name));
  return *nameMap[name];
}


const OSDMapPlaceSystem& OSDMapPlaceSystem::getSystem(const __u16 identifier) {
  assert(identifierMap.count(identifier));
  return *identifierMap[identifier];
}


OSDMapPlaceSystem::OSDMapPlaceSystem(const std::string& name, const __u16 identifier)
  : name(name), identifier(identifier)
{
  nameMap[name] = this;
  identifierMap[identifier] = this;
}


OSDMapPlaceSystem::~OSDMapPlaceSystem()
{
  nameMap.erase(name);
}


std::map<std::string,OSDPlaceSystem*> OSDPlaceSystem::nameMap;
std::map<std::string,OSDMonitorPlaceSystem*> OSDMonitorPlaceSystem::nameMap;


OSDPlaceSystem::OSDPlaceSystem(const std::string& name, const __u16 id)
  : name(name), id(id)
{
  nameMap[name] = this;
}


OSDPlaceSystem::~OSDPlaceSystem()
{
  nameMap.erase(name);
}


OSDMonitorPlaceSystem::OSDMonitorPlaceSystem(const std::string& name, const __u16 id)
  : name(name), id(id)
{
  nameMap[name] = this;
}


OSDMonitorPlaceSystem::~OSDMonitorPlaceSystem()
{
  nameMap.erase(name);
}


const OSDPlaceSystem& OSDPlaceSystem::getSystem() {
  const string& name = g_conf->osd_placement_system;
  assert(nameMap.count(name));
  return *nameMap[name];
}


const OSDMonitorPlaceSystem& OSDMonitorPlaceSystem::getSystem() {
  const string& name = g_conf->osd_placement_system;
  assert(nameMap.count(name));
  return *nameMap[name];
}
