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


std::map<std::string,PlaceSystem*> PlaceSystem::nameMap;
std::map<__u16,PlaceSystem*> PlaceSystem::identifierMap;


PlaceSystem* PlaceSystem::getSystem(const std::string& name) {
    if (nameMap.count(name)) {
        return nameMap[name];
    } else {
        return NULL;
    }
}


PlaceSystem* PlaceSystem::getSystem(const __u16 identifier) {
    if (identifierMap.count(identifier)) {
        return identifierMap[identifier];
    } else {
        return NULL;
    }
}


PlaceSystem::PlaceSystem(const std::string& name, const __u16 identifier)
  : name(name), identifier(identifier)
{
  nameMap[name] = this;
  identifierMap[identifier] = this;
}
