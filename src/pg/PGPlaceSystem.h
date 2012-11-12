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


#ifndef CEPH_PGPLACESYSTEM_H
#define CEPH_PGPLACESYSTEM_H


#include "osd/PlaceSystem.h"
#include "PGOSDMap.h"


class PGPlaceSystem : public PlaceSystem {
  const static __u16 systemIdentifier;
  const static std::string systemName;
  const static PGPlaceSystem* singleton;

  PGPlaceSystem() : PlaceSystem(systemName, systemIdentifier) {}

public:
  ~PGPlaceSystem() {}

  OSDMap* newMap() const { return new PGOSDMap(); }
  OSDMap::Incremental* newMapIncremental() const { return new PGOSDMap::Incremental(); }
};


#endif // CEPH_PGPLACESYSTEM_H
