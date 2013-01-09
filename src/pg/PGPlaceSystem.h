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
#include "PGOSD.h"
#include "mon/PGOSDMonitor.h"


// used to hold key identifiers
struct PGPlaceSystem {
public:
  static const std::string systemName;
  static const __u16 systemIdentifier;
};


class PGOSDMapPlaceSystem : public OSDMapPlaceSystem {
public:

  PGOSDMapPlaceSystem(const std::string& name, const __u16 id) :
    OSDMapPlaceSystem(name, id)
  {}

  virtual PGOSDMap* newOSDMap() const {
    return new PGOSDMap();
  }

  virtual PGOSDMap::Incremental* newOSDMapIncremental() const {
    return new PGOSDMap::Incremental();
  }
}; // class PGOSDMapPlaceSystem


class PGOSDPlaceSystem : public OSDPlaceSystem {
public:
  PGOSDPlaceSystem(const std::string& name, const __u16 id) :
    OSDPlaceSystem(name, id)
  {}

  virtual PGOSD* newOSD(int id,
			Messenger *internal, Messenger *external,
			Messenger *hbmin, Messenger *hbmout, MonClient *mc,
			const std::string &dev,
			const std::string &jdev) const
  {
    return new PGOSD(id, internal, external, hbmin, hbmout, mc, dev, jdev);
  }
};


class PGOSDMonitorPlaceSystem : public OSDMonitorPlaceSystem {
public:
  PGOSDMonitorPlaceSystem(const std::string& name, const __u16 id) :
    OSDMonitorPlaceSystem(name, id)
  {}

  virtual PGOSDMonitor* newOSDMonitor(Monitor* mon, Paxos* p) const {
    return new PGOSDMonitor(mon, p);
  }
};


#endif // CEPH_PGPLACESYSTEM_H
