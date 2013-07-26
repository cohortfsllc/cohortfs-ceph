// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 */


#ifndef CEPH_COHORTPLACESYSTEM_H
#define CEPH_COHORTPLACESYSTEM_H


#include "mon/CohortOSDMonitor.h"
#include "osd/PlaceSystem.h"
#include "CohortOSDMap.h"
#include "CohortOSD.h"


// used to hold key identifiers
struct CohortPlaceSystem {
  static const std::string systemName;
  static const __u16 systemIdentifier;
};


class CohortOSDMapPlaceSystem : public OSDMapPlaceSystem {
public:

  CohortOSDMapPlaceSystem(const std::string& name, const __u16 id) :
    OSDMapPlaceSystem(name, id)
  {}

  virtual CohortOSDMap* newOSDMap() const {
    return new CohortOSDMap();
  }

  virtual CohortOSDMap::Incremental* newOSDMapIncremental() const {
    return new CohortOSDMap::Incremental();
  }
};


class CohortOSDPlaceSystem : public OSDPlaceSystem {
public:

  CohortOSDPlaceSystem(const std::string& name, const __u16 id) :
    OSDPlaceSystem(name, id)
  {}

  virtual CohortOSD* newOSD(int id, Messenger *internal, Messenger *external,
			    Messenger *hb_client, Messenger *hb_front_server,
			    Messenger *hb_back_server, MonClient *mc,
			    const std::string &dev,
			    const std::string &jdev) const {
    return new CohortOSD(id, internal, external, hb_client, hb_front_server,
			 hb_back_server, mc, dev, jdev);
  }
};


class CohortOSDMonitorPlaceSystem : public OSDMonitorPlaceSystem {
public:
  CohortOSDMonitorPlaceSystem(const std::string& name, const __u16 id) :
    OSDMonitorPlaceSystem(name, id)
  {}

  virtual CohortOSDMonitor* newOSDMonitor(Monitor* mon, Paxos* p,
					  const string& service_name) const {
    return new CohortOSDMonitor(mon, p, service_name);
  }
};

#endif /* CEPH_COHORTPLACESYSTEM_H */
