// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com> All rights
 * reserved. TODO: NB: CHECK ON PROPER COPYRIGHT NOTICE GIVEN SOME OF
 * THIS MIGHT BE ORIGINAL CEPH CODE MOVED IN HERE.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 */


#include "PlaceFunc.h"
#include "osd_types.h"


class PGToCrushData : public PlaceFuncPart::PartialData {

public:
  pg_t pgid;

  PGToCrushData(pg_t pgid_p) : pgid(pgid_p) { }
  ~PGToCrushData();

}; // PGToCrushData


class PGHashPlaceFunc : public PlaceFuncPart {
public:
  const static string name;

  PGHashPlaceFunc()
    : PlaceFuncPart(PGHashPlaceFunc::name) {
    // empty for now
  }

  ~PGHashPlaceFunc() { }

  bool canBegin() { return true; }
  bool canEnd() { return false; }

  int firstStep(const OSDMap& osdMap,
		const object_locator_t& locator,
		const object_t& oid,
		PartialData*& outData);

}; // class PGHashPlaceFunc


class PGCrushPlaceFunc : public PlaceFuncPart {
public:
  const static string name;

  PGCrushPlaceFunc()
    : PlaceFuncPart(name) {
    // empty for now
  }
  ~PGCrushPlaceFunc() { }

  bool canBegin() { return false; }
  bool canEnd() { return true; }

  int lastStep(const OSDMap& osdMap,
	       const PartialData* partial,
	       vector<int>& result);

}; // class PGCrushPlaceFunc
