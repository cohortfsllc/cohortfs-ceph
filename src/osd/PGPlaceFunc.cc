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


#include "PGPlaceFunc.h"

using namespace std;


// **************** PGToCrushData ****************


PGToCrushData::PGToCrushData()
  : PlaceFuncPart::PartialData() {
    // empty for now
}


PGToCrushData::~PGToCrushData() {
    // empty for now
}


// **************** PGHashPlaceFunc ****************


const string PGHashPlaceFunc::name = "PGCrushPlaceFunc";


int PGHashPlaceFunc::firstStep(const PlaceFunc::FileSystemLocator& locator,
			       PartialData*& outData) {
  return PlaceFunc::UNIMPLEMENTED;
}


// **************** PGCrushPlaceFunc ****************


const string PGCrushPlaceFunc::name = "PGCrushPlaceFunc";


int PGCrushPlaceFunc::lastStep(const PartialData* inData, vector<int>& result) {
  return PlaceFunc::UNIMPLEMENTED;
}
