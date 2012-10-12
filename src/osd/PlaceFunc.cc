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


#include <map>
#include "PlaceFunc.h"

using namespace std;


// **************** PlaceFuncPart ****************


map<string,PlaceFuncPart*> PlaceFuncPart::partsMap;


// **************** PlaceFunc ****************


PlaceFunc::~PlaceFunc() {
  for (vector<PlaceFuncPart*>::iterator it = algParts.begin();
       it != algParts.end();
       ++it) {
    algParts.erase(it);
    delete *it;
  }
}


void PlaceFunc::addFuncPart(const string& name) {
  if (0 == PlaceFuncPart::partsMap.count(name)) {
    throw PlaceFuncException("tried to add unknown placement"
				 " functional partial \"" + name + "\"");
  } else {
    PlaceFuncPart* part = PlaceFuncPart::partsMap[name];
    if (algParts.empty() && !part->canStart()) {
      throw PlaceFuncException("added placement algorithm part that"
				   " cannot start the chain");
    }
    algParts.push_back(part);
  }
}


bool PlaceFunc::isComplete() const {
  if (algParts.empty()) {
    return false;
  }
  return algParts.back()->canEnd();
}


/*
 * Goes from a FileSystemLocator to a vector of OSD integer
 * identifiers. Return code is 0 if all went well, non-zero indicates
 * an error.
 */
int PlaceFunc::execute(const OSDMap& osdMap,
		       const object_locator_t& locator,
		       const object_t& oid,
		       vector<int>& result) const
{
  if (!isComplete()) {
    return PlaceFunc::INCOMPLETE;
  }

  if (algParts.size() == 1) {
    int errorCode = (*algParts.begin())->wholeStep(osdMap, locator, oid, result);
    return errorCode;
  } else {
    vector<PlaceFuncPart*>::const_iterator it =
      algParts.begin();
    PlaceFuncPart::PartialData* partialData = NULL;

    // run first step
    int errorCode = (*it)->firstStep(osdMap, locator, oid, partialData);
    if (errorCode) goto early_out;
      
    // run intermediate steps
    for (++it; *it != algParts.back(); ++it) {
      PlaceFuncPart::PartialData* tempPartialData = NULL;
      errorCode = (*it)->oneStep(osdMap, partialData, tempPartialData);
      delete partialData;
      partialData = tempPartialData;
      if (errorCode) goto early_out;
    }

    // run final step
    errorCode = (*it)->lastStep(osdMap, partialData, result);

  early_out:
    if (partialData) {
      delete partialData;
    }
    return errorCode;
  }
}


// **************** PlaceFuncException ****************


ostream& operator<<(ostream& os, PlaceFuncException e) {
  os << e.message;
  return os;
}
