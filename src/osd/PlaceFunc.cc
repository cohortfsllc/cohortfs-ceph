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


// STATIC VARIABLES


map<string,PlacementFuncPart*> PlacementFuncPart::partsMap;


// PLACEMENTFUNC FUNCTIONS


PlacementFunc::~PlacementFunc() {
  for (vector<PlacementFuncPart*>::iterator it = algParts.begin();
       it != algParts.end();
       ++it) {
    algParts.erase(it);
    delete *it;
  }
}


void PlacementFunc::addFuncPart(const string& name) {
  if (0 == PlacementFuncPart::partsMap.count(name)) {
    throw PlacementFuncException("tried to add unknown placement"
				 " functional partial \"" + name + "\"");
  } else {
    PlacementFuncPart* part = PlacementFuncPart::partsMap[name];
    if (algParts.empty() && !part->canStart()) {
      throw PlacementFuncException("added placement algorithm part that"
				   " cannot start the chain");
    }
    algParts.push_back(part);
  }
}


bool PlacementFunc::isComplete() const {
  if (algParts.empty()) {
    return false;
  }
  return algParts.back()->canEnd();
}


void PlacementFunc::execute(const FileSystemLocator& locator,
			    vector<int>& result) const {
  if (!isComplete()) {
    throw PlacementFuncException("tried to execute an incomplete"
				 " placement function");
  }

  if (algParts.size() == 1) {
    (*algParts.begin())->wholeStep(locator, result);
  } else {
    vector<PlacementFuncPart*>::const_iterator it =
      algParts.begin();
    PlacementFuncPart::PartialData* partialResult =
      (*it)->firstStep(locator);
    ++it;
    for ( ; *it != algParts.back(); ++it) {
      PlacementFuncPart::PartialData* tempPartialResult =
	(*it)->oneStep(partialResult);
      delete partialResult;
      partialResult = tempPartialResult;
    }
    (*it)->lastStep(partialResult, result);
    delete partialResult;
  }
}


// PLACEMENTFUNCEXCEPTION FUNCTIONS


ostream& operator<<(ostream& os, PlacementFuncException e) {
  os << e.message;
  return os;
}
