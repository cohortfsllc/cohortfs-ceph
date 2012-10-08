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

map<string,PlacementFunc::PlacementFuncPart*> PlacementFunc::PlacementFuncPart::partsMap;


void PlacementFunc::execute(const FileSystemLocator& locator, vector<int>& result) const {
  if (!isComplete()) {
    throw PlacementFuncException("tried to execute an incomplete placement function");
  }

  if (algParts.size() == 1) {
    (*algParts.begin())->wholeStep(locator, result);
  } else {
    vector<PlacementFuncPart*>::const_iterator it = algParts.begin();
    PartialData* partial = (*it)->firstStep(locator);
    ++it;
    for ( ; *it != algParts.back(); ++it) {
      PartialData* newPartial = (*it)->oneStep(partial);
      delete partial;
      partial = newPartial;
    }
    (*it)->lastStep(partial, result);
    delete partial;
  }
}
