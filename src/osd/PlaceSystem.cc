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


template<class T>
std::map<std::string,T*>* PlaceSystemBase<T>::nameMap = NULL;

template<class T>
std::map<__u16,T*>* PlaceSystemBase<T>::identifierMap = NULL;

int force_template_invocation() {
  int count = 0;
  if (PlaceSystemBase<OSD>::nameMap == NULL) {
    ++count;
  }
  if (PlaceSystemBase<OSDMap>::nameMap == NULL) {
    ++count;
  }
  if (PlaceSystemBase<OSDMonitor>::nameMap == NULL) {
    ++count;
  }
  return count;
}
