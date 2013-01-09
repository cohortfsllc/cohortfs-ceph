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


#include "PGPlaceSystem.h"


const std::string PGPlaceSystem::systemName = "pg+crush";
const __u16 PGPlaceSystem::systemIdentifier = 0x07b3; // lower 4 bits of 'p', 'g', '+', & 'c'
