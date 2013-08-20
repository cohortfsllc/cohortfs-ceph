// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#include <dlfcn.h>
#include <stdlib.h>
#include "CohortVolume.h"

void CohortVolume::compile(void)
{
}

CohortVolume::~CohortVolume(void)
{
  if (place_shared) {
    dlclose(place_shared); /* It's not like we can do anything on error. */
    place_shared = NULL;
  }
}

void CohortVolume::encode(bufferlist &bl) const
{
  inherited::encode(bl);
  ::encode(place_text, bl);
}

void CohortVolume::decode(bufferlist::iterator& bl)
{
  inherited::decode(bl);
  ::decode(place_text, bl);
}
