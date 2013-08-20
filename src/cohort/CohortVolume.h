// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_COHORTVOLUME_H
#define COHORT_COHORTVOLUME_H

#include "vol/Volume.h"

/* Superclass of all Cohort volume types, supporting dynamically
   generated placement. */

class CohortVolume : public Volume {
  typedef Volume inherited;
protected:
  void compile(void);

  bufferlist place_text;
  void *place_shared;

  CohortVolume(const vol_type t, const string n,
	       const bufferlist &p) :
    Volume(t, n),
    place_text(p), place_shared(NULL) { }

public:

  ~CohortVolume();

  virtual void encode(bufferlist& bl) const;
  virtual void decode(bufferlist::iterator& bl);

  /* Signature subject to change */
  virtual int64_t place(const object_t& o,
			uint32_t rule,
			uint32_t replica) = 0;
  virtual uint32_t num_rules(void) = 0;

  virtual bool valid(string& error);
  virtual int update(VolumeCRef v);
};

#endif // COHORT_COHORTVOLUME_H
