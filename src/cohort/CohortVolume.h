// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_COHORTVOLUME_H
#define COHORT_COHORTVOLUME_H

#include "common/Mutex.h"
#include "vol/Volume.h"
#include "cohort/erasure.h"

/* Superclass of all Cohort volume types, supporting dynamically
   generated placement. */

class CohortVolume : public Volume {
  typedef Volume inherited;
protected:
  Mutex compile_lock;
  void compile(void);

  bufferlist place_text;
  void *place_shared;
  epoch_t compiled_epoch;
  vector<string> symbols;
  vector<void *> entry_points;
  erasure_params erasure;

  CohortVolume(const vol_type t, const string n,
	       const bufferlist &p,
	       const vector<string> &s) :
    Volume(t, n),
    compile_lock("CohortVolume::compile_lock"),
    place_text(p), place_shared(NULL),
    compiled_epoch(0),
    symbols(s), entry_points(symbols.size()) { }

public:

  ~CohortVolume();

  /* Signature subject to change */
  virtual int64_t place(const object_t& o,
			uint32_t rule,
			uint32_t replica) = 0;
  virtual uint32_t num_rules(void) = 0;

  virtual int update(VolumeCRef v);
};

#endif // COHORT_COHORTVOLUME_H
