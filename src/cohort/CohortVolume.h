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

typedef int(*place_func)(void*, const uuid_t, const char*,
			 const ceph_file_layout, bool(*)(void*, int),
			 bool(*)(void*, int));

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
  vector<place_func> entry_points;
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

  virtual uint32_t num_rules(void);

  virtual int place(const object_t& object,
		    const OSDMap& map,
		    const ceph_file_layout& layout,
		    vector<int>& osds);

  virtual int update(VolumeCRef v);
};

#endif // COHORT_COHORTVOLUME_H
