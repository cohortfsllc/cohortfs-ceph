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
#include "cohort/erasure.h"
#include "common/RWLock.h"

/* Superclass of all Cohort volume types, supporting dynamically
   generated placement. */

class CohortVolume : public Volume
{
  typedef Volume inherited;

protected:
  RWLock compile_lock;
  void compile(epoch_t epoch);

  bufferlist place_text;
  void *place_shared;
  epoch_t compiled_epoch;
  vector<string> symbols;
  vector<void*> entry_points;
  erasure_params erasure;

  CohortVolume(vol_type t)
    : Volume(t), compile_lock("CohortVolume::compile_lock"),
      place_text(), place_shared(NULL),
      compiled_epoch(0),
      symbols(), entry_points() { }

  public:

  ~CohortVolume();

  virtual uint32_t num_rules(void);

  virtual int place(const object_t& object,
		    const OSDMap& map,
		    const unsigned int rule_index,
		    vector<int>& osds);

  virtual int update(VolumeCRef v);

  virtual void common_decode(bufferlist::iterator& bl,
			     __u8 v, vol_type t);
  virtual void common_encode(bufferlist& bl) const;
  virtual void encode(bufferlist& bl) const {
    common_encode(bl);
  }

  friend VolumeRef CohortVolFactory(bufferlist::iterator& bl, __u8 v,
				    vol_type t);

  static VolumeRef create(const string& name, const epoch_t last_update,
			  const string& place_text, const string& symbols,
			  const string& erasure_type,
			  const string& data_blocks, const string& code_blocks,
			  const string& word_size, const string& packet_size,
			  const string& size, string& error_message);

};

#endif // COHORT_COHORTVOLUME_H
