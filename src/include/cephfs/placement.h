// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015, CohortFS, LLC <info@cohortfs.com>
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 *
 */

#ifndef C_PLACEMENT
#define C_PLACEMENT

enum placer_type {
  NotAPlacerType = 0,
  ErasureCPlacerType,
  StripedPlacerType,
  MaxPlacerType
};


struct striped_placer {
  uint32_t	stripe_unit;
  uint32_t	stripe_width;
};

struct erasure_c_placer {
  uint32_t	placeholder;
};

struct cohort_placer {
  enum placer_type	type;
  uint8_t		volume_id[16];
  union {
    struct striped_placer	striped;
    struct erasure_c_placer	erasure;
  };
};

#endif /* C_PLACEMENT */
