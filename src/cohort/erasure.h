// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_COHORTVOLUME_H
#define COHORT_COHORTVOLUME_H

/* Currently the set of encodings supported by Jerasure, but I reserve
   the right to add more in the future. */

enum encoders {
  reed_solomon_vandermonde, /* Classic Reed-Solomon with a
			       Vandermonde matrix, using arithmetic in
			       GF(2^w). */
  reed_solomon_vandermonde_raid_6, /* Vandermonde Reed-Solomon
				      optimized for RAID6
				      configurations (those with two
				      encoding devices.) */
  reed_solomon_cauchy, /* Xorrible Reed-Solomon, using Cauchy
			  matrices. */
  liberation, /* Word-size must be prime, m must be 2. */
  blaum_rothm, /* Word-size must be one less than a prime, m bust be 2. */
  liber8tion, /* Word-size must be 8, m must be 2. */
};

/* We go with just plain old 'int' for all these since it's what
   Jerasure uses. */

struct erasure_params {
  encoders type; /* Type of encoding to use */
  int k; /* Count of data stripes in a block/object */
  int m; /* Count of coding stripes in a block/object */
  int w; /* Word size */
  int packetsize; /* Size of packets (subdivisions of a stripe) */
  int size; /* Size of each stripe, in bytes. */
};

#endif /* COHORT_ERASURE_H */
