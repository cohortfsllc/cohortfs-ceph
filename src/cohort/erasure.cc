// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#include <boost/lexical_cast.hpp>
#include "erasure.h"

using boost::lexical_cast;
using boost::bad_lexical_cast;

static bool decode_type(const string& erasure_type,
			erasure_encoder& type,
			string& error_message)
{
  if (erasure_type == "no_erasure") {
    type = no_erasure;
  } else if (erasure_type == "reed_solomon_vandermonde") {
    type = reed_solomon_vandermonde;
  } else if (erasure_type == "reed_solomon_vandermonde_raid_6") {
    type = reed_solomon_vandermonde_raid_6;
  } else if (erasure_type == "reed_solomon_cauchy") {
    type = reed_solomon_cauchy;
  } else if (erasure_type == "liberation") {
    type = liberation;
  } else if (erasure_type == "blaum_roth") {
    type = blaum_roth;
  } else if (erasure_type == "liber8tion") {
    type = liber8tion;
  } else {
    error_message = erasure_type + " is not a valid erasure type.";
    return false;
  }

  return true;
}


bool erasure_params::fill_out(const string& erasure_type,
			      const int64_t data_blocks,
			      const int64_t code_blocks,
			      const int64_t word_size,
			      const int64_t packet_size,
			      const int64_t size,
			      erasure_params& params,
			      string& error_message)
{
  if (!decode_type(erasure_type, params.type, error_message))
    return false;

  if (params.type == no_erasure)
    return true;

  string cur;

  try {
    cur = "data_blocks";
    params.k = static_cast<int>(data_blocks);

    cur = "code_blocks";
    params.m = static_cast<int>(code_blocks);

    cur = "word_size";
    params.w = static_cast<int>(word_size);

    cur = "packet_size";
    params.packetsize = static_cast<int>(packet_size);

    cur = "size";
    params.size = static_cast<int>(size);
  } catch (bad_lexical_cast&) {
    error_message = cur + " is not a valid integer.";
    return false;
  }

  if (params.type == reed_solomon_vandermonde_raid_6 ||
      params.type == liberation ||
      params.type == blaum_roth ||
      params.type == liber8tion) {
    error_message = "For this erasure type, code_blocks must be 2.";
    return false;
  }

  if (params.type == liber8tion &&
      params.w != 8) {
    error_message = "Liber8tion requires a word_size of 8.";
    return false;
  }

  return true;
}
