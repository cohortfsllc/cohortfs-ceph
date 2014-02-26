// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#include <boost/lexical_cast.hpp>
#include "boost/assign.hpp"
#include "erasure.h"

using boost::lexical_cast;
using boost::bad_lexical_cast;
using namespace boost::assign;

static map <erasure_encoder, string> type_map = map_list_of
  ( no_erasure, "no_erasure" )
  ( reed_solomon_vandermonde, "reed_solomon_vandermonde" )
  ( reed_solomon_vandermonde_raid_6, "reed_solomon_vandermonde_raid_6" )
  ( reed_solomon_cauchy, "reed_solomon_cauchy" )
  ( liberation, "liberation" )
  ( blaum_roth, "blaum_roth" )
  ( liber8tion, "liber8tion" );

static bool decode_type(const string& erasure_type,
			erasure_encoder& type,
			string& error_message)
{
  for (map<erasure_encoder, string>::const_iterator p = type_map.begin();
       p != type_map.end();
       ++p) {
    if (erasure_type == p->second) {
      type = p->first;
      return true;
    }
  }
  error_message = erasure_type + " is not a valid erasure type.";
  return false;
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

void erasure_params::encode(bufferlist& bl) const
{
  __u8 v = 1;
  ::encode(v, bl);
  uint64_t utype = (uint64_t)type;
  ::encode(utype, bl);
  ::encode(k, bl);
  ::encode(m, bl);
  ::encode(w, bl);
  ::encode(packetsize, bl);
  ::encode(size, bl);
}

void erasure_params::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  uint64_t utype;
  ::decode(utype, bl);
  type = (erasure_encoder)utype;
  ::decode(k, bl);
  ::decode(m, bl);
  ::decode(w, bl);
  ::decode(packetsize, bl);
  ::decode(size, bl);
}

ostream& operator<<(ostream& out, const erasure_params& erasure)
{
  out << "type " << type_map[erasure.type]
      << " data_blocks " << erasure.k
      << " code_blocks " << erasure.m
      << " word_size " << erasure.w
      << " packet_size " << erasure.packetsize
      << " size " << erasure.size;
  return out;
}
