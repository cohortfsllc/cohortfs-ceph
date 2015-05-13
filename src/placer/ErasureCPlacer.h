// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_ERASURECPLACER_H
#define COHORT_ERASURECPLACER_H

#include <mutex>
#include "placer/Placer.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "osdc/ObjectOperation.h"

class AttachedErasureCPlacer;

class ErasureCPlacer : public Placer {
  friend AttachedErasureCPlacer;
  typedef Placer inherited;

protected:
  string erasure_plugin;
  map<string, string> erasure_params;
  uint64_t suggested_unit; // Specified by the user
  bufferlist place_text;
  vector<std::string> symbols;

protected:

  ErasureCPlacer()
	: Placer(ErasureCPlacerType) { }

public:

  virtual APlacerRef attach(CephContext* cct) const;

  virtual int update(const std::shared_ptr<const Placer>& pl);

  virtual void dump(Formatter *f) const;
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);
  virtual void encode(bufferlist& bl) const;

  friend PlacerRef ErasureCPlacerFactory(bufferlist::iterator& bl, uint8_t v);

  static PlacerRef create(CephContext *cct,
			  const string& name,
			  int64_t _suggested_width,
			  const string& erasure_plugin,
			  const string& erasure_params,
			  const string& place_text, const string& symbols,
			  std::stringstream& ss);

};

#endif // COHORT_ERASURECPLACER_H
