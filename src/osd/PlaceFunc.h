// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 */


#ifndef CEPH_PLACEFUNC_H
#define CEPH_PLACEFUNC_H

#include <inttypes.h>

#include "include/types.h"

#include "mds/mdstypes.h"

#include "osd/osd_types.h"
// #include "osd/OSDMap.h"

#include <string>
#include <vector>
#include <map>


using namespace std;


class OSDMap;
class PlaceFuncPart;


class PlaceFunc {

public:

  enum ResultCodes {
    UNIMPLEMENTED = 11264, // find better starting point
    INCOMPLETE,
  };

private:

  vector<PlaceFuncPart*> algParts;

public:

  PlaceFunc() {
    // empty
  }

  ~PlaceFunc();


  bool isComplete() const;

  int execute(const OSDMap& osdMap,
	      const object_locator_t& locator,
	      const object_t& oid,
	      vector<int>& result) const;

  void addFuncPart(const string& name);

}; // class PlaceFunc


class PlaceFuncException {

  friend PlaceFunc;
  friend ostream& operator<<(ostream&, PlaceFuncException);

private:

  const string message;

public:

  PlaceFuncException(const string& message_p)
  : message(message_p)
  {
    // empty
  }
}; // class PlaceFuncException


class PlaceFuncPart {

  friend PlaceFunc;

public:

  /*
   * Base class for all types of partials -- partially computed
   * placements. All others are expected to extend this class either
   * directly or indirectly.
   */
  class PartialData {
  public:
    PartialData() {}
    virtual ~PartialData() {}
  }; // class PartialData


  class OpaquePartialData : public PartialData {
  private:
    char* buffer;
    int size;

    void initBuffer(const char* b, int s) {
      size = s;
      buffer = new char[s];
      memmove(buffer, b, s);
    }

  public:

    OpaquePartialData(const string& data)
    {
      initBuffer(data.c_str(), data.length());
    }

    OpaquePartialData(const char* data, int size_p)
    {
      initBuffer(data, size_p);
    }

    ~OpaquePartialData() {
      delete[] buffer;
    }

    int getSize() { return size; }
    const char* getBuffer() { return buffer; }
  }; // class OpaquePartialData


private:

  const string name;

  static map<string,PlaceFuncPart*> partsMap;

public:

  PlaceFuncPart(const string& name_p)
  : name(name_p)
  {
    partsMap[name_p] = this;
  }

  virtual ~PlaceFuncPart() {
    partsMap.erase(name);
  }

  virtual bool canStart() const = 0;
  virtual bool canEnd() const = 0;

  // PartialData* returned will be deleted by caller
  virtual int firstStep(const OSDMap& osdMap,
			const object_locator_t& locator,
			const object_t& oid,
			PartialData*& resultData) {
			
    return PlaceFunc::UNIMPLEMENTED;
  }

  virtual int wholeStep(const OSDMap& osdMap,
			const object_locator_t& locator,
			const object_t& oid,
			vector<int>& result) {
    return PlaceFunc::UNIMPLEMENTED;
  }

  // PartialData* returned will be deleted by caller as will
  // PartialData* passed in
  virtual int oneStep(const OSDMap& osdMap,
		      const PartialData* inData,
		      PartialData*& outData) {
    return PlaceFunc::UNIMPLEMENTED;
  }

  // PartialData* passed in will be deleted by caller
  virtual int lastStep(const OSDMap& osdMap,
		       const PartialData* partial,
		       vector<int>& result) {
    return PlaceFunc::UNIMPLEMENTED;
  }
}; // PlaceFuncPart


#endif // CEPH_PLACEALG_H
