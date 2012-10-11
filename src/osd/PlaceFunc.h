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

#include <string>
#include <vector>
#include <map>


using namespace std;

class PlaceFuncPart;


class PlaceFunc {

public:

  enum ResultCodes {
    UNIMPLEMENTED = 11264, // find better starting point
    INCOMPLETE,
  };

  class FileSystemLocator {
  private:
    int64_t volNum; // can also be pool number
    object_t oid;
  public:
    FileSystemLocator(int64_t volNum_p, object_t oid_p)
      : volNum(volNum_p), oid(oid_p)
    { }
    /*
      FileSystemLocator(int64_t volNum_p,
      inodeno_t inodeNo, frag_t frag, const char* suffix)
      : volNum(volNum_p),
      oid(CInode::get_object_name(inodeNo, frag, suffix))
      { }
    */
  }; // class FileSystemLocator

private:

  vector<PlaceFuncPart*> algParts;

public:

  PlaceFunc() {
    // empty
  }

  ~PlaceFunc();


  bool isComplete() const;
  int execute(const FileSystemLocator& locator, vector<int>& result) const;

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
  virtual int firstStep(const PlaceFunc::FileSystemLocator& locator,
			PartialData*& resultData) {
    return PlaceFunc::UNIMPLEMENTED;
  }

  virtual int wholeStep(const PlaceFunc::FileSystemLocator& locator,
			vector<int>& result) {
    return PlaceFunc::UNIMPLEMENTED;
  }

  // PartialData* returned will be deleted by caller as will
  // PartialData* passed in
  virtual int oneStep(const PartialData* inData, PartialData*& outData) {
    return PlaceFunc::UNIMPLEMENTED;
  }

  // PartialData* passed in will be deleted by caller
  virtual int lastStep(const PartialData* partial, vector<int>& result) {
    return PlaceFunc::UNIMPLEMENTED;
  }
}; // PlaceFuncPart


#endif // CEPH_PLACEALG_H
