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

class PlacementFuncPart;


class PlacementFunc {


private:

  vector<PlacementFuncPart*> algParts;

public:

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


  PlacementFunc() {
    // empty
  }

  ~PlacementFunc();


  bool isComplete() const;
  void execute(const FileSystemLocator& locator, vector<int>& result) const;

  void addFuncPart(const string& name);

}; // class PlacementFunc


class PlacementFuncException {

  friend PlacementFunc;
  friend ostream& operator<<(ostream&, PlacementFuncException);

private:

  const string message;

public:

  PlacementFuncException(const string& message_p)
  : message(message_p)
  {
    // empty
  }
}; // class PlacementFuncException


class PlacementFuncPart {

  friend PlacementFunc;

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

  static map<string,PlacementFuncPart*> partsMap;

public:

  PlacementFuncPart(const string& name_p)
  : name(name_p)
  {
    partsMap[name_p] = this;
  }

  virtual ~PlacementFuncPart() {
    partsMap.erase(name);
  }

  virtual bool canStart() const = 0;
  virtual bool canEnd() const = 0;

  // PartialData* returned will be deleted by caller
  virtual PartialData* firstStep(const PlacementFunc::FileSystemLocator& locator) {
    throw PlacementFuncException("unimplemented");
    return NULL;
  }

  virtual void wholeStep(const PlacementFunc::FileSystemLocator& locator,
			 vector<int>& result) {
    throw PlacementFuncException("unimplemented");
  }

  // PartialData* returned will be deleted by caller as will
  // PartialData* passed in
  virtual PartialData* oneStep(const PartialData* partial) {
    throw PlacementFuncException("unimplemented");
    return NULL;
  }

  // PartialData* passed in will be deleted by caller
  virtual void lastStep(const PartialData* partial, vector<int>& result) {
    throw PlacementFuncException("unimplemented");
  }
}; // PlacementFuncPart


#endif // CEPH_PLACEALG_H
