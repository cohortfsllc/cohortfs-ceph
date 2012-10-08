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


class PlacementFunc {
  class PlacementFuncException {

    friend PlacementFunc;

    const string message;

    PlacementFuncException(const string& message_p)
    : message(message_p)
    {
      // empty
    }
  }; // class PlacementFuncException


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



  class PlacementFuncPart {

    friend PlacementFunc;

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
    virtual PartialData* firstStep(const FileSystemLocator& locator) {
      throw PlacementFuncException("unimplemented");
      return new PartialData();
    }

    virtual void wholeStep(const FileSystemLocator& locator, vector<int>& result) {
      throw PlacementFuncException("unimplemented");
    }

    // PartialData* returned will be deleted by caller as will
    // PartialData* passed in
    virtual PartialData* oneStep(const PartialData* partial) {
      throw PlacementFuncException("unimplemented");
      return new PartialData();
    }

    // PartialData* passed in will be deleted by caller
    virtual void lastStep(const PartialData* partial, vector<int>& result) {
      throw PlacementFuncException("unimplemented");
    }
  }; // class PlacementFunc


private:

  vector<PlacementFuncPart*> algParts;

public:

  PlacementFunc() {
    // empty
  }

  ~PlacementFunc() {
    for (vector<PlacementFuncPart*>::iterator it = algParts.begin();
	 it != algParts.end();
	 ++it) {
      algParts.erase(it);
      delete *it;
    }
  }

  void addFuncPart(const string& name) {
    if (0 == PlacementFuncPart::partsMap.count(name)) {
      throw PlacementFuncException("tried to add unknown placement"
				   " functional partial \"" + name + "\"");
    } else {
      PlacementFuncPart* part = PlacementFuncPart::partsMap[name];
      if (algParts.empty() && !part->canStart()) {
	throw PlacementFuncException("added placement algorithm part that"
				    " cannot start the chain");
      }
      algParts.push_back(part);
    }
  }

  bool isComplete() const {
    if (algParts.empty()) {
      return false;
    }
    return algParts.back()->canEnd();
  }

  void execute(const FileSystemLocator& locator, vector<int>& result) const;
};

#endif // CEPH_PLACEALG_H
