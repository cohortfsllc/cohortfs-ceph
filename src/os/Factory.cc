// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <dlfcn.h>

#include <string>
#include <map>

#include "common/Mutex.h"
#include "Factory.h"

using namespace std;

namespace ceph {

  static Mutex mtx;
  static map<string, ObjectStoreFactory*> modules;

  ObjectStore* ObjectStoreFactory::factory(CephContext* cct,
					   const string& name,
					   const string& data,
					   const string& journal)
  {
    Mutex::Locker lock(mtx);

    ObjectStore* os = NULL;
    ObjectStoreFactory* factory = NULL;

    // cached?
    auto iter = modules.find(name);
    if (iter != modules.end())
      return iter->second->factory(cct, name, data, journal);

    // ok, try the file system

    // try as a full path
    string path = name;
    void* module = ::dlopen(path.c_str(), RTLD_NOW);
    if (! module) {
      // try relative
      path = cct->_conf->osd_module_dir + name + ".so";
      module = ::dlopen(path.c_str(), RTLD_NOW);
      if (! module)
	return NULL;
    }

    objectstore_dllinit dllinit = (objectstore_dllinit)
      dlsym(module, OBJECTSTORE_INIT_FUNC);

    if (! dllinit)
      goto out;

    factory = reinterpret_cast<ObjectStoreFactory*>(dllinit());
    if (!! factory) {
      modules[name] = factory;
      os = factory->factory(cct, name, data, journal);
    }

  out:
    return os;
  }

} /* namespace ceph */
