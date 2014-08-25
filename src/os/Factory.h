// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef OS_FACTORY_H
#define OS_FACTORY_H

#include "ObjectStore.h"

namespace ceph {

  typedef void* (*objectstore_dllinit)(void);
  typedef ObjectStore* (*objectstore_factory_method)(CephContext* cct,
						     const string& type,
						     const string& data,
						     const string& journal);

  const char* OBJECTSTORE_INIT_FUNC = "objectstore_dllinit";

  class ObjectStoreFactory
  {
  public:

    /**
     * create - create an ObjectStore instance.
     *
     * This is invoked once at initialization time.
     *
     * @param type type of store. This is a string from the configuration file.
     * @param data path (or other descriptor) for data
     * @param journal path (or other descriptor) for journal (optional)
     */
    ObjectStore* factory(CephContext* cct,
			 const string& type,
			 const string& data,
			 const string& journal);

  };

} /* namespace ceph */

#endif /* OS_FACTORY_H */
