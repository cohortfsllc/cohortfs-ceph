// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <unistd.h>

#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeExample.h"

class ErasureCodePluginExample : public ErasureCodePlugin {
public:
  virtual int factory(const map<std::string,std::string> &parameters,
		      ErasureCodeInterfaceRef *erasure_code)
  {
    *erasure_code = ErasureCodeInterfaceRef(new ErasureCodeExample());
    return 0;
  }
};

int __erasure_code_init(char *plugin_name)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginExample());
}
