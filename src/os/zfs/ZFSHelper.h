// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef COHORT_ZFSHELPER_H
#define COHORT_ZFSHELPER_H

#include <iostream>
#include <vector>
#include <map>
#include <tuple>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

namespace cohort_zfs {

  // name, type (""), list of device paths
  typedef std::tuple<std::string, std::string,
		     std::vector<std::string>> zp_desc_type;

  typedef std::map<std::string, zp_desc_type> zp_desc_map;

  typedef std::vector<boost::iterator_range<std::string::iterator>>
  fv_type;

  void print_zp_desc_map(const zp_desc_map& zpm)
  {
    using std::get;
    for (auto& it : zpm) {
      const std::string& pool_name = it.first;
      std::cout << "zpool: " << pool_name
		<< " type: " << get<1>(it.second)
		<< " paths:";
      const std::vector<std::string>& devs = get<2>(it.second);
      for (auto& dev_iter : devs) {
	std::cout << " " << dev_iter;
      }
      std::cout << std::endl;
    }
  }

  int parse_zp_desc(std::string desc, zp_desc_map& zpm) {

    namespace ba = boost::algorithm;

    zpm.clear();

    fv_type fv_outer;
    ba::split(fv_outer, desc, ba::is_any_of(";"));
    for (auto& oit : fv_outer) {
      fv_type fv_inner;
      ba::split(fv_inner, oit, ba::is_any_of(","));
      if (fv_inner.size() >= 3) {
	std::vector<std::string> dev_paths;
	std::string name{fv_inner[1].begin(), fv_inner[1].end()};
	std::string type{fv_inner[0].begin(), fv_inner[0].end()};
	fv_type::iterator dev_iter = fv_inner.begin() + 2;
	for (; dev_iter != fv_inner.end(); ++dev_iter) {
	  dev_paths.push_back(std::string{dev_iter->begin(),
		dev_iter->end()});
	}
	zpm.insert(zp_desc_map::value_type{
	    name, zp_desc_type{name, type, dev_paths}});
      }
    }

    return zpm.size();;
  }

} /* cohort_zfs */

#endif /* COHORT_ZFSHELPER_H */
