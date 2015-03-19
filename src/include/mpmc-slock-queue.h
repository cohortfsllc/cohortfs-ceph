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

#ifndef COHORT_SLOCK_QUEUE_H
#define COHORT_SLOCK_QUEUE_H

#include <thread>
#include <array>
#include <vector>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <boost/intrusive/list.hpp>
#include "common/likely.h"

namespace cohort {

  namespace slock_queue {

    namespace bi = boost::intrusive;
    typedef bi::link_mode<bi::safe_link> link_mode;

    template <typename T>
    class Object {
    public:
      bi::list_member_hook<link_mode> q_hook;      
      T data;

      typedef bi::list<Object,
		       bi::member_hook<Object,
				       bi::list_member_hook<link_mode>,
				       &Object::q_hook>,
		       bi::constant_time_size<true>> Queue;

    };

    template <typename T>
    class Queue {
    public:
      std::mutex mtx;
      std::condition_variable cv;
      typename Object<T>::Queue q;
     
    };

  } // namespace slock_queue
} // namespace cohort

#endif // COHORT_SLOCK_QUEUE_H
