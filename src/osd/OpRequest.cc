// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "OpRequest.h"

#if OPREQUEST_FREELIST
OpRequest::Alloc OpRequest::alloc;
OpRequest::FreeList OpRequest::freelist(OPREQUEST_FREELIST, OpRequest::alloc);
#endif
