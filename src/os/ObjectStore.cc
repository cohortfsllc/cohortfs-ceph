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
 * Foundation.	See file COPYING.
 *
 */
#include <ctype.h>
#include <sstream>
#include "ObjectStore.h"
#include "Transaction.h"
#include "common/Formatter.h"
#include "common/safe_io.h"

using namespace ceph::os;

void ObjectStore::C_DeleteTransaction::finish(int r) { delete t; }

int ObjectStore::write_meta(const std::string& key,
			    const std::string& value)
{
  string v = value;
  v += "\n";
  int r = safe_write_file(path.c_str(), key.c_str(),
			  v.c_str(), v.length());
  if (r < 0)
    return r;
  return 0;
}

int ObjectStore::read_meta(const std::string& key,
			   std::string* value)
{
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
			 buf, sizeof(buf));
  if (r <= 0)
    return r;
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  *value = string(buf, r);
  return 0;
}

unsigned ObjectStore::apply_transactions(list<Transaction*> &tls,
					 Context *ondisk)
{
  // use op pool
  std::condition_variable my_cond;
  std::mutex my_lock;
  int r = 0;
  bool done;
  C_SafeCond* onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);

  queue_transactions(tls, onreadable, ondisk);

  std::unique_lock<std::mutex> myl(my_lock);
  while (!done)
    my_cond.wait(myl);
  myl.unlock();
  return r;
}

int ObjectStore::queue_transactions(list<Transaction*>& tls,
                                    Context *onreadable, Context *ondisk,
                                    Context *onreadable_sync,
                                    OpRequestRef op)
{
  assert(!tls.empty());
  C_GatherBuilder g_onreadable(onreadable);
  C_GatherBuilder g_ondisk(ondisk);
  C_GatherBuilder g_onreadable_sync(onreadable_sync);
  for (list<Transaction*>::iterator i = tls.begin(); i != tls.end(); ++i) {
    if (onreadable)
      (*i)->register_on_applied(g_onreadable.new_sub());
    if (ondisk)
      (*i)->register_on_commit(g_ondisk.new_sub());
    if (onreadable_sync)
      (*i)->register_on_applied_sync(g_onreadable_sync.new_sub());
  }
  if (onreadable)
    g_onreadable.activate();
  if (ondisk)
    g_ondisk.activate();
  if (onreadable_sync)
    g_onreadable_sync.activate();
  return queue_transactions(tls, op);
}

int ObjectStore::queue_transactions(list<Transaction*>& tls,
                                    Context *onreadable, Context *oncommit,
                                    Context *onreadable_sync,
                                    Context *oncomplete,
                                    OpRequestRef op = OpRequestRef())
{
  RunOnDeleteRef _complete(new RunOnDelete(oncomplete));
  Context *_onreadable = new Wrapper<RunOnDeleteRef>(onreadable, _complete);
  Context *_oncommit = new Wrapper<RunOnDeleteRef>(oncommit, _complete);
  return queue_transactions(tls, _onreadable, _oncommit, onreadable_sync, op);
}

int ObjectStore::collection_list(CollectionHandle ch,
				 vector<hoid_t>& o)
{
  vector<hoid_t> go;
  int ret = collection_list(ch, go);
  if (ret == 0) {
    o.reserve(go.size());
    for (vector<hoid_t>::iterator i = go.begin(); i != go.end() ; ++i)
      o.push_back(*i);
  }
  return ret;
}

int ObjectStore::collection_list_partial(CollectionHandle ch,
					 hoid_t start,
					 int min, int max,
					 vector<hoid_t>* ls,
					 hoid_t* next)
{
  vector<hoid_t> go;
  hoid_t gnext, gstart(start);
  int ret = collection_list_partial(ch, gstart, min, max, &go, &gnext);
  if (ret == 0) {
    *next = gnext;
    ls->reserve(go.size());
    for (vector<hoid_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(*i);
  }
  return ret;
}

int ObjectStore::collection_list_range(CollectionHandle ch,
				       hoid_t start,
				       hoid_t end,
				       vector<hoid_t>* ls)
{
  vector<hoid_t> go;
  hoid_t gstart(start), gend(end);
  int ret = collection_list_range(ch, gstart, gend, &go);
  if (ret == 0) {
    ls->reserve(go.size());
    for (vector<hoid_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(*i);
  }
  return ret;
}
