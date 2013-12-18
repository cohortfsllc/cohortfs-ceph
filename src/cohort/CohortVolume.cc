// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#include <cstring>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <dlfcn.h>
#include "osd/OSDMap.h"
#include "CohortVolume.h"

VolumeRef CohortVolFactory(bufferlist::iterator& bl, __u8 v, vol_type t)
{
  CohortVolume *vol = new CohortVolume(t);
  vol->common_decode(bl, v, t);
  return VolumeRef(vol);
}

/* Epoch should be the current epoch of the OSDMap. */

void CohortVolume::compile(epoch_t epoch)
{
  char cfilename[uuid_d::char_rep_buf_size + 5];
  char objfilename[uuid_d::char_rep_buf_size + 5];
  char sofilename[uuid_d::char_rep_buf_size + 5];

  pid_t child;

  uuid.print(cfilename);
  strcpy(objfilename, cfilename);
  strcpy(sofilename, cfilename);
  strcat(cfilename, ".c");
  strcat(objfilename, ".o");
  strcat(sofilename, ".so");

  const char *cargv[] = {
    [0] = "gcc", [1] = "-O3", [2] = "-fPIC",
    [3] = "-c", [4] = cfilename, [5] = "-o",
    [6] = objfilename, [7] = NULL
  };

  const char *largv[] = {
    [0] = "gcc", [1] = "-shared", [2] = "-o",
    [3] = "module.so", [4] = "module.o", [5] = "-lm",
    [6] = "-lc", [7] = NULL
  };

  if (compiled_epoch >= last_update) {
    return;
  }

  /* Better error handling, when we figure out what to do on
     error. Also figure out some directory we should be using,
     possibly under /var/lib.  Also come back and deal with
     concurrency.  We don't want to restrict this to a single thread
     but we don't want a placement function jumping through here while
     we're messing with it. */

  if (place_shared) {
    dlclose(place_shared); /* It's not like we can do anything on error. */
    place_shared = NULL;
  }

  place_text.write_file(cfilename);

  child = fork();
  if (!child) {
    execvp("gcc", (char **)cargv);
  } else {
  cretry:
    int status = 0;
    waitpid(child, &status, 0);
    if (!(WIFEXITED(status) || WIFSIGNALED(status))) {
      goto cretry;
    }
  }

  unlink(cfilename);

  if (!child) {
    execvp("gcc", (char **)largv);
  } else {
  lretry:
    int status = 0;
    waitpid(child, &status, 0);
    if (!(WIFEXITED(status) || WIFSIGNALED(status))) {
      goto lretry;
    }
  }

  unlink(objfilename);
  place_shared = dlopen(sofilename, RTLD_LAZY | RTLD_GLOBAL);

  unlink(sofilename);

  for(vector<string>::size_type i = 0;
      i < symbols.size();
      ++i) {
    entry_points[i]
      = (place_func) dlsym(place_shared, symbols[i].c_str());
  }

  compiled_epoch = epoch;
}

CohortVolume::~CohortVolume(void)
{
  if (place_shared) {
    dlclose(place_shared); /* It's not like we can do anything on error. */
    place_shared = NULL;
  }
}

int CohortVolume::update(VolumeCRef v)
{
  return 0;
}

uint32_t CohortVolume::num_rules(void)
{
  return entry_points.size();
}

struct placement_context
{
  const OSDMap *map;
  vector<int> *osds;
};


/* Return 'true' if the OSD is marked as 'in' */

static bool test_osd(void *data, int osd)
{
  placement_context *context = (placement_context *)data;
  return context->map->is_in(osd);
}

/* This function adds an OSD to the list returned to the client ONLY
   if the OSD is marked in. */

static bool return_osd(void *data, int osd)
{
  placement_context *context = (placement_context *)data;
  if (context->map->is_in(osd)) {
    context->osds->push_back(osd);
    return true;
  }

  return false;
}

int CohortVolume::place(const object_t& object,
			const OSDMap& map,
			const ceph_file_layout& layout,
			vector<int>& osds)
{
  placement_context context = {
    .map = &map,
    .osds = &osds
  };

  compile_lock.get_read();
  if ((compiled_epoch < last_update) || !place_shared) {
    compile_lock.unlock();
    compile_lock.get_write();
    compile(map.get_epoch());
  }

  if (layout.fl_rule_index >= entry_points.size()) {
    return -1;
  }

  int rc = entry_points[layout.fl_rule_index](&context, object.volume.uuid,
					      object.name.c_str(), &layout,
					      test_osd, return_osd);
  compile_lock.unlock();

  return rc;
}

void CohortVolume::common_decode(bufferlist::iterator& bl,
				 __u8 v, vol_type t)
{
  inherited::common_decode(bl, v, t);

  ::decode(place_text, bl);
  uint32_t count;
  ::decode(count, bl);
  symbols.reserve(count);
  for(uint32_t i = 0; i < count; ++i) {
    string symbol;
    ::decode(symbol, bl);
    symbols[i] = symbol;
  }
  entry_points.reserve(count);
}

void CohortVolume::common_encode(bufferlist& bl) const
{
  inherited::common_encode(bl);

  ::encode(place_text, bl);
  ::encode(symbols, bl);

  uint32_t count = symbols.size();
  ::encode(count, bl);
  for(uint32_t i = 0; i < count; ++i) {
    ::encode(symbols[i], bl);
  }
}
