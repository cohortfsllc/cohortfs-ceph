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
#include "CohortVolume.h"

/* Epoch should be the current epoch of the OSDMap. */

void CohortVolume::compile(epoch_t epoch)
{
  Mutex::Locker l(compile_lock);
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

  unlink(scmfilename);

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
      = dlsym(place_shared, symbols[i].c_str());
  }
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

uint32_t num_rules(void)
{
  return entry_points.length();
}

struct placemenet_context
{
  OSDMap *map;
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
  if (context->map->is_in(osd))
    context->osds.push_back(osd);
  else
    return false;
}

int place(const object_t& object,
	  const OSDMap& map,
	  const ceph_file_layout& layout,
	  vector<int>& osds)
{
  placement_context context = {
    .map = &map,
    .osds = &osds
  };

  if ((compiled_epoch < last_update) || !place_shared) {
    compile();
  }

  if (layout.fl_rule_index >= entry_points.length) {
    return -1;
  }

  return entry_points[layout.fl_rule_index](&context, object.vol->uuid,
					    object.name.c_str(), &layout,
					    test_osd, return_osd);

}
