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

void CohortVolume::compile(void)
{
  Mutex::Locker l(compile_lock);
  char scmfilename[uuid_d::char_rep_buf_size + 5];
  char objfilename[uuid_d::char_rep_buf_size + 5];
  char sofilename[uuid_d::char_rep_buf_size + 5];

  pid_t child;

  const char *bargv[] = {
    [0] = "bigloo", [1] = "-O3", [2] = "-fcfa-arithmetic",
    [3] = "-q", [4] = "-copt", [5] = "-fPIC",
    [6] = scmfilename, [7] = "-c", [8] = "-dload-init-sym",
    [9] = "module_init", [10] = NULL
  };

  const char *cargv[] = {
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

  uuid.print(scmfilename);
  strcpy(objfilename, scmfilename);
  strcpy(sofilename, scmfilename);
  strcat(scmfilename, ".scm");
  strcat(objfilename, ".o");
  strcat(sofilename, ".so");
  place_text.write_file(scmfilename);

  child = fork();
  if (!child) {
    execvp("bigloo", (char **)bargv);
  } else {
  bretry:
    int status = 0;
    waitpid(child, &status, 0);
    if (!(WIFEXITED(status) || WIFSIGNALED(status))) {
      goto bretry;
    }
  }

  unlink(scmfilename);

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

void CohortVolume::encode(bufferlist &bl) const
{
  inherited::encode(bl);
  ::encode(place_text, bl);
}

void CohortVolume::decode(bufferlist::iterator& bl)
{
  inherited::decode(bl);
  ::decode(place_text, bl);
}
