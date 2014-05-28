// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_COHORTVOLUME_H
#define COHORT_COHORTVOLUME_H

#include "vol/Volume.h"
#include "cohort/erasure.h"
#include "common/RWLock.h"

struct writestripe;

/* Superclass of all Cohort volume types, supporting dynamically
   generated placement. */

class CohortVolume : public Volume
{
  typedef Volume inherited;

protected:
  RWLock compile_lock;
  void compile(epoch_t epoch);

  bufferlist place_text;
  vector<std::string> symbols;
  erasure_params erasure;

  /* These are internal and are not serialized */
  vector<void*> entry_points;
  epoch_t compiled_epoch;
  void *place_shared;

  CohortVolume(vol_type t)
    : Volume(t), compile_lock("CohortVolume::compile_lock"),
      place_text(), symbols(),
      entry_points(),
      compiled_epoch(0),
      place_shared(NULL) { }

  void make_stripes(uint64_t off, unsigned len,
		    bufferlist &blin,
		    vector<writestripe> &stripes);

  size_t ostripe(const uint64_t off) {
    return (off / erasure.size) % erasure.k;
  }
  uint64_t stripe_offset(const uint64_t off) {
    uint64_t sw = erasure.k * erasure.size;
    return (off / sw) * erasure.size + off % erasure.size;
  }

  /* I protest having to use these types of failure. One day we need
     to remove all the 'le64' and 'u32' and such from the Cohort
     versions of Ceph. */

  void stripe_extent(const uint64_t off, const uint64_t len,
		     const size_t stripe, ceph_le64 &stripeoff,
		     ceph_le64 &stripelen) {
    size_t first = ostripe(off);
    size_t span = osd_span(off, len);
    if ((span == 0 && !first) ||
	((span < erasure.k) &&
	 (stripe > ((first + span - 1) % erasure.k)))) {
      stripeoff = 0;
      stripelen = 0;
      return;
    }
    uint64_t last_byte = len ? off + len - 1 : 0;
    size_t last = ostripe(last_byte);
    uint64_t stripe_last_byte;

    if (first == stripe) {
      stripeoff = stripe_offset(off);
    } else {
      stripeoff = stripe_offset(
	(off / erasure.size) * erasure.size + erasure.size *
	(stripe > first ? stripe - first : erasure.k - first + stripe));
    }

    if (len == 0) {
      stripelen = 0;
      return;
    }

    if (last == stripe) {
      stripe_last_byte = stripe_offset(last_byte);
    } else {
      uint64_t filled_byte = (last_byte + 1) % erasure.size == 0 ?
	last_byte : (last_byte / erasure.size + 1) * erasure.size - 1;
      stripe_last_byte = stripe_offset(
	filled_byte - erasure.size *
	(stripe < last ? last - stripe : last + erasure.k - stripe));
    }
    stripelen = stripe_last_byte + 1 - stripeoff;
  }

  size_t osd_span(const uint64_t off, const uint64_t len) {
    uint32_t first = off / erasure.size;
    if (len == 0) {
      return 0;
    }
    uint32_t last = (off + len - 1) / erasure.size;
    return min(last - first + 1, erasure.k);
  }
  object_t stripulate(const object_t& oid, bool code, uint16_t num);
  object_t data_stripe(const object_t& oid, size_t stripe);

  public:

  ~CohortVolume();

  static const uint64_t one_op;

  uint64_t op_size() {
    return one_op * erasure.k;
  }

  virtual uint32_t num_rules(void);

  virtual int place(const object_t& object,
		    const OSDMap& map,
		    const unsigned int rule_index,
		    vector<int>& osds);

  virtual int update(VolumeCRef v);

  virtual void dump(Formatter *f) const;
  virtual void decode_payload(bufferlist::iterator& bl, __u8 v);
  virtual void encode(bufferlist& bl) const;

  friend VolumeRef CohortVolFactory(bufferlist::iterator& bl, __u8 v,
				    vol_type t);

  static VolumeRef create(const string& name, const epoch_t last_update,
			  const string& place_text, const string& symbols,
			  const string& erasure_type,
			  int64_t data_blocks, int64_t code_blocks,
			  int64_t word_size, int64_t packet_size,
			  int64_t size, string& error_message);


#if 0
  int create(const object_t& oid,
	     utime_t mtime,
	     int global_flags, int create_flags,
	     Context *onack, Context *oncommit,
	     Objecter *objecter,
	     eversion_t *objver = NULL,
	     ObjectOperation *extra_ops = NULL);

  int write(const object_t& oid, uint64_t off, uint64_t len,
	    const bufferlist &bl,
	    utime_t mtime, int flags, Context *onack,
	    Context *oncommit, Objecter *objecter,
	    eversion_t *objver = NULL,
	    ObjectOperation *extra_ops = NULL);
  int append(const object_t& oid, uint64_t len,
	     const bufferlist &bl, utime_t mtime, int flags, Context *onack,
	     Context *oncommit, Objecter *objecter, eversion_t *objver = NULL,
	     ObjectOperation *extra_ops = NULL);
  int write_full(const object_t& oid,
		 const bufferlist &bl, utime_t mtime, int flags,
		 Context *onack, Context *oncommit, Objecter *objecter,
		 eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL);
  int md_read(const object_t& oid, ObjectOperation& op,
	      bufferlist *pbl, int flags,
	      Context *onack, Objecter *objecter, eversion_t *objver = NULL);
  int read(const object_t& oid, uint64_t off, uint64_t len,
	   bufferlist *pbl, int flags,
	   Context *onfinish, Objecter *objecter,
	   eversion_t *objver = NULL,
	   ObjectOperation *extra_ops = NULL);
  int remove(const object_t& oid,
	     utime_t mtime, int flags,
	     Context *onack, Context *oncommit,
	     Objecter *objecter,
	     eversion_t *objver = NULL,
	     ObjectOperation *extra_ops = NULL);
  int stat(const object_t& oid, uint64_t *psize,
	   utime_t *pmtime, int flags, Context *onfinish,
	   Objecter *objecter, eversion_t *objver = NULL,
	   ObjectOperation *extra_ops = NULL);
  int getxattr(const object_t& oid, const char *name,
	       bufferlist *pbl, int flags, Context *onfinish,
	       Objecter *objecter, eversion_t *objver = NULL,
	       ObjectOperation *extra_ops = NULL);
  int removexattr(const object_t& oid, const char *name,
		  utime_t mtime, int flags,
		  Context *onack, Context *oncommit,
		  Objecter *objecter, eversion_t *objver = NULL,
		  ObjectOperation *extra_ops = NULL);
  int setxattr(const object_t& oid, const char *name,
	       const bufferlist &bl,
	       utime_t mtime, int flags, Context *onack,
	       Context *oncommit, Objecter *objecter,
	       eversion_t *objver = NULL,
	       ObjectOperation *extra_ops = NULL);
  int getxattrs(const object_t& oid,
		map<string, bufferlist>& attrset, int flags,
		Context *onfinish, Objecter *objecter,
		eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL);
  int trunc(const object_t& oid,
	    utime_t mtime, int flags,
	    uint64_t trunc_size, __u32 trunc_seq,
	    Context *onack, Context *oncommit,
	    Objecter *objecter, eversion_t *objver = NULL,
	    ObjectOperation *extra_ops = NULL);
  int zero(const object_t& oid, uint64_t off, uint64_t len,
	   utime_t mtime, int flags,
	   Context *onack, Context *oncommit, Objecter *objecter,
	   eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL);

  int mutate_md(const object_t& oid, ObjectOperation& op,
		utime_t mtime,
		int flags, Context *onack, Context *oncommit,
		Objecter *objecter, eversion_t *objver = NULL);
#endif
};

#endif // COHORT_COHORTVOLUME_H
