// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "include/types.h"
#include "msg/msg_types.h"

#include <iostream>

#include <errno.h>
#include <stdlib.h>
#include <time.h>

#include "cls/lock/cls_lock_types.h"
#include "cls/lock/cls_lock_ops.h"
#include "cls/lock/cls_lock_client.h"

namespace rados {
  namespace cls {
    namespace lock {

      void lock(ObjOpUse rados_op,
		const string& name, ClsLockType type,
		const string& cookie, const string& tag,
		const string& description,
		const ceph::timespan& duration, uint8_t flags)
      {
	cls_lock_lock_op op;
	op.name = name;
	op.type = type;
	op.cookie = cookie;
	op.tag = tag;
	op.description = description;
	op.duration = duration;
	op.flags = flags;
	bufferlist in;
	::encode(op, in);
	rados_op->call("lock", "lock", in);
      }

      int lock(Objecter* o,
	       VolumeRef vol,
	       const oid_t& oid,
	       const string& name, ClsLockType type,
	       const string& cookie, const string& tag,
	       const string& description, const ceph::timespan& duration,
	       uint8_t flags)
      {
	ObjectOperation op(vol->op());
	// WHY the heck is this happening? I'm not using namespace std
	// in any header.
	rados::cls::lock::lock(op, name, type, cookie, tag, description,
			       duration, flags);
	return o->mutate(oid, vol, op);
      }

      void unlock(ObjOpUse rados_op,
		  const string& name, const string& cookie)
      {
	cls_lock_unlock_op op;
	op.name = name;
	op.cookie = cookie;
	bufferlist in;
	::encode(op, in);

	rados_op->call("lock", "unlock", in);
      }

      int unlock(Objecter* o, VolumeRef vol, const oid_t& oid,
		 const string& name, const string& cookie)
      {
	ObjectOperation op(vol->op());
	unlock(op, name, cookie);
	return o->mutate(oid, vol, op);
      }

      void break_lock(ObjOpUse& rados_op,
		      const string& name, const string& cookie,
		      const entity_name_t& locker)
      {
	cls_lock_break_op op;
	op.name = name;
	op.cookie = cookie;
	op.locker = locker;
	bufferlist in;
	::encode(op, in);
	rados_op->call("lock", "break_lock", in);
      }

      int break_lock(Objecter* o, VolumeRef vol, const oid_t& oid,
		     const string& name, const string& cookie,
		     const entity_name_t& locker)
      {
	ObjectOperation op(vol->op());
	break_lock(op, name, cookie, locker);
	return o->mutate(oid, vol, op);
      }

      int list_locks(Objecter* o, VolumeRef vol, const oid_t& oid,
		     list<string>& locks)
      {
	bufferlist in, out;
	ObjectOperation op(vol->op());
	op->call("lock", "list_locks", in, &out);
	int r = o->read(oid, vol, op);
	if (r < 0)
	  return r;

	cls_lock_list_locks_reply ret;
	bufferlist::iterator iter = out.begin();
	try {
	  ::decode(ret, iter);
	} catch (buffer::error& err) {
	  return -EBADMSG;
	}

	locks = ret.locks;

	return 0;
      }

      void get_lock_info_start(ObjOpUse rados_op,
			       const string& name,
			       bufferlist& out)
      {
	bufferlist in;
	cls_lock_get_info_op op;
	op.name = name;
	::encode(op, in);
	rados_op->call("lock", "get_info", in, &out);
      }

      int get_lock_info_finish(bufferlist::iterator& iter,
			       map<locker_id_t, locker_info_t>* lockers,
			       ClsLockType* type, string* tag)
      {
	cls_lock_get_info_reply ret;
	try {
	  ::decode(ret, iter);
	} catch (buffer::error& err) {
	  return -EBADMSG;
	}

	if (lockers) {
	  *lockers = ret.lockers;
	}

	if (type) {
	  *type = ret.lock_type;
	}

	if (tag) {
	  *tag = ret.tag;
	}

	return 0;
      }

      int get_lock_info(Objecter* o, VolumeRef vol, const oid_t& oid,
			const string& name,
			map<locker_id_t, locker_info_t>* lockers,
			ClsLockType* type, string* tag)
      {
	ObjectOperation op(vol->op());
	bufferlist out;
	get_lock_info_start(op, name, out);
	int r = o->read(oid, vol, op);
	if (r < 0)
	  return r;
	bufferlist::iterator it = out.begin();
	return get_lock_info_finish(it, lockers, type, tag);
      }

      void Lock::lock_shared(ObjOpUse op)
      {
	lock(op, name, LOCK_SHARED,
	     cookie, tag, description, duration, flags);
      }

      int Lock::lock_shared(Objecter* o, VolumeRef vol, const oid_t& oid)
      {
	return lock(o, vol, oid, name, LOCK_SHARED, cookie, tag, description,
		    duration, flags);
      }

      void Lock::lock_exclusive(ObjOpUse op)
      {
	lock(op, name, LOCK_EXCLUSIVE, cookie, tag, description, duration,
	     flags);
      }

      int Lock::lock_exclusive(Objecter* o, VolumeRef vol, const oid_t& oid)
      {
	return lock(o, vol, oid, name, LOCK_EXCLUSIVE, cookie, tag,
		    description, duration, flags);
      }

      void Lock::unlock(ObjOpUse op)
      {
	rados::cls::lock::unlock(op, name, cookie);
      }

      int Lock::unlock(Objecter* o, VolumeRef vol, const oid_t& oid)
      {
	return rados::cls::lock::unlock(o, vol, oid, name, cookie);
      }

      void Lock::break_lock(ObjOpUse op, const entity_name_t& locker)
      {
	rados::cls::lock::break_lock(op, name, cookie, locker);
      }

      int Lock::break_lock(Objecter* o, VolumeRef vol, const oid_t& oid,
			   const entity_name_t& locker)
      {
	return rados::cls::lock::break_lock(o, vol, oid, name, cookie, locker);
      }
    } // namespace lock
  } // namespace cls
} // namespace rados

