// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOCK_CLIENT_H
#define CEPH_CLS_LOCK_CLIENT_H


#include "include/types.h"
#include "cls/lock/cls_lock_types.h"
#include "osdc/Objecter.h"

namespace rados {
  namespace cls {
    namespace lock {

      extern void lock(ObjOpUse rados_op,
		       const std::string& name, ClsLockType type,
		       const std::string& cookie, const std::string& tag,
		       const std::string& description,
		       const ceph::timespan& duration,
		       uint8_t flags);

      extern int lock(Objecter* o,
		      VolumeRef v,
		      const oid_t& oid,
		      const std::string& name, ClsLockType type,
		      const std::string& cookie, const std::string& tag,
		      const std::string& description,
		      const ceph::timespan& duration,
		      uint8_t flags);

      extern void unlock(ObjOpUse rados_op,
			 const std::string& name, const std::string& cookie);

      extern int unlock(Objecter* o, VolumeRef, const oid_t& oid,
			const std::string& name, const std::string& cookie);

      extern void break_lock(ObjOpUse& op,
			     const std::string& name,
			     const std::string& cookie,
			     const entity_name_t& locker);

      extern int break_lock(Objecter* o, VolumeRef vol,
			    const oid_t& oid,
			    const std::string& name, const std::string& cookie,
			    const entity_name_t& locker);

      extern int list_locks(Objecter* o, VolumeRef vol, const oid_t& oid,
			    list<string>& locks);

      extern void get_lock_info_start(ObjOpUse rados_op,
				      const std::string& name);
      extern int get_lock_info_finish(ceph::bufferlist::iterator *out,
				      map<locker_id_t, locker_info_t> *lockers,
				      ClsLockType *type, std::string *tag);

      extern int get_lock_info(Objecter* o, VolumeRef vol, const oid_t& oid,
			       const std::string& name,
			       map<locker_id_t, locker_info_t> *lockers,
			       ClsLockType *type, std::string *tag);

      class Lock {
	std::string name;
	std::string cookie;
	std::string tag;
	std::string description;
	ceph::timespan duration;
	uint8_t flags;

      public:

	Lock(const std::string& _n) : name(_n), flags(0) {}

	void set_cookie(const std::string& c) { cookie = c; }
	void set_tag(const std::string& t) { tag = t; }
	void set_description(const std::string& desc) { description = desc; }
	void set_duration(const ceph::timespan& e) { duration = e; }
	void set_renew(bool renew) {
	  if (renew) {
	    flags |= LOCK_FLAG_RENEW;
	  } else {
	    flags &= ~LOCK_FLAG_RENEW;
	  }
	}

	/* ObjectWriteOperation */
	void lock_exclusive(ObjOpUse op);
	void lock_shared(ObjOpUse op);
	void unlock(ObjOpUse op);
	void break_lock(ObjOpUse op, const entity_name_t& locker);

	/* IoCtx */
	int lock_exclusive(Objecter* o, VolumeRef v, const oid_t& oid);
	int lock_shared(Objecter* o, VolumeRef v, const oid_t& oid);
	int unlock(Objecter* o, VolumeRef v, const oid_t& oid);
	int break_lock(Objecter* o, VolumeRef v, const oid_t& oid,
		       const entity_name_t& locker);
      };

    } // namespace lock
  }  // namespace cls
} // namespace rados

#endif
