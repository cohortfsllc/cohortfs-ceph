// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 *
 * Copyright (C) 2013 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_MDS_RESERVATION_H
#define CEPH_MDS_RESERVATION_H

#include <errno.h>

#include "common/debug.h"
#include "mdstypes.h"

inline ostream& operator<<(ostream& out, ceph_reservation& rsv) {
    out << "type: " << rsv.type
        << " start: " << rsv.offset
        << " length: " << rsv.length
        << " client: " << rsv.client
        << " flags: " << rsv.flags /* decode */
        << " id: " << rsv.id /* format */
        << std::endl;
  return out;
}

inline bool operator==(ceph_reservation& lhs, ceph_reservation& rhs) {
  return
  ( (lhs.offset == lhs.offset) &&
    (lhs.length == rhs.length) &&
    (lhs.client == rhs.client) &&
    (lhs.type == rhs.type) );
}

/*
 * Cmp functor for reservations sorted by client,type,offset,length
 */
namespace std {
  template<>
  class less<ceph_reservation> {
  public: 
    bool operator()(const ceph_reservation &lhs,
		    const ceph_reservation &rhs) {
      if (lhs.client < rhs.client)
      return (true);
      if (lhs.client == rhs.client) {
	if (lhs.type < rhs.type)
	  return (true);
	if (lhs.type == rhs.type) {
	  if (lhs.offset < rhs.offset)
	    return (true);
	  if (lhs.offset == rhs.offset) {
	    if (lhs.length < rhs.length)
	      return (true);
	  }
	}
    }
      return (false);
    }
  };
}

class reservation_state_t
{
public:
/* TODO:  switch to boost::intrusive to permit sharing */
  set<ceph_reservation> reservations;
  /* rsv_id, rsv> tuples: */
  map<uint64_t, ceph_reservation> reservations_id;
  /* <rsv_id, osd_id> tuples */
  set<pair<uint64_t,uint64_t> > reservations_osd;
  /* rsv -> osds */
  multimap<uint64_t, uint64_t> osds_by_rsv;

  uint64_t max_id;

  bool add_rsv(ceph_reservation& rsv);
  bool remove_rsv(const ceph_reservation& rsv);
  bool remove_rsv_client(const client_t client);
  bool remove_expired(void);
  bool register_osd(ceph_reservation &rsv, uint64_t osd);
  bool unregister_osd(ceph_reservation &rsv, uint64_t osd);
  void unregister_osd_all(uint64_t osd);

private:
  // nothing yet

public:
  void encode(bufferlist& bl) const {
      ::encode(reservations, bl);
      ::encode(reservations_osd, bl);
  }
  void decode(bufferlist::iterator& bl) {
      ::decode(reservations, bl); // also expands reservations_id
      ::decode(reservations_osd, bl); // also expands osds_by_rsv
  }
};
WRITE_CLASS_ENCODER(reservation_state_t)

inline ostream& operator<<(ostream& out, reservation_state_t& r) {

    out << "reservations (: " << r.reservations_id.size() << ")\n";
    for (multimap<uint64_t, ceph_reservation>::iterator iter =
             r.reservations_id.begin(); iter != r.reservations_id.end();
             ++iter) {
                  out << "<" << iter->first << ", " << iter->second << "> ";
             }

  return out;
}

#endif /* CEPH_MDS_RESERVATION_H */
