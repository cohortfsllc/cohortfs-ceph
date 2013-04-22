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
        << " start: " << rsv.start
        << " length: " << rsv.length
        << " client: " << rsv.client
        << " flags: " << rsv.flags /* decode */
        << " id: " << rsv.id /* format */
        << std::endl;
  return out;
}

inline bool operator==(ceph_reservation& lhs, ceph_reservation& rhs) {
  return
  ( (lhs.start == lhs.start) &&
    (lhs.length == rhs.length) &&
    (lhs.client == rhs.client) &&
    (lhs.type == rhs.type) );
}

class reservation_state_t
{
public:
/* TODO:  switch to intrusive maps and ptr to permit sharing */
    /* rsv_id, rsv> tuples: */
    multimap<uint64_t, ceph_reservation> reservations_id;
    /* client_t, rsv> tuples */
    multimap<client_t, ceph_reservation> reservations_client;
    /* <rsv_id, osd_id> tuples: */
    multimap<uint64_t, uint64_t> reservations_osd;

    bool add_rsv(ceph_reservation& rsv, bool wait_on_fail, bool replay);
    void remove_rsv(ceph_reservation rsv);
    bool remove_rsv_client(client_t client);
    bool register_osd(ceph_reservation &rsv, uint64_t osd);
    void unregister_osd(ceph_reservation &rsv, uint64_t osd);
    void unregister_osd_all(uint64_t osd);

private:
  // nothing yet

public:
  void encode(bufferlist& bl) const {
      ::encode(reservations_id, bl);
      ::encode(reservations_osd, bl);
  }
  void decode(bufferlist::iterator& bl) {
      ::decode(reservations_id, bl); // also expands by client
      ::decode(reservations_osd, bl);
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
