
#include "Reservation.h"


bool reservation_state_t::add_rsv(ceph_reservation& rsv)
{
    pair<set<ceph_reservation, rsv_key_cmp>::iterator,bool> ret;

    ret = reservations.insert(rsv);
    if (! ret.second)
        return (false);

    ceph_reservation& nrsv = const_cast<ceph_reservation&>(*(ret.first));
    nrsv.id = ++max_id;

    // populate lookup tables
    reservations_id.insert(pair<uint64_t,ceph_reservation>(rsv.id, rsv));
    reservations_client.insert(pair<uint64_t,ceph_reservation>(rsv.client,rsv));

    rsv = nrsv;

    return (true);
}
