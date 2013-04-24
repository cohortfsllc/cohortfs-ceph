
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

bool reservation_state_t::remove_rsv(ceph_reservation& rsv)
{
    // rsv.client and source client are confirmed eq
    if (reservations.erase(rsv)) {

        reservations_id.erase(rsv.id);
        reservations_client.erase(rsv.client);

        // fixup max_id
        uint64_t hkey = (reservations_id.size() == 0) ? 0 :
            reservations_id.rbegin()->first;

        if (max_id > hkey)
            max_id = hkey;

        return (true);
    }

    return (false);
}
