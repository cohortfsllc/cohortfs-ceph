
#include "Reservation.h"


bool reservation_state_t::add_rsv(ceph_reservation& rsv)
{
    pair<set<ceph_reservation>::iterator,bool> ret;

    ret = reservations.insert(rsv);
    if (! ret.second)
        return (false);

    ceph_reservation& nrsv = const_cast<ceph_reservation&>(*(ret.first));
    nrsv.id = ++max_id;

    // populate lookup tables
    reservations_id.insert(pair<uint64_t,ceph_reservation>(rsv.id, rsv));

    rsv = nrsv;

    return (true);
}

bool reservation_state_t::remove_rsv(ceph_reservation& rsv)
{
    // rsv.client and source client are confirmed eq
    if (reservations.erase(rsv)) {

        reservations_id.erase(rsv.id);

        // fixup max_id
        uint64_t hkey = (reservations_id.size() == 0) ? 0 :
            reservations_id.rbegin()->first;
        if (max_id > hkey)
            max_id = hkey;

        // remove OSD mappings
        multimap<uint64_t, uint64_t>::iterator iter;
        iter = osds_by_rsv.find(rsv.id);
        while((iter != osds_by_rsv.end()) && (iter->first == rsv.id)) {
            // TODO: async fence rsv at OSD
            reservations_osd.erase(*iter);
        }
        osds_by_rsv.erase(rsv.id);
        return (true);
    }

    return (false);
}
