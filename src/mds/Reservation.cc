
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

    // set an expiration (1d)
    nrsv.expiration = ceph_clock_now(g_ceph_context) + 86400;

    rsv = nrsv;

    return (true);
}

bool reservation_state_t::remove_rsv(const ceph_reservation& rsv)
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

bool reservation_state_t::remove_rsv_client(const client_t client)
{
    set<ceph_reservation>::iterator iter, iter2;

    iter = reservations.begin();
    while (iter != reservations.end()) {
        if (iter->client == client) {
            iter2 = iter++;
            remove_rsv(*iter);
            continue;
        }
        iter++;
    }

    return (false);
}

bool reservation_state_t::remove_expired(void)
{
    utime_t now = ceph_clock_now(g_ceph_context);
    set<ceph_reservation>::iterator iter, iter2;

    iter = reservations.begin();
    while (iter != reservations.end()) {
        if (iter->expiration <= now) {
            iter2 = iter++;
            remove_rsv(*iter);
            continue;
        }
        iter++;
    }

    return (false);
}

bool reservation_state_t::register_osd(uint64_t rsv_id, uint64_t osd)
{
    pair<uint64_t,uint64_t> pr = pair<uint64_t,uint64_t>(rsv_id,osd);
    pair<set<pair<uint64_t,uint64_t> >::iterator,bool> ret;

    ret = reservations_osd.insert(pr);
    if (! ret.second)
        return (false);

    // and the multi-map
    osds_by_rsv.insert(pr);

    return (true);
}

bool reservation_state_t::unregister_osd(uint64_t rsv_id, uint64_t osd)
{
    if (reservations_osd.erase(pair<uint64_t,uint64_t>(rsv_id,osd))) {
        osds_by_rsv.erase(rsv_id);
        return (true);
    }

    return (false);
}

void reservation_state_t::unregister_osd_all(uint64_t osd)
{
    set<pair<uint64_t,uint64_t> >::iterator iter, iter2;

    iter = reservations_osd.begin();
    while (iter != reservations_osd.end()) {
        if (iter->second == osd) {
            iter2 = iter++;
            if (reservations_osd.erase(*iter)) {
              osds_by_rsv.erase(iter->first);
             }
            continue;
        }
        iter++;
    }
}
