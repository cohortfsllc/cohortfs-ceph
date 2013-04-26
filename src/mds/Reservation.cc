
#include "Reservation.h"


bool reservation_state_t::add_rsv(ceph_reservation& rsv, bool adjust)
{
    pair<set<ceph_reservation>::iterator,bool> ret;

    ret = reservations.insert(rsv);
    if (! ret.second)
        return (false);

    ceph_reservation& nrsv = const_cast<ceph_reservation&>(*(ret.first));
    if (adjust) {
        nrsv.id = ++max_id;

        // set an expiration (1d)
        nrsv.expiration = ceph_clock_now(g_ceph_context) + 86400;
    }

    rsv = nrsv;

    // populate lookup tables
    reservations_id.insert(pair<uint64_t,ceph_reservation>(nrsv.id, nrsv));

    return (true);
}

bool reservation_state_t::put_rsv(uint64_t rsv_id, const client_t client)
{
    map<uint64_t, ceph_reservation>::iterator iter =
        reservations_id.find(rsv_id);

    // not found
    if (iter == reservations_id.end())
        return (false);

    ceph_reservation& rsv = iter->second;
    if (! (rsv.client == client))
        return (false);

    bool r = remove_rsv(rsv);
    return (r);
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

void reservation_state_t::on_update_inode(set<ceph_reservation>& rsv,
                                          set<pair<uint64_t,uint64_t> > rsv_osd)
{
    reservations.clear();
    reservations_id.clear();
    reservations_osd.clear();
    osds_by_rsv.clear();

    max_id = 0;

    set<ceph_reservation>::iterator rsv_iter;
    for (rsv_iter = rsv.begin(); rsv_iter != rsv.end(); ++rsv_iter) {
        ceph_reservation &rsv = const_cast<ceph_reservation&>(*rsv_iter);
        /* populates reservations and reservations_id */
        add_rsv(rsv, false /* no adjust */);
    }

    set<pair<uint64_t,uint64_t> >::iterator osd_iter;
    for (osd_iter = rsv_osd.begin(); osd_iter != rsv_osd.end(); ++osd_iter) {
        register_osd(osd_iter->first, osd_iter->second);
    }
}
