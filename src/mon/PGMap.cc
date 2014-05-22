// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGMap.h"

#define dout_subsys ceph_subsys_mon
#include "common/debug.h"
#include "common/TextTable.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"
#include "mon/MonitorDBStore.h"

// --

void PGMap::Incremental::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    uint8_t v = 4;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(pg_stat_updates, bl);
    ::encode(osd_stat_updates, bl);
    ::encode(osd_stat_rm, bl);
    ::encode(osdmap_epoch, bl);
    ::encode(pg_scan, bl);
    ::encode(full_ratio, bl);
    ::encode(nearfull_ratio, bl);
    ::encode(pg_remove, bl);
    return;
  }

  ENCODE_START(7, 5, bl);
  ::encode(version, bl);
  ::encode(pg_stat_updates, bl);
  ::encode(osd_stat_updates, bl);
  ::encode(osd_stat_rm, bl);
  ::encode(osdmap_epoch, bl);
  ::encode(pg_scan, bl);
  ::encode(full_ratio, bl);
  ::encode(nearfull_ratio, bl);
  ::encode(pg_remove, bl);
  ::encode(stamp, bl);
  ::encode(osd_epochs, bl);
  ENCODE_FINISH(bl);
}

void PGMap::Incremental::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(version, bl);
  if (struct_v < 3) {
    pg_stat_updates.clear();
    uint32_t n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_t pgid = opgid;
      ::decode(pg_stat_updates[pgid], bl);
    }
  } else {
    ::decode(pg_stat_updates, bl);
  }
  ::decode(osd_stat_updates, bl);
  ::decode(osd_stat_rm, bl);
  ::decode(osdmap_epoch, bl);
  ::decode(pg_scan, bl);
  if (struct_v >= 2) {
    ::decode(full_ratio, bl);
    ::decode(nearfull_ratio, bl);
  }
  if (struct_v < 3) {
    pg_remove.clear();
    uint32_t n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_remove.insert(pg_t(opgid));
    }
  } else {
    ::decode(pg_remove, bl);
  }
  if (struct_v < 4 && full_ratio == 0) {
    full_ratio = -1;
  }
  if (struct_v < 4 && nearfull_ratio == 0) {
    nearfull_ratio = -1;
  }
  if (struct_v >= 6)
    ::decode(stamp, bl);
  if (struct_v >= 7) {
    ::decode(osd_epochs, bl);
  } else {
    for (map<int32_t, osd_stat_t>::iterator i = osd_stat_updates.begin();
	 i != osd_stat_updates.end();
	 ++i) {
      // This isn't accurate, but will cause trimming to behave like
      // previously.
      osd_epochs.insert(make_pair(i->first, osdmap_epoch));
    }
  }
  DECODE_FINISH(bl);
}

void PGMap::Incremental::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("osdmap_epoch", osdmap_epoch);
  f->dump_unsigned("pg_scan_epoch", pg_scan);
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("nearfull_ratio", nearfull_ratio);

  f->open_array_section("pg_stat_updates");
  for (map<pg_t,pg_stat_t>::const_iterator p = pg_stat_updates.begin(); p != pg_stat_updates.end(); ++p) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_updates");
  for (map<int32_t,osd_stat_t>::const_iterator p = osd_stat_updates.begin(); p != osd_stat_updates.end(); ++p) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_removals");
  for (set<int>::const_iterator p = osd_stat_rm.begin(); p != osd_stat_rm.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();

  f->open_array_section("pg_removals");
  for (set<pg_t>::const_iterator p = pg_remove.begin(); p != pg_remove.end(); ++p)
    f->dump_stream("pgid") << *p;
  f->close_section();
}

void PGMap::Incremental::generate_test_instances(list<PGMap::Incremental*>& o)
{
  o.push_back(new Incremental);
  o.push_back(new Incremental);
  o.back()->version = 1;
  o.back()->stamp = utime_t(123,345);
  o.push_back(new Incremental);
  o.back()->version = 2;
  o.back()->pg_stat_updates[pg_t(1,2,3)] = pg_stat_t();
  o.back()->osd_stat_updates[5] = osd_stat_t();
  o.back()->osd_epochs[5] = 12;
  o.push_back(new Incremental);
  o.back()->version = 3;
  o.back()->osdmap_epoch = 1;
  o.back()->pg_scan = 2;
  o.back()->full_ratio = .2;
  o.back()->nearfull_ratio = .3;
  o.back()->pg_stat_updates[pg_t(4,5,6)] = pg_stat_t();
  o.back()->osd_stat_updates[6] = osd_stat_t();
  o.back()->osd_epochs[6] = 12;
  o.back()->pg_remove.insert(pg_t(1,2,3));
  o.back()->osd_stat_rm.insert(5);
}


// --

void PGMap::apply_incremental(CephContext *cct, const Incremental& inc)
{
  assert(inc.version == version+1);
  version++;

  utime_t delta_t;
  delta_t = inc.stamp;
  delta_t -= stamp;
  stamp = inc.stamp;

  pool_stat_t pg_sum_old = pg_sum;
  ceph::unordered_map<uint64_t, pool_stat_t> pg_pool_sum_old;

  bool ratios_changed = false;
  if (inc.full_ratio != full_ratio && inc.full_ratio != -1) {
    full_ratio = inc.full_ratio;
    ratios_changed = true;
  }
  if (inc.nearfull_ratio != nearfull_ratio && inc.nearfull_ratio != -1) {
    nearfull_ratio = inc.nearfull_ratio;
    ratios_changed = true;
  }
  if (ratios_changed)
    redo_full_sets();

  for (map<pg_t,pg_stat_t>::const_iterator p = inc.pg_stat_updates.begin();
       p != inc.pg_stat_updates.end();
       ++p) {
    const pg_t &update_pg(p->first);
    const pg_stat_t &update_stat(p->second);

    if (pg_pool_sum_old.count(update_pg.pool()) == 0)
      pg_pool_sum_old[update_pg.pool()] = pg_pool_sum[update_pg.pool()];

    ceph::unordered_map<pg_t,pg_stat_t>::iterator t = pg_stat.find(update_pg);
    if (t == pg_stat.end()) {
      ceph::unordered_map<pg_t,pg_stat_t>::value_type v(update_pg, update_stat);
      pg_stat.insert(v);
      // did we affect the min?
    } else {
      stat_pg_sub(update_pg, t->second);
      t->second = update_stat;
    }
    stat_pg_add(update_pg, update_stat);
  }
  assert(osd_stat.size() == osd_epochs.size());
  for (map<int32_t,osd_stat_t>::const_iterator p =
	 inc.get_osd_stat_updates().begin();
       p != inc.get_osd_stat_updates().end();
       ++p) {
    int osd = p->first;
    const osd_stat_t &new_stats(p->second);

    ceph::unordered_map<int32_t,osd_stat_t>::iterator t = osd_stat.find(osd);
    if (t == osd_stat.end()) {
      ceph::unordered_map<int32_t,osd_stat_t>::value_type v(osd, new_stats);
      osd_stat.insert(v);
    } else {
      stat_osd_sub(t->second);
      t->second = new_stats;
    }
    ceph::unordered_map<int32_t,epoch_t>::iterator i = osd_epochs.find(osd);
    map<int32_t,epoch_t>::const_iterator j = inc.get_osd_epochs().find(osd);
    assert(j != inc.get_osd_epochs().end());

    if (i == osd_epochs.end())
      osd_epochs.insert(*j);
    else
      i->second = j->second;

    stat_osd_add(new_stats);

    // adjust [near]full status
    register_nearfull_status(osd, new_stats);
  }
  set<int64_t> deleted_pools;
  for (set<pg_t>::const_iterator p = inc.pg_remove.begin();
       p != inc.pg_remove.end();
       ++p) {
    const pg_t &removed_pg(*p);
    ceph::unordered_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(removed_pg);
    if (s != pg_stat.end()) {
      stat_pg_sub(removed_pg, s->second);
      pg_stat.erase(s);
    }
    if (removed_pg.ps() == 0)
      deleted_pools.insert(removed_pg.pool());
  }
  for (set<int64_t>::iterator p = deleted_pools.begin();
       p != deleted_pools.end();
       ++p) {
    dout(20) << " deleted pool " << *p << dendl;
    deleted_pool(*p);
  }

  for (set<int>::iterator p = inc.get_osd_stat_rm().begin();
       p != inc.get_osd_stat_rm().end();
       ++p) {
    ceph::unordered_map<int32_t,osd_stat_t>::iterator t = osd_stat.find(*p);
    if (t != osd_stat.end()) {
      stat_osd_sub(t->second);
      osd_stat.erase(t);
    }

    // remove these old osds from full/nearfull set(s), too
    nearfull_osds.erase(*p);
    full_osds.erase(*p);
  }

  // calculate a delta, and average over the last 2 deltas.
  pool_stat_t d = pg_sum;
  d.stats.sub(pg_sum_old.stats);
  pg_sum_deltas.push_back(make_pair(d, delta_t));
  stamp_delta += delta_t;

  pg_sum_delta.stats.add(d.stats);
  if (pg_sum_deltas.size() > (std::list< pair<pool_stat_t, utime_t> >::size_type)MAX(1, cct ? cct->_conf->mon_stat_smooth_intervals : 1)) {
    pg_sum_delta.stats.sub(pg_sum_deltas.front().first.stats);
    stamp_delta -= pg_sum_deltas.front().second;
    pg_sum_deltas.pop_front();
  }

  update_pool_deltas(cct, inc.stamp, pg_pool_sum_old);

  if (inc.osdmap_epoch)
    last_osdmap_epoch = inc.osdmap_epoch;
  if (inc.pg_scan)
    last_pg_scan = inc.pg_scan;
}

void PGMap::redo_full_sets()
{
  full_osds.clear();
  nearfull_osds.clear();
  for (ceph::unordered_map<int32_t, osd_stat_t>::iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    register_nearfull_status(i->first, i->second);
  }
}

void PGMap::register_nearfull_status(int osd, const osd_stat_t& s)
{
  float ratio = ((float)s.kb_used) / ((float)s.kb);

  if (full_ratio > 0 && ratio > full_ratio) {
    // full
    full_osds.insert(osd);
    nearfull_osds.erase(osd);
  } else if (nearfull_ratio > 0 && ratio > nearfull_ratio) {
    // nearfull
    full_osds.erase(osd);
    nearfull_osds.insert(osd);
  } else {
    // ok
    full_osds.erase(osd);
    nearfull_osds.erase(osd);
  }
}

void PGMap::calc_stats()
{
  num_pg_by_state.clear();
  num_pg = 0;
  num_osd = 0;
  pg_pool_sum.clear();
  pg_sum = pool_stat_t();
  osd_sum = osd_stat_t();

  for (ceph::unordered_map<pg_t,pg_stat_t>::iterator p = pg_stat.begin();
       p != pg_stat.end();
       ++p) {
    stat_pg_add(p->first, p->second);
  }
  for (ceph::unordered_map<int32_t,osd_stat_t>::iterator p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    stat_osd_add(p->second);

  redo_full_sets();
}

void PGMap::update_pg(pg_t pgid, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  ceph::unordered_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(pgid);
  if (s != pg_stat.end())
    stat_pg_sub(pgid, s->second);
  pg_stat_t& r = pg_stat[pgid];
  ::decode(r, p);
  stat_pg_add(pgid, r);
}

void PGMap::remove_pg(pg_t pgid)
{
  ceph::unordered_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(pgid);
  if (s != pg_stat.end()) {
    stat_pg_sub(pgid, s->second);
    pg_stat.erase(s);
  }
}

void PGMap::update_osd(int osd, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  ceph::unordered_map<int32_t,osd_stat_t>::iterator o = osd_stat.find(osd);
  if (o != osd_stat.end())
    stat_osd_sub(o->second);
  osd_stat_t& r = osd_stat[osd];
  ::decode(r, p);
  stat_osd_add(r);

  // adjust [near]full status
  register_nearfull_status(osd, r);
}

void PGMap::remove_osd(int osd)
{
  ceph::unordered_map<int32_t,osd_stat_t>::iterator o = osd_stat.find(osd);
  if (o != osd_stat.end()) {
    stat_osd_sub(o->second);
    osd_stat.erase(o);

    // remove these old osds from full/nearfull set(s), too
    nearfull_osds.erase(osd);
    full_osds.erase(osd);
  }
}

void PGMap::stat_pg_add(const pg_t &pgid, const pg_stat_t &s)
{
  num_pg++;
  num_pg_by_state[s.state]++;
  pg_pool_sum[pgid.pool()].add(s);
  pg_sum.add(s);
  if (s.state & PG_STATE_CREATING) {
    creating_pgs.insert(pgid);
    if (s.osd >= 0)
      creating_pgs_by_osd[s.osd].insert(pgid);
  }
}

void PGMap::stat_pg_sub(const pg_t &pgid, const pg_stat_t &s)
{
  num_pg--;
  if (--num_pg_by_state[s.state] == 0)
    num_pg_by_state.erase(s.state);

  pool_stat_t& ps = pg_pool_sum[pgid.pool()];
  ps.sub(s);
  if (ps.is_zero())
    pg_pool_sum.erase(pgid.pool());

  pg_sum.sub(s);
  if (s.state & PG_STATE_CREATING) {
    creating_pgs.erase(pgid);
    if (s.osd >= 0) {
      creating_pgs_by_osd[s.osd].erase(pgid);
      if (creating_pgs_by_osd[s.osd].size() == 0)
	creating_pgs_by_osd.erase(s.osd);
    }
  }
}

void PGMap::stat_osd_add(const osd_stat_t &s)
{
  num_osd++;
  osd_sum.add(s);
}

void PGMap::stat_osd_sub(const osd_stat_t &s)
{
  num_osd--;
  osd_sum.sub(s);
}

void PGMap::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    uint8_t v = 3;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(pg_stat, bl);
    ::encode(osd_stat, bl);
    ::encode(last_osdmap_epoch, bl);
    ::encode(last_pg_scan, bl);
    ::encode(full_ratio, bl);
    ::encode(nearfull_ratio, bl);
    return;
  }

  ENCODE_START(6, 4, bl);
  ::encode(version, bl);
  ::encode(pg_stat, bl);
  ::encode(osd_stat, bl);
  ::encode(last_osdmap_epoch, bl);
  ::encode(last_pg_scan, bl);
  ::encode(full_ratio, bl);
  ::encode(nearfull_ratio, bl);
  ::encode(stamp, bl);
  ::encode(osd_epochs, bl);
  ENCODE_FINISH(bl);
}

void PGMap::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 4, 4, bl);
  ::decode(version, bl);
  if (struct_v < 3) {
    pg_stat.clear();
    uint32_t n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_t pgid = opgid;
      ::decode(pg_stat[pgid], bl);
    }
  } else {
    ::decode(pg_stat, bl);
  }
  ::decode(osd_stat, bl);
  ::decode(last_osdmap_epoch, bl);
  ::decode(last_pg_scan, bl);
  if (struct_v >= 2) {
    ::decode(full_ratio, bl);
    ::decode(nearfull_ratio, bl);
  }
  if (struct_v >= 5)
    ::decode(stamp, bl);
  if (struct_v >= 6) {
    ::decode(osd_epochs, bl);
  } else {
    for (ceph::unordered_map<int32_t, osd_stat_t>::iterator i = osd_stat.begin();
	 i != osd_stat.end();
	 ++i) {
      // This isn't accurate, but will cause trimming to behave like
      // previously.
      osd_epochs.insert(make_pair(i->first, last_osdmap_epoch));
    }
  }
  DECODE_FINISH(bl);

  calc_stats();
}

void PGMap::dirty_all(Incremental& inc)
{
  inc.osdmap_epoch = last_osdmap_epoch;
  inc.pg_scan = last_pg_scan;
  inc.full_ratio = full_ratio;
  inc.nearfull_ratio = nearfull_ratio;

  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p = pg_stat.begin(); p != pg_stat.end(); ++p) {
    inc.pg_stat_updates[p->first] = p->second;
  }
  for (ceph::unordered_map<int32_t, osd_stat_t>::const_iterator p = osd_stat.begin(); p != osd_stat.end(); ++p) {
    assert(osd_epochs.count(p->first));
    inc.update_stat(p->first,
		   inc.get_osd_epochs().find(p->first)->second,
		   p->second);
  }
}

void PGMap::dump(Formatter *f) const
{
  dump_basic(f);
  dump_pg_stats(f, false);
  dump_pool_stats(f);
  dump_osd_stats(f);
}

void PGMap::dump_basic(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("last_osdmap_epoch", last_osdmap_epoch);
  f->dump_unsigned("last_pg_scan", last_pg_scan);
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("near_full_ratio", nearfull_ratio);
  
  f->open_object_section("pg_stats_sum");
  pg_sum.dump(f);
  f->close_section();

  f->open_object_section("osd_stats_sum");
  osd_sum.dump(f);
  f->close_section();

  dump_delta(f);
}

void PGMap::dump_delta(Formatter *f) const
{
  f->open_object_section("pg_stats_delta");
  pg_sum_delta.dump(f);
  f->close_section();
}

void PGMap::dump_pg_stats(Formatter *f, bool brief) const
{
  f->open_array_section("pg_stats");
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    if (brief)
      i->second.dump_brief(f);
    else
      i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pool_stats(Formatter *f) const
{
  f->open_array_section("pool_stats");
  for (ceph::unordered_map<int,pool_stat_t>::const_iterator p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p) {
    f->open_object_section("pool_stat");
    f->dump_int("poolid", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_osd_stats(Formatter *f) const
{
  f->open_array_section("osd_stats");
  for (ceph::unordered_map<int32_t,osd_stat_t>::const_iterator q = osd_stat.begin();
       q != osd_stat.end();
       ++q) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", q->first);
    q->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pg_stats_plain(ostream& ss,
				const ceph::unordered_map<pg_t, pg_stat_t>& pg_stats) const
{
  ss << "pg_stat\tobjects\tbytes\tlog\tdisklog\tstate"
     << "\tstate_stamp\tv\treported\tosd"
     << std::endl;
  for (ceph::unordered_map<pg_t, pg_stat_t>::const_iterator i
	 = pg_stats.begin();
       i != pg_stats.end(); ++i) {
    const pg_stat_t &st(i->second);
    ss << i->first
       << "\t" << st.stats.sum.num_objects
       << "\t" << st.stats.sum.num_bytes
       << "\t" << st.log_size
       << "\t" << st.ondisk_log_size
       << "\t" << pg_state_string(st.state)
       << "\t" << st.last_change
       << "\t" << st.version
       << "\t" << st.reported_epoch << ":" << st.reported_seq
       << "\t" << st.osd
       << std::endl;
  }
}

void PGMap::dump(ostream& ss) const
{
  ss << "version " << version << std::endl;
  ss << "stamp " << stamp << std::endl;
  ss << "last_osdmap_epoch " << last_osdmap_epoch << std::endl;
  ss << "last_pg_scan " << last_pg_scan << std::endl;
  ss << "full_ratio " << full_ratio << std::endl;
  ss << "nearfull_ratio " << nearfull_ratio << std::endl;
  dump_pg_stats_plain(ss, pg_stat);
  for (ceph::unordered_map<int,pool_stat_t>::const_iterator p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p)
    ss << "pool " << p->first
       << "\t" << p->second.stats.sum.num_objects
      //<< "\t" << p->second.num_object_copies
       << "\t" << p->second.stats.sum.num_bytes
       << "\t" << p->second.log_size
       << "\t" << p->second.ondisk_log_size
       << std::endl;
  ss << " sum\t" << pg_sum.stats.sum.num_objects
     << "\t" << pg_sum.stats.sum.num_bytes
     << "\t" << pg_sum.log_size
     << "\t" << pg_sum.ondisk_log_size
     << std::endl;
  ss << "osdstat\tkbused\tkbavail\tkb\thb in\thb out" << std::endl;
  for (ceph::unordered_map<int32_t,osd_stat_t>::const_iterator p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    ss << p->first
       << "\t" << p->second.kb_used
       << "\t" << p->second.kb_avail 
       << "\t" << p->second.kb
       << "\t" << p->second.hb_in
       << "\t" << p->second.hb_out
       << std::endl;
  ss << " sum\t" << osd_sum.kb_used
     << "\t" << osd_sum.kb_avail 
     << "\t" << osd_sum.kb
     << std::endl;
}

void PGMap::dump_osd_perf_stats(Formatter *f) const
{
  f->open_array_section("osd_perf_infos");
  for (ceph::unordered_map<int32_t, osd_stat_t>::const_iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    f->open_object_section("osd");
    f->dump_int("id", i->first);
    {
      f->open_object_section("perf_stats");
      i->second.fs_perf_stat.dump(f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}
void PGMap::print_osd_perf_stats(std::ostream *ss) const
{
  TextTable tab;
  tab.define_column("osdid", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("fs_commit_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("fs_apply_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  for (ceph::unordered_map<int32_t, osd_stat_t>::const_iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    tab << i->first;
    tab << i->second.fs_perf_stat.filestore_commit_latency;
    tab << i->second.fs_perf_stat.filestore_apply_latency;
    tab << TextTable::endrow;
  }
  (*ss) << tab;
}

void PGMap::client_io_rate_summary(Formatter *f, ostream *out,
				   const pool_stat_t& delta_sum,
				   utime_t delta_stamp) const
{
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)delta_stamp;
      if (f) {
	f->dump_int("read_bytes_sec", rd);
      } else {
	*out << pretty_si_t(rd) << "B/s rd, ";
      }
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)delta_stamp;
      if (f) {
	f->dump_int("write_bytes_sec", wr);
      } else {
	*out << pretty_si_t(wr) << "B/s wr, ";
      }
    }
    int64_t iops = (pos_delta.stats.sum.num_rd + pos_delta.stats.sum.num_wr) / (double)delta_stamp;
    if (f) {
      f->dump_int("op_per_sec", iops);
    } else {
      *out << pretty_si_t(iops) << "op/s";
    }
  }
}

void PGMap::overall_client_io_rate_summary(Formatter *f, ostream *out) const
{
  client_io_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMap::pool_client_io_rate_summary(Formatter *f, ostream *out,
                                        uint64_t poolid) const
{
  ceph::unordered_map<uint64_t,pair<pool_stat_t,utime_t> >::const_iterator p =
    per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;
  ceph::unordered_map<uint64_t,utime_t>::const_iterator ts =
    per_pool_sum_deltas_stamps.find(p->first);
  assert(ts != per_pool_sum_deltas_stamps.end());
  client_io_rate_summary(f, out, p->second.first, ts->second);
}

/**
 * update aggregated delta
 *
 * @param cct               ceph context
 * @param ts                Timestamp for the stats being delta'ed
 * @param old_pool_sum      Previous stats sum
 * @param last_ts           Last timestamp for pool
 * @param result_pool_sum   Resulting stats
 * @param result_ts_delta   Resulting timestamp delta
 * @param delta_avg_list    List of last N computed deltas, used to average
 */
void PGMap::update_delta(CephContext *cct,
                         const utime_t ts,
                         const pool_stat_t& old_pool_sum,
                         utime_t *last_ts,
                         const pool_stat_t& current_pool_sum,
                         pool_stat_t *result_pool_delta,
                         utime_t *result_ts_delta,
                         list<pair<pool_stat_t,utime_t> > *delta_avg_list)
{
  /* @p ts is the timestamp we want to associate with the data
   * in @p old_pool_sum, and on which we will base ourselves to
   * calculate the delta, stored in 'delta_t'.
   */
  utime_t delta_t;
  delta_t = ts;         // start with the provided timestamp
  delta_t -= *last_ts;  // take the last timestamp we saw
  *last_ts = ts;        // @p ts becomes the last timestamp we saw

  // calculate a delta, and average over the last 2 deltas.
  /* start by taking a copy of our current @p result_pool_sum, and by
   * taking out the stats from @p old_pool_sum.  This generates a stats
   * delta.  Stash this stats delta in @p delta_avg_list, along with the
   * timestamp delta for these results.
   */
  pool_stat_t d = current_pool_sum;
  d.stats.sub(old_pool_sum.stats);
  delta_avg_list->push_back(make_pair(d,delta_t));
  *result_ts_delta += delta_t;

  /* Aggregate current delta, and take out the last seen delta (if any) to
   * average it out.
   */
  result_pool_delta->stats.add(d.stats);
  size_t s = MAX(1, cct ? cct->_conf->mon_stat_smooth_intervals : 1);
  if (delta_avg_list->size() > s) {
    result_pool_delta->stats.sub(delta_avg_list->front().first.stats);
    *result_ts_delta -= delta_avg_list->front().second;
    delta_avg_list->pop_front();
  }
}

/**
 * update aggregated delta
 *
 * @param cct            ceph context
 * @param ts             Timestamp
 * @param pg_sum_old     Old pg_sum
 */
void PGMap::update_global_delta(CephContext *cct,
                         const utime_t ts, const pool_stat_t& pg_sum_old)
{
  update_delta(cct, ts, pg_sum_old, &stamp, pg_sum, &pg_sum_delta,
               &stamp_delta, &pg_sum_deltas);
}

/**
 * Update a given pool's deltas
 *
 * @param cct           Ceph Context
 * @param ts            Timestamp for the stats being delta'ed
 * @param pool          Pool's id
 * @param old_pool_sum  Previous stats sum
 */
void PGMap::update_one_pool_delta(CephContext *cct,
                                  const utime_t ts,
                                  const uint64_t pool,
                                  const pool_stat_t& old_pool_sum)
{
  if (per_pool_sum_deltas.count(pool) == 0) {
    assert(per_pool_sum_deltas_stamps.count(pool) == 0);
    assert(per_pool_sum_delta.count(pool) == 0);
  }

  pair<pool_stat_t,utime_t>& sum_delta = per_pool_sum_delta[pool];

  update_delta(cct, ts, old_pool_sum, &sum_delta.second, pg_pool_sum[pool],
               &sum_delta.first, &per_pool_sum_deltas_stamps[pool],
               &per_pool_sum_deltas[pool]);
}

/**
 * Update pools' deltas
 *
 * @param cct               CephContext
 * @param ts                Timestamp for the stats being delta'ed
 * @param pg_pool_sum_old   Map of pool stats for delta calcs.
 */
void PGMap::update_pool_deltas(CephContext *cct, const utime_t ts,
                               const ceph::unordered_map<uint64_t,pool_stat_t>& pg_pool_sum_old)
{
  for (ceph::unordered_map<uint64_t,pool_stat_t>::const_iterator it = pg_pool_sum_old.begin();
       it != pg_pool_sum_old.end(); ++it) {
    update_one_pool_delta(cct, ts, it->first, it->second);
  }
}

void PGMap::clear_delta()
{
  pg_sum_delta = pool_stat_t();
  pg_sum_deltas.clear();
  stamp_delta = utime_t();
}

void PGMap::print_summary(Formatter *f, ostream *out) const
{
  std::stringstream ss;
  if (f)
    f->open_array_section("pgs_by_state");

  for (ceph::unordered_map<int,int>::const_iterator p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    if (f) {
      f->open_object_section("pgs_by_state_element");
      f->dump_string("state_name", pg_state_string(p->first));
      f->dump_unsigned("count", p->second);
      f->close_section();
    } else {
      ss.setf(std::ios::right);
      ss << "             " << std::setw(7) << p->second << " " << pg_state_string(p->first) << "\n";
      ss.unsetf(std::ios::right);
    }
  }
  if (f)
    f->close_section();

  if (f) {
    f->dump_unsigned("version", version);
    f->dump_unsigned("num_pgs", pg_stat.size());
    f->dump_unsigned("data_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_unsigned("bytes_used", osd_sum.kb_used * 1024ull);
    f->dump_unsigned("bytes_avail", osd_sum.kb_avail * 1024ull);
    f->dump_unsigned("bytes_total", osd_sum.kb * 1024ull);
  } else {
    *out << "      pgmap v" << version << ": "
	 << pg_stat.size() << " pgs, " << pg_pool_sum.size() << " pools, "
	 << prettybyte_t(pg_sum.stats.sum.num_bytes) << " data, "
	 << pretty_si_t(pg_sum.stats.sum.num_objects) << "objects\n";
    *out << "            "
	 << kb_t(osd_sum.kb_used) << " used, "
	 << kb_t(osd_sum.kb_avail) << " / "
	 << kb_t(osd_sum.kb) << " avail\n";
  }

  std::stringstream ssr;
  overall_client_io_rate_summary(f, &ssr);
  if (!f && ssr.str().length())
    *out << "  client io " << ssr.str() << "\n";


}

void PGMap::print_oneline_summary(ostream *out) const
{
  std::stringstream ss;

  for (ceph::unordered_map<int,int>::const_iterator p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    if (p != num_pg_by_state.begin())
      ss << ", ";
    ss << p->second << " " << pg_state_string(p->first);
  }

  string states = ss.str();
  *out << "v" << version << ": "
       << pg_stat.size() << " pgs: "
       << states << "; "
       << prettybyte_t(pg_sum.stats.sum.num_bytes) << " data, "
       << kb_t(osd_sum.kb_used) << " used, "
       << kb_t(osd_sum.kb_avail) << " / "
       << kb_t(osd_sum.kb) << " avail";

  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = pg_sum_delta;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    *out << "; ";
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)stamp_delta;
      *out << pretty_si_t(rd) << "B/s rd, ";
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)stamp_delta;
      *out << pretty_si_t(wr) << "B/s wr, ";
    }
    int64_t iops = (pos_delta.stats.sum.num_rd + pos_delta.stats.sum.num_wr) / (double)stamp_delta;
    *out << pretty_si_t(iops) << "op/s";
  }
}

void PGMap::generate_test_instances(list<PGMap*>& o)
{
  o.push_back(new PGMap);
  o.push_back(new PGMap);
  list<Incremental*> inc;
  Incremental::generate_test_instances(inc);
  inc.pop_front();
  while (!inc.empty()) {
    o.back()->apply_incremental(NULL, *inc.front());
    delete inc.front();
    inc.pop_front();
  }
}
