// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mdstypes.h"
#include "common/Formatter.h"

/*
 * frag_info_t
 */

void frag_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(mtime, bl);
  ::encode(nfiles, bl);
  ::encode(nsubdirs, bl);
  ENCODE_FINISH(bl);
}

void frag_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(mtime, bl);
  ::decode(nfiles, bl);
  ::decode(nsubdirs, bl);
  DECODE_FINISH(bl);
}

void frag_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("num_files", nfiles);
  f->dump_unsigned("num_subdirs", nsubdirs);
}

void frag_info_t::generate_test_instances(list<frag_info_t*>& ls)
{
  ls.push_back(new frag_info_t);
  ls.push_back(new frag_info_t);
  ls.back()->version = 1;
  ls.back()->mtime = ceph::real_time(std::chrono::seconds(2) +
				     std::chrono::nanoseconds(3));
  ls.back()->nfiles = 4;
  ls.back()->nsubdirs = 5;
}

ostream& operator<<(ostream &out, const frag_info_t &f)
{
  if (f == frag_info_t())
    return out << "f()";
  out << "f(v" << f.version;
  if (f.mtime != ceph::real_time::min())
    out << " m" << f.mtime;
  if (f.nfiles || f.nsubdirs)
    out << " " << f.size() << "=" << f.nfiles << "+" << f.nsubdirs;
  out << ")";
  return out;
}


/*
 * nest_info_t
 */

void nest_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(rbytes, bl);
  ::encode(rfiles, bl);
  ::encode(rsubdirs, bl);
  ::encode(ranchors, bl);
  ::encode(rctime, bl);
  ENCODE_FINISH(bl);
}

void nest_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(rbytes, bl);
  ::decode(rfiles, bl);
  ::decode(rsubdirs, bl);
  ::decode(ranchors, bl);
  ::decode(rctime, bl);
  DECODE_FINISH(bl);
}

void nest_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_unsigned("rbytes", rbytes);
  f->dump_unsigned("rfiles", rfiles);
  f->dump_unsigned("rsubdirs", rsubdirs);
  f->dump_unsigned("ranchors", ranchors);
  f->dump_stream("rctime") << rctime;
}

void nest_info_t::generate_test_instances(list<nest_info_t*>& ls)
{
  ls.push_back(new nest_info_t);
  ls.push_back(new nest_info_t);
  ls.back()->version = 1;
  ls.back()->rbytes = 2;
  ls.back()->rfiles = 3;
  ls.back()->rsubdirs = 4;
  ls.back()->ranchors = 5;
  ls.back()->rctime = ceph::real_time(std::chrono::seconds(7) +
				      std::chrono::nanoseconds(8));
}

ostream& operator<<(ostream &out, const nest_info_t &n)
{
  if (n == nest_info_t())
    return out << "n()";
  out << "n(v" << n.version;
  if (n.rctime != ceph::real_time::min())
    out << " rc" << n.rctime;
  if (n.rbytes)
    out << " b" << n.rbytes;
  if (n.ranchors)
    out << " a" << n.ranchors;
  if (n.rfiles || n.rsubdirs)
    out << " " << n.rsize() << "=" << n.rfiles << "+" << n.rsubdirs;
  out << ")";
  return out;
}


/*
 * client_writeable_range_t
 */

void client_writeable_range_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(range.first, bl);
  ::encode(range.last, bl);
  ENCODE_FINISH(bl);
}

void client_writeable_range_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(range.first, bl);
  ::decode(range.last, bl);
  DECODE_FINISH(bl);
}

void client_writeable_range_t::dump(Formatter *f) const
{
  f->open_object_section("byte range");
  f->dump_unsigned("first", range.first);
  f->dump_unsigned("last", range.last);
  f->close_section();
}

void client_writeable_range_t::generate_test_instances(list<client_writeable_range_t*>& ls)
{
  ls.push_back(new client_writeable_range_t);
  ls.push_back(new client_writeable_range_t);
  ls.back()->range.first = 123;
  ls.back()->range.last = 456;
}

ostream& operator<<(ostream& out, const client_writeable_range_t& r)
{
  return out << r.range.first << '-' << r.range.last;
}


/*
 * inode_t
 */
void inode_t::encode(bufferlist &bl) const
{
  ENCODE_START(10, 6, bl);

  ::encode(ino, bl);
  ::encode(rdev, bl);
  ::encode(ctime, bl);

  ::encode(mode, bl);
  ::encode(uid, bl);
  ::encode(gid, bl);

  ::encode(nlink, bl);
  ::encode(anchored, bl);

  ::encode(size, bl);
  ::encode(truncate_seq, bl);
  ::encode(truncate_size, bl);
  ::encode(truncate_from, bl);
  ::encode(truncate_pending, bl);
  ::encode(mtime, bl);
  ::encode(atime, bl);
  ::encode(time_warp_seq, bl);
  ::encode(client_ranges, bl);

  ::encode(dirstat, bl);
  ::encode(rstat, bl);
  ::encode(accounted_rstat, bl);

  ::encode(version, bl);
  ::encode(file_data_version, bl);
  ::encode(xattr_version, bl);
  ::encode(backtrace_version, bl);
  ::encode(old_volumes, bl);
  ::encode(max_size_ever, bl);
  ::encode(inline_version, bl);
  ::encode(inline_data, bl);

  ENCODE_FINISH(bl);
}

void inode_t::decode(bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(10, 6, 6, p);

  ::decode(ino, p);
  ::decode(rdev, p);
  ::decode(ctime, p);

  ::decode(mode, p);
  ::decode(uid, p);
  ::decode(gid, p);

  ::decode(nlink, p);
  ::decode(anchored, p);

  // was decode dir_layout if struct_v >= 4
  ::decode(size, p);
  ::decode(truncate_seq, p);
  ::decode(truncate_size, p);
  ::decode(truncate_from, p);
  if (struct_v >= 5)
    ::decode(truncate_pending, p);
  else
    truncate_pending = 0;
  ::decode(mtime, p);
  ::decode(atime, p);
  ::decode(time_warp_seq, p);
  if (struct_v >= 3) {
    ::decode(client_ranges, p);
  } else {
    map<client_t, client_writeable_range_t::byte_range_t> m;
    ::decode(m, p);
    for (map<client_t, client_writeable_range_t::byte_range_t>::iterator
	q = m.begin(); q != m.end(); ++q)
      client_ranges[q->first].range = q->second;
  }

  ::decode(dirstat, p);
  ::decode(rstat, p);
  ::decode(accounted_rstat, p);

  ::decode(version, p);
  ::decode(file_data_version, p);
  ::decode(xattr_version, p);
  if (struct_v >= 2)
    ::decode(backtrace_version, p);
  if (struct_v >= 7)
    ::decode(old_volumes, p);
  if (struct_v >= 8)
    ::decode(max_size_ever, p);
  if (struct_v >= 9) {
    ::decode(inline_version, p);
    ::decode(inline_data, p);
  } else {
    inline_version = CEPH_INLINE_NONE;
  }
  if (struct_v < 10)
    backtrace_version = 0; // force update backtrace

  DECODE_FINISH(p);
}

void inode_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_unsigned("mode", mode);
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("nlink", nlink);
  f->dump_unsigned("anchored", (int)anchored);

  f->open_array_section("old_volumes");
  auto i = old_volumes.cbegin();
  while(i != old_volumes.cend()) {
    f->dump_stream("uuid") << *i;
  }
  f->close_section();

  f->dump_unsigned("size", size);
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->dump_unsigned("truncate_from", truncate_from);
  f->dump_unsigned("truncate_pending", truncate_pending);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_unsigned("time_warp_seq", time_warp_seq);

  f->open_array_section("client_ranges");
  for (map<client_t,client_writeable_range_t>::const_iterator p = client_ranges.begin(); p != client_ranges.end(); ++p) {
    f->open_object_section("client");
    f->dump_unsigned("client", p->first.v);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("dirstat");
  dirstat.dump(f);
  f->close_section();

  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();

  f->dump_unsigned("version", version);
  f->dump_unsigned("file_data_version", file_data_version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("backtrace_version", backtrace_version);
}

void inode_t::generate_test_instances(list<inode_t*>& ls)
{
  ls.push_back(new inode_t);
  ls.push_back(new inode_t);
  ls.back()->ino = 1;
  // i am lazy.
}


/*
 * fnode_t
 */
void fnode_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(fragstat, bl);
  ::encode(accounted_fragstat, bl);
  ::encode(rstat, bl);
  ::encode(accounted_rstat, bl);
  ENCODE_FINISH(bl);
}

void fnode_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(fragstat, bl);
  ::decode(accounted_fragstat, bl);
  ::decode(rstat, bl);
  ::decode(accounted_rstat, bl);
  DECODE_FINISH(bl);
}

void fnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);

  f->open_object_section("fragstat");
  fragstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_fragstat");
  accounted_fragstat.dump(f);
  f->close_section();

  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();
}

void fnode_t::generate_test_instances(list<fnode_t*>& ls)
{
  ls.push_back(new fnode_t);
  ls.push_back(new fnode_t);
  ls.back()->version = 1;
  list<frag_info_t*> fls;
  frag_info_t::generate_test_instances(fls);
  ls.back()->fragstat = *fls.back();
  ls.back()->accounted_fragstat = *fls.front();
  list<nest_info_t*> nls;
  nest_info_t::generate_test_instances(nls);
  ls.back()->rstat = *nls.front();
  ls.back()->accounted_rstat = *nls.back();
}


/*
 * session_info_t
 */
void session_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(inst, bl);
  ::encode(completed_requests, bl);
  ::encode(prealloc_inos, bl);   // hacky, see below.
  ::encode(used_inos, bl);
  ENCODE_FINISH(bl);
}

void session_info_t::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, p);
  ::decode(inst, p);
  if (struct_v <= 2) {
    set<ceph_tid_t> s;
    ::decode(s, p);
    while (!s.empty()) {
      completed_requests[*s.begin()] = inodeno_t();
      s.erase(s.begin());
    }
  } else {
    ::decode(completed_requests, p);
  }
  ::decode(prealloc_inos, p);
  ::decode(used_inos, p);
  prealloc_inos.insert(used_inos);
  used_inos.clear();
  DECODE_FINISH(p);
}

void session_info_t::dump(Formatter *f) const
{
  f->dump_stream("inst") << inst;

  f->open_array_section("completed_requests");
  for (map<ceph_tid_t,inodeno_t>::const_iterator p = completed_requests.begin();
       p != completed_requests.end();
       ++p) {
    f->open_object_section("request");
    f->dump_unsigned("tid", p->first);
    f->dump_stream("created_ino") << p->second;
    f->close_section();
  }
  f->close_section();

  f->open_array_section("prealloc_inos");
  for (interval_set<inodeno_t>::const_iterator p = prealloc_inos.begin();
       p != prealloc_inos.end();
       ++p) {
    f->open_object_section("ino_range");
    f->dump_unsigned("start", p.get_start());
    f->dump_unsigned("length", p.get_len());
    f->close_section();
  }
  f->close_section();

  f->open_array_section("used_inos");
  for (interval_set<inodeno_t>::const_iterator p = prealloc_inos.begin();
       p != prealloc_inos.end();
       ++p) {
    f->open_object_section("ino_range");
    f->dump_unsigned("start", p.get_start());
    f->dump_unsigned("length", p.get_len());
    f->close_section();
  }
  f->close_section();
}

void session_info_t::generate_test_instances(list<session_info_t*>& ls)
{
  ls.push_back(new session_info_t);
  ls.push_back(new session_info_t);
  ls.back()->inst = entity_inst_t(entity_name_t::MDS(12), entity_addr_t());
  ls.back()->completed_requests.insert(make_pair(234, inodeno_t(111222)));
  ls.back()->completed_requests.insert(make_pair(237, inodeno_t(222333)));
  ls.back()->prealloc_inos.insert(333, 12);
  ls.back()->prealloc_inos.insert(377, 112);
  // we can't add used inos; they're cleared on decode
}


/*
 * MDSCacheObjectInfo
 */
void MDSCacheObjectInfo::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(ino, bl);
  ::encode(dirfrag, bl);
  ::encode(dname, bl);
  ENCODE_FINISH(bl);
}

void MDSCacheObjectInfo::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  ::decode(ino, p);
  ::decode(dirfrag, p);
  ::decode(dname, p);
  DECODE_FINISH(p);
}

void MDSCacheObjectInfo::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_stream("dirfrag") << dirfrag;
  f->dump_string("name", dname);
}

void MDSCacheObjectInfo::generate_test_instances(list<MDSCacheObjectInfo*>& ls)
{
  ls.push_back(new MDSCacheObjectInfo);
  ls.push_back(new MDSCacheObjectInfo);
  ls.back()->ino = 1;
  ls.back()->dirfrag = dirfrag_t(2, 3);
  ls.back()->dname = "fooname";
  ls.push_back(new MDSCacheObjectInfo);
  ls.back()->ino = 121;
  ls.back()->dirfrag = dirfrag_t(222, 0);
  ls.back()->dname = "bar foo";
}


/*
 * mds_table_pending_t
 */
void mds_table_pending_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(reqid, bl);
  ::encode(mds, bl);
  ::encode(tid, bl);
  ENCODE_FINISH(bl);
}

void mds_table_pending_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(reqid, bl);
  ::decode(mds, bl);
  ::decode(tid, bl);
  DECODE_FINISH(bl);
}

void mds_table_pending_t::dump(Formatter *f) const
{
  f->dump_unsigned("reqid", reqid);
  f->dump_unsigned("mds", mds);
  f->dump_unsigned("tid", tid);
}

void mds_table_pending_t::generate_test_instances(list<mds_table_pending_t*>& ls)
{
  ls.push_back(new mds_table_pending_t);
  ls.push_back(new mds_table_pending_t);
  ls.back()->reqid = 234;
  ls.back()->mds = 2;
  ls.back()->tid = 35434;
}


/*
 * inode_load_vec_t
 */
void inode_load_vec_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  for (int i=0; i<NUM; i++)
    ::encode(vec[i], bl);
  ENCODE_FINISH(bl);
}

void inode_load_vec_t::decode(bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  for (int i=0; i<NUM; i++)
    ::decode(vec[i], p);
  DECODE_FINISH(p);
}

void inode_load_vec_t::dump(Formatter *f)
{
  f->open_array_section("Decay Counters");
  for (vector<DecayCounter>::const_iterator i = vec.begin(); i != vec.end(); ++i) {
    f->open_object_section("Decay Counter");
    i->dump(f);
    f->close_section();
  }
  f->close_section();
}

void inode_load_vec_t::generate_test_instances(list<inode_load_vec_t*>& ls)
{
  ceph::real_time sample;
  ls.push_back(new inode_load_vec_t(sample));
}


/*
 * dirfrag_load_vec_t
 */
void dirfrag_load_vec_t::dump(Formatter *f) const
{
  f->open_array_section("Decay Counters");
  for (vector<DecayCounter>::const_iterator i = vec.begin(); i != vec.end(); ++i) {
    f->open_object_section("Decay Counter");
    i->dump(f);
    f->close_section();
  }
  f->close_section();
}

void dirfrag_load_vec_t::generate_test_instances(list<dirfrag_load_vec_t*>& ls)
{
  ceph::real_time sample;
  ls.push_back(new dirfrag_load_vec_t(sample));
}

/*
 * mds_load_t
 */
void mds_load_t::encode(bufferlist &bl) const {
  ENCODE_START(2, 2, bl);
  ::encode(auth, bl);
  ::encode(all, bl);
  ::encode(req_rate, bl);
  ::encode(cache_hit_rate, bl);
  ::encode(queue_len, bl);
  ::encode(cpu_load_avg, bl);
  ENCODE_FINISH(bl);
}

void mds_load_t::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(auth, bl);
  ::decode(all, bl);
  ::decode(req_rate, bl);
  ::decode(cache_hit_rate, bl);
  ::decode(queue_len, bl);
  ::decode(cpu_load_avg, bl);
  DECODE_FINISH(bl);
}

void mds_load_t::dump(Formatter *f) const
{
  f->dump_float("request rate", req_rate);
  f->dump_float("cache hit rate", cache_hit_rate);
  f->dump_float("queue length", queue_len);
  f->dump_float("cpu load", cpu_load_avg);
  f->open_object_section("auth dirfrag");
  auth.dump(f);
  f->close_section();
  f->open_object_section("all dirfrags");
  all.dump(f);
  f->close_section();
}

void mds_load_t::generate_test_instances(list<mds_load_t*>& ls)
{
  ceph::real_time sample;
  ls.push_back(new mds_load_t(sample));
}

/*
 * cap_reconnect_t
 */
void cap_reconnect_t::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode_old(bl); // extract out when something changes
  ENCODE_FINISH(bl);
}

void cap_reconnect_t::encode_old(bufferlist& bl) const {
  ::encode(path, bl);
  capinfo.flock_len = flockbl.length();
  ::encode(capinfo, bl);
  ::encode_nohead(flockbl, bl);
}

void cap_reconnect_t::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  decode_old(bl); // extract out when something changes
  DECODE_FINISH(bl);
}

void cap_reconnect_t::decode_old(bufferlist::iterator& bl) {
  ::decode(path, bl);
  ::decode(capinfo, bl);
  ::decode_nohead(capinfo.flock_len, flockbl, bl);
}

void cap_reconnect_t::dump(Formatter *f) const
{
  f->dump_string("path", path);
  f->dump_int("cap_id", capinfo.cap_id);
  f->dump_string("cap wanted", ccap_string(capinfo.wanted));
  f->dump_string("cap issued", ccap_string(capinfo.issued));
  f->dump_int("path base ino", capinfo.pathbase);
  f->dump_string("has file locks", capinfo.flock_len ? "true" : "false");
}

void cap_reconnect_t::generate_test_instances(list<cap_reconnect_t*>& ls)
{
  ls.push_back(new cap_reconnect_t);
  ls.back()->path = "/test/path";
  ls.back()->capinfo.cap_id = 1;
}
