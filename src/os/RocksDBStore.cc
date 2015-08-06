// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <set>
#include <map>
#include <string>
#include <memory>
#include <errno.h>

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/convenience.h"
using std::string;
#include "common/perf_counters.h"
#include "include/str_map.h"
#include "KeyValueDB.h"
#include "RocksDBStore.h"

int string2bool(string val, bool &b_val)
{
  if (strcasecmp(val.c_str(), "false") == 0) {
    b_val = false;
    return 0;
  } else if (strcasecmp(val.c_str(), "true") == 0) {
    b_val = true;
    return 0;
  } else {
    std::string err;
    int b = strict_strtol(val.c_str(), 10, &err);
    if (!err.empty())
      return -EINVAL;
    b_val = !!b;
    return 0;
  }
}
  
int RocksDBStore::tryInterpret(const string key, const string val, rocksdb::Options &opt)
{
  if (key == "compaction_threads") {
    std::string err;
    int f = strict_sistrtoll(val.c_str(), &err);
    if (!err.empty())
      return -EINVAL;
    //Low priority threadpool is used for compaction
    opt.env->SetBackgroundThreads(f, rocksdb::Env::Priority::LOW);
  } else if (key == "flusher_threads") {
    std::string err;
    int f = strict_sistrtoll(val.c_str(), &err);
    if (!err.empty())
      return -EINVAL;
    //High priority threadpool is used for flusher
    opt.env->SetBackgroundThreads(f, rocksdb::Env::Priority::HIGH);
  } else if (key == "compact_on_mount") {
    int ret = string2bool(val, compact_on_mount);
    if (ret != 0)
      return ret;
  } else if (key == "disableWAL") {
    int ret = string2bool(val, disableWAL);
    if (ret != 0)
      return ret;
  } else {
    //unrecognize config options.
    return -EINVAL;
  }
  return 0;
}

int RocksDBStore::ParseOptionsFromString(const string opt_str, rocksdb::Options &opt)
{
  map<string, string> str_map;
  int r = get_str_map(opt_str, "\n;", &str_map);
  if (r < 0)
    return r;
  map<string, string>::iterator it;
  for(it = str_map.begin(); it != str_map.end(); ++it) {
    string this_opt = it->first + "=" + it->second;
    rocksdb::Status status = rocksdb::GetOptionsFromString(opt, this_opt , &opt); 
    if (!status.ok()) {
      //unrecognized by rocksdb, try to interpret by ourselves.
      r = tryInterpret(it->first, it->second, opt);
      if (r < 0) {
	derr << status.ToString() << dendl;
	return -EINVAL;
      }
    }
  }
  return 0;
}

int RocksDBStore::init(string _options_str)
{
  options_str = _options_str;
  rocksdb::Options opt;
  //try parse options
  int r = ParseOptionsFromString(options_str, opt); 
  if (r != 0) {
    return -EINVAL;
  }
  return 0;
}

int RocksDBStore::do_open(ostream &out, bool create_if_missing)
{
  rocksdb::Options opt;
  rocksdb::Status status;

  int r = ParseOptionsFromString(options_str, opt); 
  if (r != 0) {
    return -EINVAL;
  }
  opt.create_if_missing = create_if_missing;

  status = rocksdb::DB::Open(opt, path, &db);
  if (!status.ok()) {
    derr << status.ToString() << dendl;
    return -EINVAL;
  }

  PerfCountersBuilder plb(g_ceph_context, "rocksdb", l_rocksdb_first, l_rocksdb_last);
  plb.add_u64_counter(l_rocksdb_gets, "rocksdb_get", "Gets");
  plb.add_u64_counter(l_rocksdb_txns, "rocksdb_transaction", "Transactions");
  plb.add_time_avg(l_rocksdb_get_latency, "rocksdb_get_latency", "Get latency");
  plb.add_time_avg(l_rocksdb_submit_latency, "rocksdb_submit_latency", "Submit Latency");
  plb.add_time_avg(l_rocksdb_submit_sync_latency, "rocksdb_submit_sync_latency", "Submit Sync Latency");
  plb.add_u64_counter(l_rocksdb_compact, "rocksdb_compact", "Compactions");
  plb.add_u64_counter(l_rocksdb_compact_range, "rocksdb_compact_range", "Compactions by range");
  plb.add_u64_counter(l_rocksdb_compact_queue_merge, "rocksdb_compact_queue_merge", "Mergings of ranges in compaction queue");
  plb.add_u64(l_rocksdb_compact_queue_len, "rocksdb_compact_queue_len", "Length of compaction queue");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  if (compact_on_mount) {
    derr << "Compacting rocksdb store..." << dendl;
    compact();
    derr << "Finished compacting rocksdb store" << dendl;
  }
  return 0;
}

int RocksDBStore::_test_init(const string& dir)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
  delete db;
  return status.ok() ? 0 : -EIO;
}

RocksDBStore::~RocksDBStore()
{
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  delete db;
}

void RocksDBStore::close()
{
  // stop compaction thread
  compact_queue_lock.Lock();
  if (compact_thread.is_started()) {
    compact_queue_stop = true;
    compact_queue_cond.Signal();
    compact_queue_lock.Unlock();
    compact_thread.join();
  } else {
    compact_queue_lock.Unlock();
  }

  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int RocksDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  utime_t start = ceph_clock_now(g_ceph_context);
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions woptions;
  woptions.disableWAL = disableWAL;
  rocksdb::Status s = db->Write(woptions, _t->bat);
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_rocksdb_txns);
  logger->tinc(l_rocksdb_submit_latency, lat);
  return s.ok() ? 0 : -1;
}

int RocksDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  utime_t start = ceph_clock_now(g_ceph_context);
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions woptions;
  woptions.sync = true;
  woptions.disableWAL = disableWAL;
  rocksdb::Status s = db->Write(woptions, _t->bat);
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_rocksdb_txns);
  logger->tinc(l_rocksdb_submit_sync_latency, lat);
  return s.ok() ? 0 : -1;
}
int RocksDBStore::get_info_log_level(string info_log_level)
{
  if (info_log_level == "debug") {
    return 0;
  } else if (info_log_level == "info") {
    return 1;
  } else if (info_log_level == "warn") {
    return 2;
  } else if (info_log_level == "error") {
    return 3;
  } else if (info_log_level == "fatal") {
    return 4;
  } else {
    return 1;
  }
}

RocksDBStore::RocksDBTransactionImpl::RocksDBTransactionImpl(RocksDBStore *_db)
{
  db = _db;
  bat = new rocksdb::WriteBatch();
}
RocksDBStore::RocksDBTransactionImpl::~RocksDBTransactionImpl()
{
  delete bat;
}
void RocksDBStore::RocksDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  string key = combine_strings(prefix, k);
  //bufferlist::c_str() is non-constant, so we need to make a copy
  bufferlist val = to_set_bl;
  bat->Delete(rocksdb::Slice(key));
  bat->Put(rocksdb::Slice(key),
	  rocksdb::Slice(val.c_str(), val.length()));
}

void RocksDBStore::RocksDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  bat->Delete(rocksdb::Slice(k));
}

void RocksDBStore::RocksDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    bat->Delete(key);
  }
}

int RocksDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  utime_t start = ceph_clock_now(g_ceph_context);
  KeyValueDB::Iterator it = get_iterator(prefix);
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    it->lower_bound(*i);
    if (it->valid() && it->key() == *i) {
      out->insert(make_pair(*i, it->value()));
    } else if (!it->valid())
      break;
  }
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_rocksdb_gets);
  logger->tinc(l_rocksdb_get_latency, lat);
  return 0;
}

string RocksDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

bufferlist RocksDBStore::to_bufferlist(rocksdb::Slice in)
{
  bufferlist bl;
  bl.append(bufferptr(in.data(), in.size()));
  return bl;
}

int RocksDBStore::split_key(rocksdb::Slice in, string *prefix, string *key)
{
  string in_prefix = in.ToString();
  size_t prefix_len = in_prefix.find('\0');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}

void RocksDBStore::compact()
{
  logger->inc(l_rocksdb_compact);
  db->CompactRange(NULL, NULL);
}


void RocksDBStore::compact_thread_entry()
{
  compact_queue_lock.Lock();
  while (!compact_queue_stop) {
    while (!compact_queue.empty()) {
      pair<string,string> range = compact_queue.front();
      compact_queue.pop_front();
      logger->set(l_rocksdb_compact_queue_len, compact_queue.size());
      compact_queue_lock.Unlock();
      logger->inc(l_rocksdb_compact_range);
      compact_range(range.first, range.second);
      compact_queue_lock.Lock();
      continue;
    }
    compact_queue_cond.Wait(compact_queue_lock);
  }
  compact_queue_lock.Unlock();
}

void RocksDBStore::compact_range_async(const string& start, const string& end)
{
  Mutex::Locker l(compact_queue_lock);

  // try to merge adjacent ranges.  this is O(n), but the queue should
  // be short.  note that we do not cover all overlap cases and merge
  // opportunities here, but we capture the ones we currently need.
  list< pair<string,string> >::iterator p = compact_queue.begin();
  while (p != compact_queue.end()) {
    if (p->first == start && p->second == end) {
      // dup; no-op
      return;
    }
    if (p->first <= end && p->first > start) {
      // merge with existing range to the right
      compact_queue.push_back(make_pair(start, p->second));
      compact_queue.erase(p);
      logger->inc(l_rocksdb_compact_queue_merge);
      break;
    }
    if (p->second >= start && p->second < end) {
      // merge with existing range to the left
      compact_queue.push_back(make_pair(p->first, end));
      compact_queue.erase(p);
      logger->inc(l_rocksdb_compact_queue_merge);
      break;
    }
    ++p;
  }
  if (p == compact_queue.end()) {
    // no merge, new entry.
    compact_queue.push_back(make_pair(start, end));
    logger->set(l_rocksdb_compact_queue_len, compact_queue.size());
  }
  compact_queue_cond.Signal();
  if (!compact_thread.is_started()) {
    compact_thread.create();
  }
}
bool RocksDBStore::check_omap_dir(string &omap_dir)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, omap_dir, &db);
  delete db;
  return status.ok();
}
void RocksDBStore::compact_range(const string& start, const string& end)
{
    rocksdb::Slice cstart(start);
    rocksdb::Slice cend(end);
    db->CompactRange(&cstart, &cend);
}
RocksDBStore::RocksDBWholeSpaceIteratorImpl::~RocksDBWholeSpaceIteratorImpl()
{
  delete dbiter;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first()
{
  dbiter->SeekToFirst();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  rocksdb::Slice slice_prefix(prefix);
  dbiter->Seek(slice_prefix);
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last()
{
  dbiter->SeekToLast();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  string limit = past_prefix(prefix);
  rocksdb::Slice slice_limit(limit);
  dbiter->Seek(slice_limit);

  if (!dbiter->Valid()) {
    dbiter->SeekToLast();
  } else {
    dbiter->Prev();
  }
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after)
{
  lower_bound(prefix, after);
  if (valid()) {
  pair<string,string> key = raw_key();
    if (key.first == prefix && key.second == after)
      next();
  }
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to)
{
  string bound = combine_strings(prefix, to);
  rocksdb::Slice slice_bound(bound);
  dbiter->Seek(slice_bound);
  return dbiter->status().ok() ? 0 : -1;
}
bool RocksDBStore::RocksDBWholeSpaceIteratorImpl::valid()
{
  return dbiter->Valid();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::next()
{
  if (valid())
  dbiter->Next();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::prev()
{
  if (valid())
    dbiter->Prev();
    return dbiter->status().ok() ? 0 : -1;
}
string RocksDBStore::RocksDBWholeSpaceIteratorImpl::key()
{
  string out_key;
  split_key(dbiter->key(), 0, &out_key);
  return out_key;
}
pair<string,string> RocksDBStore::RocksDBWholeSpaceIteratorImpl::raw_key()
{
  string prefix, key;
  split_key(dbiter->key(), &prefix, &key);
  return make_pair(prefix, key);
}
bufferlist RocksDBStore::RocksDBWholeSpaceIteratorImpl::value()
{
  return to_bufferlist(dbiter->value());
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::status()
{
  return dbiter->status().ok() ? 0 : -1;
}

string RocksDBStore::past_prefix(const string &prefix)
{
  string limit = prefix;
  limit.push_back(1);
  return limit;
}


RocksDBStore::WholeSpaceIterator RocksDBStore::_get_iterator()
{
  return std::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBWholeSpaceIteratorImpl(
      db->NewIterator(rocksdb::ReadOptions())
    )
  );
}

RocksDBStore::WholeSpaceIterator RocksDBStore::_get_snapshot_iterator()
{
  const rocksdb::Snapshot *snapshot;
  rocksdb::ReadOptions options;

  snapshot = db->GetSnapshot();
  options.snapshot = snapshot;

  return std::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBSnapshotIteratorImpl(db, snapshot,
      db->NewIterator(options))
  );
}

RocksDBStore::RocksDBSnapshotIteratorImpl::~RocksDBSnapshotIteratorImpl()
{
  db->ReleaseSnapshot(snapshot);
}
