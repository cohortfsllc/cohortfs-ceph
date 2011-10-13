// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "PG.h"
#include "ReplicatedPG.h"
#include "OSD.h"
#include "PGLS.h"

#include "common/errno.h"
#include "common/perf_counters.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGTrim.h"

#include "messages/MOSDPing.h"
#include "messages/MWatchNotify.h"

#include "Watch.h"

#include "mds/inode_backtrace.h" // Ugh

#include "common/config.h"

#define DOUT_SUBSYS osd
#define DOUT_PREFIX_ARGS this, osd->whoami, osd->osdmap
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, osd->whoami, osd->osdmap)
static ostream& _prefix(std::ostream *_dout, PG *pg, int whoami, OSDMap *osdmap) {
  return *_dout << "osd." << whoami
		<< " " << (osdmap ? osdmap->get_epoch():0) << " " << *pg << " ";
}


#include <sstream>

#include <errno.h>

static const int LOAD_LATENCY    = 1;
static const int LOAD_QUEUE_SIZE = 2;
static const int LOAD_HYBRID     = 3;

// Blank object locator
static const object_locator_t OLOC_BLANK;

PGLSFilter::PGLSFilter()
{
}

PGLSFilter::~PGLSFilter()
{
}

// =======================
// pg changes

bool ReplicatedPG::same_for_read_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_modify_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_rep_modify_since(epoch_t e)
{
  // check osd map: same set, or primary+acker?
  return e >= info.history.same_primary_since;
}

// ====================
// missing objects

bool ReplicatedPG::is_missing_object(const hobject_t& soid)
{
  return missing.missing.count(soid);
}

void ReplicatedPG::wait_for_missing_object(const hobject_t& soid, Message *m)
{
  assert(is_missing_object(soid));

  // we don't have it (yet).
  map<hobject_t, Missing::item>::const_iterator g = missing.missing.find(soid);
  assert(g != missing.missing.end());
  const eversion_t &v(g->second.need);

  map<hobject_t, pull_info_t>::const_iterator p = pulling.find(soid);
  if (p != pulling.end()) {
    dout(7) << "missing " << soid << " v " << v << ", already pulling." << dendl;
  }
  else if (missing_loc.find(soid) == missing_loc.end()) {
    dout(7) << "missing " << soid << " v " << v << ", is unfound." << dendl;
  }
  else {
    dout(7) << "missing " << soid << " v " << v << ", pulling." << dendl;
    pull(soid);
  }
  waiting_for_missing_object[soid].push_back(m);
}

bool ReplicatedPG::is_degraded_object(const hobject_t& soid)
{
  if (missing.missing.count(soid))
    return true;
  for (unsigned i = 1; i < acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
	peer_missing[peer].missing.count(soid))
      return true;
  }
  return false;
}

void ReplicatedPG::wait_for_degraded_object(const hobject_t& soid, Message *m)
{
  assert(is_degraded_object(soid));

  // we don't have it (yet).
  if (pushing.count(soid)) {
    dout(7) << "degraded "
	    << soid 
	    << ", already pushing"
	    << dendl;
  } else {
    dout(7) << "degraded " 
	    << soid 
	    << ", pushing"
	    << dendl;
    eversion_t v;
    for (unsigned i = 1; i < acting.size(); i++) {
      int peer = acting[i];
      if (peer_missing.count(peer) &&
	  peer_missing[peer].missing.count(soid)) {
	v = peer_missing[peer].missing[soid].need;
	break;
      }
    }
    recover_object_replicas(soid, v);
  }
  waiting_for_degraded_object[soid].push_back(m);
}

bool PGLSParentFilter::filter(bufferlist& xattr_data, bufferlist& outdata)
{
  bufferlist::iterator iter = xattr_data.begin();
  inode_backtrace_t bt;

  generic_dout(0) << "PGLSParentFilter::filter" << dendl;

  ::decode(bt, iter);

  vector<inode_backpointer_t>::iterator vi;
  for (vi = bt.ancestors.begin(); vi != bt.ancestors.end(); ++vi) {
    generic_dout(0) << "vi->dirino=" << vi->dirino << " parent_ino=" << parent_ino << dendl;
    if ( vi->dirino == parent_ino) {
      ::encode(*vi, outdata);
      return true;
    }
  }

  return false;
}

bool PGLSPlainFilter::filter(bufferlist& xattr_data, bufferlist& outdata)
{
  if (val.size() != xattr_data.length())
    return false;

  if (memcmp(val.c_str(), xattr_data.c_str(), val.size()))
    return false;

  return true;
}

bool ReplicatedPG::pgls_filter(PGLSFilter *filter, hobject_t& sobj, bufferlist& outdata)
{
  bufferlist bl;

  int ret = osd->store->getattr(coll_t(info.pgid), sobj, filter->get_xattr().c_str(), bl);
  dout(0) << "getattr (sobj=" << sobj << ", attr=" << filter->get_xattr() << ") returned " << ret << dendl;
  if (ret < 0)
    return false;

  return filter->filter(bl, outdata);
}

int ReplicatedPG::get_pgls_filter(bufferlist::iterator& iter, PGLSFilter **pfilter)
{
  string type;
  PGLSFilter *filter;

  ::decode(type, iter);

  if (type.compare("parent") == 0) {
    filter = new PGLSParentFilter(iter);
  } else if (type.compare("plain") == 0) {
    filter = new PGLSPlainFilter(iter);
  } else {
    return -EINVAL;
  }

  *pfilter = filter;

  return  0;
}


// ==========================================================
void ReplicatedPG::do_pg_op(MOSDOp *op)
{
  dout(10) << "do_pg_op " << *op << dendl;

  bufferlist outdata;
  int result = 0;
  string cname, mname;
  PGLSFilter *filter = NULL;
  bufferlist filter_out;

  snapid_t snapid = op->get_snapid();

  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); p++) {
    bufferlist::iterator bp = p->data.begin();
    switch (p->op.op) {
    case CEPH_OSD_OP_PGLS_FILTER:
      ::decode(cname, bp);
      ::decode(mname, bp);

      result = get_pgls_filter(bp, &filter);
      if (result < 0)
        break;

      assert(filter);

      // fall through

    case CEPH_OSD_OP_PGLS:
      if (op->get_pg() != info.pgid) {
        dout(10) << " pgls pg=" << op->get_pg() << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
        dout(10) << " pgls pg=" << op->get_pg() << " count " << p->op.pgls.count << dendl;
	// read into a buffer
        vector<hobject_t> sentries;
        PGLSResponse response;
	::decode(response.handle, bp);

	// reset cookie?
	if (p->op.pgls.start_epoch &&
	    p->op.pgls.start_epoch < info.history.same_primary_since) {
	  dout(10) << " pgls sequence started epoch " << p->op.pgls.start_epoch
		   << " < same_primary_since " << info.history.same_primary_since
		   << ", resetting cookie" << dendl;
	  response.handle = collection_list_handle_t();
	}

	if (response.handle.in_missing_set) {
	  // it's an offset into the missing set
	  version_t v = response.handle.index;
	  dout(10) << " handle low/missing " << v << dendl;
	  map<version_t, hobject_t>::iterator mp = missing.rmissing.lower_bound(v);
	  result = 0;
	  while (sentries.size() < p->op.pgls.count) {
	    if (mp == missing.rmissing.end()) {
	      dout(10) << " handle finished low/missing, moving to high/ondisk" << dendl;
	      response.handle.in_missing_set = false;
	      break;
	    }
	    sentries.push_back(mp->second);
	    response.handle.index = mp->first + 1;
	  }
	}
	if (sentries.size() < p->op.pgls.count &&
	    !response.handle.in_missing_set) {
	  // it's a readdir cookie
	  dout(10) << " handle high/missing " << response.handle << dendl;
	  osr.flush();  // order wrt preceeding writes
	  result = osd->store->collection_list_partial(coll, snapid,
						       sentries, p->op.pgls.count - sentries.size(),
						       &response.handle);
	  response.handle.in_missing_set = false;
	}

	if (result == 0) {
          vector<hobject_t>::iterator iter;
          for (iter = sentries.begin(); iter != sentries.end(); ++iter) {
	    bool keep = true;
	    // skip snapdir objects
	    if (iter->snap == CEPH_SNAPDIR)
	      continue;

	    if (snapid != CEPH_NOSNAP) {
	      // skip items not defined for this snapshot
	      if (iter->snap == CEPH_NOSNAP) {
		bufferlist bl;
		osd->store->getattr(coll, *iter, SS_ATTR, bl);
		SnapSet snapset(bl);
		if (snapid <= snapset.seq)
		  continue;
	      } else {
		bufferlist bl;
		osd->store->getattr(coll, *iter, OI_ATTR, bl);
		object_info_t oi(bl);
		bool exists = false;
		for (vector<snapid_t>::iterator i = oi.snaps.begin(); i != oi.snaps.end(); ++i)
		  if (*i == snapid) {
		    exists = true;
		    break;
		  }
		dout(10) << *iter << " has " << oi.snaps << " .. exists=" << exists << dendl;
		if (!exists)
		  continue;
	      }
	    }
	    if (filter)
	      keep = pgls_filter(filter, *iter, filter_out);

            if (keep)
	      response.entries.push_back(iter->oid);
          }
	  ::encode(response, outdata);
          if (filter)
	    ::encode(filter_out, outdata);
        }
	dout(10) << " pgls result=" << result << " outdata.length()=" << outdata.length() << dendl;
      }
      break;

    default:
      result = -EINVAL;
      break;
    }
  }

  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(),
				       CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK); 
  reply->set_data(outdata);
  reply->set_result(result);
  osd->client_messenger->send_message(reply, op->get_connection());
  op->put();
  delete filter;
}

void ReplicatedPG::calc_trim_to()
{
  if (!is_degraded() && !is_scrubbing() &&
      (is_clean() ||
       log.head.version - log.tail.version > (unsigned)info.stats.stats.sum.num_objects)) {
    if (min_last_complete_ondisk != eversion_t() &&
	min_last_complete_ondisk != pg_trim_to) {
      dout(10) << "calc_trim_to " << pg_trim_to << " -> " << min_last_complete_ondisk << dendl;
      pg_trim_to = min_last_complete_ondisk;
      assert(pg_trim_to <= log.head);
    }
  } else {
    // don't trim
    pg_trim_to = eversion_t();
  }
}

ReplicatedPG::ReplicatedPG(OSD *o, PGPool *_pool, pg_t p, const hobject_t& oid, const hobject_t& ioid) : 
  PG(o, _pool, p, oid, ioid), snap_trimmer_machine(this)
{ 
  snap_trimmer_machine.initiate();
}

void ReplicatedPG::get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc)
{
  src_oloc = oloc;
  if (oloc.key.empty())
    src_oloc.key = oid.name;
}

/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void ReplicatedPG::do_op(MOSDOp *op) 
{
  if ((op->get_rmw_flags() & CEPH_OSD_FLAG_PGOP))
    return do_pg_op(op);

  dout(10) << "do_op " << *op << (op->may_write() ? " may_write" : "") << dendl;
  if (finalizing_scrub && op->may_write()) {
    dout(20) << __func__ << ": waiting for scrub" << dendl;
    waiting_for_active.push_back(op);
    return;
  }

  entity_inst_t client = op->get_source_inst();

  ObjectContext *obc;
  bool can_create = op->may_write();
  snapid_t snapid;
  int r = find_object_context(hobject_t(op->get_oid(), 
					op->get_object_locator().key,
					op->get_snapid(), op->get_pg().ps()),
			      op->get_object_locator(),
			      &obc, can_create, &snapid);
  if (r) {
    if (r == -EAGAIN) {
      // If we're not the primary of this OSD, and we have
      // CEPH_OSD_FLAG_LOCALIZE_READS set, we just return -EAGAIN. Otherwise,
      // we have to wait for the object.
      if (is_primary() || (!(op->get_rmw_flags() & CEPH_OSD_FLAG_LOCALIZE_READS))) {
	// missing the specific snap we need; requeue and wait.
	assert(!can_create); // only happens on a read
	hobject_t soid(op->get_oid(), op->get_object_locator().key,
		       snapid, op->get_pg().ps());
	wait_for_missing_object(soid, op);
	return;
      }
    }
    osd->reply_op_error(op, r);
    return;
  }
  
  // make sure locator is consistent
  if (op->get_object_locator() != obc->obs.oi.oloc) {
    dout(10) << " provided locator " << op->get_object_locator() 
	     << " != object's " << obc->obs.oi.oloc
	     << " on " << obc->obs.oi.soid << dendl;
    osd->clog.warn() << "bad locator " << op->get_object_locator() 
		     << " on object " << obc->obs.oi.oloc
		     << " loc " << op->get_object_locator() 
		     << " op " << *op << "\n";
  }

  if ((op->may_read()) && (obc->obs.oi.lost)) {
    // This object is lost. Reading from it returns an error.
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
	     << " is lost" << dendl;
    osd->reply_op_error(op, -ENFILE);
    return;
  }
  dout(25) << __func__ << ": object " << obc->obs.oi.soid
	   << " has oi of " << obc->obs.oi << dendl;
  
  bool ok;
  dout(10) << "do_op mode is " << mode << dendl;
  assert(!mode.wake);   // we should never have woken waiters here.
  if ((op->may_read() && op->may_write()) ||
      (op->get_flags() & CEPH_OSD_FLAG_RWORDERED))
    ok = mode.try_rmw(client);
  else if (op->may_write())
    ok = mode.try_write(client);
  else if (op->may_read())
    ok = mode.try_read(client);
  else
    assert(0);
  if (!ok) {
    dout(10) << "do_op waiting on mode " << mode << dendl;
    mode.waiting.push_back(op);
    return;
  }

  if (!op->may_write() && !obc->obs.exists) {
    osd->reply_op_error(op, -ENOENT);
    put_object_context(obc);
    return;
  }

  dout(10) << "do_op mode now " << mode << dendl;

  // src_oids
  map<hobject_t,ObjectContext*> src_obc;
  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); p++) {
    OSDOp& osd_op = *p;
    object_locator_t src_oloc;
    get_src_oloc(op->get_oid(), op->get_object_locator(), src_oloc);
    hobject_t toid(osd_op.soid, src_oloc.key, op->get_pg().ps());
    if (osd_op.soid.oid.name.length()) {
      if (!src_obc.count(toid)) {
	ObjectContext *sobc;
	snapid_t ssnapid;

	int r = find_object_context(hobject_t(toid.oid, src_oloc.key,
					      toid.snap, op->get_pg().ps()),
				    src_oloc,
				    &sobc, false, &ssnapid);
	if (r == -EAGAIN) {
	  // missing the specific snap we need; requeue and wait.
	  hobject_t wait_oid(osd_op.soid.oid, src_oloc.key, ssnapid, op->get_pg().ps());
	  wait_for_missing_object(wait_oid, op);
	} else if (r) {
	  osd->reply_op_error(op, r);
	} else if (is_degraded_object(sobc->obs.oi.soid)) { 
	  wait_for_degraded_object(sobc->obs.oi.soid, op);
	} else if (sobc->obs.oi.oloc.key != obc->obs.oi.oloc.key &&
		   sobc->obs.oi.oloc.key != obc->obs.oi.soid.oid.name &&
		   sobc->obs.oi.soid.oid.name != obc->obs.oi.oloc.key) {
	  dout(1) << " src_oid " << osd_op.soid << " oloc " << sobc->obs.oi.oloc << " != "
		  << op->get_oid() << " oloc " << obc->obs.oi.oloc << dendl;
	  osd->reply_op_error(op, -EINVAL);
	} else {
	  src_obc[toid] = sobc;
	  continue;
	}
	// Error cleanup below
      } else {
	continue;
      }
      // Error cleanup below
    } else {
      continue;
    }
    put_object_contexts(src_obc);
    put_object_context(obc);
    return;
  }

  const hobject_t& soid = obc->obs.oi.soid;
  OpContext *ctx = new OpContext(op, op->get_reqid(), op->ops,
				 &obc->obs, obc->ssc, 
				 this);
  ctx->obc = obc;
  ctx->src_obc = src_obc;

  if (op->may_write()) {
    // snap
    if (pool->info.is_pool_snaps_mode()) {
      // use pool's snapc
      ctx->snapc = pool->snapc;
    } else {
      // client specified snapc
      ctx->snapc.seq = op->get_snap_seq();
      ctx->snapc.snaps = op->get_snaps();
    }
    if ((op->get_flags() & CEPH_OSD_FLAG_ORDERSNAP) &&
	ctx->snapc.seq < obc->ssc->snapset.seq) {
      dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	       << " < snapset seq " << obc->ssc->snapset.seq
	       << " on " << soid << dendl;
      delete ctx;
      put_object_context(obc);
      put_object_contexts(src_obc);
      osd->reply_op_error(op, -EOLDSNAPC);
      return;
    }

    eversion_t oldv = log.get_request_version(ctx->reqid);
    if (oldv != eversion_t()) {
      dout(3) << "do_op dup " << ctx->reqid << " was " << oldv << dendl;
      delete ctx;
      put_object_context(obc);
      put_object_contexts(src_obc);
      if (oldv <= last_update_ondisk) {
	osd->reply_op_error(op, 0, oldv);
      } else {
	dout(10) << " waiting for " << oldv << " to commit" << dendl;
	waiting_for_ondisk[oldv].push_back(op);
      }
      return;
    }

    // version
    ctx->at_version = log.head;

    ctx->at_version.epoch = osd->osdmap->get_epoch();
    ctx->at_version.version++;
    assert(ctx->at_version > info.last_update);
    assert(ctx->at_version > log.head);

    ctx->mtime = op->get_mtime();
    
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version 
	     << " snapc " << ctx->snapc
	     << " snapset " << obc->ssc->snapset
	     << dendl;  
  } else {
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;  
  }

  // note my stats
  utime_t now = ceph_clock_now(g_ceph_context);

  // note some basic context for op replication that prepare_transaction may clobber
  eversion_t old_last_update = log.head;
  bool old_exists = obc->obs.exists;
  uint64_t old_size = obc->obs.oi.size;
  eversion_t old_version = obc->obs.oi.version;

  if (op->may_read()) {
    dout(10) << " taking ondisk_read_lock" << dendl;
    obc->ondisk_read_lock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " taking ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_lock();
  }

  int result = prepare_transaction(ctx);

  if (op->may_read()) {
    dout(10) << " dropping ondisk_read_lock" << dendl;
    obc->ondisk_read_unlock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " dropping ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_unlock();
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
    return;
  }

  // prepare the reply
  ctx->reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), 0); 
  ctx->reply->set_data(ctx->outdata);
  ctx->reply->get_header().data_off = ctx->data_off;
  ctx->reply->set_result(result);

  if (result >= 0)
    ctx->reply->set_version(ctx->reply_version);

  // read or error?
  if (ctx->op_t.empty() || result < 0) {
    log_op_stats(ctx);
    
    MOSDOpReply *reply = ctx->reply;
    ctx->reply = NULL;
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    osd->client_messenger->send_message(reply, op->get_connection());
    op->put();
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
    return;
  }

  assert(op->may_write());

  // trim log?
  calc_trim_to();

  append_log(ctx->log, pg_trim_to, ctx->local_t);
  
  // continuing on to write path, make sure object context is registered
  assert(obc->registered);

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(ctx, obc, rep_tid);  // new repop claims our obc, src_obc refs
  // note: repop now owns ctx AND ctx->op

  repop->src_obc.swap(src_obc); // and src_obc.

  issue_repop(repop, now, old_last_update, old_exists, old_size, old_version);

  eval_repop(repop);
  repop->put();
}


void ReplicatedPG::log_op_stats(OpContext *ctx)
{
  MOSDOp *op = (MOSDOp*)ctx->op;

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= ctx->op->get_recv_stamp();

  utime_t rlatency;
  if (ctx->readable_stamp != utime_t()) {
    rlatency = ctx->readable_stamp;
    rlatency -= ctx->op->get_recv_stamp();
  }

  uint64_t inb = ctx->bytes_written;
  uint64_t outb = ctx->bytes_read;

  osd->logger->inc(l_osd_op);

  osd->logger->inc(l_osd_op_outb, outb);
  osd->logger->inc(l_osd_op_inb, inb);
  osd->logger->fset(l_osd_op_lat, latency);

  if (op->may_read() && op->may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->fset(l_osd_op_rw_rlat, rlatency);
    osd->logger->fset(l_osd_op_rw_lat, latency);
  } else if (op->may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->fset(l_osd_op_r_lat, latency);
  } else if (op->may_write()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->fset(l_osd_op_w_rlat, rlatency);
    osd->logger->fset(l_osd_op_w_lat, latency);
  } else
    assert(0);

  dout(15) << "log_op_stats " << *op
	   << " inb " << inb
	   << " outb " << outb
	   << " rlat " << rlatency
	   << " lat " << latency << dendl;
}

void ReplicatedPG::log_subop_stats(MOSDSubOp *op, int tag_inb, int tag_lat)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->get_recv_stamp();

  uint64_t inb = op->get_data().length();

  osd->logger->inc(l_osd_sop);

  osd->logger->inc(l_osd_sop_inb, inb);
  osd->logger->fset(l_osd_sop_lat, latency);

  if (tag_inb)
    osd->logger->inc(tag_inb, inb);
  osd->logger->fset(tag_lat, latency);

  dout(15) << "log_subop_stats " << *op << " inb " << inb << " latency " << latency << dendl;
}



void ReplicatedPG::do_sub_op(MOSDSubOp *op)
{
  dout(15) << "do_sub_op " << *op << dendl;

  if (op->ops.size() >= 1) {
    OSDOp& first = op->ops[0];
    switch (first.op.op) {
    case CEPH_OSD_OP_PULL:
      sub_op_pull(op);
      return;
    case CEPH_OSD_OP_PUSH:
      sub_op_push(op);
      return;
    case CEPH_OSD_OP_SCRUB_RESERVE:
      sub_op_scrub_reserve(op);
      return;
    case CEPH_OSD_OP_SCRUB_UNRESERVE:
      sub_op_scrub_unreserve(op);
      return;
    case CEPH_OSD_OP_SCRUB_STOP:
      sub_op_scrub_stop(op);
      return;
    case CEPH_OSD_OP_SCRUB_MAP:
      sub_op_scrub_map(op);
      return;
    }
  }

  sub_op_modify(op);
}

void ReplicatedPG::do_sub_op_reply(MOSDSubOpReply *r)
{
  if (r->ops.size() >= 1) {
    OSDOp& first = r->ops[0];
    switch (first.op.op) {
    case CEPH_OSD_OP_PUSH:
      // continue peer recovery
      sub_op_push_reply(r);
      return;

    case CEPH_OSD_OP_SCRUB_RESERVE:
      sub_op_scrub_reserve_reply(r);
      return;
    }
  }

  sub_op_modify_reply(r);
}

/* Returns head of snap_trimq as snap_to_trim and the relevant objects as 
 * obs_to_trim */
bool ReplicatedPG::get_obs_to_trim(snapid_t &snap_to_trim,
				   coll_t &col_to_trim,
				   vector<hobject_t> &obs_to_trim)
{
  assert_locked();
  obs_to_trim.clear();

  interval_set<snapid_t> s;
  s.intersection_of(snap_trimq, info.purged_snaps);
  if (!s.empty()) {
    dout(0) << "WARNING - snap_trimmer: snap_trimq contained snaps already in "
	    << "purged_snaps" << dendl;
    snap_trimq.subtract(s);
  }

  dout(10) << "get_obs_to_trim , purged_snaps " << info.purged_snaps << dendl;

  if (snap_trimq.size() == 0) return false;

  snap_to_trim = snap_trimq.range_start();
  col_to_trim = coll_t(info.pgid, snap_to_trim);

  if (!snap_collections.contains(snap_to_trim)) {
    return true;
  }

  // flush pg ops to fs so we can rely on collection_list()
  osr.flush();

  osd->store->collection_list(col_to_trim, obs_to_trim);

  return true;
}

ReplicatedPG::RepGather *ReplicatedPG::trim_object(const hobject_t &coid,
						   const snapid_t &sn)
{
  // load clone info
  bufferlist bl;
  ObjectContext *obc = 0;
  int r = find_object_context(hobject_t(coid.oid, coid.get_key(), sn, coid.hash),
			      OLOC_BLANK, &obc, false, NULL);
  if (r == -ENOENT || coid.snap != obc->obs.oi.soid.snap) {
    if (obc) put_object_context(obc);
    return 0;
  }
  assert(r == 0);
  assert(obc->registered);
  object_info_t &coi = obc->obs.oi;
  vector<snapid_t>& snaps = coi.snaps;

  // get snap set context
  if (!obc->ssc)
    obc->ssc = get_snapset_context(coid.oid, coid.get_key(), coid.hash, false);
  SnapSetContext *ssc = obc->ssc;
  assert(ssc);
  SnapSet& snapset = ssc->snapset;

  dout(10) << coid << " snaps " << snaps << " old snapset " << snapset << dendl;
  assert(snapset.seq);

  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->cluster_messenger->get_myname(), 0, rep_tid);
  OpContext *ctx = new OpContext(NULL, reqid, ops, &obc->obs, ssc, this);
  ctx->mtime = ceph_clock_now(g_ceph_context);

  ctx->at_version.epoch = osd->osdmap->get_epoch();
  ctx->at_version.version = log.head.version + 1;


  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;
    
  // trim clone's snaps
  vector<snapid_t> newsnaps;
  for (unsigned i=0; i<snaps.size(); i++)
    if (!pool->info.is_removed_snap(snaps[i]))
      newsnaps.push_back(snaps[i]);

  if (newsnaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << " ... deleting" << dendl;
    t->remove(coll, coid);
    t->collection_remove(coll_t(info.pgid, snaps[0]), coid);
    if (snaps.size() > 1)
      t->collection_remove(coll_t(info.pgid, snaps[snaps.size()-1]), coid);

    // ...from snapset
    snapid_t last = coid.snap;
    vector<snapid_t>::iterator p;
    for (p = snapset.clones.begin(); p != snapset.clones.end(); p++)
      if (*p == last)
	break;
    object_stat_sum_t delta;
    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      interval_set<uint64_t> keep;
      keep.union_of(snapset.clone_overlap[*n], snapset.clone_overlap[*p]);
      add_interval_usage(keep, delta);  // not deallocated
      snapset.clone_overlap[*n].intersection_of(snapset.clone_overlap[*p]);
    } else {
      add_interval_usage(snapset.clone_overlap[last], delta);  // not deallocated
    }
    delta.num_objects--;
    delta.num_object_clones--;
    delta.num_bytes -= snapset.clone_size[last];
    delta.num_kb -= SHIFT_ROUND_UP(snapset.clone_size[last], 10);
    info.stats.stats.add(delta, obc->obs.oi.category);

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);
	
    ctx->log.push_back(Log::Entry(Log::Entry::DELETE, coid, ctx->at_version, ctx->obs->oi.version,
				  osd_reqid_t(), ctx->mtime));
    ctx->at_version.version++;
  } else {
    // save adjusted snaps for this object
    dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << dendl;
    coi.snaps.swap(newsnaps);
    vector<snapid_t>& oldsnaps = newsnaps;
    coi.prior_version = coi.version;
    coi.version = ctx->at_version;
    bl.clear();
    ::encode(coi, bl);
    t->setattr(coll, coid, OI_ATTR, bl);

    if (oldsnaps[0] != snaps[0]) {
      t->collection_remove(coll_t(info.pgid, oldsnaps[0]), coid);
      if (oldsnaps.size() > 1 && oldsnaps[snaps.size() - 1] != snaps[0])
	t->collection_add(coll_t(info.pgid, snaps[0]), coll, coid);
    }
    if (oldsnaps.size() > 1 && oldsnaps[oldsnaps.size()-1] != snaps[snaps.size()-1]) {
      t->collection_remove(coll_t(info.pgid, oldsnaps[oldsnaps.size()-1]), coid);
      if (snaps.size() > 1)
	t->collection_add(coll_t(info.pgid, snaps[snaps.size()-1]), coll, coid);
    }	      

    ctx->log.push_back(Log::Entry(Log::Entry::MODIFY, coid, coi.version, coi.prior_version,
				  osd_reqid_t(), ctx->mtime));
    ::encode(coi, ctx->log.back().snaps);
    ctx->at_version.version++;
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << dendl;

  hobject_t snapoid(coid.oid, coid.get_key(), snapset.head_exists ? CEPH_NOSNAP:CEPH_SNAPDIR, coid.hash);
  ctx->snapset_obc = get_object_context(snapoid, coi.oloc, false);
  assert(ctx->snapset_obc->registered);
  if (snapset.clones.empty() && !snapset.head_exists) {
    dout(10) << coid << " removing " << snapoid << dendl;
    ctx->log.push_back(Log::Entry(Log::Entry::DELETE, snapoid, ctx->at_version, 
				  ctx->snapset_obc->obs.oi.version, osd_reqid_t(), ctx->mtime));
    ctx->snapset_obc->obs.exists = false;

    t->remove(coll, snapoid);
  } else {
    dout(10) << coid << " updating snapset on " << snapoid << dendl;
    ctx->log.push_back(Log::Entry(Log::Entry::MODIFY, snapoid, ctx->at_version, 
				  ctx->snapset_obc->obs.oi.version, osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc->obs.oi.prior_version = ctx->snapset_obc->obs.oi.version;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;

    bl.clear();
    ::encode(snapset, bl);
    t->setattr(coll, snapoid, SS_ATTR, bl);

    bl.clear();
    ::encode(ctx->snapset_obc->obs.oi, bl);
    t->setattr(coll, snapoid, OI_ATTR, bl);
  }

  return repop;
}

bool ReplicatedPG::snap_trimmer()
{
  lock();
  dout(10) << "snap_trimmer entry" << dendl;
  if (is_primary()) {
    entity_inst_t nobody;
    if (!mode.try_write(nobody)) {
      dout(10) << " can't write, requeueing" << dendl;
      queue_snap_trim();
      unlock();
      put();
      return true;
    }
    if (!finalizing_scrub) {
      dout(10) << "snap_trimmer posting" << dendl;
      snap_trimmer_machine.process_event(SnapTrim());
    }

    if (snap_trimmer_machine.need_share_pg_info) {
      dout(10) << "snap_trimmer share_pg_info" << dendl;
      snap_trimmer_machine.need_share_pg_info = false;
      unlock();
      osd->map_lock.get_read();
      lock();
      share_pg_info();
      osd->map_lock.put_read();
    }
  } else if (is_active() && 
	     last_complete_ondisk.epoch > info.history.last_epoch_started) {
    // replica collection trimming
    snap_trimmer_machine.process_event(SnapTrim());
  }
  if (snap_trimmer_machine.requeue) {
    dout(10) << "snap_trimmer requeue" << dendl;
    queue_snap_trim();
  }
  unlock();
  put();
  return true;
}

int ReplicatedPG::do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr)
{
  __u64 v2;
  if (xattr.length())
    v2 = atoll(xattr.c_str());
  else
    v2 = 0;

  dout(20) << "do_xattr_cmp_u64 '" << v1 << "' vs '" << v2 << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1 == v2);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1 != v2);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1 > v2);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1 >= v2);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1 < v2);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1 <= v2);
  default:
    return -EINVAL;
  }
}

int ReplicatedPG::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
{
  const char *v1, *v2;
  v1 = v1s.data();
  string v2s;
  if (xattr.length()) {
    v2s = string(xattr.c_str(), xattr.length());
    v2 = v2s.c_str();
  } else
    v2 = "";

  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2 << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (strcmp(v1, v2) == 0);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (strcmp(v1, v2) != 0);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (strcmp(v1, v2) > 0);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (strcmp(v1, v2) >= 0);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (strcmp(v1, v2) < 0);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (strcmp(v1, v2) <= 0);
  default:
    return -EINVAL;
  }
}

void ReplicatedPG::dump_watchers(ObjectContext *obc)
{
  assert(osd->watch_lock.is_locked());
  
  dout(0) << "dump_watchers " << obc->obs.oi.soid << " " << obc->obs.oi << dendl;
  for (map<entity_name_t, OSD::Session *>::iterator iter = obc->watchers.begin(); 
       iter != obc->watchers.end();
       ++iter)
    dout(0) << " * obc->watcher: " << iter->first << " session=" << iter->second << dendl;
  
  for (map<entity_name_t, watch_info_t>::iterator oi_iter = obc->obs.oi.watchers.begin();
       oi_iter != obc->obs.oi.watchers.end();
       oi_iter++) {
    watch_info_t& w = oi_iter->second;
    dout(0) << " * oi->watcher: " << oi_iter->first << " cookie=" << w.cookie << dendl;
  }
}

void ReplicatedPG::remove_watcher(ObjectContext *obc, entity_name_t entity)
{
  assert_locked();
  assert(osd->watch_lock.is_locked());
  dout(10) << "remove_watcher " << *obc << " " << entity << dendl;
  map<entity_name_t, OSD::Session *>::iterator iter = obc->watchers.find(entity);
  assert(iter != obc->watchers.end());
  OSD::Session *session = iter->second;
  dout(10) << "remove_watcher removing session " << session << dendl;

  obc->watchers.erase(iter);
  assert(session->watches.count(obc));
  session->watches.erase(obc);

  put_object_context(obc);
  session->put();
}

void ReplicatedPG::remove_notify(ObjectContext *obc, Watch::Notification *notif)
{
  assert_locked();
  assert(osd->watch_lock.is_locked());
  map<Watch::Notification *, bool>::iterator niter = obc->notifs.find(notif);

  // Cancel notification
  if (notif->timeout)
    osd->watch_timer.cancel_event(notif->timeout);
  osd->watch->remove_notification(notif);

  assert(niter != obc->notifs.end());

  niter->first->session->put();
  niter->first->session->con->put();
  obc->notifs.erase(niter);

  put_object_context(obc);
}

void ReplicatedPG::remove_watchers_and_notifies()
{
  assert_locked();

  dout(10) << "remove_watchers" << dendl;

  osd->watch_lock.Lock();
  for (map<hobject_t, ObjectContext*>::iterator oiter = object_contexts.begin();
       oiter != object_contexts.end();
       ) {
    map<hobject_t, ObjectContext *>::iterator iter = oiter++;
    ObjectContext *obc = iter->second;
    obc->ref++;
    for (map<entity_name_t, OSD::Session *>::iterator witer = obc->watchers.begin();
	 witer != obc->watchers.end();
	 remove_watcher(obc, (witer++)->first));
    for (map<entity_name_t, Context *>::iterator iter = obc->unconnected_watchers.begin();
	 iter != obc->unconnected_watchers.end();
      ) {
      map<entity_name_t, Context *>::iterator i = iter++;
      unregister_unconnected_watcher(obc, i->first);
    }
    for (map<Watch::Notification *, bool>::iterator niter = obc->notifs.begin();
	 niter != obc->notifs.end();
	 remove_notify(obc, (niter++)->first));
    put_object_context(obc);
  }
  osd->watch_lock.Unlock();
}

// ========================================================================
// low level osd ops

int ReplicatedPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops,
			     bufferlist& odata)
{
  int result = 0;
  SnapSetContext *ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;

  bool maybe_created = false;

  const hobject_t& soid = oi.soid;

  ObjectStore::Transaction& t = ctx->op_t;

  dout(10) << "do_osd_op " << soid << " " << ops << dendl;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); p++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op; 

    dout(10) << "do_osd_op  " << osd_op << dendl;

    bufferlist::iterator bp = osd_op.data.begin();

    // user-visible modifcation?
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
      break;
    default:
      if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;
    }

    ObjectContext *src_obc = 0;
    if (ceph_osd_op_type_multi(op.op)) {
      src_obc = ctx->src_obc[hobject_t(osd_op.soid, 
				       ((MOSDOp *)ctx->op)->get_object_locator().key,
				       soid.hash)];
      assert(src_obc);
    }

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
	obs.exists &&
	op.extent.offset + op.extent.length >= oi.size) {
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      
      // --- READS ---

    case CEPH_OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->read(coll, soid, op.extent.offset, op.extent.length, bl);
	if (odata.length() == 0)
	  ctx->data_off = op.extent.offset;
	odata.claim(bl);
	if (r >= 0) 
	  op.extent.length = r;
	else {
	  result = r;
	  op.extent.length = 0;
	}
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " read got " << r << " / " << op.extent.length << " bytes from obj " << soid << dendl;

	__u32 seq = oi.truncate_seq;
	// are we beyond truncate_size?
	if ( (seq < op.extent.truncate_seq) &&
	     (op.extent.offset + op.extent.length > op.extent.truncate_size) ) {

	  // truncated portion of the read
	  unsigned from = MAX(op.extent.offset, op.extent.truncate_size);  // also end of data
	  unsigned to = op.extent.offset + op.extent.length;
	  unsigned trim = to-from;

	  op.extent.length = op.extent.length - trim;

	  bufferlist keep;

	  // keep first part of odata; trim at truncation point
	  dout(10) << " obj " << soid << " seq " << seq
	           << ": trimming overlap " << from << "~" << trim << dendl;
	  keep.substr_of(odata, 0, odata.length() - trim);
          odata.claim(keep);
	}
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_MAPEXT:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->fiemap(coll, soid, op.extent.offset, op.extent.length, bl);
/*
	if (odata.length() == 0)
	  ctx->data_off = op.extent.offset; */
	odata.claim(bl);
	if (r < 0)
	  result = r;
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " map_extents done on object " << soid << dendl;
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      {
        if (op.extent.truncate_seq) {
          dout(0) << "sparse_read does not support truncation sequence " << dendl;
          result = -EINVAL;
          break;
        }
	// read into a buffer
	bufferlist bl;
        int total_read = 0;
	int r = osd->store->fiemap(coll, soid, op.extent.offset, op.extent.length, bl);
	if (r < 0)  {
	  result = r;
          break;
	}
        map<uint64_t, uint64_t> m;
        bufferlist::iterator iter = bl.begin();
        ::decode(m, iter);
        map<uint64_t, uint64_t>::iterator miter;
        bufferlist data_bl;
        for (miter = m.begin(); miter != m.end(); ++miter) {
          bufferlist tmpbl;
          r = osd->store->read(coll, soid, miter->first, miter->second, tmpbl);
          if (r < 0)
            break;

          if (r < (int)miter->second) /* this is usually happen when we get extent that exceeds the actual file size */
            miter->second = r;
          total_read += r;
          dout(10) << "sparse-read " << miter->first << "@" << miter->second << dendl;
	  data_bl.claim_append(tmpbl);
        }

        if (r < 0) {
          result = r;
          break;
        }

        op.extent.length = total_read;

        ::encode(m, odata);
        ::encode(data_bl, odata);

	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;

	dout(10) << " sparse_read got " << total_read << " bytes from object " << soid << dendl;
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	string cname, mname;
	bp.copy(op.cls.class_len, cname);
	bp.copy(op.cls.method_len, mname);

	bufferlist indata;
	bp.copy(op.cls.indata_len, indata);

	ClassHandler::ClassData *cls;
	int result = osd->class_handler->open_class(cname, &cls);
	assert(result == 0);

	ClassHandler::ClassMethod *method = cls->get_method(mname.c_str());
	if (!method) {
	  dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	  result = -EINVAL;
	  break;
	}

	int flags = method->get_flags();
	if (flags & CLS_METHOD_WR)
	  ctx->user_modify = true;

	bufferlist outdata;
	dout(10) << "call method " << cname << "." << mname << dendl;
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);
	dout(10) << "method called response length=" << outdata.length() << dendl;
	op.extent.length = outdata.length();
	odata.claim_append(outdata);
      }
      break;

    case CEPH_OSD_OP_STAT:
      {
	if (obs.exists) {
	  ::encode(oi.size, odata);
	  ::encode(oi.mtime, odata);
	  dout(10) << "stat oi has " << oi.size << " " << oi.mtime << dendl;
	} else {
	  result = -ENOENT;
	  dout(10) << "stat oi object does not exist" << dendl;
	}
	if (1) {  // REMOVE ME LATER!
          struct stat st;
          memset(&st, 0, sizeof(st));
          int checking_result = osd->store->stat(coll, soid, &st);
          if ((checking_result != result) ||
              ((uint64_t)st.st_size != oi.size)) {
            osd->clog.error() << info.pgid << " " << soid << " oi.size " << oi.size
                              << " but stat got " << checking_result << " size " << st.st_size << "\n";
            assert(0 == "oi disagrees with stat, or error code on stat");
          }
        }

	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = osd->store->getattr(coll, soid, name.c_str(), odata);
	if (r >= 0) {
	  op.xattr.value_len = r;
	  result = 0;
	} else
	  result = r;
	ctx->delta_stats.num_rd++;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
      {
	map<string,bufferptr> attrset;
        result = osd->store->getattrs(coll, soid, attrset, true);
        map<string, bufferptr>::iterator iter;
        map<string, bufferlist> newattrs;
        for (iter = attrset.begin(); iter != attrset.end(); ++iter) {
           bufferlist bl;
           bl.append(iter->second);
           newattrs[iter->first] = bl;
        }
        
        bufferlist bl;
        ::encode(newattrs, bl);
        odata.claim_append(bl);
      }
      break;
      
    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_SRC_CMPXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;
	
	bufferlist xattr;
	if (op.op == CEPH_OSD_OP_CMPXATTR)
	  result = osd->store->getattr(coll, soid, name.c_str(), xattr);
	else
	  result = osd->store->getattr(coll, src_obc->obs.oi.soid, name.c_str(), xattr);
	if (result < 0 && result != -EEXIST && result !=-ENODATA)
	  break;
	
	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	  {
	    string val;
	    bp.copy(op.xattr.value_len, val);
	    val[op.xattr.value_len] = 0;
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	  }
	  break;

        case CEPH_OSD_CMPXATTR_MODE_U64:
	  {
	    uint64_t u64val;
	    ::decode(u64val, bp);
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << u64val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_u64(op.xattr.cmp_op, u64val, xattr);
	  }
	  break;

	default:
	  dout(10) << "bad cmp mode " << (int)op.xattr.cmp_mode << dendl;
	  result = -EINVAL;
	}

	if (!result) {
	  dout(10) << "comparison returned false" << dendl;
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  dout(10) << "comparison returned " << result << " " << cpp_strerror(-result) << dendl;
	  break;
	}

	dout(10) << "comparison returned true" << dendl;
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      {
	uint64_t ver = op.watch.ver;
	if (!ver)
	  result = -EINVAL;
        else if (ver < oi.user_version.version)
	  result = -ERANGE;
	else if (ver > oi.user_version.version)
	  result = -EOVERFLOW;
	break;
      }

    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      {
	uint64_t ver = op.watch.ver;
	if (!ver)
	  result = -EINVAL;
        else if (ver < src_obc->obs.oi.user_version.version)
	  result = -ERANGE;
	else if (ver > src_obc->obs.oi.user_version.version)
	  result = -EOVERFLOW;
	break;
      }

   case CEPH_OSD_OP_NOTIFY:
      {
	uint32_t ver;
	uint32_t timeout;
        bufferlist bl;

	try {
          ::decode(ver, bp);
	  ::decode(timeout, bp);
          ::decode(bl, bp);
	} catch (const buffer::error &e) {
	  timeout = 0;
	}
	if (!timeout || timeout > (uint32_t)g_conf->osd_max_notify_timeout)
	  timeout = g_conf->osd_max_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.cookie = op.watch.cookie;
        n.bl = bl;
	ctx->notifies.push_back(n);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      {
	osd->watch_lock.Lock();
	entity_name_t source = ctx->op->get_source();
	map<entity_name_t, watch_info_t>::iterator oi_iter = oi.watchers.find(source);
	Watch::Notification *notif = osd->watch->get_notif(op.watch.cookie);
	if (oi_iter != oi.watchers.end() && notif) {
	  ctx->notify_acks.push_back(op.watch.cookie);
	} else {
	  if (!notif)
	    dout(10) << " no pending notify for cookie " << op.watch.cookie << dendl;
	  else
	    dout(10) << " not registered as a watcher" << dendl;
	  result = -EINVAL;
	}
	osd->watch_lock.Unlock();
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      { // write
        __u32 seq = oi.truncate_seq;
        if (seq && (seq > op.extent.truncate_seq) &&
            (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
        }
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		   << ", truncating to " << op.extent.truncate_size << dendl;
	  t.truncate(coll, soid, op.extent.truncate_size);
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}
        if (op.extent.length) {
	  bufferlist nbl;
	  bp.copy(op.extent.length, nbl);
	  t.write(coll, soid, op.extent.offset, op.extent.length, nbl);
        } else {
          t.touch(coll, soid);
        }
	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.extent.offset, op.extent.length, true);
	maybe_created = true;
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      { // write full object
	bufferlist nbl;
	bp.copy(op.extent.length, nbl);
	if (obs.exists)
	  t.truncate(coll, soid, 0);
	else
	  maybe_created = true;
	t.write(coll, soid, op.extent.offset, op.extent.length, nbl);
	if (ssc->snapset.clones.size() && oi.size > 0) {
	  interval_set<uint64_t> ch;
	  ch.insert(0, oi.size);
	  ctx->modified_ranges.union_of(ch);
	  oi.size = 0;
	}
	if (op.extent.length != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  ctx->delta_stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  ctx->delta_stats.num_bytes += op.extent.length;
	  ctx->delta_stats.num_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	  oi.size = op.extent.length;
	}
	ctx->delta_stats.num_wr++;
	ctx->delta_stats.num_wr_kb += SHIFT_ROUND_UP(op.extent.length, 10);
      }
      break;

    case CEPH_OSD_OP_ROLLBACK :
      result = _rollback_to(ctx, op);
      break;

    case CEPH_OSD_OP_ZERO:
      { // zero
	assert(op.extent.length);
	if (obs.exists) {
	  t.zero(coll, soid, op.extent.offset, op.extent.length);
	  if (ssc->snapset.clones.size()) {
	    interval_set<uint64_t> ch;
	    ch.insert(op.extent.offset, op.extent.length);
	    ctx->modified_ranges.union_of(ch);
	    add_interval_usage(ch, ctx->delta_stats);
	  }
	  ctx->delta_stats.num_wr++;
	} else {
	  // no-op
	}
      }
      break;
    case CEPH_OSD_OP_CREATE:
      { // zero
        int flags = le32_to_cpu(op.flags);
	if (obs.exists && (flags & CEPH_OSD_OP_FLAG_EXCL))
          result = -EEXIST; /* this is an exclusive create */
        else {
          t.touch(coll, soid);
          maybe_created = true;
        }
	if (osd_op.data.length()) {
	  bufferlist::iterator p = osd_op.data.begin();
	  string category;
	  ::decode(category, p);
          if (category.size()) {
	    if (obs.exists) {
	      if (obs.oi.category != category)
	        result = -EEXIST;
	    } else {
	      obs.oi.category = category;
	    }
          }
	}
      }
      break;
      
    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      { // truncate
	if (!obs.exists) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}

	if (op.extent.truncate_seq) {
	  assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
		     << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
		   << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	t.truncate(coll, soid, op.extent.offset);
	if (ssc->snapset.clones.size()) {
	  snapid_t newest = *ssc->snapset.clones.rbegin();
	  interval_set<uint64_t> trim;
	  if (oi.size > op.extent.offset) {
	    trim.insert(op.extent.offset, oi.size-op.extent.offset);
	    ctx->modified_ranges.union_of(trim);
	    trim.intersection_of(ssc->snapset.clone_overlap[newest]);
	    add_interval_usage(trim, ctx->delta_stats);
	  }
	}
	if (op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  ctx->delta_stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
	  ctx->delta_stats.num_bytes += op.extent.offset;
	  ctx->delta_stats.num_kb += SHIFT_ROUND_UP(op.extent.offset, 10);
	  oi.size = op.extent.offset;
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.
      }
      break;
    
    case CEPH_OSD_OP_DELETE:
      if (!obs.exists) {
	result = -ENOENT;
      } else {
	_delete_head(ctx);
      }
      break;

    case CEPH_OSD_OP_CLONERANGE:
      {
	if (!obs.exists) {
	  t.touch(coll, obs.oi.soid);
	  maybe_created = true;
	}
	if (op.clonerange.src_offset + op.clonerange.length > src_obc->obs.oi.size) {
	  dout(10) << " clonerange source " << osd_op.soid << " "
		   << op.clonerange.src_offset << "~" << op.clonerange.length
		   << " extends past size " << src_obc->obs.oi.size << dendl;
	  result = -EINVAL;
	  break;
	}
	t.clone_range(coll, src_obc->obs.oi.soid,
		      obs.oi.soid, op.clonerange.src_offset,
		      op.clonerange.length, op.clonerange.offset);
		      

	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.clonerange.offset, op.clonerange.length, false);
      }
      break;
      
    case CEPH_OSD_OP_WATCH:
      {
        uint64_t cookie = op.watch.cookie;
	bool do_watch = op.watch.flag & 1;
        entity_name_t entity = ctx->reqid.name;
	ObjectContext *obc = ctx->obc;

	dout(0) << "watch: ctx->obc=" << (void *)obc << " cookie=" << cookie
		<< " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(0) << "watch: oi.user_version=" << oi.user_version.version << dendl;

	watch_info_t w = {cookie, 30};  // FIXME: where does the timeout come from?
	if (do_watch) {
	  if (oi.watchers.count(entity) && oi.watchers[entity] == w) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[entity] = w;
	    t.nop();  // make sure update the object_info on disk!
	  }
	  ctx->watch_connect = true;
	  ctx->watch_info = w;
	  assert(obc->registered);
        } else {
	  map<entity_name_t, watch_info_t>::iterator oi_iter = oi.watchers.find(entity);
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by " << entity << dendl;
            oi.watchers.erase(entity);
	    t.nop();  // update oi on disk

	    ctx->watch_disconnect = true;

	    // FIXME: trigger notifies?

	  } else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
        }
      }
      break;


      // -- object attrs --
      
    case CEPH_OSD_OP_SETXATTR:
      {
	if (!obs.exists) {
	  t.touch(coll, soid);
	  maybe_created = true;
	}
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t.setattr(coll, soid, name, bl);
 	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	t.rmattr(coll, soid, name);
 	ctx->delta_stats.num_wr++;
      }
      break;
    

      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
      {
	// just do it inline; this works because we are happy to execute
	// fancy op on replicas as well.
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITE;
	newop.op.extent.offset = oi.size;
	newop.op.extent.length = op.extent.length;
        newop.data = osd_op.data;
	do_osd_ops(ctx, nops, odata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      t.start_sync();
      break;


      // -- trivial map --
    case CEPH_OSD_OP_TMAPGET:
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	do_osd_ops(ctx, nops, odata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      {
	//_dout_lock.Lock();
	//osd_op.data.hexdump(*_dout);
	//_dout_lock.Unlock();

	// verify
	if (0) {
	  bufferlist header;
	  map<string, bufferlist> m;
	  ::decode(header, bp);
	  ::decode(m, bp);
	  assert(bp.end());
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.data.length();
	newop.data = osd_op.data;
	bufferlist r;
	do_osd_ops(ctx, nops, r);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      if (bp.end()) {
	dout(10) << "tmapup is a no-op" << dendl;
      } else {
	// read the whole object
	bufferlist ibl;
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	do_osd_ops(ctx, nops, ibl);
	dout(10) << "tmapup read " << ibl.length() << dendl;

	dout(30) << " starting is \n";
	ibl.hexdump(*_dout);
	*_dout << dendl;

	bufferlist::iterator ip = ibl.begin();
	bufferlist obl;

	dout(30) << "the update command is: \n";
	osd_op.data.hexdump(*_dout);
	*_dout << dendl;

	if (0) {
	  // parse
	  bufferlist header;
	  map<string, bufferlist> m;
	  //ibl.hexdump(*_dout);
	  if (ibl.length()) {
	    ::decode(header, ip);
	    ::decode(m, ip);
	    //dout(0) << "m is " << m.size() << " " << m << dendl;
	    assert(ip.end());
	  }
	  
	  // do the update(s)
	  while (!bp.end()) {
	    __u8 op;
	    string key;
	    ::decode(op, bp);
	    
	    switch (op) {
	    case CEPH_OSD_TMAP_SET: // insert key
	      {
		::decode(key, bp);
		bufferlist data;
		::decode(data, bp);
		m[key] = data;
	      }
	      break;
	      
	    case CEPH_OSD_TMAP_CREATE: // create (new) key
	      {
		::decode(key, bp);
		bufferlist data;
		::decode(data, bp);
		if (m.count(key))
		  return -EEXIST;
		m[key] = data;
	      }
	      break;

	    case CEPH_OSD_TMAP_RM: // remove key
	      ::decode(key, bp);
	      if (m.count(key)) {
		m.erase(key);
	      }
	      break;
	      
	    case CEPH_OSD_TMAP_HDR: // update header
	      {
		::decode(header, bp);
	      }
	      break;
	      
	    default:
	      return -EINVAL;
	    }
	  }

	  // reencode
	  ::encode(header, obl);
	  ::encode(m, obl);
	} else {
	  // header
	  bufferlist header;
	  __u32 nkeys = 0;
	  if (ibl.length()) {
	    ::decode(header, ip);
	    ::decode(nkeys, ip);
	  }
	  dout(10) << "tmapup header " << header.length() << dendl;

	  if (!bp.end() && *bp == CEPH_OSD_TMAP_HDR) {
	    ++bp;
	    ::decode(header, bp);
	    dout(10) << "tmapup new header " << header.length() << dendl;
	  }
	  
	  ::encode(header, obl);

	  dout(20) << "tmapup initial nkeys " << nkeys << dendl;

	  // update keys
	  bufferlist newkeydata;
	  string nextkey;
	  bufferlist nextval;
	  bool have_next = false;
	  if (!ip.end()) {
	    have_next = true;
	    ::decode(nextkey, ip);
	    ::decode(nextval, ip);
	  }
	  result = 0;
	  while (!bp.end() && !result) {
	    __u8 op;
	    string key;
	    ::decode(op, bp);
	    ::decode(key, bp);
	    
	    dout(10) << "tmapup op " << (int)op << " key " << key << dendl;

	    // skip existing intervening keys
	    bool key_exists = false;
	    while (have_next && !key_exists) {
	      dout(20) << "  (have_next=" << have_next << " nextkey=" << nextkey << ")" << dendl;
	      if (nextkey > key)
		break;
	      if (nextkey < key) {
		// copy untouched.
		::encode(nextkey, newkeydata);
		::encode(nextval, newkeydata);
		dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	      } else {
		// don't copy; discard old value.  and stop.
		dout(20) << "  drop " << nextkey << " " << nextval.length() << dendl;
		key_exists = true;
		nkeys--;
	      }	      
	      if (!ip.end()) {
		::decode(nextkey, ip);
		::decode(nextval, ip);
	      } else {
		have_next = false;
	      }
	    }
	    
	    if (op == CEPH_OSD_TMAP_SET) {
	      bufferlist val;
	      ::decode(val, bp);
	      ::encode(key, newkeydata);
	      ::encode(val, newkeydata);
	      dout(20) << "   set " << key << " " << val.length() << dendl;
	      nkeys++;
	    } else if (op == CEPH_OSD_TMAP_CREATE) {
	      if (key_exists) {
		result = -EEXIST;
		break;
	      }
	      bufferlist val;
	      ::decode(val, bp);
	      ::encode(key, newkeydata);
	      ::encode(val, newkeydata);
	      dout(20) << "   create " << key << " " << val.length() << dendl;
	      nkeys++;
	    } else if (op == CEPH_OSD_TMAP_RM) {
	      if (!key_exists) {
		result = -ENOENT;
		break;
	      }
	      // do nothing.
	    }
	  }

	  // copy remaining
	  if (have_next) {
	    ::encode(nextkey, newkeydata);
	    ::encode(nextval, newkeydata);
	    dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	  }
	  if (!ip.end()) {
	    bufferlist rest;
	    rest.substr_of(ibl, ip.get_off(), ibl.length() - ip.get_off());
	    dout(20) << "  keep trailing " << rest.length()
		     << " at " << newkeydata.length() << dendl;
	    newkeydata.claim_append(rest);
	  }

	  // encode final key count + key data
	  dout(20) << "tmapup final nkeys " << nkeys << dendl;
	  ::encode(nkeys, obl);
	  obl.claim_append(newkeydata);
	}

	if (0) {
	  dout(30) << " final is \n";
	  obl.hexdump(*_dout);
	  *_dout << dendl;

	  // sanity check
	  bufferlist::iterator tp = obl.begin();
	  bufferlist h;
	  ::decode(h, tp);
	  map<string,bufferlist> d;
	  ::decode(d, tp);
	  assert(tp.end());
	  dout(0) << " **** debug sanity check, looks ok ****" << dendl;
	}

	// write it out
	if (!result) {
	  dout(20) << "tmapput write " << obl.length() << dendl;
	  newop.op.op = CEPH_OSD_OP_WRITEFULL;
	  newop.op.extent.offset = 0;
	  newop.op.extent.length = obl.length();
	  newop.data = obl;
	  do_osd_ops(ctx, nops, odata);
	}
      }
      break;


    default:
      dout(1) << "unrecognized osd op " << op.op
	      << " " << ceph_osd_op_name(op.op)
	      << dendl;
      result = -EOPNOTSUPP;
    }

    if (!obs.exists && maybe_created) {
      ctx->delta_stats.num_objects++;
      obs.exists = true;
    }

    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK))
      result = 0;

    if (result < 0)
      break;
  }
  return result;
}

inline void ReplicatedPG::_delete_head(OpContext *ctx)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  ObjectStore::Transaction& t = ctx->op_t;

  if (obs.exists)
    t.remove(coll, soid);
  if (snapset.clones.size()) {
    snapid_t newest = *snapset.clones.rbegin();
    add_interval_usage(snapset.clone_overlap[newest], ctx->delta_stats);

    if (oi.size > 0) {
      interval_set<uint64_t> ch;
      ch.insert(0, oi.size);
      ctx->modified_ranges.union_of(ch);
    }
  }
  if (obs.exists) {
    ctx->delta_stats.num_objects--;
    ctx->delta_stats.num_bytes -= oi.size;
    ctx->delta_stats.num_kb -= SHIFT_ROUND_UP(oi.size, 10);
    oi.size = 0;
    snapset.head_exists = false;
    obs.exists = false;
  }      
  ctx->delta_stats.num_wr++;
}

int ReplicatedPG::_rollback_to(OpContext *ctx, ceph_osd_op& op)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  ObjectStore::Transaction& t = ctx->op_t;
  snapid_t snapid = (uint64_t)op.snap.snapid;
  snapid_t cloneid = 0;

  dout(10) << "_rollback_to " << soid << " snapid " << snapid << dendl;

  ObjectContext *rollback_to;
  int ret = find_object_context(hobject_t(soid.oid, oi.oloc.key, snapid, soid.hash), 
				oi.oloc, &rollback_to, false, &cloneid);
  if (ret) {
    if (-ENOENT == ret) {
      // there's no snapshot here, or there's no object.
      // if there's no snapshot, we delete the object; otherwise, do nothing.
      dout(20) << "_rollback_to deleting head on " << soid.oid
	       << " because got ENOENT on find_object_context" << dendl;
      _delete_head(ctx);
      ret = 0;
    } else if (-EAGAIN == ret) {
      /* a different problem, like degraded pool
       * with not-yet-restored object. We shouldn't have been able
       * to get here; recovery should have completed first! */
      hobject_t rollback_target(soid.oid, soid.get_key(), cloneid, soid.hash);
      assert(is_missing_object(rollback_target));
      dout(20) << "_rollback_to attempted to roll back to a missing object " 
	       << rollback_target << " (requested snapid: ) " << snapid << dendl;
      wait_for_missing_object(rollback_target, ctx->op);
    } else {
      // ummm....huh? It *can't* return anything else at time of writing.
      assert(0);
    }
  } else { //we got our context, let's use it to do the rollback!
    hobject_t& rollback_to_sobject = rollback_to->obs.oi.soid;
    if (is_degraded_object(rollback_to_sobject)) {
      dout(20) << "_rollback_to attempted to roll back to a degraded object " 
	       << rollback_to_sobject << " (requested snapid: ) " << snapid << dendl;
      wait_for_degraded_object(rollback_to_sobject, ctx->op);
      ret = -EAGAIN;
    } else if (rollback_to->obs.oi.soid.snap == CEPH_NOSNAP) {
      // rolling back to the head; we just need to clone it.
      ctx->modify = true;
    } else {
      /* 1) Delete current head
       * 2) Clone correct snapshot into head
       * 3) Calculate clone_overlaps by following overlaps
       *    forward from rollback snapshot */
      dout(10) << "_rollback_to deleting " << soid.oid
	       << " and rolling back to old snap" << dendl;

      if (obs.exists)
	t.remove(coll, soid);
      
      map<string, bufferptr> attrs;
      t.clone(coll,
	      rollback_to_sobject, soid);
      osd->store->getattrs(coll,
			   rollback_to_sobject, attrs, false);
      osd->filter_xattrs(attrs);
      t.setattrs(coll, soid, attrs);
      snapset.head_exists = true;

      map<snapid_t, interval_set<uint64_t> >::iterator iter =
	snapset.clone_overlap.lower_bound(snapid);
      interval_set<uint64_t> overlaps = iter->second;
      assert(iter != snapset.clone_overlap.end());
      for ( ;
	    iter != snapset.clone_overlap.end();
	    ++iter)
	overlaps.intersection_of(iter->second);

      if (obs.oi.size > 0) {
	interval_set<uint64_t> modified;
	modified.insert(0, obs.oi.size);
	overlaps.intersection_of(modified);
	modified.subtract(overlaps);
	ctx->modified_ranges.union_of(modified);
      }

      // Adjust the cached objectcontext
      if (!obs.exists) {
	obs.exists = true; //we're about to recreate it
	ctx->delta_stats.num_objects++;
      }
      ctx->delta_stats.num_bytes -= obs.oi.size;
      ctx->delta_stats.num_bytes -= SHIFT_ROUND_UP(obs.oi.size, 10);
      ctx->delta_stats.num_bytes += rollback_to->obs.oi.size;
      ctx->delta_stats.num_bytes += SHIFT_ROUND_UP(rollback_to->obs.oi.size, 10);
      obs.oi.size = rollback_to->obs.oi.size;
      snapset.head_exists = true;
    }
    put_object_context(rollback_to);
  }
  return ret;
}

void ReplicatedPG::_make_clone(ObjectStore::Transaction& t,
			       const hobject_t& head, const hobject_t& coid,
			       object_info_t *poi)
{
  bufferlist bv;
  ::encode(*poi, bv);

  map<string, bufferptr> attrs;
  osd->store->getattrs(coll, head, attrs);
  osd->filter_xattrs(attrs);

  t.clone(coll, head, coid);
  t.setattr(coll, coid, OI_ATTR, bv);
  t.setattrs(coll, coid, attrs);
}

void ReplicatedPG::make_writeable(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  SnapContext& snapc = ctx->snapc;
  /*
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  */
  ObjectStore::Transaction t;

  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ctx->snapset
	   << "  snapc=" << snapc << dendl;;
  
  // use newer snapc?
  if (ctx->new_snapset.seq > snapc.seq) {
    snapc.seq = ctx->new_snapset.seq;
    snapc.snaps = ctx->new_snapset.snaps;
    dout(10) << " using newer snapc " << snapc << dendl;
  }
  
  if (ctx->obs->exists &&               // head exist(ed)
      snapc.snaps.size() &&                 // there are snaps
      snapc.snaps[0] > ctx->new_snapset.seq) {  // existing object is old
    // clone
    hobject_t coid = soid;
    coid.snap = snapc.seq;
    
    unsigned l;
    for (l=1; l<snapc.snaps.size() && snapc.snaps[l] > ctx->new_snapset.seq; l++) ;
    
    vector<snapid_t> snaps(l);
    for (unsigned i=0; i<l; i++)
      snaps[i] = snapc.snaps[i];
    
    // prepare clone
    object_info_t static_snap_oi(coid, ctx->obs->oi.oloc);
    object_info_t *snap_oi;
    if (is_primary()) {
      ctx->clone_obc = new ObjectContext(static_snap_oi, true, NULL);
      ctx->clone_obc->get();
      register_object_context(ctx->clone_obc);
      snap_oi = &ctx->clone_obc->obs.oi;
    } else {
      snap_oi = &static_snap_oi;
    }
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);
    snap_oi->snaps = snaps;
    _make_clone(t, soid, coid, snap_oi);
    
    // add to snap bound collections
    coll_t fc = make_snap_collection(t, snaps[0]);
    t.collection_add(fc, coll, coid);
    if (snaps.size() > 1) {
      coll_t lc = make_snap_collection(t, snaps[snaps.size()-1]);
      t.collection_add(lc, coll, coid);
    }
    
    ctx->delta_stats.num_objects++;
    ctx->delta_stats.num_object_clones++;
    ctx->new_snapset.clones.push_back(coid.snap);
    ctx->new_snapset.clone_size[coid.snap] = ctx->obs->oi.size;

    // clone_overlap should contain an entry for each clone 
    // (an empty interval_set if there is no overlap)
    ctx->new_snapset.clone_overlap[coid.snap];
    if (ctx->obs->oi.size)
      ctx->new_snapset.clone_overlap[coid.snap].insert(0, ctx->obs->oi.size);
    
    // log clone
    dout(10) << " cloning v " << ctx->obs->oi.version
	     << " to " << coid << " v " << ctx->at_version
	     << " snaps=" << snaps << dendl;
    ctx->log.push_back(Log::Entry(PG::Log::Entry::CLONE, coid, ctx->at_version,
				  ctx->obs->oi.version, ctx->reqid, ctx->new_obs.oi.mtime));
    ::encode(snaps, ctx->log.back().snaps);

    ctx->at_version.version++;
  }

  // update most recent clone_overlap
  if (ctx->new_snapset.clones.size() > 0) {
    interval_set<uint64_t> &newest_overlap = ctx->new_snapset.clone_overlap.rbegin()->second;
    ctx->modified_ranges.intersection_of(newest_overlap);
    newest_overlap.subtract(ctx->modified_ranges);
  }
  
  // prepend transaction to op_t
  t.append(ctx->op_t);
  t.swap(ctx->op_t);

  // update snapset with latest snap context
  ctx->new_snapset.seq = snapc.seq;
  ctx->new_snapset.snaps = snapc.snaps;
  ctx->new_snapset.head_exists = ctx->new_obs.exists;
  dout(20) << "make_writeable " << soid << " done, snapset=" << ctx->new_snapset << dendl;
}


void ReplicatedPG::write_update_size_and_usage(object_stat_sum_t& delta_stats, object_info_t& oi,
					       SnapSet& ss, interval_set<uint64_t>& modified,
					       uint64_t offset, uint64_t length, bool count_bytes)
{
  if (ss.clones.size()) {
    interval_set<uint64_t> ch;
    if (length)
      ch.insert(offset, length);
    modified.union_of(ch);
    add_interval_usage(ch, delta_stats);
  }
  if (length && (offset + length > oi.size)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes += new_size - oi.size;
    delta_stats.num_kb += SHIFT_ROUND_UP(new_size, 10) - SHIFT_ROUND_UP(oi.size, 10);
    oi.size = new_size;
  }
  delta_stats.num_wr++;
  if (count_bytes)
    delta_stats.num_wr_kb += SHIFT_ROUND_UP(length, 10);
}

void ReplicatedPG::add_interval_usage(interval_set<uint64_t>& s, object_stat_sum_t& delta_stats)
{
  for (interval_set<uint64_t>::const_iterator p = s.begin(); p != s.end(); ++p) {
    delta_stats.num_bytes += p.get_len();
    delta_stats.num_kb += SHIFT_ROUND_UP(p.get_start() + p.get_len(), 10) - (p.get_start() >> 10);
  }
}

void ReplicatedPG::do_osd_op_effects(OpContext *ctx)
{
  if (ctx->watch_connect || ctx->watch_disconnect ||
      !ctx->notifies.empty() || !ctx->notify_acks.empty()) {
    OSD::Session *session = (OSD::Session *)ctx->op->get_connection()->get_priv();
    ObjectContext *obc = ctx->obc;
    object_info_t& oi = ctx->new_obs.oi;
    hobject_t& soid = oi.soid;
    entity_name_t entity = ctx->reqid.name;

    dout(10) << "do_osd_op_effects applying watch/notify effects on session " << session << dendl;

    osd->watch_lock.Lock();
    dump_watchers(obc);
    
    map<entity_name_t, OSD::Session *>::iterator iter = obc->watchers.find(entity);
    if (ctx->watch_connect) {
      watch_info_t w = ctx->watch_info;

      if (iter == obc->watchers.end()) {
	dout(10) << " connected to " << w << " by " << entity << " session " << session << dendl;
	obc->watchers[entity] = session;
	session->get();
	session->watches[obc] = osd->osdmap->object_locator_to_pg(soid.oid, obc->obs.oi.oloc);
	obc->ref++;
      } else if (iter->second == session) {
	// already there
	dout(10) << " already connected to " << w << " by " << entity
		 << " session " << session << dendl;
      } else {
	// weird: same entity, different session.
	dout(10) << " reconnected (with different session!) watch " << w << " by " << entity
		 << " session " << session << " (was " << iter->second << ")" << dendl;
	iter->second->watches.erase(obc);
	iter->second->put();
	iter->second = session;
	session->get();
	session->watches[obc] = osd->osdmap->object_locator_to_pg(soid.oid, obc->obs.oi.oloc);
      }
      map<entity_name_t, Context *>::iterator un_iter =
	obc->unconnected_watchers.find(entity);
      if (un_iter != obc->unconnected_watchers.end()) {
	unregister_unconnected_watcher(obc, un_iter->first);
      }

      map<Watch::Notification *, bool>::iterator niter;
      for (niter = obc->notifs.begin(); niter != obc->notifs.end(); ++niter) {
	Watch::Notification *notif = niter->first;
	map<entity_name_t, Watch::WatcherState>::iterator iter = notif->watchers.find(entity);
	if (iter != notif->watchers.end()) {
	  /* there is a pending notification for this watcher, we should resend it anyway
	     even if we already sent it as it might not have received it */
	  MWatchNotify *notify_msg = new MWatchNotify(w.cookie, oi.user_version.version, notif->id, WATCH_NOTIFY, notif->bl);
	  osd->client_messenger->send_message(notify_msg, session->con);
	}
      }
    }
    
    if (ctx->watch_disconnect) {
      if (iter != obc->watchers.end()) {
	remove_watcher(obc, entity);
      } else {
	assert(obc->unconnected_watchers.count(entity));
	unregister_unconnected_watcher(obc, entity);
      }
    }

    for (list<notify_info_t>::iterator p = ctx->notifies.begin();
	 p != ctx->notifies.end();
	 ++p) {

      dout(10) << " " << *p << dendl;

      Watch::Notification *notif = new Watch::Notification(ctx->reqid.name, session, p->cookie, p->bl);
      session->get();  // notif got a reference
      session->con->get();
      notif->pgid = osd->osdmap->object_locator_to_pg(soid.oid, obc->obs.oi.oloc);

      osd->watch->add_notification(notif);

      // connected
      for (map<entity_name_t, watch_info_t>::iterator i = obc->obs.oi.watchers.begin();
	   i != obc->obs.oi.watchers.end();
	   ++i) {
	map<entity_name_t, OSD::Session*>::iterator q = obc->watchers.find(i->first);
	if (q != obc->watchers.end()) {
	  entity_name_t name = q->first;
	  OSD::Session *s = q->second;
	  watch_info_t& w = obc->obs.oi.watchers[q->first];

	  notif->add_watcher(name, Watch::WATCHER_NOTIFIED); // adding before send_message to avoid race
	  s->add_notif(notif, name);

	  MWatchNotify *notify_msg = new MWatchNotify(w.cookie, oi.user_version.version, notif->id, WATCH_NOTIFY, notif->bl);
	  osd->client_messenger->send_message(notify_msg, s->con);
	} else {
	  // unconnected
	  utime_t now = ceph_clock_now(g_ceph_context);
	  entity_name_t name = i->first;
	  notif->add_watcher(name, Watch::WATCHER_PENDING);
	}
      }

      notif->reply = new MWatchNotify(p->cookie, oi.user_version.version, notif->id, WATCH_NOTIFY_COMPLETE, notif->bl);
      if (notif->watchers.empty()) {
	osd->complete_notify(notif, obc);
      } else {
	obc->notifs[notif] = true;
	obc->ref++;
	notif->obc = obc;
	notif->timeout = new Watch::C_NotifyTimeout(osd, notif);
	osd->watch_timer.add_event_after(p->timeout, notif->timeout);
      }
    }

    for (list<uint64_t>::iterator p = ctx->notify_acks.begin(); p != ctx->notify_acks.end(); ++p) {
      uint64_t cookie = *p;
      
      dout(10) << " notify_ack " << cookie << dendl;
      map<entity_name_t, watch_info_t>::iterator oi_iter = oi.watchers.find(entity);
      assert(oi_iter != oi.watchers.end());

      Watch::Notification *notif = osd->watch->get_notif(cookie);
      assert(notif);

      session->del_notif(notif);
      osd->ack_notification(entity, notif, obc, this);
    }

    osd->watch_lock.Unlock();
    session->put();
  }
}


int ReplicatedPG::prepare_transaction(OpContext *ctx)
{
  assert(!ctx->ops.empty());
  
  const hobject_t& soid = ctx->obs->oi.soid;

  // we'll need this to log
  eversion_t old_version = ctx->obs->oi.version;

  bool head_existed = ctx->obs->exists;

  // prepare the actual mutation
  int result = do_osd_ops(ctx, ctx->ops, ctx->outdata);
  if (result < 0)
    return result;

  // finish side-effects
  if (result == 0)
    do_osd_op_effects(ctx);

  ctx->bytes_read = ctx->outdata.length();

  // read-op?  done?
  if (ctx->op_t.empty() && !ctx->modify) {
    ctx->reply_version = ctx->obs->oi.user_version;
    return result;
  }


  // there was a modification!

  // valid snap context?
  if (!ctx->snapc.is_valid()) {
    dout(10) << " invalid snapc " << ctx->snapc << dendl;
    return -EINVAL;
  }

  make_writeable(ctx);

  if (ctx->user_modify) {
    /* update the user_version for any modify ops, except for the watch op */
    ctx->new_obs.oi.user_version = ctx->at_version;
  }
  
  ctx->reply_version = ctx->new_obs.oi.user_version;

  ctx->bytes_written = ctx->op_t.get_encoded_bytes();

  // finish and log the op.
  ctx->new_obs.oi.version = ctx->at_version;
 
  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  assert(ctx->new_obs.exists == ctx->new_snapset.head_exists);

  // append to log
  int logopcode = Log::Entry::MODIFY;
  if (!ctx->new_obs.exists)
    logopcode = Log::Entry::DELETE;
  ctx->log.push_back(Log::Entry(logopcode, soid, ctx->at_version, old_version,
				ctx->reqid, ctx->mtime));

  if (ctx->new_obs.exists) {
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = old_version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->new_obs.oi, bv);
    ctx->op_t.setattr(coll, soid, OI_ATTR, bv);

    dout(10) << " final snapset " << ctx->new_snapset
	     << " in " << soid << dendl;
    ctx->op_t.setattr(coll, soid, SS_ATTR, bss);   
    if (!head_existed) {
      // if we logically recreated the head, remove old _snapdir object
      hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash);

      ctx->snapset_obc = get_object_context(snapoid, ctx->new_obs.oi.oloc, false);
      if (ctx->snapset_obc && ctx->snapset_obc->obs.exists) {
	ctx->op_t.remove(coll, snapoid);
	dout(10) << " removing old " << snapoid << dendl;

	ctx->at_version.version++;
	ctx->log.push_back(Log::Entry(Log::Entry::DELETE, snapoid, ctx->at_version, old_version,
				      osd_reqid_t(), ctx->mtime));

	ctx->snapset_obc->obs.exists = false;
	assert(ctx->snapset_obc->registered);
      }
    }
  } else if (ctx->new_snapset.clones.size()) {
    // save snapset on _snap
    hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash);
    dout(10) << " final snapset " << ctx->new_snapset
	     << " in " << snapoid << dendl;
    ctx->at_version.version++;
    ctx->log.push_back(Log::Entry(Log::Entry::MODIFY, snapoid, ctx->at_version, old_version,
				  osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc = get_object_context(snapoid, ctx->new_obs.oi.oloc, true);
    ctx->snapset_obc->obs.exists = true;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;
    ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
    ctx->snapset_obc->obs.oi.mtime = ctx->mtime;
    assert(ctx->snapset_obc->registered);

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->snapset_obc->obs.oi, bv);
    ctx->op_t.touch(coll, snapoid);
    ctx->op_t.setattr(coll, snapoid, OI_ATTR, bv);
    ctx->op_t.setattr(coll, snapoid, SS_ATTR, bss);
  }

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;
  ctx->obc->ssc->snapset = ctx->new_snapset;
  info.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);

  return result;
}



// ========================================================================
// rep op gather

class C_OSD_OpApplied : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpApplied(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->op_applied(repop);
    pg->put();
  }
};

class C_OSD_OpCommit : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpCommit(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->op_commit(repop);
    pg->put();
  }
};

void ReplicatedPG::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applying);
  assert(!repop->applied);

  repop->applying = true;

  repop->tls.push_back(&repop->ctx->op_t);
  repop->tls.push_back(&repop->ctx->local_t);

  repop->obc->ondisk_write_lock();
  if (repop->ctx->clone_obc)
    repop->ctx->clone_obc->ondisk_write_lock();

  Context *oncommit = new C_OSD_OpCommit(this, repop);
  Context *onapplied = new C_OSD_OpApplied(this, repop);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(repop->obc,
							repop->ctx->clone_obc);
  int r = osd->store->queue_transactions(&osr, repop->tls, onapplied, oncommit, onapplied_sync);
  if (r) {
    derr << "apply_repop  queue_transactions returned " << r << " on " << *repop << dendl;
    assert(0);
  }
}

void ReplicatedPG::op_applied(RepGather *repop)
{
  lock();
  dout(10) << "op_applied " << *repop << dendl;

  // discard my reference to the buffer
  if (repop->ctx->op)
    repop->ctx->op->clear_data();
  
  repop->applying = false;
  repop->applied = true;

  // (logical) local ack.
  int whoami = osd->get_nodeid();

  if (!repop->aborted) {
    assert(repop->waitfor_ack.count(whoami));
    repop->waitfor_ack.erase(whoami);
  }
  
  if (repop->ctx->clone_obc) {
    put_object_context(repop->ctx->clone_obc);
    repop->ctx->clone_obc = 0;
  }
  if (repop->ctx->snapset_obc) {
    put_object_context(repop->ctx->snapset_obc);
    repop->ctx->snapset_obc = 0;
  }

  dout(10) << "op_applied mode was " << mode << dendl;
  mode.write_applied();
  dout(10) << "op_applied mode now " << mode << " (finish_write)" << dendl;

  put_object_context(repop->obc);
  put_object_contexts(repop->src_obc);
  repop->obc = 0;

  last_update_applied = repop->v;
  if (last_update_applied == info.last_update && finalizing_scrub) {
    dout(10) << "requeueing scrub for cleanup" << dendl;
    osd->scrub_wq.queue(this);
  }
  update_stats();

  if (!repop->aborted)
    eval_repop(repop);

  repop->put();
  unlock();
}

void ReplicatedPG::op_commit(RepGather *repop)
{
  lock();

  if (repop->aborted) {
    dout(10) << "op_commit " << *repop << " -- aborted" << dendl;
  } else if (repop->waitfor_disk.count(osd->get_nodeid()) == 0) {
    dout(10) << "op_commit " << *repop << " -- already marked ondisk" << dendl;
  } else {
    dout(10) << "op_commit " << *repop << dendl;
    repop->waitfor_disk.erase(osd->get_nodeid());
    //repop->waitfor_nvram.erase(osd->get_nodeid());

    last_update_ondisk = repop->v;
    if (waiting_for_ondisk.count(repop->v)) {
      osd->requeue_ops(this, waiting_for_ondisk[repop->v]);
      waiting_for_ondisk.erase(repop->v);
    }

    last_complete_ondisk = repop->pg_local_last_complete;
    eval_repop(repop);
  }

  repop->put();
  unlock();
}



void ReplicatedPG::eval_repop(RepGather *repop)
{
  MOSDOp *op = (MOSDOp *)repop->ctx->op;

  if (op)
    dout(10) << "eval_repop " << *repop
	     << " wants=" << (op->wants_ack() ? "a":"") << (op->wants_ondisk() ? "d":"")
	     << dendl;
  else
    dout(10) << "eval_repop " << *repop << " (no op)" << dendl;
 
  // apply?
  if (!repop->applied && !repop->applying &&
      ((mode.is_delayed_mode() &&
	repop->waitfor_ack.size() == 1) ||  // all other replicas have acked
       mode.is_rmw_mode()))
    apply_repop(repop);
  
  if (op) {

    // an 'ondisk' reply implies 'ack'. so, prefer to send just one
    // ondisk instead of ack followed by ondisk.

    // ondisk?
    if (repop->waitfor_disk.empty()) {

      log_op_stats(repop->ctx);

      if (op->wants_ondisk() && !repop->sent_disk) {
	// send commit.
	MOSDOpReply *reply = repop->ctx->reply;
	if (reply)
	  repop->ctx->reply = NULL;
	else
	  reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), 0);
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending commit on " << *repop << " " << reply << dendl;
	assert(entity_name_t::TYPE_OSD != op->get_connection()->peer_type);
	osd->client_messenger->send_message(reply, op->get_connection());
	repop->sent_disk = true;
      }
    }

    // applied?
    if (repop->waitfor_ack.empty()) {
      if (op->wants_ack() && !repop->sent_ack && !repop->sent_disk) {
	// send ack
	MOSDOpReply *reply = repop->ctx->reply;
	if (reply)
	  repop->ctx->reply = NULL;
	else
	  reply = new MOSDOpReply(op, 0, osd->osdmap->get_epoch(), 0);
	reply->add_flags(CEPH_OSD_FLAG_ACK);
	dout(10) << " sending ack on " << *repop << " " << reply << dendl;
        assert(entity_name_t::TYPE_OSD != op->get_connection()->peer_type);
	osd->client_messenger->send_message(reply, op->get_connection());
	repop->sent_ack = true;
      }

      // note the write is now readable (for rlatency calc).  note
      // that this will only be defined if the write is readable
      // _prior_ to being committed; it will not get set with
      // writeahead journaling, for instance.
      if (repop->ctx->readable_stamp == utime_t())
	repop->ctx->readable_stamp = ceph_clock_now(g_ceph_context);
    }
  }

  // done.
  if (repop->waitfor_ack.empty() && repop->waitfor_disk.empty()) {
    calc_min_last_complete_ondisk();

    // kick snap_trimmer if necessary
    if (repop->queue_snap_trimmer) {
      queue_snap_trim();
    }

    dout(10) << " removing " << *repop << dendl;
    assert(!repop_queue.empty());
    dout(20) << "   q front is " << *repop_queue.front() << dendl; 
    if (repop_queue.front() != repop) {
      dout(0) << " removing " << *repop << dendl;
      dout(0) << "   q front is " << *repop_queue.front() << dendl; 
      assert(repop_queue.front() == repop);
    }
    repop_queue.pop_front();
    remove_repop(repop);
  }
}

void ReplicatedPG::issue_repop(RepGather *repop, utime_t now,
			       eversion_t old_last_update, bool old_exists, uint64_t old_size, eversion_t old_version)
{
  OpContext *ctx = repop->ctx;
  const hobject_t& soid = ctx->obs->oi.soid;
  MOSDOp *op = (MOSDOp *)ctx->op;

  dout(7) << "issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << dendl;

  repop->v = ctx->at_version;

  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    
    // forward the write/update/whatever
    MOSDSubOp *wr = new MOSDSubOp(repop->ctx->reqid, info.pgid, soid,
				  false, acks_wanted,
				  osd->osdmap->get_epoch(), 
				  repop->rep_tid, repop->ctx->at_version);

    if (op && op->get_flags() & CEPH_OSD_FLAG_PARALLELEXEC) {
      // replicate original op for parallel execution on replica
      wr->oloc = repop->ctx->obs->oi.oloc;
      wr->ops = repop->ctx->ops;
      wr->mtime = repop->ctx->mtime;
      wr->old_exists = old_exists;
      wr->old_size = old_size;
      wr->old_version = old_version;
      wr->snapset = repop->obc->ssc->snapset;
      wr->snapc = repop->ctx->snapc;
      wr->set_data(repop->ctx->op->get_data());   // _copy_ bufferlist
    } else {
      // ship resulting transaction, log entries, and pg_stats
      ::encode(repop->ctx->op_t, wr->get_data());
      ::encode(repop->ctx->log, wr->logbl);
      wr->pg_stats = info.stats;
    }
    
    wr->pg_trim_to = pg_trim_to;
    osd->cluster_messenger->send_message(wr, osd->osdmap->get_cluster_inst(peer));

    // keep peer_info up to date
    Info &in = peer_info[peer];
    in.last_update = ctx->at_version;
    if (in.last_complete == old_last_update)
      in.last_update = ctx->at_version;
  }
}

ReplicatedPG::RepGather *ReplicatedPG::new_repop(OpContext *ctx, ObjectContext *obc,
						 tid_t rep_tid)
{
  if (ctx->op)
    dout(10) << "new_repop rep_tid " << rep_tid << " on " << *ctx->op << dendl;
  else
    dout(10) << "new_repop rep_tid " << rep_tid << " (no op)" << dendl;

  RepGather *repop = new RepGather(ctx, obc, rep_tid, info.last_complete);

  dout(10) << "new_repop mode was " << mode << dendl;
  mode.write_start();
  dout(10) << "new_repop mode now " << mode << " (start_write)" << dendl;

  // initialize gather sets
  for (unsigned i=0; i<acting.size(); i++) {
    int osd = acting[i];
    repop->waitfor_ack.insert(osd);
    repop->waitfor_disk.insert(osd);
  }

  repop->start = ceph_clock_now(g_ceph_context);

  repop_queue.push_back(&repop->queue_item);
  repop_map[repop->rep_tid] = repop;
  repop->get();

  if (osd->logger)
    osd->logger->set(l_osd_op_wip, repop_map.size());

  return repop;
}
 
void ReplicatedPG::remove_repop(RepGather *repop)
{
  repop_map.erase(repop->rep_tid);
  repop->put();

  if (osd->logger)
    osd->logger->set(l_osd_op_wip, repop_map.size());
}

void ReplicatedPG::repop_ack(RepGather *repop, int result, int ack_type,
			     int fromosd, eversion_t peer_lcod)
{
  MOSDOp *op = (MOSDOp *)repop->ctx->op;

  if (op)
    dout(7) << "repop_ack rep_tid " << repop->rep_tid << " op " << *op
	    << " result " << result
	    << " ack_type " << ack_type
	    << " from osd." << fromosd
	    << dendl;
  else
    dout(7) << "repop_ack rep_tid " << repop->rep_tid << " (no op) "
	    << " result " << result
	    << " ack_type " << ack_type
	    << " from osd." << fromosd
	    << dendl;
  
  if (ack_type & CEPH_OSD_FLAG_ONDISK) {
    // disk
    if (repop->waitfor_disk.count(fromosd)) {
      repop->waitfor_disk.erase(fromosd);
      //repop->waitfor_nvram.erase(fromosd);
      repop->waitfor_ack.erase(fromosd);
      peer_last_complete_ondisk[fromosd] = peer_lcod;
    }
/*} else if (ack_type & CEPH_OSD_FLAG_ONNVRAM) {
    // nvram
    repop->waitfor_nvram.erase(fromosd);
    repop->waitfor_ack.erase(fromosd);*/
  } else {
    // ack
    repop->waitfor_ack.erase(fromosd);
  }

  eval_repop(repop);
}







// -------------------------------------------------------

void ReplicatedPG::populate_obc_watchers(ObjectContext *obc)
{
  if (!is_active() || is_degraded_object(obc->obs.oi.soid) ||
      is_missing_object(obc->obs.oi.soid))
    return;

  if (!obc->obs.oi.watchers.empty()) {
    Mutex::Locker l(osd->watch_lock);
    assert(obc->unconnected_watchers.size() == 0);
    assert(obc->watchers.size() == 0);
    // populate unconnected_watchers
    utime_t now = ceph_clock_now(g_ceph_context);
    for (map<entity_name_t, watch_info_t>::iterator p = obc->obs.oi.watchers.begin();
	 p != obc->obs.oi.watchers.end();
	 p++) {
      utime_t expire = now;
      expire += p->second.timeout_seconds;
      dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
      register_unconnected_watcher(obc, p->first, expire);
    }
  }
}

void ReplicatedPG::unregister_unconnected_watcher(void *_obc,
						  entity_name_t entity)
{
  ObjectContext *obc = static_cast<ObjectContext *>(_obc);
  osd->watch_timer.cancel_event(obc->unconnected_watchers[entity]);
  obc->unconnected_watchers.erase(entity);
  put_object_context(obc);
  put();
}

void ReplicatedPG::register_unconnected_watcher(void *_obc,
						entity_name_t entity,
						utime_t expire)
{
  ObjectContext *obc = static_cast<ObjectContext *>(_obc);
  pg_t pgid = info.pgid;
  pgid.set_ps(obc->obs.oi.soid.hash);
  get();
  obc->ref++;
  Context *cb = new Watch::C_WatchTimeout(osd,
					  static_cast<void *>(obc),
					  this,
					  entity, expire);
  osd->watch_timer.add_event_at(expire, cb);
  obc->unconnected_watchers[entity] = cb;
}

void ReplicatedPG::handle_watch_timeout(void *_obc,
					entity_name_t entity,
					utime_t expire)
{
  ObjectContext *obc = static_cast<ObjectContext *>(_obc);
  obc->unconnected_watchers.erase(entity);
  obc->obs.oi.watchers.erase(entity);

  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->cluster_messenger->get_myname(), 0, rep_tid);
  OpContext *ctx = new OpContext(NULL, reqid, ops, &obc->obs, obc->ssc, this);
  ctx->mtime = ceph_clock_now(g_ceph_context);

  ctx->at_version.epoch = osd->osdmap->get_epoch();
  ctx->at_version.version = log.head.version + 1;

  entity_inst_t nobody;

  /* Currently, mode.try_write always returns true.  If this changes, we will
   * need to delay the repop accordingly */
  assert(mode.try_write(nobody));
  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;

  ctx->log.push_back(Log::Entry(Log::Entry::MODIFY, obc->obs.oi.soid,
				ctx->at_version,
				obc->obs.oi.version,
				osd_reqid_t(), ctx->mtime));

  eversion_t old_last_update = log.head;
  bool old_exists = repop->obc->obs.exists;
  uint64_t old_size = repop->obc->obs.oi.size;
  eversion_t old_version = repop->obc->obs.oi.version;

  obc->obs.oi.prior_version = old_version;
  obc->obs.oi.version = ctx->at_version;
  bufferlist bl;
  ::encode(obc->obs.oi, bl);
  t->setattr(coll, obc->obs.oi.soid, OI_ATTR, bl);

  ctx->at_version.version++;

  append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);

  // obc ref swallowed by repop!
  issue_repop(repop, repop->ctx->mtime, old_last_update, old_exists,
	      old_size, old_version);
  eval_repop(repop);
}

ReplicatedPG::ObjectContext *ReplicatedPG::get_object_context(const hobject_t& soid,
							      const object_locator_t& oloc,
							      bool can_create)
{
  map<hobject_t, ObjectContext*>::iterator p = object_contexts.find(soid);
  ObjectContext *obc;
  if (p != object_contexts.end()) {
    obc = p->second;
    dout(10) << "get_object_context " << soid << " " << obc->ref
	     << " -> " << (obc->ref+1) << dendl;
  } else {
    // check disk
    bufferlist bv;
    int r = osd->store->getattr(coll, soid, OI_ATTR, bv);
    if (r < 0) {
      if (!can_create)
	return NULL;   // -ENOENT!
      object_info_t oi(soid, oloc);
      obc = new ObjectContext(oi, false, NULL);
    }
    else {
      object_info_t oi(bv);

      // if the on-disk oloc is bad/undefined, set up the pool value
      if (oi.oloc.get_pool() < 0) {
	oi.oloc.pool = info.pgid.pool();
	oi.oloc.preferred = info.pgid.preferred();
      }

      SnapSetContext *ssc = NULL;
      if (can_create)
	ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, true);
      obc = new ObjectContext(oi, true, ssc);
    }
    register_object_context(obc);

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, true);

    if (r >= 0) {
      obc->obs.oi.decode(bv);
      obc->obs.exists = true;

      populate_obc_watchers(obc);
    } else {
      obc->obs.exists = false;
    }
    dout(10) << "get_object_context " << soid << " read " << obc->obs.oi << dendl;
  }
  obc->ref++;
  return obc;
}

void ReplicatedPG::context_registry_on_change()
{
  remove_watchers_and_notifies();
  if (object_contexts.size()) {
    for (map<hobject_t, ObjectContext *>::iterator p = object_contexts.begin();
	 p != object_contexts.end();
	 ++p) {
      dout(0) << "Object " << p->second->obs.oi.soid << " still has ref count of " 
	      << p->second->ref << dendl;
    }
    assert(!object_contexts.size());
  }
}


int ReplicatedPG::find_object_context(const hobject_t& oid,
				      const object_locator_t& oloc,
				      ObjectContext **pobc,
				      bool can_create,
				      snapid_t *psnapid)
{
  // want the head?
  hobject_t head(oid.oid, oid.get_key(), CEPH_NOSNAP, oid.hash);
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContext *obc = get_object_context(head, oloc, can_create);
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << oid.snap << dendl;
    *pobc = obc;

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, true);

    return 0;
  }

  // we want a snap
  SnapSetContext *ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, can_create);
  if (!ssc)
    return -ENOENT;

  dout(10) << "find_object_context " << oid << " @" << oid.snap
	   << " snapset " << ssc->snapset << dendl;
 
  // head?
  if (oid.snap > ssc->snapset.seq) {
    if (ssc->snapset.head_exists) {
      ObjectContext *obc = get_object_context(head, oloc, false);
      dout(10) << "find_object_context  " << head
	       << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	       << " -- HIT " << obc->obs
	       << dendl;
      if (!obc->ssc)
	obc->ssc = ssc;
      else {
	assert(ssc == obc->ssc);
	put_snapset_context(ssc);
      }
      *pobc = obc;
      return 0;
    }
    dout(10) << "find_object_context  " << head
	     << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	     << " but head dne -- DNE"
	     << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < oid.snap)
    k++;
  if (k == ssc->snapset.clones.size()) {
    dout(10) << "find_object_context  no clones with last >= oid.snap " << oid.snap << " -- DNE" << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }
  hobject_t soid(oid.oid, oid.get_key(), ssc->snapset.clones[k], oid.hash);

  put_snapset_context(ssc); // we're done with ssc
  ssc = 0;

  if (missing.is_missing(soid)) {
    dout(20) << "find_object_context  " << soid << " missing, try again later" << dendl;
    if (psnapid)
      *psnapid = soid.snap;
    return -EAGAIN;
  }

  ObjectContext *obc = get_object_context(soid, oloc, false);

  // clone
  dout(20) << "find_object_context  " << soid << " snaps " << obc->obs.oi.snaps << dendl;
  snapid_t first = obc->obs.oi.snaps[obc->obs.oi.snaps.size()-1];
  snapid_t last = obc->obs.oi.snaps[0];
  if (first <= oid.snap) {
    dout(20) << "find_object_context  " << soid << " [" << first << "," << last
	     << "] contains " << oid.snap << " -- HIT " << obc->obs << dendl;
    *pobc = obc;
    return 0;
  } else {
    dout(20) << "find_object_context  " << soid << " [" << first << "," << last
	     << "] does not contain " << oid.snap << " -- DNE" << dendl;
    put_object_context(obc);
    return -ENOENT;
  }
}

void ReplicatedPG::put_object_context(ObjectContext *obc)
{
  dout(10) << "put_object_context " << obc->obs.oi.soid << " "
	   << obc->ref << " -> " << (obc->ref-1) << dendl;

  if (mode.wake) {
    osd->requeue_ops(this, mode.waiting);
    for (list<Cond*>::iterator p = mode.waiting_cond.begin(); p != mode.waiting_cond.end(); p++)
      (*p)->Signal();
    mode.wake = false;
  }

  --obc->ref;
  if (obc->ref == 0) {
    if (obc->ssc)
      put_snapset_context(obc->ssc);

    if (obc->registered)
      object_contexts.erase(obc->obs.oi.soid);
    delete obc;

    if (object_contexts.empty())
      kick();
  }
}

void ReplicatedPG::put_object_contexts(map<hobject_t,ObjectContext*>& obcv)
{
  if (obcv.empty())
    return;
  dout(10) << "put_object_contexts " << obcv << dendl;
  for (map<hobject_t,ObjectContext*>::iterator p = obcv.begin(); p != obcv.end(); ++p)
    put_object_context(p->second);
  obcv.clear();
}

ReplicatedPG::SnapSetContext *ReplicatedPG::get_snapset_context(const object_t& oid,
								const string& key,
								ps_t seed,
								bool can_create)
{
  SnapSetContext *ssc;
  map<object_t, SnapSetContext*>::iterator p = snapset_contexts.find(oid);
  if (p != snapset_contexts.end()) {
    ssc = p->second;
  } else {
    bufferlist bv;
    hobject_t head(oid, key, CEPH_NOSNAP, seed);
    int r = osd->store->getattr(coll, head, SS_ATTR, bv);
    if (r < 0) {
      // try _snapset
      hobject_t snapdir(oid, key, CEPH_SNAPDIR, seed);
      r = osd->store->getattr(coll, snapdir, SS_ATTR, bv);
      if (r < 0 && !can_create)
	return NULL;
    }
    ssc = new SnapSetContext(oid);
    register_snapset_context(ssc);
    if (r >= 0) {
      bufferlist::iterator bvp = bv.begin();
      ssc->snapset.decode(bvp);
    }
  }
  assert(ssc);
  dout(10) << "get_snapset_context " << ssc->oid << " "
	   << ssc->ref << " -> " << (ssc->ref+1) << dendl;
  ssc->ref++;
  return ssc;
}

void ReplicatedPG::put_snapset_context(SnapSetContext *ssc)
{
  dout(10) << "put_snapset_context " << ssc->oid << " "
	   << ssc->ref << " -> " << (ssc->ref-1) << dendl;

  --ssc->ref;
  if (ssc->ref == 0) {
    if (ssc->registered)
      snapset_contexts.erase(ssc->oid);
    delete ssc;
  }
}

// sub op modify

void ReplicatedPG::sub_op_modify(MOSDSubOp *op)
{
  const hobject_t& soid = op->poid;

  const char *opname;
  if (op->noop)
    opname = "no-op";
  else if (op->ops.size())
    opname = ceph_osd_op_name(op->ops[0].op.op);
  else
    opname = "trans";

  dout(10) << "sub_op_modify " << opname 
           << " " << soid 
           << " v " << op->version
	   << (op->noop ? " NOOP" : "")
	   << (op->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << op->logbl.length()
	   << dendl;  

  // sanity checks
  assert(op->map_epoch >= info.history.same_interval_since);
  assert(is_active());
  assert(is_replica());
  
  // we better not be missing this.
  assert(!missing.is_missing(soid));

  int ackerosd = acting[0];
  
  RepModify *rm = new RepModify;
  rm->pg = this;
  get();
  rm->op = op;
  rm->ctx = 0;
  rm->ackerosd = ackerosd;
  rm->last_complete = info.last_complete;

  if (!op->noop) {
    if (op->logbl.length()) {
      // shipped transaction and log entries
      vector<Log::Entry> log;
      
      bufferlist::iterator p = op->get_data().begin();
      ::decode(rm->opt, p);
      p = op->logbl.begin();
      ::decode(log, p);
      
      info.stats = op->pg_stats;
      update_snap_collections(log);
      append_log(log, op->pg_trim_to, rm->localt);

      rm->tls.push_back(&rm->opt);
      rm->tls.push_back(&rm->localt);

    } else {
      // do op
      assert(0);

      // TODO: this is severely broken because we don't know whether this object is really lost or
      // not. We just always assume that it's not right now.
      // Also, we're taking the address of a variable on the stack. 
      object_info_t oi(soid, op->oloc);
      oi.lost = false; // I guess?
      oi.version = op->old_version;
      oi.size = op->old_size;
      ObjectState obs(oi, op->old_exists);
      SnapSetContext ssc(op->poid.oid);
      
      rm->ctx = new OpContext(op, op->reqid, op->ops, &obs, &ssc, this);
      
      rm->ctx->mtime = op->mtime;
      rm->ctx->at_version = op->version;
      rm->ctx->snapc = op->snapc;

      ssc.snapset = op->snapset;
      rm->ctx->obc->ssc = &ssc;
      
      prepare_transaction(rm->ctx);
      append_log(rm->ctx->log, op->pg_trim_to, rm->ctx->local_t);
    
      rm->tls.push_back(&rm->ctx->op_t);
      rm->tls.push_back(&rm->ctx->local_t);
    }

    rm->bytes_written = rm->opt.get_encoded_bytes();

  } else {
    // just trim the log
    if (op->pg_trim_to != eversion_t()) {
      trim(rm->localt, op->pg_trim_to);
      rm->tls.push_back(&rm->localt);
    }
  }
  
  Context *oncommit = new C_OSD_RepModifyCommit(rm);
  Context *onapply = new C_OSD_RepModifyApply(rm);
  int r = osd->store->queue_transactions(&osr, rm->tls, onapply, oncommit);
  if (r) {
    dout(0) << "error applying transaction: r = " << r << dendl;
    assert(0);
  }
  // op is cleaned up by oncommit/onapply when both are executed
}

void ReplicatedPG::sub_op_modify_applied(RepModify *rm)
{
  lock();
  dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op << dendl;

  if (!rm->committed) {
    // send ack to acker only if we haven't sent a commit already
    MOSDSubOpReply *ack = new MOSDSubOpReply(rm->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
    ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
    osd->cluster_messenger->send_message(ack, osd->osdmap->get_cluster_inst(rm->ackerosd));
  }

  rm->applied = true;
  bool done = rm->applied && rm->committed;

  last_update_applied = rm->op->version;
  if (last_update_applied == info.last_update && finalizing_scrub) {
    assert(active_rep_scrub);
    osd->rep_scrub_wq.queue(active_rep_scrub);
    active_rep_scrub->put();
    active_rep_scrub = 0;
  }

  unlock();
  if (done) {
    delete rm->ctx;
    rm->op->put();
    delete rm;
    put();
  }
}

void ReplicatedPG::sub_op_modify_commit(RepModify *rm)
{
  lock();

  // send commit.
  dout(10) << "sub_op_modify_commit on op " << *rm->op
           << ", sending commit to osd." << rm->ackerosd
           << dendl;

  log_subop_stats(rm->op, l_osd_sop_w_inb, l_osd_sop_w_lat);

  if (osd->osdmap->is_up(rm->ackerosd)) {
    last_complete_ondisk = rm->last_complete;
    MOSDSubOpReply *commit = new MOSDSubOpReply(rm->op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    commit->set_last_complete_ondisk(rm->last_complete);
    commit->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
    osd->cluster_messenger->send_message(commit, osd->osdmap->get_cluster_inst(rm->ackerosd));
  }
  
  rm->committed = true;
  bool done = rm->applied && rm->committed;

  unlock();
  if (done) {
    delete rm->ctx;
    rm->op->put();
    delete rm;
    put();
  }
}

void ReplicatedPG::sub_op_modify_reply(MOSDSubOpReply *r)
{
  // must be replication.
  tid_t rep_tid = r->get_tid();
  int fromosd = r->get_source().num();
  
  if (repop_map.count(rep_tid)) {
    // oh, good.
    repop_ack(repop_map[rep_tid], 
	      r->get_result(), r->ack_type,
	      fromosd, 
	      r->get_last_complete_ondisk());
  }

  r->put();
}










// ===========================================================

void ReplicatedPG::calc_head_subsets(SnapSet& snapset, const hobject_t& head,
				     Missing& missing,
				     interval_set<uint64_t>& data_subset,
				     map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  struct stat st;
  osd->store->stat(coll, head, &st);

  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (st.st_size)
    prev.insert(0, st.st_size);    
  
  for (int j=snapset.clones.size()-1; j>=0; j--) {
    hobject_t c = head;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c)) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // what's left for us to push?
  if (st.st_size)
    data_subset.insert(0, st.st_size);
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedPG::calc_clone_subsets(SnapSet& snapset, const hobject_t& soid,
				      Missing& missing,
				      interval_set<uint64_t>& data_subset,
				      map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];

  unsigned i;
  for (i=0; i < snapset.clones.size(); i++)
    if (snapset.clones[i] == soid.snap)
      break;

  // any overlap with next older clone?
  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);    
  for (int j=i-1; j>=0; j--) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c)) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }
  
  // overlap with next newest?
  interval_set<uint64_t> next;
  if (size)
    next.insert(0, size);    
  for (unsigned j=i+1; j<snapset.clones.size(); j++) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);
    if (!missing.is_missing(c)) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }
  
  // what's left for us to push?
  if (size)
    data_subset.insert(0, size);
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}


/** pull - request object from a peer
 */

/*
 * Return values:
 *  NONE  - didn't pull anything
 *  YES   - pulled what the caller wanted
 *  OTHER - needed to pull something else first (_head or _snapdir)
 */
enum { PULL_NONE, PULL_OTHER, PULL_YES };

int ReplicatedPG::pull(const hobject_t& soid)
{
  eversion_t v = missing.missing[soid].need;

  int fromosd = -1;
  map<hobject_t,set<int> >::iterator q = missing_loc.find(soid);
  if (q != missing_loc.end()) {
    for (set<int>::iterator p = q->second.begin();
	 p != q->second.end();
	 p++) {
      if (osd->osdmap->is_up(*p)) {
	fromosd = *p;
	break;
      }
    }
  }
  if (fromosd < 0) {
    dout(7) << "pull " << soid
	    << " v " << v 
	    << " but it is unfound" << dendl;
    return PULL_NONE;
  }
  
  dout(7) << "pull " << soid
          << " v " << v 
	  << " on osds " << missing_loc[soid]
	  << " from osd." << fromosd
	  << dendl;

  map<hobject_t, interval_set<uint64_t> > clone_subsets;
  interval_set<uint64_t> data_subset;
  bool need_size = false;

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head and/or snapdir?
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling head " << head << dendl;
	return PULL_NONE;
      } else {
	int r = pull(head);
	if (r != PULL_NONE)
	  return PULL_OTHER;
	return PULL_NONE;
      }
    }
    head.snap = CEPH_SNAPDIR;
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling snapdir " << head << dendl;
	return false;
      } else {
  	int r = pull(head);
	if (r != PULL_NONE)
	  return PULL_OTHER;
	return PULL_NONE;
      }
    }

    // check snapset
    SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
    dout(10) << " snapset " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, missing,
		       data_subset, clone_subsets);
    put_snapset_context(ssc);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << data_subset << ", will clone " << clone_subsets
	     << dendl;
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    need_size = true;
    data_subset.insert(0, (uint64_t)-1);
  }

  // only pull so much at a time
  interval_set<uint64_t> pullsub;
  pullsub.span_of(data_subset, 0, g_conf->osd_recovery_max_chunk);

  // take note
  assert(pulling.count(soid) == 0);
  pull_from_peer[fromosd].insert(soid);
  pull_info_t& p = pulling[soid];
  p.version = v;
  p.from = fromosd;
  p.data_subset = data_subset;
  p.data_subset_pulling = pullsub;
  p.need_size = need_size;

  send_pull_op(soid, v, true, p.data_subset_pulling, fromosd);
  
  start_recovery_op(soid);
  return PULL_YES;
}

void ReplicatedPG::send_pull_op(const hobject_t& soid, eversion_t v, bool first,
				const interval_set<uint64_t>& data_subset, int fromosd)
{
  // send op
  osd_reqid_t rid;
  tid_t tid = osd->get_tid();

  dout(10) << "send_pull_op " << soid << " " << v
	   << " first=" << first
	   << " data " << data_subset << " from osd." << fromosd
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, false, CEPH_OSD_FLAG_ACK,
				   osd->osdmap->get_epoch(), tid, v);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->data_subset = data_subset;
  subop->first = first;

  // do not include clone_subsets in pull request; we will recalculate this
  // when the object is pushed back.
  //subop->clone_subsets.swap(clone_subsets);

  osd->cluster_messenger->send_message(subop, osd->osdmap->get_cluster_inst(fromosd));

  osd->logger->inc(l_osd_pull);
}


/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
void ReplicatedPG::push_to_replica(ObjectContext *obc, const hobject_t& soid, int peer)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << "push_to_replica " << soid << " v" << oi.version << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t> > clone_subsets;
  interval_set<uint64_t> data_subset;
  
  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {	
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    if (peer_missing[peer].is_missing(head) &&
	peer_missing[peer].have_old(head) == oi.prior_version) {
      dout(10) << "push_to_replica osd." << peer << " has correct old " << head
	       << " v" << oi.prior_version 
	       << ", pushing " << soid << " attrs as a clone op" << dendl;
      interval_set<uint64_t> data_subset;
      map<hobject_t, interval_set<uint64_t> > clone_subsets;
      if (size)
	clone_subsets[head].insert(0, size);
      push_start(soid, peer, size, oi.version, data_subset, clone_subsets);
      return;
    }

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) to do that.
    if (missing.is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return push_start(soid, peer);
    }
    hobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (missing.is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir << ", pushing raw clone" << dendl;
      return push_start(snapdir, peer);
    }
    
    SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, peer_missing[peer],
		       data_subset, clone_subsets);
    put_snapset_context(ssc);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(ssc->snapset, soid, peer_missing[peer], data_subset, clone_subsets);
    put_snapset_context(ssc);
  }

  push_start(soid, peer, size, oi.version, data_subset, clone_subsets);
}

void ReplicatedPG::push_start(const hobject_t& soid, int peer)
{
  struct stat st;
  int r = osd->store->stat(coll, soid, &st);
  assert(r == 0);
  uint64_t size = st.st_size;

  bufferlist bl;
  r = osd->store->getattr(coll, soid, OI_ATTR, bl);
  object_info_t oi(bl);

  interval_set<uint64_t> data_subset;
  map<hobject_t, interval_set<uint64_t> > clone_subsets;
  data_subset.insert(0, size);

  push_start(soid, peer, size, oi.version, data_subset, clone_subsets);
}

void ReplicatedPG::push_start(const hobject_t& soid, int peer,
			      uint64_t size, eversion_t version,
			      interval_set<uint64_t> &data_subset,
			      map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  // take note.
  push_info_t *pi = &pushing[soid][peer];
  pi->size = size;
  pi->version = version;
  pi->data_subset = data_subset;
  pi->clone_subsets = clone_subsets;

  pi->data_subset_pushing.span_of(pi->data_subset, 0, g_conf->osd_recovery_max_chunk);
  bool complete = pi->data_subset_pushing == pi->data_subset;

  dout(10) << "push_start " << soid << " size " << size << " data " << data_subset
	   << " cloning " << clone_subsets << dendl;    
  send_push_op(soid, version, peer, size, true, complete, pi->data_subset_pushing, pi->clone_subsets);
}


/*
 * push - send object to a peer
 */

int ReplicatedPG::send_push_op(const hobject_t& soid, eversion_t version, int peer, 
			       uint64_t size, bool first, bool complete,
			       interval_set<uint64_t> &data_subset,
			       map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  // read data+attrs
  bufferlist bl;
  map<string,bufferptr> attrset;

  for (interval_set<uint64_t>::iterator p = data_subset.begin();
       p != data_subset.end();
       ++p) {
    bufferlist bit;
    osd->store->read(coll,
		     soid, p.get_start(), p.get_len(), bit);
    if (p.get_len() != bit.length()) {
      dout(10) << " extent " << p.get_start() << "~" << p.get_len()
	       << " is actually " << p.get_start() << "~" << bit.length() << dendl;
      p.set_len(bit.length());
    }
    bl.claim_append(bit);
  }

  osd->store->getattrs(coll, soid, attrset);

  bufferlist bv;
  bv.push_back(attrset[OI_ATTR]);
  object_info_t oi(bv);

  if (oi.version != version) {
    osd->clog.error() << info.pgid << " push " << soid << " v " << version << " to osd." << peer
		      << " failed because local copy is " << oi.version << "\n";
    return -1;
  }

  // ok
  dout(7) << "send_push_op " << soid << " v " << oi.version 
    	  << " size " << size
	  << " subset " << data_subset
          << " data " << bl.length()
          << " to osd." << peer
          << dendl;

  osd->logger->inc(l_osd_push);
  osd->logger->inc(l_osd_push_outb, bl.length());
  
  // send
  osd_reqid_t rid;  // useless?
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, false, 0,
				   osd->osdmap->get_epoch(), osd->get_tid(), oi.version);
  subop->oloc = oi.oloc;
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;
  //subop->ops[0].op.extent.offset = 0;
  //subop->ops[0].op.extent.length = size;
  subop->ops[0].data = bl;
  subop->data_subset = data_subset;
  subop->clone_subsets = clone_subsets;
  subop->attrset.swap(attrset);
  subop->old_size = size;
  subop->first = first;
  subop->complete = complete;
  osd->cluster_messenger->
    send_message(subop, osd->osdmap->get_cluster_inst(peer));
  return 0;
}

void ReplicatedPG::send_push_op_blank(const hobject_t& soid, int peer)
{
  // send a blank push back to the primary
  osd_reqid_t rid;
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, false, 0,
				   osd->osdmap->get_epoch(), osd->get_tid(), eversion_t());
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;
  subop->first = false;
  subop->complete = false;
  osd->cluster_messenger->send_message(subop, osd->osdmap->get_cluster_inst(peer));
}

void ReplicatedPG::sub_op_push_reply(MOSDSubOpReply *reply)
{
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  
  int peer = reply->get_source().num();
  const hobject_t& soid = reply->get_poid();
  
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;
  } else {
    push_info_t *pi = &pushing[soid][peer];

    bool complete = false;
    if (pi->data_subset.empty() ||
	pi->data_subset.range_end() == pi->data_subset_pushing.range_end())
      complete = true;

    if (!complete) {
      // push more
      uint64_t from = pi->data_subset_pushing.range_end();
      pi->data_subset_pushing.span_of(pi->data_subset, from, g_conf->osd_recovery_max_chunk);
      dout(10) << " pushing more, " << pi->data_subset_pushing << " of " << pi->data_subset << dendl;
      complete = pi->data_subset.range_end() == pi->data_subset_pushing.range_end();
      send_push_op(soid, pi->version, peer, pi->size, false, complete,
		   pi->data_subset_pushing, pi->clone_subsets);
    } else {
      // done!
      peer_missing[peer].got(soid, pi->version);
      
      pushing[soid].erase(peer);
      pi = NULL;
      
      update_stats();
      
      if (pushing[soid].empty()) {
	pushing.erase(soid);
	dout(10) << "pushed " << soid << " to all replicas" << dendl;
	finish_recovery_op(soid);
	if (waiting_for_degraded_object.count(soid)) {
	  osd->requeue_ops(this, waiting_for_degraded_object[soid]);
	  waiting_for_degraded_object.erase(soid);
	}
	map<hobject_t, ObjectContext *>::iterator i =
	  object_contexts.find(soid);
	if (i != object_contexts.end()) {
	  populate_obc_watchers(i->second);
	}
      } else {
	dout(10) << "pushed " << soid << ", still waiting for push ack from " 
		 << pushing[soid].size() << " others" << dendl;
      }
    }
  }
  reply->put();
}


/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_pull(MOSDSubOp *op)
{
  const hobject_t soid = op->poid;

  dout(7) << "op_pull " << soid << " v " << op->version
          << " from " << op->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  struct stat st;
  int r = osd->store->stat(coll, soid, &st);
  if (r != 0) {
    osd->clog.error() << info.pgid << " " << op->get_source() << " tried to pull " << soid
		      << " but got " << cpp_strerror(-r) << "\n";
    send_push_op_blank(soid, op->get_source().num());
  } else {
    uint64_t size = st.st_size;

    bool complete = false;
    if (!op->data_subset.empty() && op->data_subset.range_end() >= size)
      complete = true;

    // complete==true implies we are definitely complete.
    // complete==false means nothing.  we don't know because the primary may
    // not be pulling the entire object.

    r = send_push_op(soid, op->version, op->get_source().num(), size, op->first, complete,
		     op->data_subset, op->clone_subsets);
    if (r < 0)
      send_push_op_blank(soid, op->get_source().num());
  }

  log_subop_stats(op, 0, l_osd_sop_pull_lat);

  op->put();
}


void ReplicatedPG::_committed_pushed_object(MOSDSubOp *op, epoch_t same_since, eversion_t last_complete)
{
  lock();
  if (same_since == info.history.same_interval_since) {
    dout(10) << "_committed_pushed_object last_complete " << last_complete << " now ondisk" << dendl;
    last_complete_ondisk = last_complete;

    if (last_complete_ondisk == info.last_update) {
      if (is_replica()) {
	// we are fully up to date.  tell the primary!
	osd->cluster_messenger->
	  send_message(new MOSDPGTrim(osd->osdmap->get_epoch(), info.pgid,
				      last_complete_ondisk),
		       osd->osdmap->get_cluster_inst(get_primary()));

	// adjust local snaps!
	adjust_local_snaps();
      } else if (is_primary()) {
	// we are the primary.  tell replicas to trim?
	if (calc_min_last_complete_ondisk())
	  trim_peers();
      }
    }

  } else {
    dout(10) << "_committed_pushed_object pg has changed, not touching last_complete_ondisk" << dendl;
  }

  log_subop_stats(op, l_osd_sop_push_inb, l_osd_sop_push_lat);

  unlock();
  put();
}

void ReplicatedPG::_applied_pushed_object(ObjectStore::Transaction *t, ObjectContext *obc)
{
  dout(10) << "_applied_pushed_object " << *obc << dendl;
  lock();
  put_object_context(obc);
  unlock();
  delete t;
}

/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedPG::sub_op_push(MOSDSubOp *op)
{
  const hobject_t& soid = op->poid;
  eversion_t v = op->version;
  OSDOp& push = op->ops[0];

  dout(7) << "op_push " 
          << soid 
          << " v " << v 
	  << " " << op->oloc
	  << " len " << push.op.extent.length
	  << " data_subset " << op->data_subset
	  << " clone_subsets " << op->clone_subsets
	  << " data len " << op->get_data().length()
          << dendl;

  if (v == eversion_t()) {
    // replica doesn't have it!
    _failed_push(op);
    return;
  }

  interval_set<uint64_t> data_subset;
  map<hobject_t, interval_set<uint64_t> > clone_subsets;

  bufferlist data;
  op->claim_data(data);

  // we need these later, and they get clobbered by t.setattrs()
  bufferlist oibl;
  if (op->attrset.count(OI_ATTR))
    oibl.push_back(op->attrset[OI_ATTR]);
  bufferlist ssbl;
  if (op->attrset.count(SS_ATTR))
    ssbl.push_back(op->attrset[SS_ATTR]);

  // determine data/clone subsets
  data_subset = op->data_subset;
  if (data_subset.empty() && push.op.extent.length && push.op.extent.length == data.length())
    data_subset.insert(0, push.op.extent.length);
  clone_subsets = op->clone_subsets;

  pull_info_t *pi = 0;
  bool first = op->first;
  bool complete = op->complete;

  // op->complete == true means we reached the end of the object (file size)
  // op->complete == false means nothing; we may not have asked for the whole thing.

  if (is_primary()) {
    if (pulling.count(soid) == 0) {
      dout(10) << " not pulling, ignoring" << dendl;
      op->put();
      return;
    }
    pi = &pulling[soid];
    
    // did we learn object size?
    if (pi->need_size) {
      dout(10) << " learned object size is " << op->old_size << dendl;
      pi->data_subset.erase(op->old_size, (uint64_t)-1 - op->old_size);
      pi->need_size = false;
    }

    if (soid.snap && soid.snap < CEPH_NOSNAP) {
      // clone.  make sure we have enough data.
      SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
      assert(ssc);

      clone_subsets.clear();   // forget what pusher said; recalculate cloning.

      interval_set<uint64_t> data_needed;
      calc_clone_subsets(ssc->snapset, soid, missing, data_needed, clone_subsets);
      pi->data_subset = data_needed;
      put_snapset_context(ssc);

      interval_set<uint64_t> overlap;
      overlap.intersection_of(data_subset, data_needed);
      
      dout(10) << "sub_op_push need " << data_needed << ", got " << data_subset
	       << ", overlap " << overlap << dendl;

      // did we get more data than we need?
      if (!data_subset.subset_of(data_needed)) {
	interval_set<uint64_t> extra = data_subset;
	interval_set<uint64_t> usable;
	usable.intersection_of(extra, data_needed);
	extra.subtract(usable);
	dout(10) << " we got some extra: " << extra << dendl;

	bufferlist result;
	int off = 0;
	for (interval_set<uint64_t>::const_iterator p = usable.begin();
	     p != usable.end();
	     ++p) {
	  interval_set<uint64_t> x;
	  x.insert(p.get_start(), p.get_len());
	  x.intersection_of(data_needed);
	  dout(20) << " data_subset object extent " << p.get_start() << "~" << p.get_len() << " need " << x << dendl;
	  if (!x.empty()) {
	    uint64_t first = x.begin().get_start();
	    uint64_t len = x.begin().get_len();
	    bufferlist sub;
	    int boff = off + (first - p.get_start());
	    dout(20) << "   keeping buffer extent " << boff << "~" << len << dendl;
	    sub.substr_of(data, boff, len);
	    result.claim_append(sub);
	  }
	  off += p.get_len();
	}
	data.claim(result);
	data_subset.intersection_of(data_needed);
	dout(20) << " new data len is " << data.length() << dendl;
      }

      // did we get everything we wanted?
      if (pi->data_subset.empty()) {
	complete = true;
      } else {
	complete = pi->data_subset.range_end() == data_subset.range_end();
      }

      if (op->complete && !complete) {
	dout(0) << " uh oh, we reached EOF on peer before we got everything we wanted" << dendl;
	_failed_push(op);
	return;
      }

    } else {
      // head|unversioned. for now, primary will _only_ pull data copies of the head (no cloning)
      assert(op->clone_subsets.empty());
    }
  }
  dout(15) << " data_subset " << data_subset
	   << " clone_subsets " << clone_subsets
	   << " first=" << first << " complete=" << complete
	   << dendl;

  coll_t target;
  if (first && complete)
    target = coll;
  else
    target = coll_t::TEMP_COLL;

  // write object and add it to the PG
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  Context *onreadable = 0;
  Context *onreadable_sync = 0;

  if (first)
    t->remove(target, soid);  // in case old version exists

  // write data
  uint64_t boff = 0;
  for (interval_set<uint64_t>::const_iterator p = data_subset.begin();
       p != data_subset.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data, boff, p.get_len());
    dout(15) << " write " << p.get_start() << "~" << p.get_len() << dendl;
    t->write(target, soid, p.get_start(), p.get_len(), bit);
    boff += p.get_len();
  }
  
  if (complete) {
    if (!first) {
      t->remove(coll, soid);
      t->collection_add(coll, target, soid);
      t->collection_remove(target, soid);
    }

    // clone bits
    for (map<hobject_t, interval_set<uint64_t> >::const_iterator p = clone_subsets.begin();
	 p != clone_subsets.end();
	 ++p)
    {
      for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	   q != p->second.end();
	   ++q)
      {
	dout(15) << " clone_range " << p->first << " "
		 << q.get_start() << "~" << q.get_len() << dendl;
	t->clone_range(coll, p->first, soid,
		       q.get_start(), q.get_len(), q.get_start());
      }
    }

    if (data_subset.empty())
      t->touch(coll, soid);

    t->setattrs(coll, soid, op->attrset);
    if (soid.snap && soid.snap < CEPH_NOSNAP &&
	op->attrset.count(OI_ATTR)) {
      bufferlist bl;
      bl.push_back(op->attrset[OI_ATTR]);
      object_info_t oi(bl);
      if (oi.snaps.size()) {
	coll_t lc = make_snap_collection(*t, oi.snaps[0]);
	t->collection_add(lc, coll, soid);
	if (oi.snaps.size() > 1) {
	  coll_t hc = make_snap_collection(*t, oi.snaps[oi.snaps.size()-1]);
	  t->collection_add(hc, coll, soid);
	}
      }
    }

    if (missing.is_missing(soid, v)) {
      dout(10) << "got missing " << soid << " v " << v << dendl;
      missing.got(soid, v);
      if (is_primary())
	missing_loc.erase(soid);
      
      // raise last_complete?
      while (log.complete_to != log.log.end()) {
	if (missing.missing.count(log.complete_to->soid))
	  break;
	if (info.last_complete < log.complete_to->version)
	  info.last_complete = log.complete_to->version;
	log.complete_to++;
      }
      dout(10) << "last_complete now " << info.last_complete << dendl;
      if (log.complete_to != log.log.end())
	dout(10) << " log.complete_to = " << log.complete_to->version << dendl;
    }

    // update pg
    write_info(*t);


    // track ObjectContext
    if (is_primary()) {
      dout(10) << " setting up obc for " << soid << dendl;
      ObjectContext *obc = get_object_context(soid, op->oloc, true);
      assert(obc->registered);
      obc->ondisk_write_lock();
      
      obc->obs.exists = true;
      obc->obs.oi.decode(oibl);
      
      // suck in snapset context?
      SnapSetContext *ssc = obc->ssc;
      if (ssbl.length()) {
	bufferlist::iterator sp = ssbl.begin();
	ssc->snapset.decode(sp);
      }

      onreadable = new C_OSD_AppliedPushedObject(this, t, obc);
      onreadable_sync = new C_OSD_OndiskWriteUnlock(obc);
    } else {
      onreadable = new ObjectStore::C_DeleteTransaction(t);
    }

  } else {
    onreadable = new ObjectStore::C_DeleteTransaction(t);
  }

  // apply to disk!
  int r = osd->store->queue_transaction(&osr, t,
					onreadable,
					new C_OSD_CommittedPushedObject(this, op,
									info.history.same_interval_since,
									info.last_complete),
					onreadable_sync);
  assert(r == 0);

  if (is_primary()) {
    assert(pi);

    if (complete) {
      // close out pull op
      pulling.erase(soid);
      pull_from_peer[pi->from].erase(soid);
      finish_recovery_op(soid);
      
      update_stats();
    } else {
      // pull more
      pi->data_subset_pulling.span_of(pi->data_subset, data_subset.range_end(), g_conf->osd_recovery_max_chunk);
      dout(10) << " pulling more, " << pi->data_subset_pulling << " of " << pi->data_subset << dendl;
      send_pull_op(soid, v, false, pi->data_subset_pulling, pi->from);
    }


    /*
    if (is_active()) {
      // are others missing this too?  (only if we're active.. skip
      // this part if we're still repeering, it'll just confuse us)
      for (unsigned i=1; i<acting.size(); i++) {
	int peer = acting[i];
	assert(peer_missing.count(peer));
	if (peer_missing[peer].is_missing(soid)) {
	  push_to_replica(soid, peer);  // ok, push it, and they (will) have it now.
	  start_recovery_op(soid);
	}
      }
    }
    */

  } else {
    // ack if i'm a replica and being pushed to.
    MOSDSubOpReply *reply = new MOSDSubOpReply(op, 0, osd->osdmap->get_epoch(), CEPH_OSD_FLAG_ACK);
    assert(entity_name_t::TYPE_OSD == op->get_connection()->peer_type);
    osd->cluster_messenger->send_message(reply, op->get_connection());
  }

  if (complete) {
    // kick waiters
    if (waiting_for_missing_object.count(soid)) {
      dout(20) << " kicking waiters on " << soid << dendl;
      osd->requeue_ops(this, waiting_for_missing_object[soid]);
      waiting_for_missing_object.erase(soid);
    } else {
      dout(20) << " no waiters on " << soid << dendl;
      /*for (hash_map<hobject_t,list<class Message*> >::iterator p = waiting_for_missing_object.begin();
	 p != waiting_for_missing_object.end();
	 p++)
      dout(20) << "   " << p->first << dendl;
    */
    }
  }
    
  op->put();  // at the end... soid is a ref to op->soid!
}

void ReplicatedPG::_failed_push(MOSDSubOp *op)
{
  const hobject_t& soid = op->poid;
  int from = op->get_source().num();
  map<hobject_t,set<int> >::iterator p = missing_loc.find(soid);
  if (p != missing_loc.end()) {
    dout(0) << "_failed_push " << soid << " from osd." << from
	    << ", reps on " << p->second << dendl;

    p->second.erase(from);          // forget about this (bad) peer replica
    if (p->second.empty())
      missing_loc.erase(p);
  } else {
    dout(0) << "_failed_push " << soid << " from osd." << from
	    << " but not in missing_loc ???" << dendl;
  }

  finish_recovery_op(soid);  // close out this attempt,
  pull_from_peer[from].erase(soid);
  pulling.erase(soid);

  op->put();
}


/*
 * pg status change notification
 */

void ReplicatedPG::on_osd_failure(int o)
{
  //dout(10) << "on_osd_failure " << o << dendl;
}

void ReplicatedPG::apply_and_flush_repops(bool requeue)
{
  list<Message*> rq;

  // apply all repops
  while (!repop_queue.empty()) {
    RepGather *repop = repop_queue.front();
    repop_queue.pop_front();
    dout(10) << " applying repop tid " << repop->rep_tid << dendl;
    if (!repop->applied && !repop->applying)
      apply_repop(repop);
    repop->aborted = true;

    if (requeue && repop->ctx->op) {
      dout(10) << " requeuing " << *repop->ctx->op << dendl;
      rq.push_back(repop->ctx->op);
      repop->ctx->op = 0;
    }

    remove_repop(repop);
  }

  if (requeue)
    osd->push_waiters(rq);
}

void ReplicatedPG::on_shutdown()
{
  dout(10) << "on_shutdown" << dendl;
  apply_and_flush_repops(false);
  remove_watchers_and_notifies();
}

void ReplicatedPG::on_activate()
{
  for (map<hobject_t, ObjectContext *>::iterator i = object_contexts.begin();
       i != object_contexts.end();
       ++i) {
    populate_obc_watchers(i->second);
  }
}

void ReplicatedPG::on_change()
{
  dout(10) << "on_change" << dendl;
  apply_and_flush_repops(is_primary());

  // clear reserved scrub state
  clear_scrub_reserved();

  // clear scrub state
  if (finalizing_scrub) {
    scrub_clear_state();
  } else if (is_scrubbing()) {
    state_clear(PG_STATE_SCRUBBING);
    state_clear(PG_STATE_REPAIR);
  }

  context_registry_on_change();

  // take object waiters
  take_object_waiters(waiting_for_missing_object);
  take_object_waiters(waiting_for_degraded_object);

  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
  pull_from_peer.clear();

  // clear snap_trimmer state
  snap_trimmer_machine.process_event(Reset());
}

void ReplicatedPG::on_role_change()
{
  dout(10) << "on_role_change" << dendl;

  // take commit waiters
  for (map<eversion_t, list<Message*> >::iterator p = waiting_for_ondisk.begin();
       p != waiting_for_ondisk.end();
       p++)
    osd->take_waiters(p->second);
  waiting_for_ondisk.clear();
}



// clear state.  called on recovery completion AND cancellation.
void ReplicatedPG::_clear_recovery_state()
{
  missing_loc.clear();
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
  pulling.clear();
  pushing.clear();
  pull_from_peer.clear();
}

void ReplicatedPG::check_recovery_op_pulls(const OSDMap *osdmap)
{
  for (map<int, set<hobject_t> >::iterator j = pull_from_peer.begin();
       j != pull_from_peer.end();
       ) {
    if (osdmap->is_up(j->first)) {
      ++j;
      continue;
    }
    dout(10) << "Reseting pulls from osd." << j->first
	     << ", osdmap has it marked down" << dendl;
    
    for (set<hobject_t>::iterator i = j->second.begin();
	 i != j->second.end();
	 ++i) {
      assert(pulling.count(*i) == 1);
      pulling.erase(*i);
      finish_recovery_op(*i);
    }
    log.last_requested = 0;
    pull_from_peer.erase(j++);
  }
}
  

int ReplicatedPG::start_recovery_ops(int max)
{
  int started = 0;
  assert(is_primary());

  int num_missing = missing.num_missing();
  int num_unfound = get_num_unfound();

  if (num_missing == 0) {
    info.last_complete = info.last_update;
  }

  if (num_missing == num_unfound) {
    // All of the missing objects we have are unfound.
    // Recover the replicas.
    started = recover_replicas(max);
  }
  if (!started) {
    // We still have missing objects that we should grab from replicas.
    started += recover_primary(max);
  }
  dout(10) << " started " << started << dendl;

  osd->logger->inc(l_osd_rop, started);

  if (started)
    return started;

  if (is_all_uptodate()) {
    dout(10) << __func__ << ": all OSDs in the PG are up-to-date!" << dendl;
    log.reset_recovery_pointers();
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    C_Contexts *fin = new C_Contexts(g_ceph_context);
    finish_recovery(*t, fin->contexts);
    int tr = osd->store->queue_transaction(&osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
    assert(tr == 0);
  }
  else {
    dout(10) << __func__ << ": some OSDs are not up-to-date yet. "
             << "Recovery isn't finished yet." << dendl;
  }

  return 0;
}

/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
int ReplicatedPG::recover_primary(int max)
{
  assert(is_primary());

  dout(10) << "recover_primary pulling " << pulling.size() << " in pg" << dendl;
  dout(10) << "recover_primary " << missing << dendl;
  dout(25) << "recover_primary " << missing.missing << dendl;

  // look at log!
  Log::Entry *latest = 0;
  int started = 0;
  int skipped = 0;

  map<version_t, hobject_t>::iterator p = missing.rmissing.lower_bound(log.last_requested);
  while (p != missing.rmissing.end()) {
    hobject_t soid;
    version_t v = p->first;

    if (log.objects.count(p->second)) {
      latest = log.objects[p->second];
      assert(latest->is_update());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    Missing::item& item = missing.missing[p->second];
    p++;

    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    bool unfound = (missing_loc.find(soid) == missing_loc.end());

    dout(10) << "recover_primary "
             << soid << " " << item.need
	     << (unfound ? " (unfound)":"")
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (pulling.count(soid) ? " (pulling)":"")
	     << (pulling.count(head) ? " (pulling head)":"")
             << dendl;
    
    if (!pulling.count(soid)) {
      if (pulling.count(head)) {
	++skipped;
      } else if (unfound) {
	++skipped;
      } else {
	// is this a clone operation that we can do locally?
	if (latest && latest->op == Log::Entry::CLONE) {
	  if (missing.is_missing(head) &&
	      missing.have_old(head) == latest->prior_version) {
	    dout(10) << "recover_primary cloning " << head << " v" << latest->prior_version
		     << " to " << soid << " v" << latest->version
		     << " snaps " << latest->snaps << dendl;
	    ObjectStore::Transaction *t = new ObjectStore::Transaction;

	    // NOTE: we know headobc exists on disk, and the oloc will be loaded with it, so
	    // it is safe to pass in a blank one here.
	    ObjectContext *headobc = get_object_context(head, OLOC_BLANK, false);

	    object_info_t oi(headobc->obs.oi);
	    oi.version = latest->version;
	    oi.prior_version = latest->prior_version;
	    bufferlist::iterator i = latest->snaps.begin();
	    ::decode(oi.snaps, i);
	    assert(oi.snaps.size() > 0);
	    oi.copy_user_bits(headobc->obs.oi);
	    _make_clone(*t, head, soid, &oi);

	    put_object_context(headobc);

	    // XXX: track objectcontext!
	    int tr = osd->store->queue_transaction(&osr, t);
	    assert(tr == 0);
	    missing.got(latest->soid, latest->version);
	    missing_loc.erase(latest->soid);
	    continue;
	  }
	}
	
	int r = pull(soid);
	switch (r) {
	case PULL_YES:
	  ++started;
	  break;
	case PULL_OTHER:
	  ++started;
	case PULL_NONE:
	  ++skipped;
	  break;
	default:
	  assert(0);
	}
	if (started >= max)
	  return started;
      }
    }
    
    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      log.last_requested = v;
  }

  return started;
}

int ReplicatedPG::recover_object_replicas(const hobject_t& soid, eversion_t v)
{
  dout(10) << "recover_object_replicas " << soid << dendl;

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContext *obc = get_object_context(soid, OLOC_BLANK, false);
  if (!obc) {
    missing.add(soid, v, eversion_t());
    bool uhoh = true;
    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      if (!peer_missing[peer].is_missing(soid, v)) {
	missing_loc[soid].insert(peer);
	dout(10) << info.pgid << " unexpectedly missing " << soid << " v" << v
		 << ", there should be a copy on osd." << peer << dendl;
	uhoh = false;
      }
    }
    if (uhoh)
      osd->clog.error() << info.pgid << " missing primary copy of " << soid << ", unfound\n";
    else
      osd->clog.error() << info.pgid << " missing primary copy of " << soid
			<< ", will try copies on " << missing_loc[soid] << "\n";
    return 0;
  }

  dout(10) << " ondisk_read_lock for " << soid << dendl;
  obc->ondisk_read_lock();
  
  start_recovery_op(soid);

  // who needs it?  
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
	peer_missing[peer].is_missing(soid)) {
      push_to_replica(obc, soid, peer);
    }
  }
  
  dout(10) << " ondisk_read_unlock on " << soid << dendl;
  obc->ondisk_read_unlock();
  put_object_context(obc);

  return 1;
}

int ReplicatedPG::recover_replicas(int max)
{
  dout(10) << __func__ << "(" << max << ")" << dendl;
  int started = 0;

  // this is FAR from an optimal recovery order.  pretty lame, really.
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    map<int, Missing>::const_iterator pm = peer_missing.find(peer);
    assert(pm != peer_missing.end());
    size_t m_sz = pm->second.num_missing();

    dout(10) << " peer osd." << peer << " missing " << m_sz << " objects." << dendl;
    dout(20) << " peer osd." << peer << " missing " << pm->second.missing << dendl;

    // oldest first!
    const Missing &m(pm->second);
    for (map<version_t, hobject_t>::const_iterator p = m.rmissing.begin();
	   p != m.rmissing.end() && started < max;
	   ++p) {
      const hobject_t soid(p->second);

      if (pushing.count(soid)) {
	dout(10) << __func__ << ": already pushing " << soid << dendl;
	continue;
      }

      if (missing.is_missing(soid)) {
	if (missing_loc.find(soid) == missing_loc.end())
	  dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	else
	  dout(10) << __func__ << ": " << soid << " still missing on primary" << dendl;
	continue;
      }

      dout(10) << __func__ << ": recover_object_replicas(" << soid << ")" << dendl;
      map<hobject_t,Missing::item>::const_iterator p = m.missing.find(soid);
      started += recover_object_replicas(soid, p->second.need);
    }
  }

  return started;
}

void ReplicatedPG::remove_object_with_snap_hardlinks(ObjectStore::Transaction& t, const hobject_t& soid)
{
  t.remove(coll, soid);
  if (soid.snap < CEPH_MAXSNAP) {
    bufferlist ba;
    int r = osd->store->getattr(coll, soid, OI_ATTR, ba);
    if (r >= 0) {
      // grr, need first snap bound, too.
      object_info_t oi(ba);
      if (oi.snaps[0] != soid.snap)
	t.remove(coll_t(info.pgid, oi.snaps[0]), soid);
      t.remove(coll_t(info.pgid, soid.snap), soid);
    }
  }
}

/** clean_up_local
 * remove any objects that we're storing but shouldn't.
 * as determined by log.
 */
void ReplicatedPG::clean_up_local(ObjectStore::Transaction& t)
{
  dout(10) << "clean_up_local" << dendl;

  assert(info.last_update >= log.tail);  // otherwise we need some help!

  if (log.backlog) {

    // be thorough.
    vector<hobject_t> ls;
    osd->store->collection_list(coll, ls);

    set<hobject_t> s;   
    for (vector<hobject_t>::iterator i = ls.begin();
         i != ls.end();
         i++)
      if (i->snap == CEPH_NOSNAP)
	s.insert(*i);

    dout(10) << " " << s.size() << " local objects" << dendl;

    set<hobject_t> did;
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (did.count(p->soid)) continue;
      did.insert(p->soid);
      
      if (p->is_delete()) {
        if (s.count(p->soid)) {
          dout(10) << " deleting " << p->soid
                   << " when " << p->version << dendl;
	  remove_object_with_snap_hardlinks(t, p->soid);
        }
        s.erase(p->soid);
      } else {
        // just leave old objects.. they're missing or whatever
        s.erase(p->soid);
      }
    }

    for (set<hobject_t>::iterator i = s.begin(); 
         i != s.end();
         i++) {
      dout(10) << " deleting stray " << *i << dendl;
      remove_object_with_snap_hardlinks(t, *i);
    }

  } else {
    // just scan the log.
    set<hobject_t> did;
    for (list<Log::Entry>::reverse_iterator p = log.log.rbegin();
         p != log.log.rend();
         p++) {
      if (did.count(p->soid))
	continue;
      did.insert(p->soid);

      if (p->is_delete()) {
        dout(10) << " deleting " << p->soid
                 << " when " << p->version << dendl;
	remove_object_with_snap_hardlinks(t, p->soid);
      } else {
        // keep old(+missing) objects, just for kicks.
      }
    }
  }
}




// ==========================================================================================
// SCRUB


int ReplicatedPG::_scrub(ScrubMap& scrubmap, int& errors, int& fixed)
{
  dout(10) << "_scrub" << dendl;

  coll_t c(info.pgid);
  bool repair = state_test(PG_STATE_REPAIR);
  const char *mode = repair ? "repair":"scrub";

  // traverse in reverse order.
  hobject_t head;
  SnapSet snapset;
  vector<snapid_t>::reverse_iterator curclone;

  object_stat_collection_t cstat;

  bufferlist last_data;

  for (map<hobject_t,ScrubMap::object>::reverse_iterator p = scrubmap.objects.rbegin(); 
       p != scrubmap.objects.rend(); 
       p++) {
    const hobject_t& soid = p->first;
    object_stat_sum_t stat;
    stat.num_objects++;

    // new snapset?
    if (soid.snap == CEPH_SNAPDIR ||
	soid.snap == CEPH_NOSNAP) {
      if (p->second.attrs.count(SS_ATTR) == 0) {
	dout(0) << mode << " no '" << SS_ATTR << "' attr on " << soid << dendl;
	errors++;
	continue;
      }
      bufferlist bl;
      bl.push_back(p->second.attrs[SS_ATTR]);
      bufferlist::iterator blp = bl.begin();
      ::decode(snapset, blp);

      // did we finish the last oid?
      if (head != hobject_t()) {
	osd->clog.error() << "Missing clone(s) for " << head << "\n";
	errors++;
      }
      
      // what will be next?
      if (snapset.clones.empty())
	head = hobject_t();  // no clones.
      else {
	curclone = snapset.clones.rbegin();
	head = p->first;
      }

      // subtract off any clone overlap
      for (map<snapid_t,interval_set<uint64_t> >::iterator q = snapset.clone_overlap.begin();
	   q != snapset.clone_overlap.end();
	   ++q) {
	for (interval_set<uint64_t>::const_iterator r = q->second.begin();
	     r != q->second.end();
	     ++r) {
	  stat.num_bytes -= r.get_len();
	  stat.num_kb -= SHIFT_ROUND_UP(r.get_start()+r.get_len(), 10) - (r.get_start() >> 10);
	}	  
      }
    }
    if (soid.snap == CEPH_SNAPDIR)
      continue;

    // basic checks.
    if (p->second.attrs.count(OI_ATTR) == 0) {
      dout(0) << mode << " no '" << OI_ATTR << "' attr on " << soid << dendl;
      errors++;
      continue;
    }
    bufferlist bv;
    bv.push_back(p->second.attrs[OI_ATTR]);
    object_info_t oi(bv);

    if (oi.size != p->second.size) {
      derr << "on disk size (" << p->second.size
	   << ") does not match object info size (" << oi.size
	   << ") for " << soid << dendl;
      ++errors;
    }

    dout(20) << mode << "  " << soid << " " << oi << dendl;

    stat.num_bytes += p->second.size;
    stat.num_kb += SHIFT_ROUND_UP(p->second.size, 10);

    //bufferlist data;
    //osd->store->read(c, poid, 0, 0, data);
    //assert(data.length() == p->size);

    if (soid.snap == CEPH_NOSNAP) {
      if (!snapset.head_exists) {
	dout(0) << mode << "  snapset.head_exists=false, but " << soid << " exists" << dendl;
	errors++;
	continue;
      }
    } else if (soid.snap) {
      // it's a clone
      assert(head != hobject_t());

      stat.num_object_clones++;
      
      assert(soid.snap == *curclone);

      assert(p->second.size == snapset.clone_size[*curclone]);

      // verify overlap?
      // ...

      // what's next?
      if (curclone != snapset.clones.rend())
	curclone++;

      if (curclone == snapset.clones.rend())
	head = hobject_t();

    } else {
      // it's unversioned.
    }

    string cat; // fixme
    cstat.add(stat, cat);
  }  
  
  dout(10) << mode << " got "
	   << cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
	   << cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
	   << cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes, "
	   << cstat.sum.num_kb << "/" << info.stats.stats.sum.num_kb << " kb."
	   << dendl;

  if (cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      cstat.sum.num_bytes != info.stats.stats.sum.num_bytes ||
      cstat.sum.num_kb != info.stats.stats.sum.num_kb) {
    osd->clog.error() << info.pgid << " " << mode
       << " stat mismatch, got "
       << cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
       << cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
       << cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes, "
       << cstat.sum.num_kb << "/" << info.stats.stats.sum.num_kb << " kb.\n";
    errors++;

    if (repair) {
      fixed++;
      info.stats.stats = cstat;
      update_stats();

      // tell replicas
      for (unsigned i=1; i<acting.size(); i++) {
	MOSDPGInfo *m = new MOSDPGInfo(osd->osdmap->get_epoch());
	m->pg_info.push_back(info);
	osd->cluster_messenger->send_message(m, osd->osdmap->get_cluster_inst(acting[i]));
      }
    }
  }

  dout(10) << "_scrub (" << mode << ") finish" << dendl;
  return errors;
}

/*---SnapTrimmer Logging---*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

void ReplicatedPG::SnapTrimmer::log_enter(const char *state_name)
{
  dout(20) << "enter " << state_name << dendl;
}

void ReplicatedPG::SnapTrimmer::log_exit(const char *state_name, utime_t enter_time)
{
  dout(20) << "exit " << state_name << dendl;
}

/*---SnapTrimmer states---*/
#undef dout_prefix
#define dout_prefix (*_dout << context< SnapTrimmer >().pg->gen_prefix() \
		     << "SnapTrimmer state<" << get_state_name() << ">: ")

/* NotTrimming */
ReplicatedPG::NotTrimming::NotTrimming(my_context ctx) : my_base(ctx)
{
  state_name = "NotTrimming";
  context< SnapTrimmer >().requeue = false;
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::NotTrimming::exit()
{
  context< SnapTrimmer >().requeue = true;
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result ReplicatedPG::NotTrimming::react(const SnapTrim&)
{
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  dout(10) << "NotTrimming react" << dendl;

  // Replica trimming
  if (pg->is_replica() && pg->is_active() &&
      pg->last_complete_ondisk.epoch >= pg->info.history.last_epoch_started) {
    return transit<RepColTrim>();
  } else if (!pg->is_primary() || !pg->is_active() || !pg->is_clean()) {
    dout(10) << "NotTrimming not primary, active, clean" << dendl;
    return discard_event();
  } else if (pg->finalizing_scrub) {
    dout(10) << "NotTrimming finalizing scrub" << dendl;
    pg->queue_snap_trim();
    return discard_event();
  }

  // Primary trimming
  vector<hobject_t> &obs_to_trim = context<SnapTrimmer>().obs_to_trim;
  snapid_t &snap_to_trim = context<SnapTrimmer>().snap_to_trim;
  coll_t &col_to_trim = context<SnapTrimmer>().col_to_trim;
  if (!pg->get_obs_to_trim(snap_to_trim,
			   col_to_trim,
			   obs_to_trim)) {
    // Nothing to trim
    dout(10) << "NotTrimming: nothing to trim" << dendl;
    return discard_event();
  }

  if (obs_to_trim.empty()) {
    // Nothing to actually trim, just update info and try again
    pg->info.purged_snaps.insert(snap_to_trim);
    pg->snap_trimq.erase(snap_to_trim);
    dout(10) << "NotTrimming: obs_to_trim empty!" << dendl;
    dout(10) << "purged_snaps now " << pg->info.purged_snaps << ", snap_trimq now " 
	     << pg->snap_trimq << dendl;
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->remove_collection(col_to_trim);
    int r = pg->osd->store->queue_transaction(NULL, t, new ObjectStore::C_DeleteTransaction(t));
    assert(r == 0);
    post_event(SnapTrim());
    return discard_event();
  } else {
    // Actually have things to trim!
    dout(10) << "NotTrimming: something to trim!" << dendl;
    return transit< TrimmingObjects >();
  }
}

/* RepColTrim */
ReplicatedPG::RepColTrim::RepColTrim(my_context ctx) : my_base(ctx)
{
  state_name = "RepColTrim";
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::RepColTrim::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result ReplicatedPG::RepColTrim::react(const SnapTrim&)
{
  dout(10) << "RepColTrim react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;

  if (to_trim.empty()) {
    to_trim.intersection_of(pg->info.purged_snaps, pg->snap_collections);
    if (to_trim.empty()) {
      return transit<NotTrimming>();
    }
  }

  snapid_t snap_to_trim(to_trim.range_start());
  coll_t col_to_trim(pg->info.pgid, snap_to_trim);
  to_trim.erase(snap_to_trim);
  
  // flush all operations to fs so we can rely on collection_list
  // below.
  pg->osr.flush();

  vector<hobject_t> obs_to_trim;
  pg->osd->store->collection_list(col_to_trim, obs_to_trim);
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<hobject_t>::iterator i = obs_to_trim.begin();
       i != obs_to_trim.end();
       ++i) {
    t->collection_remove(col_to_trim, *i);
  }
  t->remove_collection(col_to_trim);
  int r = pg->osd->store->queue_transaction(NULL, t, new ObjectStore::C_DeleteTransaction(t));
  assert(r == 0);
  pg->snap_collections.erase(snap_to_trim);
  return discard_event();
}

/* TrimmingObjects */
ReplicatedPG::TrimmingObjects::TrimmingObjects(my_context ctx) : my_base(ctx)
{
  state_name = "Trimming/TrimmingObjects";
  position = context<SnapTrimmer>().obs_to_trim.begin();
  assert(position != context<SnapTrimmer>().obs_to_trim.end()); // Would not have transitioned if coll empty
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::TrimmingObjects::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result ReplicatedPG::TrimmingObjects::react(const SnapTrim&)
{
  dout(10) << "TrimmingObjects react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  vector<hobject_t> &obs_to_trim = context<SnapTrimmer>().obs_to_trim;
  snapid_t &snap_to_trim = context<SnapTrimmer>().snap_to_trim;
  set<RepGather *> &repops = context<SnapTrimmer>().repops;

  // Done, 
  if (position == obs_to_trim.end()) {
    post_event(SnapTrim());
    return transit< WaitingOnReplicas >();
  }

  dout(10) << "TrimmingObjects react trimming " << *position << dendl;
  RepGather *repop = pg->trim_object(*position, snap_to_trim);

  if (repop) {
    repop->queue_snap_trimmer = true;
    eversion_t old_last_update = pg->log.head;
    bool old_exists = repop->obc->obs.exists;
    uint64_t old_size = repop->obc->obs.oi.size;
    eversion_t old_version = repop->obc->obs.oi.version;
    
    pg->append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);
    pg->issue_repop(repop, repop->ctx->mtime, old_last_update, old_exists, old_size, old_version);
    pg->eval_repop(repop);
    
    repops.insert(repop);
    ++position;
  } else {
    // object has already been trimmed, this is an extra
    coll_t col_to_trim(pg->info.pgid, snap_to_trim);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->collection_remove(col_to_trim, *position);
    int r = pg->osd->store->queue_transaction(NULL, t, new ObjectStore::C_DeleteTransaction(t));
    assert(r == 0);
  }
  return discard_event();
}
/* WaitingOnReplicasObjects */
ReplicatedPG::WaitingOnReplicas::WaitingOnReplicas(my_context ctx) : my_base(ctx)
{
  state_name = "Trimming/WaitingOnReplicas";
  context< SnapTrimmer >().log_enter(state_name);
  context< SnapTrimmer >().requeue = false;
}

void ReplicatedPG::WaitingOnReplicas::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);

  // Clean up repops in case of reset
  set<RepGather *> &repops = context<SnapTrimmer>().repops;
  for (set<RepGather *>::iterator i = repops.begin();
       i != repops.end();
       repops.erase(i++)) {
    (*i)->put();
  }
}

boost::statechart::result ReplicatedPG::WaitingOnReplicas::react(const SnapTrim&)
{
  // Have all the repops applied?
  dout(10) << "Waiting on Replicas react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  set<RepGather *> &repops = context<SnapTrimmer>().repops;
  for (set<RepGather *>::iterator i = repops.begin();
       i != repops.end();
       repops.erase(i++)) {
    if (!(*i)->applied || !(*i)->waitfor_ack.empty()) {
      return discard_event();
    } else {
      (*i)->put();
    }
  }

  // All applied, now to update and share info
  snapid_t &sn = context<SnapTrimmer>().snap_to_trim;
  coll_t &c = context<SnapTrimmer>().col_to_trim;

  pg->info.purged_snaps.insert(sn);
  pg->snap_trimq.erase(sn);
  dout(10) << "purged_snaps now " << pg->info.purged_snaps << ", snap_trimq now " 
	   << pg->snap_trimq << dendl;
  
  // remove snap collection
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  dout(10) << "removing snap " << sn << " collection " << c << dendl;
  pg->snap_collections.erase(sn);
  pg->write_info(*t);
  t->remove_collection(c);
  int tr = pg->osd->store->queue_transaction(&pg->osr, t);
  assert(tr == 0);

  context<SnapTrimmer>().need_share_pg_info = true;

  // Back to the start
  post_event(SnapTrim());
  return transit< NotTrimming >();
}

