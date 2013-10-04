// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "OSDVol.h"

void OSDVol::do_request(OpRequestRef op)
{
  // do any pending flush
  do_pending_flush();

  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }
  if (can_discard_request(op)) {
    return;
  }
  if (!flushed) {
    waiting_for_active.push_back(op);
    return;
  }

  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (is_replay() || !is_active()) {
      waiting_for_active.push_back(op);
      return;
    }
    do_op(op); // do it now
    break;

  case MSG_OSD_SUBOP:
    do_sub_op(op);
    break;

  case MSG_OSD_SUBOPREPLY:
    do_sub_op_reply(op);
    break;

  case MSG_OSD_PG_SCAN:
    do_scan(op);
    break;

  case MSG_OSD_PG_BACKFILL:
    do_backfill(op);
    break;

  default:
    /* Bogus */
    break;
  }
}

void OSDVol::do_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);

  // asking for SNAPDIR is only ok for reads
  if (m->get_snapid() == CEPH_SNAPDIR && op->may_write()) {
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  entity_inst_t client = m->get_source_inst();

  ObjectContext *obc;
  bool can_create = op->may_write();
  snapid_t snapid;
  int r = find_object_context(hobject_t(m->get_oid(),
					m->get_snapid());
			      &obc, can_create, &snapid);
  if (r) {
    osd->reply_op_error(op, r);
    return;
  }

  if ((op->may_read()) && (obc->obs.oi.lost)) {
    // This object is lost. Reading from it returns an error.
	     << " is lost" << dendl;
    osd->reply_op_error(op, -ENFILE);
    return;
  }

  bool ok;
  if ((op->may_read() && op->may_write()) ||
      (m->get_flags() & CEPH_OSD_FLAG_RWORDERED)) {
    ok = mode.try_rmw(client);
  } else if (op->may_write()) {
    ok = mode.try_write(client);
  } else if (op->may_read()) {
    ok = mode.try_read(client);
  } else {
    osd->reply_op_error(op, -ENFILE);
    return;
  }

  if (!ok) {
    mode.waiting.push_back(op);
    return;
  }

  if (!op->may_write() && !obc->obs.exists) {
    osd->reply_op_error(op, -ENOENT);
    put_object_context(obc);
    return;
  }

  // are writes blocked by another object?
  if (obc->blocked_by) {
    wait_for_degraded_object(obc->blocked_by->obs.oi.soid, op);
    put_object_context(obc);
    return;
  }

  // src_oids
  map<hobject_t,ObjectContext*> src_obc;
  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); ++p) {
    OSDOp& osd_op = *p;

    // make sure LIST_SNAPS is on CEPH_SNAPDIR and nothing else
    if (osd_op.op.op == CEPH_OSD_OP_LIST_SNAPS &&
	m->get_snapid() != CEPH_SNAPDIR) {
      osd->reply_op_error(op, -EINVAL);
      return;
    }

    if (!ceph_osd_op_type_multi(osd_op.op.op))
      continue;
    if (osd_op.soid.oid.name.length()) {
      object_locator_t src_oloc;
      get_src_oloc(m->get_oid(), m->get_object_locator(), src_oloc);
      hobject_t src_oid(osd_op.soid, src_oloc.key, m->get_pg().ps(),
			info.pgid.pool());
      if (!src_obc.count(src_oid)) {
	ObjectContext *sobc;
	snapid_t ssnapid;

	int r = find_object_context(src_oid, src_oloc, &sobc, false, &ssnapid);
	if (r) {
	  osd->reply_op_error(op, r);
	}  else {
	  src_obc[src_oid] = sobc;
	  continue;
	}
	// Error cleanup below
      } else {
	continue;
      }
      // Error cleanup below
    } else {
      osd->reply_op_error(op, -EINVAL);
    }
    put_object_contexts(src_obc);
    put_object_context(obc);
    return;
  }

  op->mark_started();

  const hobject_t& soid = obc->obs.oi.soid;
  OpContext *ctx = new OpContext(op, m->get_reqid(), m->ops,
				 &obc->obs, obc->ssc,
				 this);
  ctx->obc = obc;
  ctx->src_obc = src_obc;

  if (op->may_write()) {
    // client specified snapc
    ctx->snapc.seq = m->get_snap_seq();
    ctx->snapc.snaps = m->get_snaps();
    if ((m->get_flags() & CEPH_OSD_FLAG_ORDERSNAP) &&
	ctx->snapc.seq < obc->ssc->snapset.seq) {
      delete ctx;
      put_object_context(obc);
      put_object_contexts(src_obc);
      osd->reply_op_error(op, -EOLDSNAPC);
      return;
    }

    op->mark_started();

    ctx->mtime = m->get_mtime();

  }

  // note my stats
  utime_t now = ceph_clock_now(g_ceph_context);

  // note some basic context for op replication that prepare_transaction may clobber
  bool old_exists = obc->obs.exists;
  uint64_t old_size = obc->obs.oi.size;
  eversion_t old_version = obc->obs.oi.version;

  if (op->may_read()) {
    obc->ondisk_read_lock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    p->second->ondisk_read_lock();
  }

  int result = prepare_transaction(ctx);

  if (op->may_read()) {
    obc->ondisk_read_unlock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    p->second->ondisk_read_unlock();
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
    return;
  }

  // check for full
  if (ctx->delta_stats.num_bytes > 0 &&
      pool.info.get_flags() & pg_pool_t::FLAG_FULL) {
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
    osd->reply_op_error(op, -ENOSPC);
    return;
  }

  // prepare the reply
  ctx->reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);

  // Write operations aren't allowed to return a data payload because
  // we can't do so reliably. If the client has to resend the request
  // and it has already been applied, we will return 0 with no
  // payload.  Non-deterministic behavior is no good.  However, it is
  // possible to construct an operation that does a read, does a guard
  // check (e.g., CMPXATTR), and then a write.  Then we either succeed
  // with the write, or return a CMPXATTR and the read value.
  if (ctx->op_t.empty() && !ctx->modify) {
    // read.
    ctx->reply->claim_op_out_data(ctx->ops);
    ctx->reply->get_header().data_off = ctx->data_off;
  } else {
    // write.  normalize the result code.
    if (result > 0)
      result = 0;
  }
  ctx->reply->set_result(result);

  if (result >= 0)
    ctx->reply->set_version(ctx->reply_version);
  else if (result == -ENOENT)
    ctx->reply->set_version(info.last_update);

  // read or error?
  if (ctx->op_t.empty() || result < 0) {
    if (result >= 0) {
      log_op_stats(ctx);
      publish_stats_to_osd();
    }

    MOSDOpReply *reply = ctx->reply;
    ctx->reply = NULL;
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    osd->send_message_osd_client(reply, m->get_connection());
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
    return;
  }

  // trim log?
  calc_trim_to();

  append_log(ctx->log, pg_trim_to, ctx->local_t);

  // verify that we are doing this in order?
  if (g_conf->osd_debug_op_order && m->get_source().is_client()) {
    map<client_t,tid_t>& cm = debug_op_order[obc->obs.oi.soid];
    tid_t t = m->get_tid();
    client_t n = m->get_source().num();
    map<client_t,tid_t>::iterator p = cm.find(n);
    if (p == cm.end()) {
      cm[n] = t;
    } else {
      p->second = t;
    }
  }

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(ctx, obc, rep_tid);  // new repop claims our obc, src_obc refs
  // note: repop now owns ctx AND ctx->op

  repop->src_obc.swap(src_obc); // and src_obc.

  issue_repop(repop, now, old_last_update, old_exists, old_size, old_version);

  eval_repop(repop);
  repop->put();
}


int OSDVol::prepare_transaction(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;

  // we'll need this to log
  eversion_t old_version = ctx->obs->oi.version;

  bool head_existed = ctx->obs->exists;

  // valid snap context?
  if (!ctx->snapc.is_valid()) {
    return -EINVAL;
  }

  // prepare the actual mutation
  int result = do_osd_ops(ctx, ctx->ops);
  if (result < 0)
    return result;

  // finish side-effects
  if (result == 0)
    do_osd_op_effects(ctx);

  // read-op?  done?
  if (ctx->op_t.empty() && !ctx->modify) {
    ctx->reply_version = ctx->obs->oi.user_version;
    unstable_stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
    return result;
  }


  // clone, if necessary
  make_writeable(ctx);

  // snapset
  bufferlist bss;
  ::encode(ctx->new_snapset, bss);

  if (ctx->new_obs.exists) {
    if (!head_existed) {
      // if we logically recreated the head, remove old _snapdir object
      hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
			info.pgid.pool());

      ctx->snapset_obc = get_object_context(snapoid, ctx->new_obs.oi.oloc, false);
      if (ctx->snapset_obc && ctx->snapset_obc->obs.exists) {
	ctx->op_t.remove(coll, snapoid);

	ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::DELETE, snapoid, ctx->at_version, old_version,
				      osd_reqid_t(), ctx->mtime));
	ctx->at_version.version++;

	ctx->snapset_obc->obs.exists = false;
      }
    }
  } else if (ctx->new_snapset.clones.size()) {
    // save snapset on _snap
    hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
		      info.pgid.pool());
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, snapoid, ctx->at_version, old_version,
				  osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc = get_object_context(snapoid, ctx->new_obs.oi.oloc, true);
    ctx->snapset_obc->obs.exists = true;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;
    ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
    ctx->snapset_obc->obs.oi.mtime = ctx->mtime;

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->snapset_obc->obs.oi, bv);
    ctx->op_t.touch(coll, snapoid);
    ctx->op_t.setattr(coll, snapoid, OI_ATTR, bv);
    ctx->op_t.setattr(coll, snapoid, SS_ATTR, bss);
    ctx->at_version.version++;
  }

  // finish and log the op.
  if (ctx->user_modify) {
    /* update the user_version for any modify ops, except for the watch op */
    ctx->new_obs.oi.user_version = ctx->at_version;
  }
  ctx->reply_version = ctx->new_obs.oi.user_version;
  ctx->bytes_written = ctx->op_t.get_encoded_bytes();
  ctx->new_obs.oi.version = ctx->at_version;
 
  if (ctx->new_obs.exists) {
    // on the head object
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = old_version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
    }

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->new_obs.oi, bv);
    ctx->op_t.setattr(coll, soid, OI_ATTR, bv);

    ctx->op_t.setattr(coll, soid, SS_ATTR, bss);
  }

  // append to log
  int logopcode = pg_log_entry_t::MODIFY;
  if (!ctx->new_obs.exists)
    logopcode = pg_log_entry_t::DELETE;
  ctx->log.push_back(pg_log_entry_t(logopcode, soid,
				    ctx->at_version, old_version, ctx->reqid,
				    ctx->mtime));

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;
  ctx->obc->ssc->snapset = ctx->new_snapset;
  info.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);

  if (backfill_target >= 0) {
    pg_info_t& pinfo = peer_info[backfill_target];
    if (soid < pinfo.last_backfill)
      pinfo.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
    else if (soid < backfill_pos)
      pending_backfill_updates[soid].stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
  }

  if (scrubber.active && scrubber.is_chunky) {
    if (soid < scrubber.start)
      scrub_cstat.add(ctx->delta_stats, ctx->obc->obs.oi.category);
  }

  return result;
}

int OSDVol::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
{
  int result = 0;
  SnapSetContext *ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;

  bool first_read = true;

  ObjectStore::Transaction& t = ctx->op_t;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;

    bufferlist::iterator bp = osd_op.indata.begin();

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
      hobject_t src_oid(osd_op.soid, soid.hash);
      src_obc = ctx->src_obc[src_oid];
    }

    // munge -1 truncate to 0 truncate
    if (op.extent.truncate_seq == 1 && op.extent.truncate_size == (-1ULL)) {
      op.extent.truncate_size = 0;
      op.extent.truncate_seq = 0;
    }

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
	obs.exists &&
	op.extent.offset < g_conf->osd_max_object_size &&
	op.extent.length >= 1 &&
	op.extent.length <= g_conf->osd_max_object_size &&
	op.extent.offset + op.extent.length >= oi.size) {
      if (op.extent.offset >= oi.size) {
	// no-op
	goto fail;
      }
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      // --- READS ---

    case CEPH_OSD_OP_READ:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->read(coll, soid, op.extent.offset,
				 op.extent.length, bl);
	if (first_read) {
	  first_read = false;
	  ctx->data_off = op.extent.offset;
	}
	osd_op.outdata.claim_append(bl);
	if (r >= 0)
	  op.extent.length = r;
	else {
	  result = r;
	  op.extent.length = 0;
	}
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;

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

	  // keep first part of osd_op.outdata; trim at truncation point
	  keep.substr_of(osd_op.outdata, 0, osd_op.outdata.length() - trim);
	  osd_op.outdata.claim(keep);
	}
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_MAPEXT:
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->fiemap(coll, soid, op.extent.offset, op.extent.length, bl);
	osd_op.outdata.claim(bl);
	if (r < 0)
	  result = r;
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      {
	if (op.extent.truncate_seq) {
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
	uint64_t last = op.extent.offset;
	for (miter = m.begin(); miter != m.end(); ++miter) {
	  // verify hole?
	  if (g_conf->osd_verify_sparse_read_holes &&
	      last < miter->first) {
	    bufferlist t;
	    uint64_t len = miter->first - last;
	    r = osd->store->read(coll, soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid
				    << " sparse-read found data in hole "
				    << last << "~" << len << "\n";
	    }
	  }

	  bufferlist tmpbl;
	  r = osd->store->read(coll, soid, miter->first, miter->second, tmpbl);
	  if (r < 0)
	    break;

	  /* this is usually happen when we get extent that exceeds
	     the actual file size */
	  if (r < (int)miter->second)
	    miter->second = r;
	  total_read += r;
	  data_bl.claim_append(tmpbl);
	  last = miter->first + r;
	}

	// verify trailing hole?
	if (g_conf->osd_verify_sparse_read_holes) {
	  uint64_t end = MIN(op.extent.offset + op.extent.length, oi.size);
	  if (last < end) {
	    bufferlist t;
	    uint64_t len = end - last;
	    r = osd->store->read(coll, soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid << " sparse-read found data in hole "
				    << last << "~" << len << "\n";
	    }
	  }
	}

	if (r < 0) {
	  result = r;
	  break;
	}

	op.extent.length = total_read;

	::encode(m, osd_op.outdata);
	::encode(data_bl, osd_op.outdata);

	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	string cname, mname;
	bufferlist indata;
	try {
	  bp.copy(op.cls.class_len, cname);
	  bp.copy(op.cls.method_len, mname);
	  bp.copy(op.cls.indata_len, indata);
	} catch (buffer::error& e) {
	  result = -EINVAL;
	  break;
	}

	ClassHandler::ClassData *cls;
	result = osd->class_handler->open_class(cname, &cls);

	ClassHandler::ClassMethod *method = cls->get_method(mname.c_str());
	if (!method) {
	  result = -EOPNOTSUPP;
	  break;
	}

	int flags = method->get_flags();
	if (flags & CLS_METHOD_WR)
	  ctx->user_modify = true;

	bufferlist outdata;
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);
	op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
      }
      break;

    case CEPH_OSD_OP_STAT:
      {
	if (obs.exists) {
	  ::encode(oi.size, osd_op.outdata);
	  ::encode(oi.mtime, osd_op.outdata);
	} else {
	  result = -ENOENT;
	}

	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = osd->store->getattr(coll, soid, name.c_str(), osd_op.outdata);
	if (r >= 0) {
	  op.xattr.value_len = r;
	  result = 0;
	  ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(r, 10);
	  ctx->delta_stats.num_rd++;
	} else
	  result = r;
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
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(bl.length(), 10);
	ctx->delta_stats.num_rd++;
	osd_op.outdata.claim_append(bl);
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
	  result = osd->store->getattr(coll, src_obc->obs.oi.soid,
				       name.c_str(), xattr);
	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;

	ctx->delta_stats.num_rd++;
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(xattr.length(), 10);

	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	  {
	    string val;
	    bp.copy(op.xattr.value_len, val);
	    val[op.xattr.value_len] = 0;
	    result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	  }
	  break;

	case CEPH_OSD_CMPXATTR_MODE_U64:
	  {
	    uint64_t u64val;
	    try {
	      ::decode(u64val, bp);
	    }
	    catch (buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    result = do_xattr_cmp_u64(op.xattr.cmp_op, u64val, xattr);
	  }
	  break;

	default:
	  result = -EINVAL;
	}

	if (!result) {
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  break;
	}
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

    case CEPH_OSD_OP_LIST_WATCHERS:
      {
	obj_list_watch_response_t resp;

	map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator oi_iter;
	for (oi_iter = oi.watchers.begin(); oi_iter != oi.watchers.end();
	     ++oi_iter) {

	  watch_item_t wi(oi_iter->first.second, oi_iter->second.cookie,
		 oi_iter->second.timeout_seconds, oi_iter->second.addr);
	  resp.entries.push_back(wi);
	}

	resp.encode(osd_op.outdata);
	result = 0;

	ctx->delta_stats.num_rd++;
	break;
      }

    case CEPH_OSD_OP_LIST_SNAPS:
      {
	obj_list_snap_response_t resp;

	if (!ssc) {
	  ssc = ctx->obc->ssc = get_snapset_context(soid.oid,
						    soid.get_key(), soid.hash, false);
	}

	int clonecount = ssc->snapset.clones.size();
	if (ssc->snapset.head_exists)
	  clonecount++;
	resp.clones.reserve(clonecount);
	for (vector<snapid_t>::const_iterator clone_iter = ssc->snapset.clones.begin();
	     clone_iter != ssc->snapset.clones.end(); ++clone_iter) {
	  clone_info ci;
	  ci.cloneid = *clone_iter;

	  hobject_t clone_oid = soid;
	  clone_oid.snap = *clone_iter;
	  ObjectContext *clone_obc = ctx->src_obc[clone_oid];
	  for (vector<snapid_t>::reverse_iterator p = clone_obc->obs.oi.snaps.rbegin();
	       p != clone_obc->obs.oi.snaps.rend();
	       ++p) {
	    ci.snaps.push_back(*p);
	  }

	  map<snapid_t, interval_set<uint64_t> >::const_iterator coi;
	  coi = ssc->snapset.clone_overlap.find(ci.cloneid);
	  if (coi == ssc->snapset.clone_overlap.end()) {
	    osd->clog.error() << "osd." << osd->whoami
				  << ": inconsistent clone_overlap found for oid "
				  << soid << " clone " << *clone_iter;
	    result = -EINVAL;
	    break;
	  }
	  const interval_set<uint64_t> &o = coi->second;
	  ci.overlap.reserve(o.num_intervals());
	  for (interval_set<uint64_t>::const_iterator r = o.begin();
	       r != o.end(); ++r) {
	    ci.overlap.push_back(pair<uint64_t,uint64_t>(r.get_start(), r.get_len()));
	  }

	  map<snapid_t, uint64_t>::const_iterator si;
	  si = ssc->snapset.clone_size.find(ci.cloneid);
	  if (si == ssc->snapset.clone_size.end()) {
	    osd->clog.error() << "osd." << osd->whoami
				  << ": inconsistent clone_size found for oid "
				  << soid << " clone " << *clone_iter;
	    result = -EINVAL;
	    break;
	  }
	  ci.size = si->second;

	  resp.clones.push_back(ci);
	}
	if (ssc->snapset.head_exists) {
	  clone_info ci;
	  ci.cloneid = CEPH_NOSNAP;

	  //Size for HEAD is oi.size
	  ci.size = oi.size;

	  resp.clones.push_back(ci);
	}
	resp.seq = ssc->snapset.seq;

	resp.encode(osd_op.outdata);
	result = 0;

	ctx->delta_stats.num_rd++;
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
	if (!timeout)
	  timeout = g_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.cookie = op.watch.cookie;
	n.bl = bl;
	ctx->notifies.push_back(n);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      {
	try {
	  uint64_t notify_id = 0;
	  uint64_t watch_cookie = 0;
	  ::decode(notify_id, bp);
	  ::decode(watch_cookie, bp);
	  OpContext::NotifyAck ack(notify_id, watch_cookie);
	  ctx->notify_acks.push_back(ack);
	} catch (const buffer::error &e) {
	  OpContext::NotifyAck ack(
	    // op.watch.cookie is actually the notify_id for historical reasons
	    op.watch.cookie
	    );
	  ctx->notify_acks.push_back(ack);
	}
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
	  op.extent.length = (op.extent.offset > oi.size ? 0 :
			      oi.size - op.extent.offset);
	}
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  if (obs.exists) {
	    t.truncate(coll, soid, op.extent.truncate_size);
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	    if (op.extent.truncate_size != oi.size) {
	      ctx->delta_stats.num_bytes -= oi.size;
	      ctx->delta_stats.num_bytes += op.extent.truncate_size;
	      oi.size = op.extent.truncate_size;
	    }
	  } else {
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	  }
	}
	result = check_offset_and_length(op.extent.offset, op.extent.length);
	if (result < 0)
	  break;
	bufferlist nbl;
	bp.copy(op.extent.length, nbl);
	t.write(coll, soid, op.extent.offset, op.extent.length, nbl);
	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.extent.offset, op.extent.length, true);
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      { // write full object
	result = check_offset_and_length(op.extent.offset, op.extent.length);
	if (result < 0)
	  break;
	bufferlist nbl;
	bp.copy(op.extent.length, nbl);
	if (obs.exists) {
	  t.truncate(coll, soid, 0);
	} else {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.write(coll, soid, op.extent.offset, op.extent.length, nbl);
	interval_set<uint64_t> ch;
	if (oi.size > 0)
	  ch.insert(0, oi.size);
	ctx->modified_ranges.union_of(ch);
	if (op.extent.length + op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  oi.size = op.extent.length + op.extent.offset;
	  ctx->delta_stats.num_bytes += oi.size;
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
	result = check_offset_and_length(op.extent.offset, op.extent.length);
	if (result < 0)
	  break;
	if (obs.exists) {
	  t.zero(coll, soid, op.extent.offset, op.extent.length);
	  interval_set<uint64_t> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->delta_stats.num_wr++;
	} else {
	  // no-op
	}
      }
      break;

    case CEPH_OSD_OP_CREATE:
      {
	int flags = le32_to_cpu(op.flags);
	if (obs.exists && (flags & CEPH_OSD_OP_FLAG_EXCL)) {
	  result = -EEXIST; /* this is an exclusive create */
	} else {
	  if (osd_op.indata.length()) {
	    bufferlist::iterator p = osd_op.indata.begin();
	    string category;
	    try {
	      ::decode(category, p);
	    }
	    catch (buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    if (category.size()) {
	      if (obs.exists) {
		if (obs.oi.category != category)
		  result = -EEXIST;  // category cannot be reset
	      } else {
		obs.oi.category = category;
	      }
	    }
	  }
	  if (result >= 0 && !obs.exists) {
	    t.touch(coll, soid);
	    ctx->delta_stats.num_objects++;
	    obs.exists = true;
	  }
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      {
	// truncate
	if (!obs.exists) {
	  break;
	}

	if (op.extent.offset > g_conf->osd_max_object_size) {
	  result = -EFBIG;
	  break;
	}

	if (op.extent.truncate_seq) {
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    break; // old
	  }
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	t.truncate(coll, soid, op.extent.offset);
	if (oi.size > op.extent.offset) {
	  interval_set<uint64_t> trim;
	  trim.insert(op.extent.offset, oi.size-op.extent.offset);
	  ctx->modified_ranges.union_of(trim);
	}
	if (op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  ctx->delta_stats.num_bytes += op.extent.offset;
	  oi.size = op.extent.offset;
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.
      }
      break;
    
    case CEPH_OSD_OP_DELETE:
      if (ctx->obc->obs.oi.watchers.size()) {
	// Cannot delete an object with watchers
	result = -EBUSY;
      } else {
	result = _delete_head(ctx);
      }
      break;

    case CEPH_OSD_OP_CLONERANGE:
      {
	if (!obs.exists) {
	  t.touch(coll, obs.oi.soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	if (op.clonerange.src_offset + op.clonerange.length >
	    src_obc->obs.oi.size) {
	  result = -EINVAL;
	  break;
	}
	t.clone_range(coll, src_obc->obs.oi.soid,
		      obs.oi.soid, op.clonerange.src_offset,
		      op.clonerange.length, op.clonerange.offset);


	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset,
				    ctx->modified_ranges,
				    op.clonerange.offset, op.clonerange.length,
				    false);
      }
      break;

    case CEPH_OSD_OP_WATCH:
      {
	uint64_t cookie = op.watch.cookie;
	bool do_watch = op.watch.flag & 1;
	entity_name_t entity = ctx->reqid.name;
	ObjectContext *obc = ctx->obc;

	// FIXME: where does the timeout come from?
	watch_info_t w(cookie, 30,
	  ctx->op->request->get_connection()->get_peer_addr());
	if (do_watch) {
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    oi.watchers[make_pair(cookie, entity)] = w;
	    t.nop();  // make sure update the object_info on disk!
	  }
	  ctx->watch_connects.push_back(w);
	} else {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    oi.watchers.erase(oi_iter);
	    t.nop();  // update oi on disk
	    ctx->watch_disconnects.push_back(w);
	  } else {
	  }
	}
      }
      break;


      // -- object attrs --

    case CEPH_OSD_OP_SETXATTR:
      {
	if (!obs.exists) {
	  t.touch(coll, soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
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
	newop.op.extent.truncate_seq = oi.truncate_seq;
	newop.indata = osd_op.indata;
	do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
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
	do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      {
	// verify sort order
	bool unsorted = false;
	if (true) {
	  bufferlist header;
	  ::decode(header, bp);
	  uint32_t n;
	  ::decode(n, bp);
	  string last_key;
	  while (n--) {
	    string key;
	    ::decode(key, bp);
	    bufferlist val;
	    ::decode(val, bp);
	    if (key < last_key) {
	      unsorted = true;
	      break;
	    }
	    last_key = key;
	  }
	}

	if (g_conf->osd_tmapput_sets_uses_tmap) {
	  oi.uses_tmap = true;
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.indata.length();
	newop.indata = osd_op.indata;

	if (unsorted) {
	  bp = osd_op.indata.begin();
	  bufferlist header;
	  map<string, bufferlist> m;
	  ::decode(header, bp);
	  ::decode(m, bp);
	  bufferlist newbl;
	  ::encode(header, newbl);
	  ::encode(m, newbl);
	  newop.indata = newbl;
	}
	do_osd_ops(ctx, nops);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      result = do_tmapup(ctx, bp, osd_op);
      break;

      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
      {
	string start_after;
	uint64_t max_return;
	try {
	  ::decode(start_after, bp);
	  ::decode(max_return, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	set<string> out_set;

	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    map<string, bufferlist>::iterator iter =
	      vals.upper_bound(start_after);
	    for (uint64_t i = 0;
		 i < max_return && iter != vals.end();
		 ++i, iter++) {
	      out_set.insert(iter->first);
	    }
	    ::encode(out_set, osd_op.outdata);
	    ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	    ctx->delta_stats.num_rd++;
	    break;
	  }
	  // No valid tmap, use omap
	}

	{
	  ObjectMap::ObjectMapIterator iter
	    = osd->store->get_omap_iterator(coll, soid);
	  iter->upper_bound(start_after);
	  for (uint64_t i = 0;
	       i < max_return && iter->valid();
	       ++i, iter->next()) {
	    out_set.insert(iter->key());
	  }
	}
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;
    case CEPH_OSD_OP_OMAPGETVALS:
      {
	string start_after;
	uint64_t max_return;
	string filter_prefix;
	try {
	  ::decode(start_after, bp);
	  ::decode(max_return, bp);
	  ::decode(filter_prefix, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	map<string, bufferlist> out_set;

	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    map<string, bufferlist>::iterator iter = vals.upper_bound(start_after);
	    if (filter_prefix > start_after) iter = vals.lower_bound(filter_prefix);
	    for (uint64_t i = 0;
		 i < max_return && iter != vals.end() &&
		   iter->first.substr(0, filter_prefix.size()) == filter_prefix;
		 ++i, iter++) {
	      out_set.insert(*iter);
	    }
	    ::encode(out_set, osd_op.outdata);
	    ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	    ctx->delta_stats.num_rd++;
	    break;
	  }
	  // No valid tmap, use omap
	}

	{
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    coll, soid
	    );
	  if (!iter) {
	    result = -ENOENT;
	    goto fail;
	  }
	  iter->upper_bound(start_after);
	  if (filter_prefix >= start_after) iter->lower_bound(filter_prefix);
	  for (uint64_t i = 0;
	       i < max_return && iter->valid() &&
		 iter->key().substr(0, filter_prefix.size()) == filter_prefix;
	       ++i, iter->next()) {
	    out_set.insert(make_pair(iter->key(), iter->value()));
	  }
	}
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;
    case CEPH_OSD_OP_OMAPGETHEADER:
      {
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    osd_op.outdata.claim(header);
	    break;
	  }
	}
	osd->store->omap_get_header(coll, soid, &osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;
    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
      {
	set<string> keys_to_get;
	try {
	  ::decode(keys_to_get, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	map<string, bufferlist> out;
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    for (set<string>::iterator iter = keys_to_get.begin();
		 iter != keys_to_get.end();
		 ++iter) {
	      if (vals.count(*iter)) {
		out.insert(*(vals.find(*iter)));
	      }
	    }
	    ::encode(out, osd_op.outdata);
	    ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	    ctx->delta_stats.num_rd++;
	    break;
	  }
	  // No valid tmap, use omap
	}
	osd->store->omap_get_values(coll, soid, keys_to_get, &out);
	::encode(out, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;
    case CEPH_OSD_OP_OMAP_CMP:
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	map<string, pair<bufferlist, int> > assertions;
	try {
	  ::decode(assertions, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}

	map<string, bufferlist> out;
	set<string> to_get;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i)
	  to_get.insert(i->first);
	int r = osd->store->omap_get_values(coll, soid, to_get, &out);
	if (r < 0) {
	  result = r;
	  break;
	}
	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	r = 0;
	bufferlist empty;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i) {
	  bufferlist &bl = out.count(i->first) ? 
	    out[i->first] : empty;
	  switch (i->second.second) {
	  case CEPH_OSD_CMPXATTR_OP_EQ:
	    if (!(bl == i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_LT:
	    if (!(bl < i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_GT:
	    if (!(bl > i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  default:
	    r = -EINVAL;
	    break;
	  }
	  if (r < 0)
	    break;
	}
	if (r < 0) {
	  result = r;
	}
      }
      break;
      // OMAP Write ops
    case CEPH_OSD_OP_OMAPSETVALS:
      {
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.touch(coll, soid);
	map<string, bufferlist> to_set;
	try {
	  ::decode(to_set, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t.omap_setkeys(coll, soid, to_set);
	ctx->delta_stats.num_wr++;
      }
      break;
    case CEPH_OSD_OP_OMAPSETHEADER:
      {
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.touch(coll, soid);
	t.omap_setheader(coll, soid, osd_op.indata);
	ctx->delta_stats.num_wr++;
      }
      break;
    case CEPH_OSD_OP_OMAPCLEAR:
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
	t.touch(coll, soid);
	t.omap_clear(coll, soid);
	ctx->delta_stats.num_wr++;
      }
      break;
    case CEPH_OSD_OP_OMAPRMKEYS:
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
	t.touch(coll, soid);
	set<string> to_rm;
	try {
	  ::decode(to_rm, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t.omap_rmkeys(coll, soid, to_rm);
	ctx->delta_stats.num_wr++;
      }
      break;
    default:
      result = -EOPNOTSUPP;
    }

    ctx->bytes_read += osd_op.outdata.length();

  fail:
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK))
      result = 0;

    if (result < 0)
      break;
  }
  return result;
}

void OSDVol::do_osd_op_effects(OpContext *ctx)
{
  ConnectionRef conn(ctx->op->request->get_connection());
  boost::intrusive_ptr<OSD::Session> session(
    (OSD::Session *)conn->get_priv());
  session->put();  // get_priv() takes a ref, and so does the intrusive_ptr
  entity_name_t entity = ctx->reqid.name;

  for (list<watch_info_t>::iterator i = ctx->watch_connects.begin();
       i != ctx->watch_connects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, entity);
    WatchRef watch;
    if (ctx->obc->watchers.count(watcher)) {
      watch = ctx->obc->watchers[watcher];
    } else {
      watch = Watch::makeWatchRef(
	this, osd, ctx->obc, i->timeout_seconds,
	i->cookie, entity, conn->get_peer_addr());
      ctx->obc->watchers.insert(
	make_pair(
	  watcher,
	  watch));
    }
    watch->connect(conn);
  }

  for (list<watch_info_t>::iterator i = ctx->watch_disconnects.begin();
       i != ctx->watch_disconnects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, entity);
    if (ctx->obc->watchers.count(watcher)) {
      WatchRef watch = ctx->obc->watchers[watcher];
      ctx->obc->watchers.erase(watcher);
      watch->remove();
    } else {
    }
  }

  for (list<notify_info_t>::iterator p = ctx->notifies.begin();
       p != ctx->notifies.end();
       ++p) {
    dout(10) << "do_osd_op_effects, notify " << *p << dendl;
    NotifyRef notif(
      Notify::makeNotifyRef(
	conn,
	ctx->obc->watchers.size(),
	p->bl,
	p->timeout,
	p->cookie,
	osd->get_next_id(get_osdmap()->get_epoch()),
	ctx->obc->obs.oi.user_version.version,
	osd));
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      dout(10) << "starting notify on watch " << i->first << dendl;
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    dout(10) << "notify_ack " << make_pair(p->watch_cookie, p->notify_id) << dendl;
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
	  p->watch_cookie.get() != i->first.first) continue;
      dout(10) << "acking notify on watch " << i->first << dendl;
      i->second->notify_ack(p->notify_id);
    }
  }
}
