// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "cohort/CohortOSD.h"
#include "cohort/OSDVol.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "osd/SnapMapper.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, osd->whoami, get_osdmap())
static ostream& _prefix(std::ostream *_dout, OSDVol* vol, int whoami,
			CohortOSDMapRef osdmap) {
  return *_dout << vol->gen_prefix();
}

std::string OSDVol::gen_prefix() const
{
  stringstream out;
  CohortOSDMapRef mapref = osdmap_ref;
  out << "osd." << osd->whoami
      << " vol_epoch: " << (mapref ? mapref->get_epoch():0)
      << " " << volume_id << " ";
  return out.str();
}

OSDVol::OSDVol(CohortOSDServiceRef o, CohortOSDMapRef curmap,
	       uuid_d u, const hobject_t& loid, const hobject_t& ioid) :
  snap_trim_item(this), volume_id(u), vol_lock("OSDVol::vol_lock"),
  map_lock("OSDVol::map_lock"), osd(o), osdmap_ref(curmap), vol_log(),
  log_oid(loid), biginfo_oid(ioid),
  osr(osd->osr_registry.lookup_or_create(u, (stringify(u)))),
  info(u), osdriver(osd->store, coll_t(), OSD::make_snapmapper_oid()),
  snap_mapper(&osdriver)
{
}

static int check_offset_and_length(uint64_t offset, uint64_t length)
{
  if (offset >= g_conf->osd_max_object_size ||
      length > g_conf->osd_max_object_size ||
      offset + length > g_conf->osd_max_object_size)
    return -EFBIG;

  return 0;
}

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

  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    do_op(op); // do it now
    break;

  case MSG_OSD_SUBOP:
    do_sub_op(op);
    break;

  case MSG_OSD_SUBOPREPLY:
    do_sub_op_reply(op);
    break;

  default:
    /* Bogus */
    break;
  }
}

int OSDVol::find_object_context(const hobject_t& oid,
				ObjectContext **pobc,
				bool can_create,
				snapid_t *psnapid)
{
  hobject_t head(oid.oid, CEPH_NOSNAP, oid.hash);
  hobject_t snapdir(oid.oid, CEPH_SNAPDIR, oid.hash);

  // want the snapdir?
  if (oid.snap == CEPH_SNAPDIR) {
    // return head or snapdir, whichever exists.
    ObjectContext *obc = get_object_context(head, can_create);
    if (obc && !obc->obs.exists) {
      // ignore it if the obc exists but the object doesn't
      put_object_context(obc);
      obc = NULL;
    }
    if (!obc) {
      obc = get_object_context(snapdir, can_create);
    }
    if (!obc)
      return -ENOENT;
    *pobc = obc;

    // always populate ssc for SNAPDIR...
    if (!obc->ssc)
      obc->ssc = get_snapset_context(oid.oid, true);
    return 0;
  }

  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContext *obc = get_object_context(head, can_create);
    if (!obc)
      return -ENOENT;
    *pobc = obc;

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(oid.oid, true);

    return 0;
  }

  // we want a snap
  SnapSetContext *ssc = get_snapset_context(oid.oid, can_create);
  if (!ssc)
    return -ENOENT;

  // head?
  if (oid.snap > ssc->snapset.seq) {
    if (ssc->snapset.head_exists) {
      ObjectContext *obc = get_object_context(head, false);
      if (!obc->ssc)
	obc->ssc = ssc;
      else {
	assert(ssc == obc->ssc);
	put_snapset_context(ssc);
      }
      *pobc = obc;
      return 0;
    }
    put_snapset_context(ssc);
    return -ENOENT;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < oid.snap)
    k++;
  if (k == ssc->snapset.clones.size()) {
    put_snapset_context(ssc);
    return -ENOENT;
  }
  hobject_t soid(oid.oid, ssc->snapset.clones[k]);

  put_snapset_context(ssc); // we're done with ssc
  ssc = 0;

  ObjectContext *obc = get_object_context(soid, false);
  assert(obc);

  // clone
  snapid_t first = obc->obs.oi.snaps[obc->obs.oi.snaps.size()-1];
  if (first <= oid.snap) {
    *pobc = obc;
    return 0;
  } else {
    put_object_context(obc);
    return -ENOENT;
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

  ObjectContext *obc;
  bool can_create = op->may_write();
  snapid_t snapid;
  int r = find_object_context(hobject_t(m->get_oid(),
					m->get_snapid()),
			      &obc, can_create, &snapid);
  if (r) {
    osd->reply_op_error(op, r);
    return;
  }

  if ((op->may_read()) && (obc->obs.oi.lost)) {
    osd->reply_op_error(op, -ENFILE);
    return;
  }

  if (!op->may_write() && !obc->obs.exists) {
    osd->reply_op_error(op, -ENOENT);
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
      hobject_t src_oid(osd_op.soid);
      if (!src_obc.count(src_oid)) {
	ObjectContext *sobc;
	snapid_t ssnapid;

	int r = find_object_context(src_oid, &sobc, false, &ssnapid);
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

  /* Since we're only doing client side replication this pass, we
     don't have to do the snapdir/clone check. */

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

    eversion_t oldv = vol_log.get_log().get_request_version(ctx->reqid);
    if (oldv != eversion_t()) {
      delete ctx;
      put_object_context(obc);
      put_object_contexts(src_obc);
      if (already_complete(oldv)) {
	osd->reply_op_error(op, 0, oldv);
      } else {
	if (m->wants_ack()) {
	  if (already_ack(oldv)) {
	    MOSDOpReply *reply
	      = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
	    reply->add_flags(CEPH_OSD_FLAG_ACK);
	    osd->send_message_osd_client(reply, m->get_connection());
	  } else {
	    waiting_for_ack[oldv].push_back(op);
	  }
	}
	// always queue ondisk waiters, so that we can requeue if needed
	waiting_for_ondisk[oldv].push_back(op);
	op->mark_delayed("waiting for ondisk");
      }
      return;
    }

    op->mark_started();

    // version
    ctx->at_version = vol_log.get_head();

    ctx->at_version.epoch = get_osdmap()->get_epoch();
    ctx->at_version.version++;
    assert(ctx->at_version > info.last_update);
    assert(ctx->at_version > vol_log.get_head());

    ctx->mtime = m->get_mtime();

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

  if (op->may_read()) {
    dout(10) << " taking ondisk_read_lock" << dendl;
    obc->ondisk_read_lock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin();
       p != src_obc.end(); ++p) {
    dout(10) << " taking ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_lock();
  }

  int result = prepare_transaction(ctx);

  if (op->may_read()) {
    dout(10) << " dropping ondisk_read_lock" << dendl;
    obc->ondisk_read_unlock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin();
       p != src_obc.end(); ++p) {
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

  assert(op->may_write());

  // trim log?
  calc_trim_to();

  append_log(ctx->log, trim_to, ctx->local_t);

  // continuing on to write path, make sure object context is registered
  assert(obc->registered);

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  // new repop claims our obc, src_obc refs
  RepGather *repop = new_repop(ctx, obc, rep_tid);
  // note: repop now owns ctx AND ctx->op

  repop->src_obc.swap(src_obc); // and src_obc.

  repop->v = ctx->at_version;

  // add myself to gather set
  repop->waitfor_ack.insert(osd->whoami);
  repop->waitfor_disk.insert(osd->whoami);

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
      hobject_t snapoid(soid.oid, CEPH_SNAPDIR);

      ctx->snapset_obc = get_object_context(snapoid, false);
      if (ctx->snapset_obc && ctx->snapset_obc->obs.exists) {
	ctx->op_t.remove(coll_t(), snapoid);

	ctx->log.push_back(vol_log_entry_t(vol_log_entry_t::DELETE, snapoid,
					   ctx->at_version, old_version,
					   osd_reqid_t(), ctx->mtime));
	ctx->at_version.version++;

	ctx->snapset_obc->obs.exists = false;
      }
    }
  } else if (ctx->new_snapset.clones.size()) {
    // save snapset on _snap
    hobject_t snapoid(soid.oid, CEPH_SNAPDIR);
    ctx->log.push_back(vol_log_entry_t(vol_log_entry_t::MODIFY, snapoid,
				       ctx->at_version, old_version,
				       osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc = get_object_context(snapoid, true);
    ctx->snapset_obc->obs.exists = true;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;
    ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
    ctx->snapset_obc->obs.oi.mtime = ctx->mtime;

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->snapset_obc->obs.oi, bv);
    ctx->op_t.touch(coll_t(), snapoid);
    ctx->op_t.setattr(coll_t(), snapoid, OI_ATTR, bv);
    ctx->op_t.setattr(coll_t(), snapoid, SS_ATTR, bss);
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
    ctx->op_t.setattr(coll_t(), soid, OI_ATTR, bv);

    ctx->op_t.setattr(coll_t(), soid, SS_ATTR, bss);
  }

  // append to log
  int logopcode = vol_log_entry_t::MODIFY;
  if (!ctx->new_obs.exists)
    logopcode = vol_log_entry_t::DELETE;
  ctx->log.push_back(vol_log_entry_t(logopcode, soid,
				     ctx->at_version, old_version, ctx->reqid,
				     ctx->mtime));

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;
  ctx->obc->ssc->snapset = ctx->new_snapset;
  info.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);

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

    // munge ZERO -> TRUNCATE?	(don't munge to DELETE or we risk hosing attributes)
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
	int r = osd->store->read(coll_t(), soid, op.extent.offset,
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
	int r = osd->store->fiemap(coll_t(), soid, op.extent.offset, op.extent.length, bl);
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
	int r = osd->store->fiemap(coll_t(), soid, op.extent.offset, op.extent.length, bl);
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
	    r = osd->store->read(coll_t(), soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll_t() << " " << soid
				<< " sparse-read found data in hole "
				<< last << "~" << len << "\n";
	    }
	  }

	  bufferlist tmpbl;
	  r = osd->store->read(coll_t(), soid, miter->first, miter->second, tmpbl);
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
	    r = osd->store->read(coll_t(), soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll_t() << " " << soid << " sparse-read found data in hole "
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
	int r = osd->store->getattr(coll_t(), soid, name.c_str(), osd_op.outdata);
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
	result = osd->store->getattrs(coll_t(), soid, attrset, true);
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
	  result = osd->store->getattr(coll_t(), soid, name.c_str(), xattr);
	else
	  result = osd->store->getattr(coll_t(), src_obc->obs.oi.soid,
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
	  ssc = ctx->obc->ssc = get_snapset_context(soid.oid, false);
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
	    t.truncate(coll_t(), soid, op.extent.truncate_size);
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
	t.write(coll_t(), soid, op.extent.offset, op.extent.length, nbl);
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
	  t.truncate(coll_t(), soid, 0);
	} else {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.write(coll_t(), soid, op.extent.offset, op.extent.length, nbl);
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
	  t.zero(coll_t(), soid, op.extent.offset, op.extent.length);
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
	    t.touch(coll_t(), soid);
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

	t.truncate(coll_t(), soid, op.extent.offset);
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
	  t.touch(coll_t(), obs.oi.soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	if (op.clonerange.src_offset + op.clonerange.length >
	    src_obc->obs.oi.size) {
	  result = -EINVAL;
	  break;
	}
	t.clone_range(coll_t(), src_obc->obs.oi.soid,
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
	  t.touch(coll_t(), soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t.setattr(coll_t(), soid, name, bl);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	t.rmattr(coll_t(), soid, name);
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
	    = osd->store->get_omap_iterator(coll_t(), soid);
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
	    coll_t(), soid
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
	osd->store->omap_get_header(coll_t(), soid, &osd_op.outdata);
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
	osd->store->omap_get_values(coll_t(), soid, keys_to_get, &out);
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
	int r = osd->store->omap_get_values(coll_t(), soid, to_get, &out);
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
	t.touch(coll_t(), soid);
	map<string, bufferlist> to_set;
	try {
	  ::decode(to_set, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t.omap_setkeys(coll_t(), soid, to_set);
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
	t.touch(coll_t(), soid);
	t.omap_setheader(coll_t(), soid, osd_op.indata);
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
	t.touch(coll_t(), soid);
	t.omap_clear(coll_t(), soid);
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
	t.touch(coll_t(), soid);
	set<string> to_rm;
	try {
	  ::decode(to_rm, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t.omap_rmkeys(coll_t(), soid, to_rm);
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
	OSDVolRef(this), osd, ctx->obc, i->timeout_seconds,
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
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
	  p->watch_cookie.get() != i->first.first) continue;
      i->second->notify_ack(p->notify_id);
    }
  }
}

void OSDVol::queue_op(OpRequestRef op)
{
  Mutex::Locker l(map_lock);
  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    return;
  }
  if (op_must_wait_for_map(get_osdmap_with_maplock(), op)) {
    waiting_for_map.push_back(op);
    return;
  }
  osd->op_wq.queue(make_pair(OSDVolRef(this), op));
}

void OSDVol::do_sub_op(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(have_same_or_newer_map(m->map_epoch));
  assert(m->get_header().type == MSG_OSD_SUBOP);

  OSDOp *first = NULL;

  if (first) {
    switch (first->op.op) {
    case CEPH_OSD_OP_DELETE:
      sub_op_remove(op);
      return;
    }
  }

  sub_op_modify(op);
}

void OSDVol::sub_op_remove(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);

  op->mark_started();

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  remove_snap_mapped_object(*t, m->poid);
  int r = osd->store->queue_transaction(osr.get(), t);
  assert(r == 0);
}

void OSDVol::do_sub_op_reply(OpRequestRef op)
{
  MOSDSubOpReply *r = static_cast<MOSDSubOpReply *>(op->request);
  assert(r->get_header().type == MSG_OSD_SUBOPREPLY);

  sub_op_modify_reply(op);
}

ObjectContext *OSDVol::get_object_context(const hobject_t& soid,
					  bool can_create)
{
  map<hobject_t, ObjectContext*>::iterator p = object_contexts.find(soid);
  ObjectContext *obc;
  if (p != object_contexts.end()) {
    obc = p->second;
  } else {
    // check disk
    bufferlist bv;
    int r = osd->store->getattr(coll_t(), soid, OI_ATTR, bv);
    if (r < 0) {
      if (!can_create)
	return NULL;   // -ENOENT!

      // new object.
      object_info_t oi(soid);
      SnapSetContext *ssc = get_snapset_context(soid.oid, true);
      return create_object_context(oi, ssc);
    }

    object_info_t oi(bv);

    SnapSetContext *ssc = NULL;
    if (can_create)
      ssc = get_snapset_context(soid.oid, true);
    obc = new ObjectContext(oi, true, ssc);
    obc->obs.oi.decode(bv);
    obc->obs.exists = true;

    register_object_context(obc);

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(soid.oid, true);

    populate_obc_watchers(obc);
  }
  obc->ref++;
  return obc;
}

void OSDVol::put_object_context(ObjectContext *obc)
{
  --obc->ref;
  if (obc->ref == 0) {
    if (obc->ssc)
      put_snapset_context(obc->ssc);

    if (obc->registered)
      object_contexts.erase(obc->obs.oi.soid);
    delete obc;
  }
}

SnapSetContext *OSDVol::get_snapset_context(const object_t& oid,
					    bool can_create)
{
  SnapSetContext *ssc;
  map<object_t, SnapSetContext*>::iterator p = snapset_contexts.find(oid);
  if (p != snapset_contexts.end()) {
    ssc = p->second;
  } else {
    bufferlist bv;
    hobject_t head(oid, CEPH_NOSNAP);
    int r = osd->store->getattr(coll_t(), head, SS_ATTR, bv);
    if (r < 0) {
      // try _snapset
      hobject_t snapdir(oid, CEPH_SNAPDIR);
      r = osd->store->getattr(coll_t(), snapdir, SS_ATTR, bv);
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

void OSDVol::put_snapset_context(SnapSetContext *ssc)
{
  --ssc->ref;
  if (ssc->ref == 0) {
    if (ssc->registered)
      snapset_contexts.erase(ssc->oid);
    delete ssc;
  }
}

void OSDVol::put_object_contexts(map<hobject_t,ObjectContext*>& obcv)
{
  if (obcv.empty())
    return;
  dout(10) << "put_object_contexts " << obcv << dendl;
  for (map<hobject_t,ObjectContext*>::iterator p = obcv.begin(); p != obcv.end(); ++p)
    put_object_context(p->second);
  obcv.clear();
}

void OSDVol::log_op_stats(OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->request);

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= ctx->op->request->get_recv_stamp();

  utime_t rlatency;
  if (ctx->readable_stamp != utime_t()) {
    rlatency = ctx->readable_stamp;
    rlatency -= ctx->op->request->get_recv_stamp();
  }

  uint64_t inb = ctx->bytes_written;
  uint64_t outb = ctx->bytes_read;

  osd->logger->inc(l_osd_op);

  osd->logger->inc(l_osd_op_outb, outb);
  osd->logger->inc(l_osd_op_inb, inb);
  osd->logger->tinc(l_osd_op_lat, latency);

  if (op->may_read() && op->may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->tinc(l_osd_op_rw_rlat, rlatency);
    osd->logger->tinc(l_osd_op_rw_lat, latency);
  } else if (op->may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->tinc(l_osd_op_r_lat, latency);
  } else if (op->may_write()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->tinc(l_osd_op_w_rlat, rlatency);
    osd->logger->tinc(l_osd_op_w_lat, latency);
  } else
    assert(0);

  dout(15) << "log_op_stats " << *m
	   << " inb " << inb
	   << " outb " << outb
	   << " rlat " << rlatency
	   << " lat " << latency << dendl;
}

void OSDVol::publish_stats_to_osd()
{
  /* No-Op for now */
}

void OSDVol::calc_trim_to(void)
{
  size_t target = g_conf->osd_min_pg_log_entries;

  if (vol_log.get_log().approx_size() > target) {
    size_t num_to_trim = vol_log.get_log().approx_size() - target;
    list<vol_log_entry_t>::const_iterator it = vol_log.get_log().log.begin();
    eversion_t new_trim_to;
    for (size_t i = 0; i < num_to_trim; ++i) {
      new_trim_to = it->version;
      ++it;
    }
    dout(10) << "calc_trim_to " << trim_to << " -> " << new_trim_to << dendl;
    trim_to = new_trim_to;
    assert(trim_to <= vol_log.get_head());
  }
}

void OSDVol::append_log(vector<vol_log_entry_t>& logv,
			eversion_t trim_to,
			ObjectStore::Transaction &t)
{
  dout(10) << "append_log " << vol_log.get_log() << " " << logv << dendl;

  map<string,bufferlist> keys;
  for (vector<vol_log_entry_t>::iterator p = logv.begin();
       p != logv.end();
       ++p) {
    p->offset = 0;
    add_log_entry(*p, keys[p->get_key_name()]);
  }

  dout(10) << "append_log  adding " << keys.size() << " keys" << dendl;
  t.omap_setkeys(coll_t::META_COLL, log_oid, keys);

  vol_log.trim(trim_to, info);

  dirty_info = true;
  write_if_dirty(t);
}

void OSDVol::make_writeable(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  SnapContext& snapc = ctx->snapc;
  ObjectStore::Transaction t;

  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ctx->snapset
	   << "	 snapc=" << snapc << dendl;;
  
  // use newer snapc?
  if (ctx->new_snapset.seq > snapc.seq) {
    snapc.seq = ctx->new_snapset.seq;
    snapc.snaps = ctx->new_snapset.snaps;
    dout(10) << " using newer snapc " << snapc << dendl;
  }

  if (ctx->obs->exists)
    filter_snapc(snapc);

  if (ctx->obs->exists &&		// head exist(ed)
      snapc.snaps.size() &&		    // there are snaps
      snapc.snaps[0] > ctx->new_snapset.seq) {	// existing object is old
    // clone
    hobject_t coid = soid;
    coid.snap = snapc.seq;
    
    unsigned l;
    for (l=1; l<snapc.snaps.size() && snapc.snaps[l] > ctx->new_snapset.seq; l++) ;
    
    vector<snapid_t> snaps(l);
    for (unsigned i=0; i<l; i++)
      snaps[i] = snapc.snaps[i];

    // prepare clone
    object_info_t static_snap_oi(coid);
    object_info_t *snap_oi;
    ctx->clone_obc = new ObjectContext(static_snap_oi, true, NULL);
    ctx->clone_obc->get();
    register_object_context(ctx->clone_obc);
    snap_oi = &ctx->clone_obc->obs.oi;
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);
    snap_oi->snaps = snaps;
    _make_clone(t, soid, coid, snap_oi);

    OSDriver::OSTransaction _t(osdriver.get_transaction(&(ctx->local_t)));
    set<snapid_t> _snaps(snaps.begin(), snaps.end());
    snap_mapper.add_oid(coid, _snaps, &_t);

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
    ctx->log.push_back(vol_log_entry_t(vol_log_entry_t::CLONE, coid,
				       ctx->at_version, ctx->obs->oi.version,
				       ctx->reqid, ctx->new_obs.oi.mtime));
    ::encode(snaps, ctx->log.back().snaps);

    ctx->at_version.version++;
  }

  // update most recent clone_overlap and usage stats
  if (ctx->new_snapset.clones.size() > 0) {
    interval_set<uint64_t> &newest_overlap = ctx->new_snapset.clone_overlap
      .rbegin()->second;
    ctx->modified_ranges.intersection_of(newest_overlap);
    // modified_ranges is still in use by the clone
    add_interval_usage(ctx->modified_ranges, ctx->delta_stats);
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

int OSDVol::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
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

int OSDVol::do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr)
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

void OSDVol::write_update_size_and_usage(object_stat_sum_t& delta_stats,
					 object_info_t& oi, SnapSet& ss,
					 interval_set<uint64_t>& modified,
					 uint64_t offset, uint64_t length,
					 bool count_bytes)
{
  interval_set<uint64_t> ch;
  if (length)
    ch.insert(offset, length);
  modified.union_of(ch);
  if (length && (offset + length > oi.size)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes += new_size - oi.size;
    oi.size = new_size;
  }
  delta_stats.num_wr++;
  if (count_bytes)
    delta_stats.num_wr_kb += SHIFT_ROUND_UP(length, 10);
}

int OSDVol::_rollback_to(OpContext *ctx, ceph_osd_op& op)
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
  int ret = find_object_context(hobject_t(soid.oid, snapid),
				&rollback_to, false, &cloneid);
  if (ret) {
    if (-ENOENT == ret) {
      // there's no snapshot here, or there's no object.
      // if there's no snapshot, we delete the object; otherwise, do nothing.
      dout(20) << "_rollback_to deleting head on " << soid.oid
	       << " because got ENOENT on find_object_context" << dendl;
      if (ctx->obc->obs.oi.watchers.size()) {
	// Cannot delete an object with watchers
	ret = -EBUSY;
      } else {
	_delete_head(ctx);
	ret = 0;
      }
    } else if (-EAGAIN == ret) {
      /* a different problem, like degraded pool
       * with not-yet-restored object. We shouldn't have been able
       * to get here; recovery should have completed first! */
      hobject_t rollback_target(soid.oid, cloneid);
      dout(20) << "_rollback_to attempted to roll back to a missing object "
	       << rollback_target << " (requested snapid: ) " << snapid
	       << dendl;
    } else {
      // ummm....huh? It *can't* return anything else at time of writing.
      assert(0);
    }
  } else { //we got our context, let's use it to do the rollback!
    hobject_t& rollback_to_sobject = rollback_to->obs.oi.soid;
    if (rollback_to->obs.oi.soid.snap == CEPH_NOSNAP) {
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
	t.remove(coll_t(), soid);
      
      t.clone(coll_t(),
	      rollback_to_sobject, soid);
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
      ctx->delta_stats.num_bytes += rollback_to->obs.oi.size;
      obs.oi.size = rollback_to->obs.oi.size;
      snapset.head_exists = true;
    }
    put_object_context(rollback_to);
  }
  return ret;
}

inline int OSDVol::_delete_head(OpContext *ctx)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  ObjectStore::Transaction& t = ctx->op_t;

  if (!obs.exists)
    return -ENOENT;

  t.remove(coll_t(), soid);

  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
  }

  ctx->delta_stats.num_objects--;
  ctx->delta_stats.num_bytes -= oi.size;

  oi.size = 0;
  snapset.head_exists = false;
  obs.exists = false;

  ctx->delta_stats.num_wr++;
  return 0;
}

int OSDVol::do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op)
{
  bufferlist::iterator orig_bp = bp;
  int result = 0;
  if (bp.end()) {
    dout(10) << "tmapup is a no-op" << dendl;
  } else {
    // read the whole object
    vector<OSDOp> nops(1);
    OSDOp& newop = nops[0];
    newop.op.op = CEPH_OSD_OP_READ;
    newop.op.extent.offset = 0;
    newop.op.extent.length = 0;
    do_osd_ops(ctx, nops);

    dout(10) << "tmapup read " << newop.outdata.length() << dendl;

    dout(30) << " starting is \n";
    newop.outdata.hexdump(*_dout);
    *_dout << dendl;

    bufferlist::iterator ip = newop.outdata.begin();
    bufferlist obl;

    dout(30) << "the update command is: \n";
    osd_op.indata.hexdump(*_dout);
    *_dout << dendl;

    // header
    bufferlist header;
    __u32 nkeys = 0;
    if (newop.outdata.length()) {
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
    string nextkey, last_in_key;
    bufferlist nextval;
    bool have_next = false;
    string last_disk_key;
    if (!ip.end()) {
      have_next = true;
      ::decode(nextkey, ip);
      ::decode(nextval, ip);
      if (nextkey < last_disk_key) {
	dout(5) << "tmapup warning: key '" << nextkey << "' < previous key '" << last_disk_key
		<< "', falling back to an inefficient (unsorted) update" << dendl;
	bp = orig_bp;
	return do_tmapup_slow(ctx, bp, osd_op, newop.outdata);
      }
      last_disk_key = nextkey;
    }
    result = 0;
    while (!bp.end() && !result) {
      __u8 op;
      string key;
      try {
	::decode(op, bp);
	::decode(key, bp);
      }
      catch (buffer::error& e) {
	return -EINVAL;
      }
      if (key < last_in_key) {
	dout(5) << "tmapup warning: key '" << key << "' < previous key '" << last_in_key
		<< "', falling back to an inefficient (unsorted) update" << dendl;
	bp = orig_bp;
	return do_tmapup_slow(ctx, bp, osd_op, newop.outdata);
      }
      last_in_key = key;

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
	  dout(20) << "	 keep " << nextkey << " " << nextval.length() << dendl;
	} else {
	  // don't copy; discard old value.  and stop.
	  dout(20) << "	 drop " << nextkey << " " << nextval.length() << dendl;
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
	try {
	  ::decode(val, bp);
	}
	catch (buffer::error& e) {
	  return -EINVAL;
	}
	::encode(key, newkeydata);
	::encode(val, newkeydata);
	dout(20) << "	set " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_CREATE) {
	if (key_exists) {
	  return -EEXIST;
	}
	bufferlist val;
	try {
	  ::decode(val, bp);
	}
	catch (buffer::error& e) {
	  return -EINVAL;
	}
	::encode(key, newkeydata);
	::encode(val, newkeydata);
	dout(20) << "	create " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_RM) {
	// do nothing.
	if (!key_exists) {
	  return -ENOENT;
	}
      } else if (op == CEPH_OSD_TMAP_RMSLOPPY) {
	// do nothing
      } else {
	dout(10) << "  invalid tmap op " << (int)op << dendl;
	return -EINVAL;
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
      rest.substr_of(newop.outdata, ip.get_off(), newop.outdata.length() - ip.get_off());
      dout(20) << "  keep trailing " << rest.length()
	       << " at " << newkeydata.length() << dendl;
      newkeydata.claim_append(rest);
    }

    // encode final key count + key data
    dout(20) << "tmapup final nkeys " << nkeys << dendl;
    ::encode(nkeys, obl);
    obl.claim_append(newkeydata);

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
      newop.indata = obl;
      do_osd_ops(ctx, nops);
      osd_op.outdata.claim(newop.outdata);
    }
  }
  return result;
}

int OSDVol::_get_tmap(OpContext *ctx,
		      map<string, bufferlist> *out,
		      bufferlist *header)
{
  vector<OSDOp> nops(1);
  OSDOp &newop = nops[0];
  newop.op.op = CEPH_OSD_OP_TMAPGET;
  do_osd_ops(ctx, nops);
  try {
    bufferlist::iterator i = newop.outdata.begin();
    ::decode(*header, i);
    ::decode(*out, i);
  } catch (...) {
    dout(20) << "unsuccessful at decoding tmap for " << ctx->new_obs.oi.soid
	     << dendl;
    return -EINVAL;
  }
  dout(20) << "successful at decoding tmap for " << ctx->new_obs.oi.soid
	   << dendl;
  return 0;
}

int OSDVol::_copy_up_tmap(OpContext *ctx)
{
  dout(20) << "copying up tmap for " << ctx->new_obs.oi.soid << dendl;
  ctx->new_obs.oi.uses_tmap = false;
  map<string, bufferlist> vals;
  bufferlist header;
  int r = _get_tmap(ctx, &vals, &header);
  if (r < 0)
    return 0;
  ctx->op_t.omap_setkeys(coll_t(), ctx->new_obs.oi.soid,
			 vals);
  ctx->op_t.omap_setheader(coll_t(), ctx->new_obs.oi.soid,
			   header);
  return 0;
}

void OSDVol::sub_op_modify(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);

  const hobject_t& soid = m->poid;

  const char *opname;
  if (m->noop)
    opname = "no-op";
  else if (m->ops.size())
    opname = ceph_osd_op_name(m->ops[0].op.op);
  else
    opname = "trans";

  dout(10) << "sub_op_modify " << opname
	   << " " << soid
	   << " v " << m->version
	   << (m->noop ? " NOOP" : "")
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;

  // sanity checks
  assert(m->map_epoch >= info.history.same_interval_since);

  op->mark_started();

  RepModify *rm = new RepModify;
  rm->vol.reset(this);
  rm->op = op;
  rm->ctx = 0;
  rm->last_complete = info.last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  if (!m->noop) {
    if (m->logbl.length()) {
      // shipped transaction and log entries
      vector<vol_log_entry_t> log;

      bufferlist::iterator p = m->get_data().begin();

      ::decode(rm->opt, p);
      if (!(m->get_connection()->get_features() & CEPH_FEATURE_OSD_SNAPMAPPER))
	rm->opt.set_tolerate_collection_add_enoent();
      p = m->logbl.begin();
      ::decode(log, p);
      if (m->hobject_incorrect_pool) {
	for (vector<vol_log_entry_t>::iterator i = log.begin();
	     i != log.end();
	     ++i) {
	}
      }
      rm->opt.set_replica();

      if (!rm->opt.empty()) {
	// If the opt is non-empty, we infer we are before
	// last_backfill (according to the primary, not our
	// not-quite-accurate value), and should update the
	// collections now.  Otherwise, we do it later on push.
	update_snap_map(log, rm->localt);
      }
      append_log(log, m->trim_to, rm->localt);

      rm->tls.push_back(&rm->localt);
      rm->tls.push_back(&rm->opt);

    } else {
      // do op
      assert(0);

      // TODO: this is severely broken because we don't know whether
      // this object is really lost or not. We just always assume that
      // it's not right now.  Also, we're taking the address of a
      // variable on the stack.
      object_info_t oi(soid);
      oi.lost = false; // I guess?
      oi.version = m->old_version;
      oi.size = m->old_size;
      ObjectState obs(oi, m->old_exists);
      SnapSetContext ssc(m->poid.oid);

      rm->ctx = new OpContext(op, m->reqid, m->ops, &obs, &ssc, this);

      rm->ctx->mtime = m->mtime;
      rm->ctx->at_version = m->version;
      rm->ctx->snapc = m->snapc;

      ssc.snapset = m->snapset;
      rm->ctx->obc->ssc = &ssc;

      prepare_transaction(rm->ctx);
      append_log(rm->ctx->log, m->trim_to, rm->ctx->local_t);

      rm->tls.push_back(&rm->ctx->op_t);
      rm->tls.push_back(&rm->ctx->local_t);
    }

    rm->bytes_written = rm->opt.get_encoded_bytes();

  } else {
    // just trim the log
    if (m->trim_to != eversion_t()) {
      vol_log.trim(m->trim_to, info);
      dirty_info = true;
      write_if_dirty(rm->localt);
      rm->tls.push_back(&rm->localt);
    }
  }

  op->mark_started();

  Context *oncommit = new C_OSD_RepModifyCommit(rm);
  Context *onapply = new C_OSD_RepModifyApply(rm);
  int r = osd->store->queue_transactions(osr.get(), rm->tls, onapply,
					 oncommit, 0, op);
  if (r) {
    dout(0) << "error applying transaction: r = " << r << dendl;
    assert(0);
  }
  // op is cleaned up by oncommit/onapply when both are executed
}

void OSDVol::remove_snap_mapped_object(ObjectStore::Transaction& t,
				       const hobject_t& soid)
{
  t.remove(coll_t(), soid);
  OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
  if (soid.snap < CEPH_MAXSNAP) {
    int r = snap_mapper.remove_oid(
      soid,
      &_t);
    if (!(r == 0 || r == -ENOENT)) {
      derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

void OSDVol::sub_op_modify_reply(OpRequestRef op)
{
  // Nothing for now.
}

ObjectContext *OSDVol::create_object_context(const object_info_t& oi,
					     SnapSetContext *ssc)
{
  ObjectContext *obc = new ObjectContext(oi, false, ssc);
  dout(10) << "create_object_context " << obc << " " << oi.soid << " "
	   << obc->ref << dendl;
  register_object_context(obc);
  populate_obc_watchers(obc);
  obc->ref++;
  return obc;
}

void OSDVol::populate_obc_watchers(ObjectContext *obc)
{
  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	obc->obs.oi.watchers.begin();
       p != obc->obs.oi.watchers.end();
       ++p) {
    utime_t expire = info.stats.last_became_active;
    expire += p->second.timeout_seconds;
    dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
	OSDVolRef(this), osd, obc, p->second.timeout_seconds, p->first.first,
	p->first.second, p->second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
	make_pair(p->first.first, p->first.second),
	watch));
  }
  // Look for watchers from blacklisted clients and drop
  check_blacklisted_obc_watchers(obc);
}

void OSDVol::add_log_entry(vol_log_entry_t& e, bufferlist& log_bl)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;

  // raise last_update.
  assert(e.version > info.last_update);
  info.last_update = e.version;

  // log mutation
  vol_log.add(e);
  dout(10) << "add_log_entry " << e << dendl;

  e.encode_with_checksum(log_bl);
}

void OSDVol::write_if_dirty(ObjectStore::Transaction& t)
{
  if (dirty_big_info || dirty_info)
    write_info(t);
  vol_log.write_log(t, log_oid);
}

void OSDVol::filter_snapc(SnapContext& snapc)
{
  bool filtering = false;
  vector<snapid_t> newsnaps;
  for (vector<snapid_t>::iterator p = snapc.snaps.begin();
       p != snapc.snaps.end();
       ++p) {
    if (snap_trimq.contains(*p) || info.purged_snaps.contains(*p)) {
      if (!filtering) {
	// start building a new vector with what we've seen so far
	dout(10) << "filter_snapc filtering " << snapc << dendl;
	newsnaps.insert(newsnaps.begin(), snapc.snaps.begin(), p);
	filtering = true;
      }
      dout(20) << "filter_snapc  removing trimq|purged snap " << *p << dendl;
    } else {
      if (filtering)
	newsnaps.push_back(*p);  // continue building new vector
    }
  }
  if (filtering) {
    snapc.snaps.swap(newsnaps);
    dout(10) << "filter_snapc  result " << snapc << dendl;
  }
}

void OSDVol::_make_clone(ObjectStore::Transaction& t,
			       const hobject_t& head, const hobject_t& coid,
			       object_info_t *poi)
{
  bufferlist bv;
  ::encode(*poi, bv);

  t.clone(coll_t(), head, coid);
  t.setattr(coll_t(), coid, OI_ATTR, bv);
  t.rmattr(coll_t(), coid, SS_ATTR);
}

void OSDVol::add_interval_usage(interval_set<uint64_t>& s,
				object_stat_sum_t& delta_stats)
{
  for (interval_set<uint64_t>::const_iterator p = s.begin();
       p != s.end();
       ++p) {
    delta_stats.num_bytes += p.get_len();
  }
}

int OSDVol::do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp,
			   OSDOp& osd_op, bufferlist& bl)
{
  // decode
  bufferlist header;
  map<string, bufferlist> m;
  if (bl.length()) {
    bufferlist::iterator p = bl.begin();
    ::decode(header, p);
    ::decode(m, p);
    assert(p.end());
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
    case CEPH_OSD_TMAP_RM: // remove key
      ::decode(key, bp);
      if (!m.count(key)) {
	return -ENOENT;
      }
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_RMSLOPPY: // remove key
      ::decode(key, bp);
      m.erase(key);
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
  bufferlist obl;
  ::encode(header, obl);
  ::encode(m, obl);

  // write it out
  vector<OSDOp> nops(1);
  OSDOp& newop = nops[0];
  newop.op.op = CEPH_OSD_OP_WRITEFULL;
  newop.op.extent.offset = 0;
  newop.op.extent.length = obl.length();
  newop.indata = obl;
  do_osd_ops(ctx, nops);
  osd_op.outdata.claim(newop.outdata);
  return 0;
}

void OSDVol::update_snap_map(vector<vol_log_entry_t> &log_entries,
			     ObjectStore::Transaction &t)
{
  for (vector<vol_log_entry_t>::iterator i = log_entries.begin();
       i != log_entries.end();
       ++i) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    if (i->soid.snap < CEPH_MAXSNAP) {
      if (i->is_delete()) {
	int r = snap_mapper.remove_oid(
	  i->soid,
	  &_t);
	assert(r == 0);
      } else {
	assert(i->snaps.length() > 0);
	vector<snapid_t> snaps;
	bufferlist::iterator p = i->snaps.begin();
	try {
	  ::decode(snaps, p);
	} catch (...) {
	  snaps.clear();
	}
	set<snapid_t> _snaps(snaps.begin(), snaps.end());

	if (i->is_clone()) {
	  snap_mapper.add_oid(
	    i->soid,
	    _snaps,
	    &_t);
	} else {
	  assert(i->is_modify());
	  int r = snap_mapper.update_snaps(
	    i->soid,
	    _snaps,
	    0,
	    &_t);
	  assert(r == 0);
	}
      }
    }
  }
}

void OSDVol::sub_op_modify_applied(RepModify *rm)
{
  lock();
  rm->op->mark_event("sub_op_applied");
  rm->applied = true;

  dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op->request
	   << " from epoch " << rm->epoch_started << " < last_peering_reset "
	   << dendl;

  bool done = rm->applied && rm->committed;
  unlock();
  if (done) {
    delete rm->ctx;
    delete rm;
  }
}

void OSDVol::sub_op_modify_commit(RepModify *rm)
{
  lock();
  rm->op->mark_commit_sent();
  rm->committed = true;

  dout(10) << "sub_op_modify_commit " << rm << " op " << *rm->op->request
	   << " from epoch " << rm->epoch_started << " < last_peering_reset "
	   << dendl;

  log_subop_stats(rm->op, l_osd_sop_w_inb, l_osd_sop_w_lat);
  bool done = rm->applied && rm->committed;
  unlock();
  if (done) {
    delete rm->ctx;
    delete rm;
  }
}

void OSDVol::check_blacklisted_obc_watchers(ObjectContext *obc)
{
  dout(20) << "OSDVol::check_blacklisted_obc_watchers for obc "
	   << obc->obs.oi.soid << dendl;
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator k =
	 obc->watchers.begin();
       k != obc->watchers.end();
    ) {
    //Advance iterator now so handle_watch_timeout() can erase element
    map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j = k++;
    dout(30) << "watch: Found " << j->second->get_entity() << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (get_osdmap()->is_blacklisted(ea)) {
      dout(10) << "watch: Found blacklisted watcher for " << ea << dendl;
      handle_watch_timeout(j->second);
    }
  }
}

void OSDVol::log_subop_stats(OpRequestRef op, int tag_inb, int tag_lat)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->request->get_recv_stamp();

  uint64_t inb = op->request->get_data().length();

  osd->logger->inc(l_osd_sop);

  osd->logger->inc(l_osd_sop_inb, inb);
  osd->logger->tinc(l_osd_sop_lat, latency);

  if (tag_inb)
    osd->logger->inc(tag_inb, inb);
  osd->logger->tinc(tag_lat, latency);

  dout(15) << "log_subop_stats " << *op->request << " inb " << inb << " latency " << latency << dendl;
}

void OSDVol::handle_watch_timeout(WatchRef watch)
{
  ObjectContext *obc = watch->get_obc(); // handle_watch_timeout owns this ref
  dout(10) << "handle_watch_timeout obc " << obc << dendl;

  obc->watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  obc->obs.oi.watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  watch->remove();

  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, obc->ssc, this);
  ctx->mtime = ceph_clock_now(g_ceph_context);

  ctx->at_version.epoch = get_osdmap()->get_epoch();
  ctx->at_version.version = vol_log.get_head().version + 1;

  entity_inst_t nobody;

  /* Currently, mode.try_write always returns true.  If this changes, we will
   * need to delay the repop accordingly */
  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;

  ctx->log.push_back(vol_log_entry_t(vol_log_entry_t::MODIFY, obc->obs.oi.soid,
				     ctx->at_version,
				     obc->obs.oi.version,
				     osd_reqid_t(), ctx->mtime));

  eversion_t old_version = repop->obc->obs.oi.version;

  obc->obs.oi.prior_version = old_version;
  obc->obs.oi.version = ctx->at_version;
  bufferlist bl;
  ::encode(obc->obs.oi, bl);
  t->setattr(coll_t(), obc->obs.oi.soid, OI_ATTR, bl);

  append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);

  eval_repop(repop);
}

OSDVol::RepGather *OSDVol::new_repop(OpContext *ctx, ObjectContext *obc,
				     tid_t rep_tid)
{
  if (ctx->op)
    dout(10) << "new_repop rep_tid " << rep_tid << " on "
	     << *ctx->op->request << dendl;
  else
    dout(10) << "new_repop rep_tid " << rep_tid << " (no op)" << dendl;

  RepGather *repop = new RepGather(ctx, obc, rep_tid, info.last_complete);

  repop->start = ceph_clock_now(g_ceph_context);

  repop_queue.push_back(&repop->queue_item);
  repop_map[repop->rep_tid] = repop;
  repop->get();

  osd->logger->set(l_osd_op_wip, repop_map.size());

  return repop;
}

void OSDVol::eval_repop(RepGather *repop)
{
  MOSDOp *m = NULL;
  if (repop->ctx->op)
    m = static_cast<MOSDOp *>(repop->ctx->op->request);

  if (m)
    dout(10) << "eval_repop " << *repop
	     << " wants=" << (m->wants_ack() ? "a":"") << (m->wants_ondisk() ? "d":"")
	     << (repop->done ? " DONE" : "")
	     << dendl;
  else
    dout(10) << "eval_repop " << *repop << " (no op)"
	     << (repop->done ? " DONE" : "")
	     << dendl;

  if (repop->done)
    return;

  // apply?
  if (!repop->applied && !repop->applying &&
      repop->waitfor_ack.size() == 1)
    apply_repop(repop);

  if (m) {

    // an 'ondisk' reply implies 'ack'. so, prefer to send just one
    // ondisk instead of ack followed by ondisk.

    // ondisk?
    if (repop->waitfor_disk.empty()) {

      log_op_stats(repop->ctx);
      publish_stats_to_osd();

      // send dup commits, in order
      if (waiting_for_ondisk.count(repop->v)) {
	assert(waiting_for_ondisk.begin()->first == repop->v);
	for (list<OpRequestRef>::iterator i = waiting_for_ondisk[repop->v].begin();
	     i != waiting_for_ondisk[repop->v].end();
	     ++i) {
	  osd->reply_op_error(*i, 0, repop->v);
	}
	waiting_for_ondisk.erase(repop->v);
      }

      // clear out acks, we sent the commits above
      if (waiting_for_ack.count(repop->v)) {
	assert(waiting_for_ack.begin()->first == repop->v);
	waiting_for_ack.erase(repop->v);
      }

      if (m->wants_ondisk() && !repop->sent_disk) {
	// send commit.
	MOSDOpReply *reply = repop->ctx->reply;
	if (reply)
	  repop->ctx->reply = NULL;
	else
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending commit on " << *repop << " " << reply << dendl;
	assert(entity_name_t::TYPE_OSD != m->get_connection()->peer_type);
	osd->send_message_osd_client(reply, m->get_connection());
	repop->sent_disk = true;
	repop->ctx->op->mark_commit_sent();
      }
    }

    // applied?
    if (repop->waitfor_ack.empty()) {

      // send dup acks, in order
      if (waiting_for_ack.count(repop->v)) {
	assert(waiting_for_ack.begin()->first == repop->v);
	for (list<OpRequestRef>::iterator i = waiting_for_ack[repop->v].begin();
	     i != waiting_for_ack[repop->v].end();
	     ++i) {
	  MOSDOp *m = (MOSDOp*)(*i)->request;
	  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
	  reply->add_flags(CEPH_OSD_FLAG_ACK);
	  osd->send_message_osd_client(reply, m->get_connection());
	}
	waiting_for_ack.erase(repop->v);
      }

      if (m->wants_ack() && !repop->sent_ack && !repop->sent_disk) {
	// send ack
	MOSDOpReply *reply = repop->ctx->reply;
	if (reply)
	  repop->ctx->reply = NULL;
	else
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
	reply->add_flags(CEPH_OSD_FLAG_ACK);
	dout(10) << " sending ack on " << *repop << " " << reply << dendl;
	assert(entity_name_t::TYPE_OSD != m->get_connection()->peer_type);
	osd->send_message_osd_client(reply, m->get_connection());
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
  if (repop->waitfor_ack.empty() && repop->waitfor_disk.empty() &&
      repop->applied) {
    repop->done = true;

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

class C_OSD_OpApplied : public Context {
public:
  OSDVol *vol;
  OSDVol::RepGather *repop;

  C_OSD_OpApplied(OSDVol *v,
		  OSDVol::RepGather *rg) :
    vol(v), repop(rg) {
    repop->get();
  }
  void finish(int r) {
    vol->op_applied(repop);
  }
};

class C_OSD_OpCommit : public Context {
public:
  OSDVol *vol;
  OSDVol::RepGather *repop;

  C_OSD_OpCommit(OSDVol *v, OSDVol::RepGather *rg) :
    vol(v), repop(rg) {
    repop->get();
  }
  void finish(int r) {
    vol->op_commit(repop);
  }
};

void OSDVol::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applying);
  assert(!repop->applied);

  repop->applying = true;

  repop->tls.push_back(&repop->ctx->local_t);
  repop->tls.push_back(&repop->ctx->op_t);

  repop->obc->ondisk_write_lock();
  if (repop->ctx->clone_obc)
    repop->ctx->clone_obc->ondisk_write_lock();

  Context *oncommit = new C_OSD_OpCommit(this, repop);
  Context *onapplied = new C_OSD_OpApplied(this, repop);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(repop->obc,
							repop->ctx->clone_obc);
  int r = osd->store->queue_transactions(osr.get(), repop->tls, onapplied,
					 oncommit, onapplied_sync,
					 repop->ctx->op);
  if (r) {
    derr << "apply_repop  queue_transactions returned " << r << " on "
	 << *repop << dendl;
    assert(0);
  }
}

void OSDVol::queue_snap_trim()
{
  if (osd->queue_for_snap_trim(this))
    dout(10) << "queue_snap_trim -- queuing" << dendl;
  else
    dout(10) << "queue_snap_trim -- already trimming" << dendl;
}

void OSDVol::remove_repop(RepGather *repop)
{
  repop->put();
}

void OSDVol::op_applied(RepGather *repop)
{
  lock();
  if (repop->ctx->op)
    repop->ctx->op->mark_event("op_applied");

  repop->applying = false;
  repop->applied = true;

  // (logical) local ack.
  int whoami = osd->get_nodeid();

  if (repop->ctx->clone_obc) {
    put_object_context(repop->ctx->clone_obc);
    repop->ctx->clone_obc = 0;
  }
  if (repop->ctx->snapset_obc) {
    put_object_context(repop->ctx->snapset_obc);
    repop->ctx->snapset_obc = 0;
  }

  put_object_context(repop->obc);
  put_object_contexts(repop->src_obc);
  repop->obc = 0;

  if (!repop->aborted) {
    assert(repop->waitfor_ack.count(whoami) ||
	   repop->waitfor_disk.count(whoami) == 0);  // commit before ondisk
    repop->waitfor_ack.erase(whoami);

    assert(info.last_update >= repop->v);
    assert(last_update_applied < repop->v);
    last_update_applied = repop->v;
  }

  if (!repop->aborted)
    eval_repop(repop);

  repop->put();
  unlock();
}

void OSDVol::op_commit(RepGather *repop)
{
  lock();
  if (repop->ctx->op)
    repop->ctx->op->mark_event("op_commit");

  if (repop->aborted) {
    dout(10) << "op_commit " << *repop << " -- aborted" << dendl;
  } else if (repop->waitfor_disk.count(osd->get_nodeid()) == 0) {
    dout(10) << "op_commit " << *repop << " -- already marked ondisk" << dendl;
  } else {
    dout(10) << "op_commit " << *repop << dendl;
    int whoami = osd->get_nodeid();

    repop->waitfor_disk.erase(whoami);

    // remove from ack waitfor list too.  sub_op_modify_commit()
    // behaves the same in that the COMMIT implies and ACK and there
    // is no separate reply sent.
    repop->waitfor_ack.erase(whoami);

    last_update_ondisk = repop->v;

    last_complete_ondisk = repop->vol_local_last_complete;
    eval_repop(repop);
  }

  repop->put();
  unlock();
}

void OSDVol::snap_trimmer(void)
{
  hobject_t pos;

  lock();
  if (deleting) {
    unlock();
    return;
  }

  entity_inst_t nobody;
  dout(10) << "snap_trimmer posting" << dendl;

  snapid_t snap_to_trim = snap_trimq.range_start();

  // Get next
  int r = snap_mapper.get_next_object_to_trim(snap_to_trim, &pos);

  if (r == -ENOENT) {
    // Done!
    unlock();
    return;
  }

  RepGather *repop = trim_object(pos);
  assert(repop);


  repop->queue_snap_trimmer = true;

  append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);
  eval_repop(repop);

  unlock();
  return;
}

OSDVol::RepGather *OSDVol::trim_object(const hobject_t &coid)
{
  // load clone info
  bufferlist bl;
  ObjectContext *obc = 0;
  find_object_context(coid, &obc, false, NULL);

  object_info_t &coi = obc->obs.oi;
  set<snapid_t> old_snaps(coi.snaps.begin(), coi.snaps.end());

  // get snap set context
  if (!obc->ssc)
    obc->ssc = get_snapset_context(coid.oid, false);

  SnapSet& snapset = obc->ssc->snapset;

  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(
    OpRequestRef(),
    reqid,
    ops,
    &obc->obs,
    obc->ssc,
    this);
  ctx->mtime = ceph_clock_now(g_ceph_context);

  ctx->at_version.epoch = get_osdmap()->get_epoch();
  ctx->at_version.version = vol_log.get_head().version + 1;

  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;
  OSDriver::OSTransaction os_t(osdriver.get_transaction(t));

  set<snapid_t> new_snaps;

  snap_mapper.update_snaps(coid, new_snaps,
			   &old_snaps, // debug
			   &os_t);

  if (new_snaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << old_snaps << " -> "
	     << new_snaps << " ... deleting" << dendl;
    t->remove(coll_t(), coid);

    // ...from snapset
    snapid_t last = coid.snap;
    vector<snapid_t>::iterator p;
    for (p = snapset.clones.begin(); p != snapset.clones.end(); ++p)
      if (*p == last)
	break;
    assert(p != snapset.clones.end());
    object_stat_sum_t delta;
    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      interval_set<uint64_t> keep;
      keep.union_of(
	snapset.clone_overlap[*n],
	snapset.clone_overlap[*p]);
      add_interval_usage(keep, delta);  // not deallocated
      snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);
    } else {
      add_interval_usage(
	snapset.clone_overlap[last],
	delta);  // not deallocated
    }
    delta.num_objects--;
    delta.num_object_clones--;
    delta.num_bytes -= snapset.clone_size[last];
    info.stats.stats.add(delta, obc->obs.oi.category);

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);

    ctx->log.push_back(
      vol_log_entry_t(
	vol_log_entry_t::DELETE,
	coid,
	ctx->at_version,
	ctx->obs->oi.version,
	osd_reqid_t(),
	ctx->mtime)
      );
    ctx->at_version.version++;
  } else {
    // save adjusted snaps for this object
    coi.snaps = vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());

    coi.prior_version = coi.version;
    coi.version = ctx->at_version;
    bl.clear();
    ::encode(coi, bl);
    t->setattr(coll_t(), coid, OI_ATTR, bl);

    ctx->log.push_back(
      vol_log_entry_t(vol_log_entry_t::MODIFY,
		      coid, coi.version,
		      coi.prior_version,
		      osd_reqid_t(),
		      ctx->mtime));
    ::encode(coi.snaps, ctx->log.back().snaps);
    ctx->at_version.version++;
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << dendl;

  hobject_t snapoid(coid.oid, snapset.head_exists ? CEPH_NOSNAP:CEPH_SNAPDIR);
  ctx->snapset_obc = get_object_context(snapoid, false);
  assert(ctx->snapset_obc->registered);

  if (snapset.clones.empty() && !snapset.head_exists) {
    dout(10) << coid << " removing " << snapoid << dendl;
    ctx->log.push_back(
      vol_log_entry_t(vol_log_entry_t::DELETE, snapoid,
		      ctx->at_version, ctx->snapset_obc->obs.oi.version,
		      osd_reqid_t(), ctx->mtime));
    ctx->snapset_obc->obs.exists = false;

    t->remove(coll_t(), snapoid);
  } else {
    dout(10) << coid << " updating snapset on " << snapoid << dendl;
    ctx->log.push_back(
      vol_log_entry_t(vol_log_entry_t::MODIFY,
		      snapoid, ctx->at_version,
		      ctx->snapset_obc->obs.oi.version,
		      osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc->obs.oi.prior_version =
      ctx->snapset_obc->obs.oi.version;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;

    bl.clear();
    ::encode(snapset, bl);
    t->setattr(coll_t(), snapoid, SS_ATTR, bl);

    bl.clear();
    ::encode(ctx->snapset_obc->obs.oi, bl);
    t->setattr(coll_t(), snapoid, OI_ATTR, bl);
  }

  return repop;
}

void OSDVol::do_pending_flush()
{
  osr->flush();
}

bool OSDVol::op_has_sufficient_caps(OpRequestRef op)
{
  // only check MOSDOp
  if (op->request->get_type() != CEPH_MSG_OSD_OP)
    return true;

  MOSDOp *req = static_cast<MOSDOp*>(op->request);

  OSD::Session *session = (OSD::Session *)req->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();

  bool cap = caps.is_capable(req->get_oid(),
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->need_class_read_cap(),
			     op->need_class_write_cap());

  dout(20) << "op_has_sufficient_caps "
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " need_class_read_cap=" << op->need_class_read_cap()
	   << " need_class_write_cap=" << op->need_class_write_cap()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

bool OSDVol::can_discard_request(OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op);
  }

  return false;
}

bool OSDVol::can_discard_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  if (OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  }

  return false;
}

bool OSDVol::op_must_wait_for_map(CohortOSDMapRef curmap, OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDOp*>(op->request)->get_map_epoch());

  case MSG_OSD_SUBOP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDSubOp*>(op->request)->map_epoch);

  case MSG_OSD_SUBOPREPLY:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDSubOpReply*>(op->request)->map_epoch);
  }
  assert(0);
  return false;
}

void OSDVol::write_info(ObjectStore::Transaction& t)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();

  _write_info(t, get_osdmap()->get_epoch(), info,
	      osd->infos_oid, dirty_big_info);

  last_persisted_osdmap_ref = osdmap_ref;

  dirty_info = false;
  dirty_big_info = false;
}

int OSDVol::_write_info(ObjectStore::Transaction& t, epoch_t epoch,
			vol_info_t &info,
			hobject_t &infos_oid,
			bool dirty_big_info)
{
  // info.  store purged_snaps separately.
  interval_set<snapid_t> purged_snaps;
  map<string,bufferlist> v;
  ::encode(epoch, v[Volume::get_epoch_key(info.volid)]);
  purged_snaps.swap(info.purged_snaps);
  ::encode(info, v[Volume::get_info_key(info.volid)]);
  purged_snaps.swap(info.purged_snaps);

  if (dirty_big_info) {
    // potentially big stuff
    bufferlist& bigbl = v[Volume::get_biginfo_key(info.volid)];
    ::encode(info.purged_snaps, bigbl);
  }

  t.omap_setkeys(coll_t::META_COLL, infos_oid, v);

  return 0;
}
