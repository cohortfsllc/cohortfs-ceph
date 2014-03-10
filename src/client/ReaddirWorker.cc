// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ReaddirWorker.h"

#include "Client.h"
#include "DirStripe.h"
#include "Inode.h"
#include "MetaRequest.h"
#include "MetaSession.h"

#include "mds/MDSMap.h"
#include "messages/MClientReply.h"

#include "include/assert.h"


#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "readdir" << " "


static const uint32_t EXISTING = 1 << 31;

static bool is_existing_chunk(uint32_t c) { return c & EXISTING; }
static uint32_t get_existing_chunk(uint32_t c) { return c & ~EXISTING; }
static uint32_t make_existing_chunk(uint32_t c) { return c | EXISTING; }


ReaddirWorker::ReaddirWorker(Client *client)
  : client(client),
    cct(client->cct),
    mutex(client->client_lock),
    shutdown(false),
    mdsmap_epoch(0),
    cond_done_ignored(false),
    cond_result_ignored(0)
{
}

bool ReaddirWorker::send_request(request_info &req)
{
  assert(mutex.is_locked());
  if (req.dir->err) {
    // signal dir result
    req.dir->chunk_error(req.stripe->stripeid, req.dir->err);

    // clean up unsent request
    client->put_request(req.request);
    return true;
  }

  int mds = client->choose_target_mds(req.request);
  if (mds < 0 || !client->mdsmap->is_active_or_stopping(mds)) {
    ldout(cct, 10) << " target mds." << mds << " not active, waiting "
      "for new mdsmap" << dendl;

    // add ourselves to waiting_for_mdsmap only once per epoch
    epoch_t epoch = client->mdsmap->get_epoch();
    if (mdsmap_epoch != epoch) {
      mdsmap_epoch = epoch;
      client->waiting_for_mdsmap.push_back(&cond);
    }
    return false;
  }

  // open a session?
  MetaSession *session = client->_get_or_open_mds_session(mds);

  // wait
  if (session->state == MetaSession::STATE_OPENING) {
    ldout(cct, 10) << "waiting for session to mds." << mds
      << " to open" << dendl;
    if (req.opening_session != session) {
      req.opening_session = session;
      session->waiting_for_open.push_back(new C_Cond(&cond, &cond_done_ignored,
						     &cond_result_ignored));
    }
    return false;
  }

  req.request->caller_cond = &cond;
  client->send_request(req.request, session);

  req.request->kick = false;

  waiting_for_reply.push_back(req);
  return true;
}

bool ReaddirWorker::handle_reply(request_info &req)
{
  assert(mutex.is_locked());
  // move back to the send queue on kick/forward
  if (req.request->kick || req.request->resend_mds >= 0) {
    req.dir->get(); // take another reference for send queue
    waiting_to_send.push_back(req);
    return true;
  }

  // wait for a reply
  if (!req.request->reply)
    return false;

  DirStripeReader *s = req.stripe;

  int r = client->complete_request(req.request);
  if (r) {
    // signal dir result
    req.dir->chunk_error(s->stripeid, r);
    return true;
  }

  ldout(cct, 10) << "reply for stripe " << s->stripeid
    << " offset " << hex << req.request->readdir_offset << dec
    << " size " << req.request->readdir_num << dendl;

  if (req.request->readdir_end) {
    // mark stripe cache complete
    DirStripe *stripe = req.dir->inode->stripes[s->stripeid];
    if (stripe && stripe->release_count == s->release_count &&
	stripe->shared_gen == s->shared_gen) {
      ldout(cct, 10) << " marking I_COMPLETE on " << *stripe << dendl;
      stripe->set_complete();
    }
  }

  // discard reply after seek
  if (s->offset != req.request->readdir_offset) {
    assert(s->readahead > 0);
    s->readahead--;
    // if refetching an existing chunk, reset pending and signal
    if (is_existing_chunk(req.chunk)) {
      uint32_t index = get_existing_chunk(req.chunk);
      assert(index < req.dir->chunks.size());
      DirChunk &chunk = req.dir->chunks[index];
      assert(chunk.is_pending());
      chunk.set_pending(false);
      req.dir->cond.Signal();
    }
    return true;
  }

  // update stripe result
  s->offset += req.request->readdir_num;
  swap(s->last_name, req.request->readdir_last_name);

  if (req.request->readdir_end) {
    s->last_name.clear();
    s->eof = true;
  }

  if (!is_existing_chunk(req.chunk)) {
    // add a new chunk
    req.dir->chunks.push_back(DirChunk());
    DirChunk &chunk = req.dir->chunks.back();

    chunk.stripeid = s->stripeid;
    chunk.offset = req.request->readdir_offset;
    swap(chunk.last_name, req.request->readdir_start);
    swap(chunk.buffer, req.request->readdir_result);
    chunk.count = chunk.buffer->size();
    chunk.set_end(s->eof);

    uint32_t index = req.dir->chunks.size() - 1;
    req.stripe->buffered.insert(index);

    // signal dir result
    req.dir->cond.Signal();

    // continue readahead
    stripe_readahead(req.dir, req.stripe, index);
    return true;
  }

  // reading an existing chunk
  uint32_t index = get_existing_chunk(req.chunk);
  assert(index < req.dir->chunks.size());
  DirChunk &chunk = req.dir->chunks[index];
  assert(chunk.is_pending());

  if (!chunk.buffer) {
    swap(chunk.buffer, req.request->readdir_result);
    req.stripe->buffered.insert(index);
  } else
    chunk.buffer->append(*req.request->readdir_result);

  if (chunk.buffer->size() < chunk.count) {
    // request the rest of the chunk
    read_chunk(req.dir, index);
    s->readahead--;
    return true;
  }

  chunk.set_reading(false);
  chunk.set_pending(false);
  // signal dir result
  req.dir->cond.Signal();

  // continue readahead
  stripe_readahead(req.dir, req.stripe, index);
  return true;
}

void ReaddirWorker::stripe_readahead(DirReader *d, DirStripeReader *s,
				     uint32_t chunk_index)
{
  if (s->eof || s->readahead >= cct->_conf->mds_readdir_readahead)
    return;

  // search for the next chunk in this stripe
  for (uint32_t i = chunk_index + 1; i < d->chunks.size(); i++) {
    if (d->chunks[i].stripeid == s->stripeid) {
      if (d->chunks[i].buffer)
	continue;

      if (d->chunks[i].is_pending()) {
	// only one pending request per stripe
	assert(d->chunks[i].offset == s->offset);
	return;
      }

      read_chunk(d, i);
      return;
    }
  }

  read_stripe(d, s);
}

void* ReaddirWorker::entry()
{
  Mutex::Locker lock(mutex);

  req_list::iterator i;

  while (!shutdown) {
    // handle replies
    i = waiting_for_reply.begin();
    while (i != waiting_for_reply.end()) {
      if (handle_reply(*i)) {
	i->dir->put(); // drop reference
	waiting_for_reply.erase(i++);
      } else
	i++;
    }

    // send requests
    i = waiting_to_send.begin();
    while (i != waiting_to_send.end()) {
      if (send_request(*i))
	waiting_to_send.erase(i++);
      else
	i++;
    }

    cond.Wait(mutex);
  }

  assert(waiting_to_send.empty());
  assert(waiting_for_reply.empty());
  return NULL;
}

MetaRequest* ReaddirWorker::create_request(DirReader *d, DirStripeReader *s,
					   uint32_t max_entries) const
{
  int op = CEPH_MDS_OP_READDIR;
  if (d->inode->snapid == CEPH_SNAPDIR)
    op = CEPH_MDS_OP_LSSNAP;

  MetaRequest *req = new MetaRequest(op);
  d->inode->make_nosnap_relative_path(req->path);
  req->set_inode(d->inode);
  req->head.args.readdir.stripe = s->stripeid;
  if (s->last_name.length()) {
    req->path2.set_path(s->last_name.c_str());
    req->readdir_start = s->last_name;
  }
  req->readdir_offset = s->offset;
  req->readdir_stripe = s->stripeid;
  req->head.args.readdir.max_entries = max_entries;

  int use_mds = -1;
  if (d->inode->caps_issued_mask(CEPH_CAP_DIRLAYOUT_SHARED))
    use_mds = d->inode->get_stripe_auth(s->stripeid);

  client->init_request(req, d->uid, d->gid, use_mds);
  return req;
}

void ReaddirWorker::read_stripe(DirReader *d, DirStripeReader *s)
{
  assert(mutex.is_locked());
  assert(!s->eof);
  assert(s->readahead < cct->_conf->mds_readdir_readahead);

  MetaRequest *req = create_request(d, s, cct->_conf->mds_readdir_chunk_size);

  request_info r = { req, d, s };
  d->get();
  waiting_to_send.push_back(r);

  s->readahead++;

  // start the worker thread
  if (!is_started())
    create();
  cond.Signal();
}

void ReaddirWorker::read_chunk(DirReader *d, uint32_t chunk_index)
{
  assert(mutex.is_locked());

  assert(chunk_index < d->chunks.size());
  DirChunk &chunk = d->chunks[chunk_index];
  chunk.set_pending(true);

  DirStripeReader *s = &d->stripes[chunk.stripeid];
  assert(!s->eof);

  // fetch enough entries to complete the chunk
  uint32_t max_entries = chunk.count;
  if (chunk.buffer)
    max_entries -= chunk.buffer->size();

  MetaRequest *req = create_request(d, s, max_entries);

  request_info r = { req, d, s, make_existing_chunk(chunk_index) };
  d->get();
  waiting_to_send.push_back(r);

  s->readahead++;

  // start the worker thread
  if (!is_started())
    create();
  cond.Signal();
}

void ReaddirWorker::stop()
{
  assert(mutex.is_locked());
  shutdown = true;
  if (is_started())
    join();
}

