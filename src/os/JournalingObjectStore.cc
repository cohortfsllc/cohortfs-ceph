// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "JournalingObjectStore.h"

#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_journal
#undef dout_prefix
#define dout_prefix *_dout << "journal "



void JournalingObjectStore::journal_start()
{
  ldout(cct, 10) << "journal_start" << dendl;
  finisher.start();
}

void JournalingObjectStore::journal_stop()
{
  ldout(cct, 10) << "journal_stop" << dendl;
  finisher.stop();
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }
  apply_manager.reset();
}

int JournalingObjectStore::journal_replay(uint64_t fs_op_seq)
{
  ldout(cct, 10) << "journal_replay fs op_seq " << fs_op_seq << dendl;

  if (cct->_conf->journal_replay_from) {
    ldout(cct, 0) << "journal_replay forcing replay from " << cct->_conf->journal_replay_from
	    << " instead of " << fs_op_seq << dendl;
    // the previous op is the last one committed
    fs_op_seq = cct->_conf->journal_replay_from - 1;
  }

  ZTracer::Trace trace; // empty trace for do_transactions()
  uint64_t op_seq = fs_op_seq;
  apply_manager.init_seq(fs_op_seq);

  if (!journal)
    return 0;

  int err = journal->open(op_seq);
  if (err < 0) {
    ldout(cct, 3) << "journal_replay open failed with "
	    << cpp_strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  replaying = true;

  int count = 0;
  while (1) {
    bufferlist bl;
    uint64_t seq = op_seq + 1;
    if (!journal->read_entry(bl, seq)) {
      ldout(cct, 3) << "journal_replay: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      ldout(cct, 3) << "journal_replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);

    ldout(cct, 3) << "journal_replay: applying op seq " << seq << dendl;
    bufferlist::iterator p = bl.begin();
    list<Transaction*> tls;
    while (!p.end()) {
      Transaction *t = new Transaction(p);
      tls.push_back(t);
    }

    int r = do_transactions(tls, seq, trace);

    op_seq = seq;

    while (!tls.empty()) {
      delete tls.front();
      tls.pop_front();
    }

    ldout(cct, 3) << "journal_replay: r = " << r << ", op_seq now " << op_seq << dendl;
  }

  replaying = false;

  submit_op_seq.store(op_seq);

  // done reading, make writeable.
  err = journal->make_writeable();
  if (err < 0)
    return err;

  return count;
}


// ------------------------------------

uint64_t JournalingObjectStore::ApplyManager::op_apply_start(uint64_t op)
{
  unique_lock l(apply_lock);
  while (blocked) {
    // note: this only happens during journal replay
    ldout(cct, 10) << "op_apply_start blocked, waiting" << dendl;
    blocked_cond.wait(l);
  }
  ldout(cct, 10) << "op_apply_start " << op << " open_ops " << open_ops << " -> " << (open_ops+1) << dendl;
  assert(!blocked);
  assert(op > committed_seq);
  open_ops++;
  return op;
}

void JournalingObjectStore::ApplyManager::op_apply_finish(uint64_t op)
{
  unique_lock l(apply_lock);
  ldout(cct, 10) << "op_apply_finish " << op << " open_ops " << open_ops
	   << " -> " << (open_ops-1)
	   << ", max_applied_seq " << max_applied_seq << " -> " << MAX(op, max_applied_seq)
	   << dendl;
  --open_ops;
  assert(open_ops >= 0);

  // signal a blocked commit_start (only needed during journal replay)
  if (blocked) {
    blocked_cond.notify_all();
  }

  // there can be multiple applies in flight; track the max value we
  // note.  note that we can't _read_ this value and learn anything
  // meaningful unless/until we've quiesced all in-flight applies.
  if (op > max_applied_seq)
    max_applied_seq = op;
}

void JournalingObjectStore::ApplyManager::add_waiter(uint64_t op, Context *c)
{
  unique_lock l(com_lock);
  assert(c);
  commit_waiters[op].push_back(*c);
}

bool JournalingObjectStore::ApplyManager::commit_start()
{
  bool ret = false;

  uint64_t _committing_seq = 0;
  {
    unique_lock l(apply_lock);
    ldout(cct, 10) << "commit_start max_applied_seq " << max_applied_seq
	     << ", open_ops " << open_ops
	     << dendl;
    blocked = true;
    while (open_ops > 0) {
      ldout(cct, 10) << "commit_start waiting for " << open_ops << " open ops to drain" << dendl;
      blocked_cond.wait(l);
    }
    assert(open_ops == 0);
    ldout(cct, 10) << "commit_start blocked, all open_ops have completed" << dendl;
    {
      unique_lock cl(com_lock);
      if (max_applied_seq == committed_seq) {
	ldout(cct, 10) << "commit_start nothing to do" << dendl;
	blocked = false;
	assert(commit_waiters.empty());
	goto out;
      }

      _committing_seq = committing_seq = max_applied_seq;

      ldout(cct, 10) << "commit_start committing " << committing_seq
	       << ", still blocked" << dendl;
    }
  }
  ret = true;

 out:
  if (journal)
    journal->commit_start(_committing_seq);  // tell the journal too
  return ret;
}

void JournalingObjectStore::ApplyManager::commit_started()
{
  unique_lock l(apply_lock);
  // allow new ops. (underlying fs should now be committing all prior ops)
  ldout(cct, 10) << "commit_started committing " << committing_seq << ", unblocking" << dendl;
  blocked = false;
  blocked_cond.notify_all();
}

void JournalingObjectStore::ApplyManager::commit_finish()
{
  unique_lock l(com_lock);
  ldout(cct, 10) << "commit_finish thru " << committing_seq << dendl;

  if (journal)
    journal->committed_thru(committing_seq);

  committed_seq = committing_seq;

  map<version_t, Context::List>::iterator p = commit_waiters.begin();
  while (p != commit_waiters.end() &&
    p->first <= committing_seq) {
    finisher.queue(std::move(p->second));
    commit_waiters.erase(p++);
  }
}

void JournalingObjectStore::_op_journal_transactions(
  list<Transaction*>& tls, uint64_t op,
  Context *onjournal, OpRequestRef osd_op, ZTracer::Trace &trace)
{
  ldout(cct, 10) << "op_journal_transactions " << op << " " << tls << dendl;

  if (journal && journal->is_writeable()) {
    bufferlist tbl;
    unsigned data_len = 0;
    int data_align = -1; // -1 indicates that we don't care about the alignment
    for (list<Transaction*>::iterator p = tls.begin();
	 p != tls.end(); ++p) {
      Transaction *t = *p;
      if (t->get_data_length() > data_len &&
	(int)t->get_data_length() >= cct->_conf->journal_align_min_size) {
	data_len = t->get_data_length();
	data_align = (t->get_data_alignment() - tbl.length()) & ~CEPH_PAGE_MASK;
      }
      ::encode(*t, tbl);
    }
    journal->submit_entry(op, tbl, data_align, onjournal, trace, osd_op);
  } else if (onjournal) {
    apply_manager.add_waiter(op, onjournal);
  }
}
