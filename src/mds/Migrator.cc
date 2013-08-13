// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Migrator.h"
#include "Locker.h"
#include "Server.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EString.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/ESessions.h"

#include "msg/Messenger.h"

#include "messages/MClientCaps.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"


/*
 * this is what the dir->dir_auth values look like
 *
 *   dir_auth  authbits  
 * export
 *   me         me      - before
 *   me, me     me      - still me, but preparing for export
 *   me, them   me      - send MExportDir (peer is preparing)
 *   them, me   me      - journaled EExport
 *   them       them    - done
 *
 * import:
 *   them       them    - before
 *   me, them   me      - journaled EImportStart
 *   me         me      - done
 *
 * which implies:
 *  - auth bit is set if i am listed as first _or_ second dir_auth.
 */

#include "common/config.h"


#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) (l <= cct->_conf->debug_mds || l <= cct->_conf->debug_mds_migrator)
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".migrator "

/* This function DOES put the passed message before returning*/
void Migrator::dispatch(Message *m)
{
  m->put();
}

/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: encode_export_dir, file_rename (if foreign)
 *
 * FIXME: the separation between CInode.encode_export and these methods 
 * is pretty arbitrary and dumb.
 */
void Migrator::encode_export_inode(CInode *in, bufferlist& enc_state, 
				   map<client_t,entity_inst_t>& exported_client_map)
{
  dout(7) << "encode_export_inode " << *in << dendl;
  assert(!in->is_replica(mds->get_nodeid()));

  // relax locks?
  if (!in->is_replicated()) {
    in->replicate_relax_locks();
    dout(20) << " did replicate_relax_locks, now " << *in << dendl;
  }

  ::encode(in->inode.ino, enc_state);
  ::encode(in->last, enc_state);
  in->encode_export(enc_state);

  // caps 
  encode_export_inode_caps(in, enc_state, exported_client_map);
}

void Migrator::encode_export_inode_caps(CInode *in, bufferlist& bl, 
					map<client_t,entity_inst_t>& exported_client_map)
{
  dout(20) << "encode_export_inode_caps " << *in << dendl;

  // encode caps
  client_cap_export_map cap_map;
  in->export_client_caps(cap_map);
  ::encode(cap_map, bl);

  in->state_set(CInode::STATE_EXPORTINGCAPS);
  in->get(CInode::PIN_EXPORTINGCAPS);

  // make note of clients named by exported capabilities
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) 
    exported_client_map[it->first] = mds->sessionmap.get_inst(entity_name_t::CLIENT(it->first.v));
}

void Migrator::finish_export_inode_caps(CInode *in)
{
  dout(20) << "finish_export_inode_caps " << *in << dendl;

  in->state_clear(CInode::STATE_EXPORTINGCAPS);
  in->put(CInode::PIN_EXPORTINGCAPS);

  // tell (all) clients about migrating caps.. 
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) {
    Capability *cap = it->second;
    dout(7) << "finish_export_inode telling client." << it->first
	    << " exported caps on " << *in << dendl;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_EXPORT,
				     in->ino(),
				     MDS_INO_ROOT,
				     cap->get_cap_id(), cap->get_last_seq(), 
				     cap->pending(), cap->wanted(), 0,
				     cap->get_mseq());
    mds->send_message_client_counted(m, it->first);
  }
  in->clear_client_caps_after_export();
  mds->locker->eval(in, CEPH_CAP_LOCKS);
}

void Migrator::finish_export_inode(CInode *in, utime_t now, list<Context*>& finished)
{
  dout(12) << "finish_export_inode " << *in << dendl;

  in->finish_export(now);

  finish_export_inode_caps(in);

  // clean
  if (in->is_dirty())
    in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->clear_replica_map();
  
  // twiddle lock states for auth -> replica transition
  in->authlock.export_twiddle();
  in->linklock.export_twiddle();
  in->filelock.export_twiddle();
  in->nestlock.export_twiddle();
  in->xattrlock.export_twiddle();
  in->flocklock.export_twiddle();
  in->policylock.export_twiddle();
  
  // mark auth
  assert(in->is_auth());
  in->state_clear(CInode::STATE_AUTH);
  in->replica_nonce = CInode::EXPORT_NONCE;
  
  in->item_open_file.remove_myself();

  // waiters
  in->take_waiting(CInode::WAIT_ANY_MASK, finished);
  
  // *** other state too?

  // move to end of LRU so we drop out of cache quickly!
  if (in->get_parent_dn()) 
    cache->lru.lru_bottouch(in->get_parent_dn());

}

void Migrator::decode_import_inode(CDentry *dn, bufferlist::iterator& blp,
                                   int oldauth, EMetaBlob *le,
				   LogSegment *ls, uint64_t log_offset,
                                   inode_cap_export_map& cap_imports)
{  
  dout(15) << "decode_import_inode on " << *dn << dendl;

  inodeno_t ino;
  snapid_t last;
  ::decode(ino, blp);
  ::decode(last, blp);

  bool added = false;
  CInode *in = cache->get_inode(ino, last);
  if (!in) {
    in = new CInode(mds->mdcache, true, 1, last);
    added = true;
  } else {
    in->state_set(CInode::STATE_AUTH);
  }

  // state after link  -- or not!  -sage
  in->decode_import(blp, ls);  // cap imports are noted for later action

  // note that we are journaled at this log offset
  in->last_journaled = log_offset;

  // caps
  decode_import_inode_caps(in, blp, cap_imports);

  // link before state  -- or not!  -sage
  if (dn->get_linkage()->get_inode() != in) {
    assert(!dn->get_linkage()->get_inode());
    dn->get_dir()->link_primary_inode(dn, in);
  }
 
  // add inode?
  if (added) {
    cache->add_inode(in);
    dout(10) << "added " << *in << dendl;
  } else {
    dout(10) << "  had " << *in << dendl;
  }

  // adjust replica list
  //assert(!in->is_replica(oldauth));  // not true on failed export
  in->add_replica(oldauth, CInode::EXPORT_NONCE);
  if (in->is_replica(mds->get_nodeid()))
    in->remove_replica(mds->get_nodeid());

  if (le)
    le->add_inode(in, false);
}

void Migrator::decode_import_inode_caps(CInode *in,
					bufferlist::iterator &blp,
                                        inode_cap_export_map& cap_imports)
{
  client_cap_export_map cap_map;
  ::decode(cap_map, blp);
  if (!cap_map.empty()) {
    cap_imports[in].swap(cap_map);
    in->get(CInode::PIN_IMPORTINGCAPS);
  }
}

void Migrator::finish_import_inode_caps(CInode *in, int from, 
                                        client_cap_export_map &cap_map)
{
  assert(!cap_map.empty());
  
  for (client_cap_export_map::iterator it = cap_map.begin();
       it != cap_map.end();
       it++) {
    dout(10) << "finish_import_inode_caps for client." << it->first << " on " << *in << dendl;
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(it->first.v));
    assert(session);

    Capability *cap = in->get_client_cap(it->first);
    if (!cap) {
      cap = in->add_client_cap(it->first, session);
    }
    cap->merge(it->second);

    mds->mdcache->do_cap_import(session, in, cap);
  }

  in->put(CInode::PIN_IMPORTINGCAPS);
}

