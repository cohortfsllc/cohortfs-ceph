// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Inktank Storage, Inc.
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef PGBACKEND_H
#define PGBACKEND_H

#include "OSDMap.h"
#include "PGLog.h"
#include "osd_types.h"
#include "common/WorkQueue.h"
#include "osd_types.h"
#include "include/Context.h"
#include "os/ObjectStore.h"
#include "common/LogClient.h"
#include <string>

 /**
  * PGBackend
  *
  * PGBackend defines an interface for logic handling IO and
  * replication on RADOS objects.  The PGBackend implementation
  * is responsible for:
  *
  * 1) Handling client operations
  * 3) Handling object access
  */
 class PGBackend {
 protected:
   ObjectStore *store;
   const coll_t coll;
   const coll_t temp_coll;
 public:
   /**
    * Provides interfaces for PGBackend callbacks
    *
    * The intention is that the parent calls into the PGBackend
    * implementation holding a lock and that the callbacks are
    * called under the same locks.
    */
   class Listener {
   public:
     /// Recovery

     /**
      * Bless a context
      *
      * Wraps a context in whatever outer layers the parent usually
      * uses to call into the PGBackend
      */
     virtual Context *bless_context(Context *c) = 0;
     virtual GenContext<ThreadPool::TPHandle&> *bless_gencontext(
       GenContext<ThreadPool::TPHandle&> *c) = 0;

     virtual void send_message(int to_osd, Message *m) = 0;
     virtual void queue_transaction(
       ObjectStore::Transaction *t,
       OpRequestRef op = OpRequestRef()
       ) = 0;
     virtual epoch_t get_epoch() const = 0;

     virtual std::string gen_dbg_prefix() const = 0;

     virtual const PGLog &get_log() const = 0;
     virtual OSDMapRef pgb_get_osdmap() const = 0;
     virtual const pg_info_t &get_info() const = 0;
     virtual const pg_pool_t &get_pool() const = 0;

     virtual ObjectContextRef get_obc(
       const hobject_t &hoid,
       map<string, bufferlist> &attrs) = 0;

     virtual void op_applied(
       const eversion_t &applied_version) = 0;

     virtual void log_operation(
       vector<pg_log_entry_t> &logv,
       bool transaction_applied,
       ObjectStore::Transaction *t) = 0;

     virtual void update_last_complete_ondisk(
       eversion_t lcod) = 0;

     virtual void update_stats(
       const pg_stat_t &stat) = 0;

     virtual void schedule_work(
       GenContext<ThreadPool::TPHandle&> *c) = 0;

     pg_t whoami_pg_t() const {
       return get_info().pgid;
     }

     virtual entity_name_t get_cluster_msgr_name() = 0;

     virtual PerfCounters *get_logger() = 0;

     virtual ceph_tid_t get_tid() = 0;

     virtual LogClientTemp clog_error() = 0;

     virtual ~Listener() {}
   };
   Listener *parent;
   Listener *get_parent() const { return parent; }
   PGBackend(Listener *l, ObjectStore *store, coll_t coll, coll_t temp_coll) :
     store(store),
     coll(coll),
     temp_coll(temp_coll),
     parent(l), temp_created(false) {}
   OSDMapRef get_osdmap() const { return get_parent()->pgb_get_osdmap(); }
   const pg_info_t &get_info() { return get_parent()->get_info(); }

   std::string gen_prefix() const {
     return parent->gen_dbg_prefix();
   }

   /**
    * RecoveryHandle
    *
    * We may want to recover multiple objects in the same set of
    * messages.  RecoveryHandle is an interface for the opaque
    * object used by the implementation to store the details of
    * the pending recovery operations.
    */
   struct RecoveryHandle {
     virtual ~RecoveryHandle() {}
   };

   /**
    * implementation should clear itself, contexts blessed prior to on_change
    * won't be called after on_change()
    */
   void on_change(ObjectStore::Transaction *t);
   virtual void _on_change(ObjectStore::Transaction *t) = 0;

   virtual void on_flushed() = 0;

   void temp_colls(list<coll_t> *out) {
     if (temp_created)
       out->push_back(temp_coll);
   }
 private:
   bool temp_created;
   set<hobject_t> temp_contents;
 public:
   coll_t get_temp_coll(ObjectStore::Transaction *t);
   coll_t get_temp_coll() const {
    return temp_coll;
   }
   bool have_temp_coll() const { return temp_created; }

   // Track contents of temp collection, clear on reset
   void add_temp_obj(const hobject_t &oid) {
     temp_contents.insert(oid);
   }
   void add_temp_objs(const set<hobject_t> &oids) {
     temp_contents.insert(oids.begin(), oids.end());
   }
   void clear_temp_obj(const hobject_t &oid) {
     temp_contents.erase(oid);
   }
   void clear_temp_objs(const set<hobject_t> &oids) {
     for (set<hobject_t>::const_iterator i = oids.begin();
	  i != oids.end();
	  ++i) {
       temp_contents.erase(*i);
     }
   }

   virtual ~PGBackend() {}

   /**
    * Client IO Interface
    */
   class PGTransaction {
   public:
     /// Write
     virtual void touch(
       const hobject_t &hoid  ///< [in] obj to touch
       ) = 0;
     virtual void stash(
       const hobject_t &hoid,   ///< [in] obj to remove
       version_t former_version ///< [in] former object version
       ) = 0;
     virtual void remove(
       const hobject_t &hoid ///< [in] obj to remove
       ) = 0;
     virtual void setattrs(
       const hobject_t &hoid,         ///< [in] object to write
       map<string, bufferlist> &attrs ///< [in] attrs, may be cleared
       ) = 0;
     virtual void setattr(
       const hobject_t &hoid,         ///< [in] object to write
       const string &attrname,        ///< [in] attr to write
       bufferlist &bl                 ///< [in] val to write, may be claimed
       ) = 0;
     virtual void rmattr(
       const hobject_t &hoid,         ///< [in] object to write
       const string &attrname         ///< [in] attr to remove
       ) = 0;
     virtual void clone(
       const hobject_t &from,
       const hobject_t &to
       ) = 0;
     virtual void rename(
       const hobject_t &from,
       const hobject_t &to
       ) = 0;
     virtual void set_alloc_hint(
       const hobject_t &hoid,
       uint64_t expected_object_size,
       uint64_t expected_write_size
       ) = 0;

     /// Optional, not supported on ec-pool
     virtual void write(
       const hobject_t &hoid, ///< [in] object to write
       uint64_t off,          ///< [in] off at which to write
       uint64_t len,          ///< [in] len to write from bl
       bufferlist &bl         ///< [in] bl to write will be claimed to len
       ) { assert(0); }
     virtual void omap_setkeys(
       const hobject_t &hoid,         ///< [in] object to write
       map<string, bufferlist> &keys  ///< [in] omap keys, may be cleared
       ) { assert(0); }
     virtual void omap_rmkeys(
       const hobject_t &hoid,         ///< [in] object to write
       set<string> &keys              ///< [in] omap keys, may be cleared
       ) { assert(0); }
     virtual void omap_clear(
       const hobject_t &hoid          ///< [in] object to clear omap
       ) { assert(0); }
     virtual void omap_setheader(
       const hobject_t &hoid,         ///< [in] object to write
       bufferlist &header             ///< [in] header
       ) { assert(0); }
     virtual void clone_range(
       const hobject_t &from,         ///< [in] from
       const hobject_t &to,           ///< [in] to
       uint64_t fromoff,              ///< [in] offset
       uint64_t len,                  ///< [in] len
       uint64_t tooff                 ///< [in] offset
       ) { assert(0); }
     virtual void truncate(
       const hobject_t &hoid,
       uint64_t off
       ) { assert(0); }
     virtual void zero(
       const hobject_t &hoid,
       uint64_t off,
       uint64_t len
       ) { assert(0); }

     /// Supported on all backends

     /// off must be the current object size
     virtual void append(
       const hobject_t &hoid, ///< [in] object to write
       uint64_t off,          ///< [in] off at which to write
       uint64_t len,          ///< [in] len to write from bl
       bufferlist &bl         ///< [in] bl to write will be claimed to len
       ) { write(hoid, off, len, bl); }

     /// to_append *must* have come from the same PGBackend (same concrete type)
     virtual void append(
       PGTransaction *to_append ///< [in] trans to append, to_append is cleared
       ) = 0;
     virtual void nop() = 0;
     virtual bool empty() const = 0;
     virtual uint64_t get_bytes_written() const = 0;
     virtual ~PGTransaction() {}
   };
   /// Get implementation specific empty transaction
   virtual PGTransaction *get_transaction() = 0;

   /// execute implementation specific transaction
   virtual void submit_transaction(
     const hobject_t &hoid,               ///< [in] object
     const eversion_t &at_version,        ///< [in] version
     PGTransaction *t,                    ///< [in] trans to execute
     vector<pg_log_entry_t> &log_entries, ///< [in] log entries for t
     Context *on_local_applied_sync,      ///< [in] called when applied locally
     Context *on_all_applied,             ///< [in] called when all acked
     Context *on_all_commit, ///< [in] called when all commit
     ceph_tid_t tid, ///< [in] tid
     osd_reqid_t reqid, ///< [in] reqid
     OpRequestRef op ///< [in] op
     ) = 0;


   /// Trim object stashed at stashed_version
   void trim_stashed_object(
     const hobject_t &hoid,
     version_t stashed_version,
     ObjectStore::Transaction *t);

   /// List objects in collection
   int objects_list_partial(
     const hobject_t &begin,
     int min,
     int max,
     vector<hobject_t> *ls,
     hobject_t *next);

   int objects_list_range(
     const hobject_t &start,
     const hobject_t &end,
     vector<hobject_t> *ls);

   int objects_get_attr(
     const hobject_t &hoid,
     const string &attr,
     bufferlist *out);

   virtual int objects_get_attrs(
     const hobject_t &hoid,
     map<string, bufferlist> *out);

   virtual int objects_read_sync(
     const hobject_t &hoid,
     uint64_t off,
     uint64_t len,
     bufferlist *bl) = 0;

   virtual void objects_read_async(
     const hobject_t &hoid,
     const list<pair<pair<uint64_t, uint64_t>,
		pair<bufferlist*, Context*> > > &to_read,
     Context *on_complete) = 0;

   virtual uint64_t be_get_ondisk_size(
     uint64_t logical_size) { assert(0); return 0; }

   static PGBackend *build_pg_backend(
     const pg_pool_t &pool,
     const OSDMapRef curmap,
     Listener *l,
     coll_t coll,
     coll_t temp_coll,
     ObjectStore *store,
     CephContext *cct);
 };

struct PG_QueueAsync : public Context {
  PGBackend::Listener *pg;
  GenContext<ThreadPool::TPHandle&> *c;
  PG_QueueAsync(
    PGBackend::Listener *pg,
    GenContext<ThreadPool::TPHandle&> *c) : pg(pg), c(c) {}
  void finish(int) {
    pg->schedule_work(c);
  }
};

#endif
