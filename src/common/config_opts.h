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

/* note: no header guard */
OPTION(host, OPT_STR, "localhost")
OPTION(fsid, OPT_UUID, uuid_d())
OPTION(public_addr, OPT_ADDR, entity_addr_t())
OPTION(cluster_addr, OPT_ADDR, entity_addr_t())
OPTION(public_network, OPT_STR, "")
OPTION(cluster_network, OPT_STR, "")
OPTION(num_client, OPT_INT, 1)
OPTION(monmap, OPT_STR, "")
OPTION(mon_host, OPT_STR, "")
OPTION(lockdep, OPT_BOOL, false)
OPTION(admin_socket, OPT_STR, "/var/run/ceph/$cluster-$name.asok")

OPTION(daemonize, OPT_BOOL, false)
OPTION(pid_file, OPT_STR, "")
OPTION(chdir, OPT_STR, "/")
OPTION(max_open_files, OPT_LONGLONG, 0)

OPTION(log_file, OPT_STR, "/var/log/ceph/$cluster-$name.log")
OPTION(log_max_new, OPT_INT, 1000)
OPTION(log_max_recent, OPT_INT, 1000000)
OPTION(log_to_stderr, OPT_BOOL, true)
OPTION(err_to_stderr, OPT_BOOL, true)
OPTION(log_to_syslog, OPT_BOOL, false)
OPTION(err_to_syslog, OPT_BOOL, false)
OPTION(log_flush_on_exit, OPT_BOOL, true)

OPTION(clog_to_monitors, OPT_BOOL, true)
OPTION(clog_to_syslog, OPT_BOOL, false)

DEFAULT_SUBSYS(0, 5)
SUBSYS(lockdep, 0, 5)
SUBSYS(context, 0, 5)
SUBSYS(crush, 1, 5)
SUBSYS(mds, 1, 5)
SUBSYS(mds_balancer, 1, 5)
SUBSYS(mds_locker, 1, 5)
SUBSYS(mds_log, 1, 5)
SUBSYS(mds_log_expire, 1, 5)
SUBSYS(mds_migrator, 1, 5)
SUBSYS(buffer, 0, 0)
SUBSYS(timer, 0, 5)
SUBSYS(filer, 0, 5)
SUBSYS(objecter, 0, 0)
SUBSYS(rados, 0, 5)
SUBSYS(rbd, 0, 5)
SUBSYS(journaler, 0, 5)
SUBSYS(objectcacher, 0, 5)
SUBSYS(client, 0, 5)
SUBSYS(osd, 0, 5)
SUBSYS(optracker, 0, 5)
SUBSYS(objclass, 0, 5)
SUBSYS(filestore, 1, 5)
SUBSYS(journal, 1, 5)
SUBSYS(ms, 0, 5)
SUBSYS(mon, 1, 5)
SUBSYS(monc, 0, 5)
SUBSYS(paxos, 0, 5)
SUBSYS(tp, 0, 5)
SUBSYS(auth, 1, 5)
SUBSYS(finisher, 1, 5)
SUBSYS(heartbeatmap, 1, 5)
SUBSYS(perfcounter, 1, 5)
SUBSYS(rgw, 1, 5)                 // log level for the Rados gateway
SUBSYS(hadoop, 1, 5)
SUBSYS(asok, 1, 5)
SUBSYS(throttle, 1, 5)

OPTION(key, OPT_STR, "")
OPTION(keyfile, OPT_STR, "")
OPTION(keyring, OPT_STR, "/etc/ceph/$cluster.keyring,/etc/ceph/keyring,/etc/ceph/keyring.bin")
OPTION(heartbeat_interval, OPT_INT, 5)
OPTION(heartbeat_file, OPT_STR, "")
OPTION(ms_tcp_nodelay, OPT_BOOL, true)
OPTION(ms_initial_backoff, OPT_DOUBLE, .2)
OPTION(ms_max_backoff, OPT_DOUBLE, 15.0)
OPTION(ms_nocrc, OPT_BOOL, false)
OPTION(ms_die_on_bad_msg, OPT_BOOL, false)
OPTION(ms_dispatch_throttle_bytes, OPT_U64, 100 << 20)
OPTION(ms_bind_ipv6, OPT_BOOL, false)
OPTION(ms_rwthread_stack_bytes, OPT_U64, 1024 << 10)
OPTION(ms_tcp_read_timeout, OPT_U64, 900)
OPTION(ms_inject_socket_failures, OPT_U64, 0)
OPTION(mon_data, OPT_STR, "/var/lib/ceph/mon/$cluster-$id")
OPTION(mon_sync_fs_threshold, OPT_INT, 5)   // sync() when writing this many objects; 0 to disable.
OPTION(mon_tick_interval, OPT_INT, 5)
OPTION(mon_subscribe_interval, OPT_DOUBLE, 300)
OPTION(mon_osd_auto_mark_in, OPT_BOOL, false)         // mark any booting osds 'in'
OPTION(mon_osd_auto_mark_auto_out_in, OPT_BOOL, true) // mark booting auto-marked-out osds 'in'
OPTION(mon_osd_auto_mark_new_in, OPT_BOOL, true)      // mark booting new osds 'in'
OPTION(mon_osd_down_out_interval, OPT_INT, 300) // seconds
OPTION(mon_osd_min_up_ratio, OPT_DOUBLE, .3)    // min osds required to be up to mark things down
OPTION(mon_osd_min_in_ratio, OPT_DOUBLE, .3)   // min osds required to be in to mark things out
OPTION(mon_lease, OPT_FLOAT, 5)       // lease interval
OPTION(mon_lease_renew_interval, OPT_FLOAT, 3) // on leader, to renew the lease
OPTION(mon_lease_ack_timeout, OPT_FLOAT, 10.0) // on leader, if lease isn't acked by all peons
OPTION(mon_clock_drift_allowed, OPT_FLOAT, .050) // allowed clock drift between monitors
OPTION(mon_clock_drift_warn_backoff, OPT_FLOAT, 5) // exponential backoff for clock drift warnings
OPTION(mon_accept_timeout, OPT_FLOAT, 10.0)    // on leader, if paxos update isn't accepted
OPTION(mon_pg_create_interval, OPT_FLOAT, 30.0) // no more than every 30s
OPTION(mon_pg_stuck_threshold, OPT_INT, 300) // number of seconds after which pgs can be considered inactive, unclean, or stale (see doc/control.rst under dump_stuck for more info)
OPTION(mon_osd_full_ratio, OPT_FLOAT, .95) // what % full makes an OSD "full"
OPTION(mon_osd_nearfull_ratio, OPT_FLOAT, .85) // what % full makes an OSD near full
OPTION(mon_globalid_prealloc, OPT_INT, 100)   // how many globalids to prealloc
OPTION(mon_osd_report_timeout, OPT_INT, 900)    // grace period before declaring unresponsive OSDs dead
OPTION(mon_force_standby_active, OPT_BOOL, true) // should mons force standby-replay mds to be active
OPTION(mon_min_osdmap_epochs, OPT_INT, 500)
OPTION(mon_max_pgmap_epochs, OPT_INT, 500)
OPTION(mon_max_log_epochs, OPT_INT, 500)
OPTION(mon_probe_timeout, OPT_DOUBLE, 2.0)
OPTION(mon_slurp_timeout, OPT_DOUBLE, 10.0)
OPTION(mon_slurp_bytes, OPT_INT, 256*1024)    // limit size of slurp messages
OPTION(paxos_max_join_drift, OPT_INT, 10)       // max paxos iterations before we must first slurp
OPTION(paxos_propose_interval, OPT_DOUBLE, 1.0)  // gather updates for this long before proposing a map update
OPTION(paxos_min_wait, OPT_DOUBLE, 0.05)  // min time to gather updates for after period of inactivity
OPTION(paxos_observer_timeout, OPT_DOUBLE, 5*60) // gather updates for this long before proposing a map update
OPTION(clock_offset, OPT_DOUBLE, 0) // how much to offset the system clock in Clock.cc
OPTION(auth_supported, OPT_STR, "none")
OPTION(auth_mon_ticket_ttl, OPT_DOUBLE, 60*60*12)
OPTION(auth_service_ticket_ttl, OPT_DOUBLE, 60*60)
OPTION(mon_client_hunt_interval, OPT_DOUBLE, 3.0)   // try new mon every N seconds until we connect
OPTION(mon_client_ping_interval, OPT_DOUBLE, 10.0)  // ping every N seconds
OPTION(client_cache_size, OPT_INT, 16384)
OPTION(client_cache_mid, OPT_FLOAT, .75)
OPTION(client_cache_stat_ttl, OPT_INT, 0) // seconds until cached stat results become invalid
OPTION(client_cache_readdir_ttl, OPT_INT, 1)  // 1 second only
OPTION(client_use_random_mds, OPT_BOOL, false)
OPTION(client_mount_timeout, OPT_DOUBLE, 30.0)
OPTION(client_unmount_timeout, OPT_DOUBLE, 10.0)
OPTION(client_tick_interval, OPT_DOUBLE, 1.0)
OPTION(client_trace, OPT_STR, "")
OPTION(client_readahead_min, OPT_LONGLONG, 128*1024)  // readahead at _least_ this much.
OPTION(client_readahead_max_bytes, OPT_LONGLONG, 0)  //8 * 1024*1024
OPTION(client_readahead_max_periods, OPT_LONGLONG, 4)  // as multiple of file layout period (object size * num stripes)
OPTION(client_snapdir, OPT_STR, ".snap")
OPTION(client_mountpoint, OPT_STR, "/")
OPTION(client_notify_timeout, OPT_INT, 10) // in seconds
OPTION(client_oc, OPT_BOOL, true)
OPTION(client_oc_size, OPT_INT, 1024*1024* 200)    // MB * n
OPTION(client_oc_max_dirty, OPT_INT, 1024*1024* 100)    // MB * n  (dirty OR tx.. bigish)
OPTION(client_oc_target_dirty, OPT_INT, 1024*1024* 8) // target dirty (keep this smallish)
OPTION(client_oc_max_dirty_age, OPT_DOUBLE, 5.0)      // max age in cache before writeback
// note: the max amount of "in flight" dirty data is roughly (max - target)
OPTION(fuse_use_invalidate_cb, OPT_BOOL, false) // use fuse 2.8+ invalidate callback to keep page cache consistent
OPTION(fuse_big_writes, OPT_BOOL, true)
OPTION(objecter_tick_interval, OPT_DOUBLE, 5.0)
OPTION(objecter_mon_retry_interval, OPT_DOUBLE, 5.0)
OPTION(objecter_timeout, OPT_DOUBLE, 10.0)    // before we ask for a map
OPTION(objecter_inflight_op_bytes, OPT_U64, 1024*1024*100) // max in-flight data (both directions)
OPTION(objecter_inflight_ops, OPT_U64, 1024)               // max in-flight ios
OPTION(journaler_allow_split_entries, OPT_BOOL, true)
OPTION(journaler_write_head_interval, OPT_INT, 15)
OPTION(journaler_prefetch_periods, OPT_INT, 10)   // * journal object size
OPTION(journaler_prezero_periods, OPT_INT, 5)     // * journal object size
OPTION(journaler_batch_interval, OPT_DOUBLE, .001)   // seconds.. max add'l latency we artificially incur
OPTION(journaler_batch_max, OPT_U64, 0)  // max bytes we'll delay flushing; disable, for now....
OPTION(mds_max_file_size, OPT_U64, 1ULL << 40)
OPTION(mds_cache_size, OPT_INT, 100000)
OPTION(mds_cache_mid, OPT_FLOAT, .7)
OPTION(mds_mem_max, OPT_INT, 1048576)        // KB
OPTION(mds_dir_commit_ratio, OPT_FLOAT, .5)
OPTION(mds_dir_max_commit_size, OPT_INT, 90) // MB
OPTION(mds_decay_halflife, OPT_FLOAT, 5)
OPTION(mds_beacon_interval, OPT_FLOAT, 4)
OPTION(mds_beacon_grace, OPT_FLOAT, 15)
OPTION(mds_blacklist_interval, OPT_FLOAT, 24.0*60.0)  // how long to blacklist failed nodes
OPTION(mds_session_timeout, OPT_FLOAT, 60)    // cap bits and leases time out if client idle
OPTION(mds_session_autoclose, OPT_FLOAT, 300) // autoclose idle session
OPTION(mds_reconnect_timeout, OPT_FLOAT, 45)  // seconds to wait for clients during mds restart
	      //  make it (mds_session_timeout - mds_beacon_grace)
OPTION(mds_tick_interval, OPT_FLOAT, 5)
OPTION(mds_dirstat_min_interval, OPT_FLOAT, 1)    // try to avoid propagating more often than this
OPTION(mds_scatter_nudge_interval, OPT_FLOAT, 5)  // how quickly dirstat changes propagate up the hierarchy
OPTION(mds_client_prealloc_inos, OPT_INT, 1000)
OPTION(mds_early_reply, OPT_BOOL, true)
OPTION(mds_use_tmap, OPT_BOOL, true)        // use trivialmap for dir updates
OPTION(mds_default_dir_hash, OPT_INT, CEPH_STR_HASH_RJENKINS)
OPTION(mds_log, OPT_BOOL, true)
OPTION(mds_log_skip_corrupt_events, OPT_BOOL, false)
OPTION(mds_log_max_events, OPT_INT, -1)
OPTION(mds_log_max_segments, OPT_INT, 30)  // segment size defined by FileLayout, above
OPTION(mds_log_max_expiring, OPT_INT, 20)
OPTION(mds_log_eopen_size, OPT_INT, 100)   // # open inodes per log entry
OPTION(mds_bal_sample_interval, OPT_FLOAT, 3.0)  // every 5 seconds
OPTION(mds_bal_replicate_threshold, OPT_FLOAT, 8000)
OPTION(mds_bal_unreplicate_threshold, OPT_FLOAT, 0)
OPTION(mds_bal_frag, OPT_BOOL, false)
OPTION(mds_bal_split_size, OPT_INT, 10000)
OPTION(mds_bal_split_rd, OPT_FLOAT, 25000)
OPTION(mds_bal_split_wr, OPT_FLOAT, 10000)
OPTION(mds_bal_split_bits, OPT_INT, 3)
OPTION(mds_bal_merge_size, OPT_INT, 50)
OPTION(mds_bal_merge_rd, OPT_FLOAT, 1000)
OPTION(mds_bal_merge_wr, OPT_FLOAT, 1000)
OPTION(mds_bal_interval, OPT_INT, 10)           // seconds
OPTION(mds_bal_fragment_interval, OPT_INT, 5)      // seconds
OPTION(mds_bal_idle_threshold, OPT_FLOAT, 0)
OPTION(mds_bal_max, OPT_INT, -1)
OPTION(mds_bal_max_until, OPT_INT, -1)
OPTION(mds_bal_mode, OPT_INT, 0)
OPTION(mds_bal_min_rebalance, OPT_FLOAT, .1)  // must be this much above average before we export anything
OPTION(mds_bal_min_start, OPT_FLOAT, .2)      // if we need less than this, we don't do anything
OPTION(mds_bal_need_min, OPT_FLOAT, .8)       // take within this range of what we need
OPTION(mds_bal_need_max, OPT_FLOAT, 1.2)
OPTION(mds_bal_midchunk, OPT_FLOAT, .3)       // any sub bigger than this taken in full
OPTION(mds_bal_minchunk, OPT_FLOAT, .001)     // never take anything smaller than this
OPTION(mds_bal_target_removal_min, OPT_INT, 5) // min balance iterations before old target is removed
OPTION(mds_bal_target_removal_max, OPT_INT, 10) // max balance iterations before old target is removed
OPTION(mds_replay_interval, OPT_FLOAT, 1.0) // time to wait before starting replay again
OPTION(mds_shutdown_check, OPT_INT, 0)
OPTION(mds_thrash_exports, OPT_INT, 0)
OPTION(mds_thrash_fragments, OPT_INT, 0)
OPTION(mds_dump_cache_on_map, OPT_BOOL, false)
OPTION(mds_dump_cache_after_rejoin, OPT_BOOL, false)
OPTION(mds_verify_scatter, OPT_BOOL, false)
OPTION(mds_debug_scatterstat, OPT_BOOL, false)
OPTION(mds_debug_frag, OPT_BOOL, false)
OPTION(mds_debug_auth_pins, OPT_BOOL, false)
OPTION(mds_debug_subtrees, OPT_BOOL, false)
OPTION(mds_kill_mdstable_at, OPT_INT, 0)
OPTION(mds_kill_export_at, OPT_INT, 0)
OPTION(mds_kill_import_at, OPT_INT, 0)
OPTION(mds_kill_link_at, OPT_INT, 0)
OPTION(mds_kill_rename_at, OPT_INT, 0)
OPTION(mds_wipe_sessions, OPT_BOOL, 0)
OPTION(mds_wipe_ino_prealloc, OPT_BOOL, 0)
OPTION(mds_skip_ino, OPT_INT, 0)
OPTION(max_mds, OPT_INT, 1)
OPTION(mds_standby_for_name, OPT_STR, "")
OPTION(mds_standby_for_rank, OPT_INT, -1)
OPTION(mds_standby_replay, OPT_BOOL, false)

// If true, uses tmap as initial value for omap on old objects
OPTION(osd_auto_upgrade_tmap, OPT_BOOL, true)

// If true, TMAPPUT sets uses_tmap DEBUGGING ONLY
OPTION(osd_tmapput_sets_uses_tmap, OPT_BOOL, false)

OPTION(osd_uuid, OPT_UUID, uuid_d())
OPTION(osd_data, OPT_STR, "/var/lib/ceph/osd/$cluster-$id")
OPTION(osd_journal, OPT_STR, "/var/lib/ceph/osd/$cluster-$id/journal")
OPTION(osd_journal_size, OPT_INT, 0)         // in mb
OPTION(osd_max_write_size, OPT_INT, 90)
OPTION(osd_balance_reads, OPT_BOOL, false)
OPTION(osd_shed_reads, OPT_INT, false)     // forward from primary to replica
OPTION(osd_shed_reads_min_latency, OPT_DOUBLE, .01)       // min local latency
OPTION(osd_shed_reads_min_latency_diff, OPT_DOUBLE, .01)  // min latency difference
OPTION(osd_shed_reads_min_latency_ratio, OPT_DOUBLE, 1.5)  // 1.2 == 20% higher than peer
OPTION(osd_client_message_size_cap, OPT_U64, 500*1024L*1024L) // default to 200MB client data allowed in-memory
OPTION(osd_stat_refresh_interval, OPT_DOUBLE, .5)
OPTION(osd_pg_bits, OPT_INT, 6)  // bits per osd
OPTION(osd_pgp_bits, OPT_INT, 6)  // bits per osd
OPTION(osd_pg_layout, OPT_INT, CEPH_PG_LAYOUT_CRUSH)
OPTION(osd_min_rep, OPT_INT, 1)
OPTION(osd_max_rep, OPT_INT, 10)
OPTION(osd_min_raid_width, OPT_INT, 3)
OPTION(osd_max_raid_width, OPT_INT, 2)
OPTION(osd_pool_default_crush_rule, OPT_INT, 0)
OPTION(osd_pool_default_size, OPT_INT, 2)
OPTION(osd_pool_default_pg_num, OPT_INT, 8)
OPTION(osd_pool_default_pgp_num, OPT_INT, 8)
OPTION(osd_map_dedup, OPT_BOOL, true)
OPTION(osd_map_cache_size, OPT_INT, 500)
OPTION(osd_map_cache_bl_size, OPT_INT, 50)
OPTION(osd_map_cache_bl_inc_size, OPT_INT, 100)
OPTION(osd_map_message_max, OPT_INT, 100)  // max maps per MOSDMap message
OPTION(osd_op_threads, OPT_INT, 2)    // 0 == no threading
OPTION(osd_disk_threads, OPT_INT, 1)
OPTION(osd_recovery_threads, OPT_INT, 1)
OPTION(osd_recover_clone_overlap, OPT_BOOL, true)   // preserve clone_overlap during recovery/migration
OPTION(osd_backfill_scan_min, OPT_INT, 64)
OPTION(osd_backfill_scan_max, OPT_INT, 512)
OPTION(osd_op_thread_timeout, OPT_INT, 30)
OPTION(osd_backlog_thread_timeout, OPT_INT, 60*60*1)
OPTION(osd_recovery_thread_timeout, OPT_INT, 30)
OPTION(osd_snap_trim_thread_timeout, OPT_INT, 60*60*1)
OPTION(osd_scrub_thread_timeout, OPT_INT, 60)
OPTION(osd_scrub_finalize_thread_timeout, OPT_INT, 60*10)
OPTION(osd_remove_thread_timeout, OPT_INT, 60*60)
OPTION(osd_command_thread_timeout, OPT_INT, 10*60)
OPTION(osd_age, OPT_FLOAT, .8)
OPTION(osd_age_time, OPT_INT, 0)
OPTION(osd_heartbeat_addr, OPT_ADDR, entity_addr_t())
OPTION(osd_heartbeat_interval, OPT_INT, 6)       // (seconds) how often we ping peers
OPTION(osd_heartbeat_grace, OPT_INT, 20)         // (seconds) how long before we decide a peer has failed
OPTION(osd_mon_heartbeat_interval, OPT_INT, 30)  // (seconds) how often to ping monitor if no peers
OPTION(osd_mon_report_interval_max, OPT_INT, 120)
OPTION(osd_mon_report_interval_min, OPT_INT, 5)  // pg stats, failures, up_thru, boot.
OPTION(osd_mon_ack_timeout, OPT_INT, 30) // time out a mon if it doesn't ack stats
OPTION(osd_min_down_reporters, OPT_INT, 1)   // number of OSDs who need to report a down OSD for it to count
OPTION(osd_min_down_reports, OPT_INT, 3)     // number of times a down OSD must be reported for it to count
OPTION(osd_default_data_pool_replay_window, OPT_INT, 45)
OPTION(osd_preserve_trimmed_log, OPT_BOOL, true)
OPTION(osd_auto_mark_unfound_lost, OPT_BOOL, false)
OPTION(osd_recovery_delay_start, OPT_FLOAT, 15)
OPTION(osd_recovery_max_active, OPT_INT, 5)
OPTION(osd_recovery_max_chunk, OPT_U64, 1<<20)  // max size of push chunk
OPTION(osd_recovery_forget_lost_objects, OPT_BOOL, false)   // off for now
OPTION(osd_max_scrubs, OPT_INT, 1)
OPTION(osd_scrub_load_threshold, OPT_FLOAT, 0.5)
OPTION(osd_scrub_min_interval, OPT_FLOAT, 300)
OPTION(osd_scrub_max_interval, OPT_FLOAT, 60*60*24)   // once a day
OPTION(osd_auto_weight, OPT_BOOL, false)
OPTION(osd_class_error_timeout, OPT_DOUBLE, 60.0)  // seconds
OPTION(osd_class_timeout, OPT_DOUBLE, 60*60.0) // seconds
OPTION(osd_class_dir, OPT_STR, CEPH_LIBDIR "/rados-classes")
OPTION(osd_check_for_log_corruption, OPT_BOOL, false)
OPTION(osd_use_stale_snap, OPT_BOOL, false)
OPTION(osd_rollback_to_cluster_snap, OPT_STR, "")
OPTION(osd_default_notify_timeout, OPT_U32, 30) // default notify timeout in seconds
OPTION(osd_kill_backfill_at, OPT_INT, 0)
OPTION(osd_min_pg_log_entries, OPT_U32, 1000) // number of entries to keep in the pg log when trimming it
OPTION(osd_op_complaint_time, OPT_FLOAT, 30) // how many seconds old makes an op complaint-worthy
OPTION(osd_command_max_records, OPT_INT, 256)
OPTION(osd_op_log_threshold, OPT_INT, 5) // how many op log messages to show in one go
OPTION(filestore, OPT_BOOL, false)
OPTION(filestore_debug_omap_check, OPT_BOOL, 0) // Expensive debugging check on sync
// Use omap for xattrs for attrs over
OPTION(filestore_xattr_use_omap, OPT_BOOL, false)
// filestore_max_inline_xattr_size or
OPTION(filestore_max_inline_xattr_size, OPT_U32, 512)
// for more than filestore_max_inline_xattrs attrs
OPTION(filestore_max_inline_xattrs, OPT_U32, 2)

OPTION(filestore_max_sync_interval, OPT_DOUBLE, 5)    // seconds
OPTION(filestore_min_sync_interval, OPT_DOUBLE, .01)  // seconds
OPTION(filestore_dev, OPT_STR, "")
OPTION(filestore_btrfs_trans, OPT_BOOL, false)
OPTION(filestore_btrfs_snap, OPT_BOOL, true)
OPTION(filestore_btrfs_clone_range, OPT_BOOL, true)
OPTION(filestore_fsync_flushes_journal_data, OPT_BOOL, false)
OPTION(filestore_fiemap, OPT_BOOL, false)     // (try to) use fiemap
OPTION(filestore_flusher, OPT_BOOL, true)
OPTION(filestore_flusher_max_fds, OPT_INT, 512)
OPTION(filestore_sync_flush, OPT_BOOL, false)
OPTION(filestore_journal_parallel, OPT_BOOL, false)
OPTION(filestore_journal_writeahead, OPT_BOOL, false)
OPTION(filestore_journal_trailing, OPT_BOOL, false)
OPTION(filestore_queue_max_ops, OPT_INT, 500)
OPTION(filestore_queue_max_bytes, OPT_INT, 100 << 20)
OPTION(filestore_queue_committing_max_ops, OPT_INT, 500)        // this is ON TOP of filestore_queue_max_*
OPTION(filestore_queue_committing_max_bytes, OPT_INT, 100 << 20) //  "
OPTION(filestore_op_threads, OPT_INT, 2)
OPTION(filestore_op_thread_timeout, OPT_INT, 60)
OPTION(filestore_op_thread_suicide_timeout, OPT_INT, 180)
OPTION(filestore_commit_timeout, OPT_FLOAT, 600)
OPTION(filestore_fiemap_threshold, OPT_INT, 4096)
OPTION(filestore_merge_threshold, OPT_INT, 10)
OPTION(filestore_split_multiple, OPT_INT, 2)
OPTION(filestore_update_collections, OPT_BOOL, false)
OPTION(filestore_blackhole, OPT_BOOL, false)     // drop any new transactions on the floor
OPTION(filestore_dump_file, OPT_STR, "")         // file onto which store transaction dumps
OPTION(filestore_kill_at, OPT_INT, 0)            // inject a failure at the n'th opportunity
OPTION(journal_dio, OPT_BOOL, true)
OPTION(journal_aio, OPT_BOOL, false)
OPTION(journal_block_align, OPT_BOOL, true)
OPTION(journal_max_write_bytes, OPT_INT, 10 << 20)
OPTION(journal_max_write_entries, OPT_INT, 100)
OPTION(journal_queue_max_ops, OPT_INT, 500)
OPTION(journal_queue_max_bytes, OPT_INT, 100 << 20)
OPTION(journal_align_min_size, OPT_INT, 64 << 10)  // align data payloads >= this.
OPTION(journal_replay_from, OPT_INT, 0)
OPTION(journal_zero_on_create, OPT_BOOL, false)
OPTION(rbd_cache, OPT_BOOL, false) // whether to enable caching (writeback unless rbd_cache_max_dirty is 0)
OPTION(rbd_cache_size, OPT_LONGLONG, 32<<20)         // cache size in bytes
OPTION(rbd_cache_max_dirty, OPT_LONGLONG, 24<<20)    // dirty limit in bytes - set to 0 for write-through caching
OPTION(rbd_cache_target_dirty, OPT_LONGLONG, 16<<20) // target dirty limit in bytes
OPTION(rbd_cache_max_dirty_age, OPT_FLOAT, 1.0)      // seconds in cache before writeback starts
OPTION(rgw_cache_enabled, OPT_BOOL, true)   // rgw cache enabled
OPTION(rgw_cache_lru_size, OPT_INT, 10000)   // num of entries in rgw cache
OPTION(rgw_socket_path, OPT_STR, "")   // path to unix domain socket, if not specified, rgw will not run as external fcgi
OPTION(rgw_dns_name, OPT_STR, "")
OPTION(rgw_swift_url, OPT_STR, "")              // 
OPTION(rgw_swift_url_prefix, OPT_STR, "swift")  // 
OPTION(rgw_enforce_swift_acls, OPT_BOOL, true)
OPTION(rgw_print_continue, OPT_BOOL, true)  // enable if 100-Continue works
OPTION(rgw_remote_addr_param, OPT_STR, "REMOTE_ADDR")  // e.g. X-Forwarded-For, if you have a reverse proxy
OPTION(rgw_op_thread_timeout, OPT_INT, 10*60)
OPTION(rgw_op_thread_suicide_timeout, OPT_INT, 0)
OPTION(rgw_thread_pool_size, OPT_INT, 100)
OPTION(rgw_maintenance_tick_interval, OPT_DOUBLE, 10.0)
OPTION(rgw_pools_preallocate_max, OPT_INT, 100)
OPTION(rgw_pools_preallocate_threshold, OPT_INT, 70)
OPTION(rgw_log_nonexistent_bucket, OPT_BOOL, false)
OPTION(rgw_log_object_name, OPT_STR, "%Y-%m-%d-%H-%i-%n")      // man date to see codes (a subset are supported)
OPTION(rgw_log_object_name_utc, OPT_BOOL, false)
OPTION(rgw_enable_ops_log, OPT_BOOL, true) // enable logging every rgw operation
OPTION(rgw_intent_log_object_name, OPT_STR, "%Y-%m-%d-%i-%n")  // man date to see codes (a subset are supported)
OPTION(rgw_intent_log_object_name_utc, OPT_BOOL, false)
OPTION(rgw_init_timeout, OPT_INT, 30) // time in seconds
OPTION(rgw_mime_types_file, OPT_STR, "/etc/mime.types")

// This will be set to true when it is safe to start threads.
// Once it is true, it will never change.
OPTION(internal_safe_to_start_threads, OPT_BOOL, false)
