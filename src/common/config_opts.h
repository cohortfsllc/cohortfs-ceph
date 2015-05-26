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
 * Foundation.	See file COPYING.
 *
 */

/* note: no header guard */
OPTION(host, OPT_STR, "localhost")
OPTION(fsid, OPT_UUID, boost::uuids::nil_uuid())
OPTION(public_addr, OPT_ADDR, entity_addr_t())
OPTION(cluster_addr, OPT_ADDR, entity_addr_t())
OPTION(public_network, OPT_STR, "")
OPTION(cluster_network, OPT_STR, "")
OPTION(cluster_rdma, OPT_BOOL, false)
OPTION(client_rdma, OPT_BOOL, false)
OPTION(num_client, OPT_INT, 1)
OPTION(monmap, OPT_STR, "")
OPTION(mon_host, OPT_STR, "")
// the "/var/run/ceph" dir, created on daemon startup
OPTION(run_dir, OPT_STR, "/var/run/ceph")

// default changed by common_preinit()
OPTION(admin_socket, OPT_STR, "$run_dir/$cluster-$name.asok")

OPTION(daemonize, OPT_BOOL, false) // default changed by common_preinit()
OPTION(pid_file, OPT_STR, "") // default changed by common_preinit()
OPTION(chdir, OPT_STR, "/")
OPTION(max_open_files, OPT_LONGLONG, 0)
OPTION(restapi_log_level, OPT_STR, "")	// default set by Python code
OPTION(restapi_base_url, OPT_STR, "")	// "

// default changed by common_preinit()
OPTION(log_file, OPT_STR, "/var/log/ceph/$cluster-$name.log")
OPTION(log_max_new, OPT_INT, 1000) // default changed by common_preinit()
OPTION(log_max_recent, OPT_INT, 10000) // default changed by common_preinit()
OPTION(log_to_stderr, OPT_BOOL, true) // default changed by common_preinit()
OPTION(err_to_stderr, OPT_BOOL, true) // default changed by common_preinit()
OPTION(log_to_syslog, OPT_BOOL, false)
OPTION(err_to_syslog, OPT_BOOL, false)
OPTION(log_flush_on_exit, OPT_BOOL, true) // changed by common_preinit()
OPTION(log_to_lttng, OPT_BOOL, false) // lttng tracepoints for log output
OPTION(log_stop_at_utilization, OPT_FLOAT, .97)  // stop logging at (near) full

OPTION(xio_trace_mempool, OPT_BOOL, false) // mempool allocation counters
OPTION(xio_trace_msgcnt, OPT_BOOL, false) // incoming/outgoing msg counters
OPTION(xio_trace_xcon, OPT_BOOL, false) // Xio message encode/decode trace
OPTION(xio_queue_depth, OPT_INT, 512) // depth of Accelio msg queue
OPTION(xio_mp_min, OPT_INT, 128) // default min mempool size
OPTION(xio_mp_max_64, OPT_INT, 65536) // max 64-byte chunks (buffer is 40)
OPTION(xio_mp_max_256, OPT_INT, 8192) // max 256-byte chunks
OPTION(xio_mp_max_1k, OPT_INT, 8192) // max 1K chunks
OPTION(xio_mp_max_page, OPT_INT, 4096) // max 1K chunks
OPTION(xio_mp_max_hint, OPT_INT, 4096) // max size-hint chunks
OPTION(clog_to_monitors, OPT_BOOL, true)
OPTION(clog_to_syslog, OPT_BOOL, false)
OPTION(clog_to_syslog_level, OPT_STR, "info")	      // this level and above
OPTION(clog_to_syslog_facility, OPT_STR, "daemon")

OPTION(mon_cluster_log_to_syslog, OPT_BOOL, false)
OPTION(mon_cluster_log_to_syslog_level, OPT_STR, "info") // this and above
OPTION(mon_cluster_log_to_syslog_facility, OPT_STR, "daemon")
OPTION(mon_cluster_log_file, OPT_STR, "/var/log/ceph/$cluster.log")
OPTION(mon_cluster_log_file_level, OPT_STR, "info")

DEFAULT_SUBSYS(0, 5)
SUBSYS(context, 0, 1)
SUBSYS(mds, 1, 5)
SUBSYS(mds_balancer, 1, 5)
SUBSYS(mds_locker, 1, 5)
SUBSYS(mds_log, 1, 5)
SUBSYS(mds_log_expire, 1, 5)
SUBSYS(mds_migrator, 1, 5)
SUBSYS(buffer, 0, 1)
SUBSYS(timer, 0, 1)
SUBSYS(filer, 0, 1)
SUBSYS(striper, 0, 1)
SUBSYS(objecter, 0, 1)
SUBSYS(rados, 0, 5)
SUBSYS(rbd, 0, 5)
SUBSYS(journaler, 0, 5)
SUBSYS(client, 0, 5)
SUBSYS(osd, 0, 5)
SUBSYS(optracker, 0, 5)
SUBSYS(objclass, 0, 5)
SUBSYS(filestore, 1, 3)
SUBSYS(keyvaluestore, 1, 3)
SUBSYS(journal, 1, 3)
SUBSYS(ms, 0, 5)
SUBSYS(mon, 1, 5)
SUBSYS(monc, 0, 10)
SUBSYS(paxos, 1, 5)
SUBSYS(tp, 0, 5)
SUBSYS(auth, 1, 5)
SUBSYS(crypto, 1, 5)
SUBSYS(finisher, 1, 1)
SUBSYS(heartbeatmap, 1, 5)
SUBSYS(rgw, 1, 5)		  // log level for the Rados gateway
SUBSYS(javaclient, 1, 5)
SUBSYS(asok, 1, 5)
SUBSYS(throttle, 1, 1)
SUBSYS(xio, 1, 5)
SUBSYS(volume, 1, 5)
SUBSYS(placer, 1, 5)

OPTION(key, OPT_STR, "")
OPTION(keyfile, OPT_STR, "")
// default changed by common_preinit() for mds and osd
OPTION(keyring, OPT_STR, "/etc/ceph/$cluster.$name.keyring,"
       "/etc/ceph/$cluster.keyring,/etc/ceph/keyring,/etc/ceph/keyring.bin")
OPTION(heartbeat_interval, OPT_TIME, 5s)
OPTION(heartbeat_file, OPT_STR, "")
// force an unhealthy heartbeat for a given time
OPTION(heartbeat_inject_failure, OPT_TIME, 0ns)

OPTION(ms_tcp_nodelay, OPT_BOOL, true)
OPTION(ms_tcp_rcvbuf, OPT_INT, 0)
OPTION(ms_initial_backoff, OPT_TIME, 2ms)
OPTION(ms_max_backoff, OPT_TIME, 15s)
// ms_datacrc&&!ms_headercrc == special, see Messenger::get_default_crc_flags
OPTION(ms_datacrc, OPT_BOOL, true)
OPTION(ms_headercrc, OPT_BOOL, false)
OPTION(ms_die_on_bad_msg, OPT_BOOL, false)
OPTION(ms_die_on_unhandled_msg, OPT_BOOL, false)
// assert if we get a dup incoming message and shouldn't have (may be
// triggered by pre-541cd3c64be0dfa04e8a2df39422e0eb9541a428 code)
OPTION(ms_die_on_old_message, OPT_BOOL, false)
OPTION(ms_dispatch_throttle_bytes, OPT_U64, 100 << 20)
OPTION(ms_bind_ipv6, OPT_BOOL, false)
OPTION(ms_bind_port_min, OPT_INT, 6800)
OPTION(ms_bind_port_max, OPT_INT, 7300)
OPTION(ms_rwthread_stack_bytes, OPT_U64, 1024 << 10)
OPTION(ms_tcp_read_timeout, OPT_TIME, 900s)
OPTION(ms_pq_max_tokens_per_priority, OPT_U64, 16777216)
OPTION(ms_pq_min_cost, OPT_U64, 65536)
OPTION(ms_inject_socket_failures, OPT_U64, 0)
OPTION(ms_inject_delay_type, OPT_STR, "")   // "osd mds mon client" allowed
// the type of message to delay, as returned by
// Message::get_type_name(). This is an additional restriction on the
// general type filter ms_inject_delay_type.
OPTION(ms_inject_delay_msg_type, OPT_STR, "")
OPTION(ms_inject_delay_max, OPT_TIME, 1s)
OPTION(ms_inject_delay_probability, OPT_DOUBLE, 0) // range [0, 1]
OPTION(ms_inject_internal_delays, OPT_TIME, 0ns)   // seconds
OPTION(ms_dump_on_send, OPT_BOOL, false)   // hexdump msg to log on send

OPTION(inject_early_sigterm, OPT_BOOL, false)

OPTION(mon_data, OPT_STR, "/var/lib/ceph/mon/$cluster-$id")
// list of initial cluster mon ids; if specified, need majority to
// form initial quorum and create new cluster
OPTION(mon_initial_members, OPT_STR, "")
// sync() when writing this many objects; 0 to disable.
OPTION(mon_sync_fs_threshold, OPT_INT, 5)
// compact leveldb on ceph-mon start
OPTION(mon_compact_on_start, OPT_BOOL, false)
// trigger leveldb compaction on bootstrap
OPTION(mon_compact_on_bootstrap, OPT_BOOL, false)
// compact (a prefix) when we trim old states
OPTION(mon_compact_on_trim, OPT_BOOL, true)
OPTION(mon_tick_interval, OPT_TIME, 5s)
OPTION(mon_subscribe_interval, OPT_TIME, 300s)
// inactivity before we reset the pg delta to 0
OPTION(mon_delta_reset_interval, OPT_TIME, 10s)
// how quickly our laggy estimations decay
OPTION(mon_osd_laggy_halflife, OPT_TIME, 1h)
// weight for new 'samples's in laggy estimations
OPTION(mon_osd_laggy_weight, OPT_DOUBLE, .3)
// true if we should scale based on laggy estimations
OPTION(mon_osd_adjust_heartbeat_grace, OPT_BOOL, true)
// true if we should scale based on laggy estimations
OPTION(mon_osd_adjust_down_out_interval, OPT_BOOL, true)
// mark any booting osds 'in'
OPTION(mon_osd_auto_mark_in, OPT_BOOL, false)
// mark booting auto-marked-out osds 'in'
OPTION(mon_osd_auto_mark_auto_out_in, OPT_BOOL, true)
// mark booting new osds 'in'
OPTION(mon_osd_auto_mark_new_in, OPT_BOOL, true)
OPTION(mon_osd_down_out_interval, OPT_TIME, 300s)
// min osds required to be up to mark things down
OPTION(mon_osd_min_up_ratio, OPT_DOUBLE, .3)
// min osds required to be in to mark things out
OPTION(mon_osd_min_in_ratio, OPT_DOUBLE, .3)
// max op age before we get concerned (make it a power of 2)
OPTION(mon_osd_max_op_age, OPT_TIME, 32s)
// largest number of PGs per "involved" OSD to let split create
OPTION(mon_osd_max_split_count, OPT_INT, 32)
// allow primary_temp to be set in the osdmap
OPTION(mon_osd_allow_primary_temp, OPT_BOOL, false)
// allow primary_affinity to be set in the osdmap
OPTION(mon_osd_allow_primary_affinity, OPT_BOOL, false)
// smooth stats over last N PGMap maps
OPTION(mon_stat_smooth_intervals, OPT_INT, 2)
// lease interval
OPTION(mon_lease, OPT_TIME, 5s)
// on leader, to renew the lease
OPTION(mon_lease_renew_interval, OPT_TIME, 3s)
// on leader, if lease isn't acked by all peons
OPTION(mon_lease_ack_timeout, OPT_TIME, 10s)
// allowed clock drift between monitors
OPTION(mon_clock_drift_allowed, OPT_TIME, 50ms)
// exponential backoff for clock drift warnings
OPTION(mon_clock_drift_warn_backoff, OPT_TIME, 5s)
// on leader, timecheck (clock drift check) interval (seconds)
OPTION(mon_timecheck_interval, OPT_TIME, 300s)
// on leader, if paxos update isn't accepted
OPTION(mon_accept_timeout, OPT_TIME, 10s)
// position between vol cache_target_full and max where we start warning
OPTION(mon_cache_target_full_warn_ratio, OPT_FLOAT, .66)
// what % full makes an OSD "full"
OPTION(mon_osd_full_ratio, OPT_FLOAT, .95)
// what % full makes an OSD near full
OPTION(mon_osd_nearfull_ratio, OPT_FLOAT, .85)
// how many globalids to prealloc
OPTION(mon_globalid_prealloc, OPT_INT, 100)
// grace period before declaring unresponsive OSDs dead
OPTION(mon_osd_report_timeout, OPT_TIME, 15min)
// should mons force standby-replay mds to be active
OPTION(mon_force_standby_active, OPT_BOOL, true)
// warn if 'mon_osd_down_out_interval == 0'
OPTION(mon_warn_on_osd_down_out_interval_zero, OPT_BOOL, true)
OPTION(mon_min_osdmap_epochs, OPT_INT, 500)
OPTION(mon_max_pgmap_epochs, OPT_INT, 500)
OPTION(mon_max_log_epochs, OPT_INT, 500)
OPTION(mon_max_mdsmap_epochs, OPT_INT, 500)
OPTION(mon_max_osd, OPT_INT, 10000)
OPTION(mon_probe_timeout, OPT_TIME, 2s)
OPTION(mon_slurp_timeout, OPT_TIME, 10s)
// limit size of slurp messages
OPTION(mon_slurp_bytes, OPT_INT, 256*1024)
// client msg data allowed in memory (in bytes)
OPTION(mon_client_bytes, OPT_U64, 100ul << 20)
// mds, osd message memory cap (in bytes)
OPTION(mon_daemon_bytes, OPT_U64, 400ul << 20)
OPTION(mon_max_log_entries_per_event, OPT_INT, 4096)
OPTION(mon_health_data_update_interval, OPT_TIME, 1min)
OPTION(mon_data_avail_crit, OPT_INT, 5)
OPTION(mon_data_avail_warn, OPT_INT, 30)
// max num bytes per config-key entry
OPTION(mon_config_key_max_entry_size, OPT_INT, 4096)
OPTION(mon_sync_timeout, OPT_TIME, 1min)
// max size for a sync chunk payload (say, 1MB)
OPTION(mon_sync_max_payload_size, OPT_U32, 1048576)
// monitor to be used as the sync leader
OPTION(mon_sync_debug_leader, OPT_INT, -1)
// monitor to be used as the sync provider
OPTION(mon_sync_debug_provider, OPT_INT, -1)
// monitor to be used as fallback if sync provider fails
OPTION(mon_sync_debug_provider_fallback, OPT_INT, -1)
// inject N second delay on each get_chunk request
OPTION(mon_inject_sync_get_chunk_delay, OPT_TIME, 0ns)
// number of OSDs who need to report a down OSD for it to count
OPTION(mon_osd_min_down_reporters, OPT_INT, 1)
// number of times a down OSD must be reported for it to count
OPTION(mon_osd_min_down_reports, OPT_INT, 3)
// force mon to trim maps to this point, regardless of
// min_last_epoch_clean (dangerous, use with care)
OPTION(mon_osd_force_trim_to, OPT_INT, 0)
// force mon to trim mdsmaps to this point (dangerous, use with care)
OPTION(mon_mds_force_trim_to, OPT_INT, 0)

// true for developper oriented testing
OPTION(mon_advanced_debug_mode, OPT_BOOL, false)
// dump transactions
OPTION(mon_debug_dump_transactions, OPT_BOOL, false)
OPTION(mon_debug_dump_location, OPT_STR, "/var/log/ceph/$cluster-$name.tdump")

// kill the sync provider at a specific point in the work flow
OPTION(mon_sync_provider_kill_at, OPT_INT, 0)
// kill the sync requester at a specific point in the work flow
OPTION(mon_sync_requester_kill_at, OPT_INT, 0)
// monitor's leveldb write buffer size
OPTION(mon_leveldb_write_buffer_size, OPT_U64, 32*1024*1024)
// monitor's leveldb cache size
OPTION(mon_leveldb_cache_size, OPT_U64, 512*1024*1024)
// monitor's leveldb block size
OPTION(mon_leveldb_block_size, OPT_U64, 64*1024)
// monitor's leveldb bloom bits per entry
OPTION(mon_leveldb_bloom_size, OPT_INT, 0)
// monitor's leveldb max open files
OPTION(mon_leveldb_max_open_files, OPT_INT, 0)
// monitor's leveldb uses compression
OPTION(mon_leveldb_compression, OPT_BOOL, false)
// monitor's leveldb paranoid flag
OPTION(mon_leveldb_paranoid, OPT_BOOL, false)
OPTION(mon_leveldb_log, OPT_STR, "")
// issue a warning when the monitor's leveldb goes over 40GB (in bytes)
OPTION(mon_leveldb_size_warn, OPT_U64, 40*1024*1024*1024)
// force monitor to join quorum even if it has been previously removed
// from the map
OPTION(mon_force_quorum_join, OPT_BOOL, false)
// how often (in commits) to stash a full copy of the PaxosService state
OPTION(paxos_stash_full_interval, OPT_INT, 25)
 // max paxos iterations before we must first sync the monitor stores
OPTION(paxos_max_join_drift, OPT_INT, 10)
// gather updates for this long before proposing a map update
OPTION(paxos_propose_interval, OPT_TIME, 1s)
// min time to gather updates for after period of inactivity
OPTION(paxos_min_wait, OPT_TIME, 50ms)
// minimum number of paxos states to keep around
OPTION(paxos_min, OPT_INT, 500)
// number of extra proposals tolerated before trimming
OPTION(paxos_trim_min, OPT_INT, 250)
// max number of extra proposals to trim at a time
OPTION(paxos_trim_max, OPT_INT, 500)
// minimum amount of versions to trigger a trim (0 disables it)
OPTION(paxos_service_trim_min, OPT_INT, 250)
// maximum amount of versions to trim during a single proposal (0 disables it)
OPTION(paxos_service_trim_max, OPT_INT, 500)
OPTION(paxos_kill_at, OPT_INT, 0)
// required of mon, mds, osd daemons
OPTION(auth_cluster_required, OPT_STR, "cephx")
// required by daemons of clients
OPTION(auth_service_required, OPT_STR, "cephx")
// what clients require of daemons
OPTION(auth_client_required, OPT_STR, "cephx, none")
// deprecated; default value for above if they are not defined.
OPTION(auth_supported, OPT_STR, "")
//  If true, don't talk to Cephx partners if they don't support
//  message signing; off by default
OPTION(cephx_require_signatures, OPT_BOOL, false)
OPTION(cephx_cluster_require_signatures, OPT_BOOL, false)
OPTION(cephx_service_require_signatures, OPT_BOOL, false)
// Default to signing session messages if supported
OPTION(cephx_sign_messages, OPT_BOOL, true)
OPTION(auth_mon_ticket_ttl, OPT_TIME, 12h)
OPTION(auth_service_ticket_ttl, OPT_TIME, 12h)
OPTION(auth_debug, OPT_BOOL, false) // if true, assert when weird things happen
// try new mon every N seconds until we connect
OPTION(mon_client_hunt_interval, OPT_TIME, 3s)
// ping every N seconds
OPTION(mon_client_ping_interval, OPT_TIME, 10s)
// fail if we don't hear back
OPTION(mon_client_ping_timeout, OPT_TIME, 30s)
// each time we reconnect to a monitor, double our timeout
OPTION(mon_client_hunt_interval_backoff, OPT_INT, 2)
// up to a max of 10*default
OPTION(mon_client_hunt_interval_max_multiple, OPT_U32, 10)
OPTION(mon_client_max_log_entries_per_message, OPT_INT, 1000)
// percent of quota at which to issue warnings
OPTION(mon_vol_quota_warn_threshold, OPT_INT, 0)
// percent of quota at which to issue errors
OPTION(mon_vol_quota_crit_threshold, OPT_INT, 0)
OPTION(client_cache_size, OPT_INT, 16384)
OPTION(client_cache_mid, OPT_FLOAT, .75)
OPTION(client_use_random_mds, OPT_BOOL, false)
OPTION(client_mount_timeout, OPT_TIME, 300s)
OPTION(client_tick_interval, OPT_TIME, 1s)
OPTION(client_trace, OPT_STR, "")
OPTION(client_readahead_min, OPT_U64, 128*1024) // _least_ readahead
OPTION(client_readahead_max_bytes, OPT_U64, 0)  //8 * 1024*1024
// as multiple of file layout period (object size * num stripes)
OPTION(client_readahead_max_periods, OPT_LONGLONG, 4)
OPTION(client_mountpoint, OPT_STR, "/")
OPTION(client_notify_timeout, OPT_TIME, 10s)
OPTION(osd_client_watch_timeout, OPT_TIME, 30s)
OPTION(client_caps_release_delay, OPT_TIME, 5s)
// always read synchronously (go to osds)
OPTION(client_debug_force_sync_read, OPT_BOOL, false)
// delay the client tick for a number of seconds
OPTION(client_debug_inject_tick_delay, OPT_TIME, 0ns)
OPTION(client_max_inline_size, OPT_U64, 4096)
// note: the max amount of "in flight" dirty data is roughly (max -
// target)
// use fuse 2.8+ invalidate callback to keep page cache consistent
OPTION(fuse_use_invalidate_cb, OPT_BOOL, false)
OPTION(fuse_allow_other, OPT_BOOL, true)
OPTION(fuse_default_permissions, OPT_BOOL, true)
OPTION(fuse_big_writes, OPT_BOOL, true)
OPTION(fuse_atomic_o_trunc, OPT_BOOL, true)
OPTION(fuse_debug, OPT_BOOL, false)
OPTION(fuse_multithreaded, OPT_BOOL, false)

OPTION(objecter_tick_interval, OPT_TIME, 5s)
// before we ask for a map
OPTION(objecter_timeout, OPT_TIME, 10s)
// max in-flight data (both directions)
OPTION(objecter_inflight_op_bytes, OPT_U64, 1024*1024*100)
// max in-flight ios
OPTION(objecter_inflight_ops, OPT_U64, 1024)
OPTION(journaler_allow_split_entries, OPT_BOOL, true)
OPTION(journaler_write_head_interval, OPT_TIME, 15s)
OPTION(journaler_prefetch_periods, OPT_INT, 10) // * journal object size
OPTION(journaler_prezero_periods, OPT_INT, 5) // * journal object size
// seconds.. max add'l latency we artificially incur
OPTION(journaler_batch_interval, OPT_TIME, 1ms)
// max bytes we'll delay flushing; disable, for now....
OPTION(journaler_batch_max, OPT_U64, 0)
OPTION(mds_data, OPT_STR, "/var/lib/ceph/mds/$cluster-$id")
OPTION(mds_max_file_size, OPT_U64, 1ULL << 40)
OPTION(mds_cache_size, OPT_INT, 100000)
OPTION(mds_cache_mid, OPT_FLOAT, .7)
OPTION(mds_mem_max, OPT_INT, 1048576)	     // KB
OPTION(mds_dir_max_commit_size, OPT_INT, 10) // MB
OPTION(mds_decay_halflife, OPT_FLOAT, 5)
OPTION(mds_beacon_interval, OPT_TIME, 4s)
OPTION(mds_beacon_grace, OPT_TIME, 15s)
OPTION(mds_enforce_unique_name, OPT_BOOL, true)
// how long to blacklist failed nodes
OPTION(mds_blacklist_interval, OPT_TIME, 1h)
// cap bits and leases time out if client idle
OPTION(mds_session_timeout, OPT_TIME, 60s)
// cap bits and leases time out if client idle
OPTION(mds_freeze_tree_timeout, OPT_TIME, 30s)
// autoclose idle session
OPTION(mds_session_autoclose, OPT_TIME, 5min)
// Time to wait for clients during mds restart
OPTION(mds_reconnect_timeout, OPT_TIME, 45s)
//  make it (mds_session_timeout - mds_beacon_grace)
OPTION(mds_tick_interval, OPT_TIME, 5s)
// try to avoid propagating more often than this
OPTION(mds_dirstat_min_interval, OPT_TIME, 1s)
// how quickly dirstat changes propagate up the hierarchy
OPTION(mds_scatter_nudge_interval, OPT_TIME, 5s)
OPTION(mds_client_prealloc_inos, OPT_INT, 1000)
OPTION(mds_early_reply, OPT_BOOL, true)
OPTION(mds_default_dir_hash, OPT_INT, CEPH_STR_HASH_RJENKINS)
OPTION(mds_log, OPT_BOOL, true)
OPTION(mds_log_skip_corrupt_events, OPT_BOOL, false)
OPTION(mds_log_max_events, OPT_INT, -1)
OPTION(mds_log_segment_size, OPT_INT, 1<<22)  // segment size for mds log,
	      // should default to stripe size (XXX doesn't - fixme?) (4MB)
OPTION(mds_log_max_segments, OPT_INT, 30)
OPTION(mds_log_max_expiring, OPT_INT, 20)
OPTION(mds_bal_sample_interval, OPT_TIME, 3s)
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
OPTION(mds_bal_interval, OPT_TIME, 10s)
OPTION(mds_bal_fragment_interval, OPT_TIME, 5s)
OPTION(mds_bal_idle_threshold, OPT_TIME, 0s)
OPTION(mds_bal_max, OPT_INT, -1)
OPTION(mds_bal_max_until, OPT_TIME, 0ns)
OPTION(mds_bal_mode, OPT_INT, 0)
// must be this much above average before we export anything
OPTION(mds_bal_min_rebalance, OPT_FLOAT, .1)
// if we need less than this, we don't do anything
OPTION(mds_bal_min_start, OPT_FLOAT, .2)
// take within this range of what we need
OPTION(mds_bal_need_min, OPT_FLOAT, .8)
OPTION(mds_bal_need_max, OPT_FLOAT, 1.2)
// any sub bigger than this taken in full
OPTION(mds_bal_midchunk, OPT_FLOAT, .3)
// never take anything smaller than this
OPTION(mds_bal_minchunk, OPT_FLOAT, .001)
// min balance iterations before old target is removed
OPTION(mds_bal_target_removal_min, OPT_INT, 5)
// max balance iterations before old target is removed
OPTION(mds_bal_target_removal_max, OPT_INT, 10)
// time to wait before starting replay again
OPTION(mds_replay_interval, OPT_TIME, 1s)
OPTION(mds_shutdown_check, OPT_TIME, 0ns)
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
OPTION(mds_kill_openc_at, OPT_INT, 0)
OPTION(mds_kill_journal_at, OPT_INT, 0)
OPTION(mds_kill_journal_expire_at, OPT_INT, 0)
OPTION(mds_kill_journal_replay_at, OPT_INT, 0)
OPTION(mds_kill_create_at, OPT_INT, 0)
OPTION(mds_open_remote_link_mode, OPT_INT, 0)
OPTION(mds_inject_traceless_reply_probability, OPT_DOUBLE, 0) /* percentage
				of MDS modify replies to skip sending the
				client a trace on [0-1]*/
OPTION(mds_wipe_sessions, OPT_BOOL, 0)
OPTION(mds_wipe_ino_prealloc, OPT_BOOL, 0)
OPTION(mds_skip_ino, OPT_INT, 0)
OPTION(max_mds, OPT_INT, 1)
OPTION(mds_standby_for_name, OPT_STR, "")
OPTION(mds_standby_for_rank, OPT_INT, -1)
OPTION(mds_standby_replay, OPT_BOOL, false)

// If true, compact leveldb store on mount
OPTION(osd_compact_leveldb_on_mount, OPT_BOOL, false)

// max agent flush ops
OPTION(osd_agent_max_ops, OPT_INT, 4)
OPTION(osd_agent_min_evict_effort, OPT_FLOAT, .1)
OPTION(osd_agent_quantize_effort, OPT_FLOAT, .1)
OPTION(osd_agent_delay_time, OPT_TIME, 5s)

// decay atime and hist histograms after how many objects go by
OPTION(osd_agent_hist_halflife, OPT_INT, 1000)

// must be this amount over the threshold to enable,
// this amount below the threshold to disable.
OPTION(osd_agent_slop, OPT_FLOAT, .02)

OPTION(osd_uuid, OPT_UUID, boost::uuids::nil_uuid())
OPTION(osd_data, OPT_STR, "/var/lib/ceph/osd/$cluster-$id")
OPTION(osd_journal, OPT_STR, "/var/lib/ceph/osd/$cluster-$id/journal")
OPTION(osd_journal_size, OPT_INT, 5120)	// in mb
OPTION(osd_max_write_size, OPT_INT, 90)
// max number of pgls entries to return
OPTION(osd_max_pgls, OPT_U64, 1024)
// client data allowed in-memory (in bytes)
OPTION(osd_client_message_size_cap, OPT_U64, 500*1024L*1024L)
// num client messages allowed in-memory
OPTION(osd_client_message_cap, OPT_U64, 100)

OPTION(osd_map_dedup, OPT_BOOL, true)
OPTION(osd_map_cache_size, OPT_INT, 500)
// max maps per MOSDMap message
OPTION(osd_map_message_max, OPT_INT, 100)
// cap on # of inc maps we send to peers, clients
OPTION(osd_map_share_max_epochs, OPT_INT, 100)
// 0 == no threading
OPTION(osd_op_threads, OPT_INT, 2)
OPTION(osd_op_pq_max_tokens_per_priority, OPT_U64, 4194304)
OPTION(osd_op_pq_min_cost, OPT_U64, 65536)
OPTION(osd_disk_threads, OPT_INT, 1)

OPTION(osd_wq_lanes, OPT_INT, 11)
OPTION(osd_wq_thrd_lowat, OPT_INT, 1)
OPTION(osd_wq_thrd_hiwat, OPT_INT, 2)

OPTION(osd_os_lru_lanes, OPT_INT, 17)
OPTION(osd_os_lru_lane_hiwat, OPT_INT, 311)
OPTION(osd_os_objcache_partitions, OPT_INT, 5)
OPTION(osd_os_objcache_cachesz, OPT_INT, 373)

OPTION(osd_early_reply_at, OPT_INT, 0) // To measure segments of the osd pipe

OPTION(osd_op_thread_timeout, OPT_TIME, 15s)
OPTION(osd_remove_thread_timeout, OPT_TIME, 1h)
OPTION(osd_command_thread_timeout, OPT_TIME, 10min)
OPTION(osd_heartbeat_addr, OPT_ADDR, entity_addr_t())
OPTION(osd_heartbeat_interval, OPT_TIME, 6s) // how often we ping peers
// how long before we decide a peer has failed
OPTION(osd_heartbeat_grace, OPT_TIME, 20s)
OPTION(osd_heartbeat_min_peers, OPT_INT, 10) // minimum number of peers

// minimum number of peers tha tmust be reachable to mark ourselves
// back up after being wrongly marked down.
OPTION(osd_heartbeat_min_healthy_ratio, OPT_FLOAT, .33)

// (seconds) how often to ping monitor if no peers
OPTION(osd_mon_heartbeat_interval, OPT_TIME, 30s)
OPTION(osd_mon_report_interval_max, OPT_TIME, 2min)
// pg stats, failures, up_thru, boot.
OPTION(osd_mon_report_interval_min, OPT_TIME, 5s)
// time out a mon if it doesn't ack stats
OPTION(osd_mon_ack_timeout, OPT_TIME, 30s)
OPTION(osd_default_data_vol_replay_window, OPT_TIME, 45s)
OPTION(osd_preserve_trimmed_log, OPT_BOOL, false)
OPTION(osd_scan_list_ping_tp_interval, OPT_U64, 100)
OPTION(osd_auto_weight, OPT_BOOL, false)
// where rados plugins are stored
OPTION(osd_class_dir, OPT_STR, CEPH_PKGLIBDIR "/rados-classes")
OPTION(osd_open_classes_on_start, OPT_BOOL, true)
OPTION(osd_check_for_log_corruption, OPT_BOOL, false)
// default notify timeout in seconds
OPTION(osd_default_notify_timeout, OPT_TIME, 30s)
// default for the erasure-code-directory=XXX property of osd vol create
OPTION(osd_erasure_code_directory, OPT_STR, CEPH_PKGLIBDIR"/erasure-code")

OPTION(osd_command_max_records, OPT_INT, 256)
// read fiemap-reported holes and verify they are zeros
OPTION(osd_verify_sparse_read_holes, OPT_BOOL, false)
// to adjust various transactions that batch smaller items
OPTION(osd_target_transaction_size, OPT_INT, 30)
// what % full makes an OSD "full" (failsafe)
OPTION(osd_failsafe_full_ratio, OPT_FLOAT, .97)
// what % full makes an OSD near full (failsafe)
OPTION(osd_failsafe_nearfull_ratio, OPT_FLOAT, .90)

// ObjectStore libraries
OPTION(osd_module_dir, OPT_STR, CEPH_PKGLIBDIR "/os-modules")
OPTION(osd_modules, OPT_STR, "") // comma-separated list of ObjectStore modules

// OSD's leveldb write buffer size
OPTION(osd_leveldb_write_buffer_size, OPT_U64, 0)
OPTION(osd_leveldb_cache_size, OPT_U64, 0) // OSD's leveldb cache size
OPTION(osd_leveldb_block_size, OPT_U64, 0) // OSD's leveldb block size
// OSD's leveldb bloom bits per entry
OPTION(osd_leveldb_bloom_size, OPT_INT, 0)
OPTION(osd_leveldb_max_open_files, OPT_INT, 0) // OSD's leveldb max open files
// OSD's leveldb uses compression
OPTION(osd_leveldb_compression, OPT_BOOL, true)
OPTION(osd_leveldb_paranoid, OPT_BOOL, false) // OSD's leveldb paranoid flag
OPTION(osd_leveldb_log, OPT_STR, "")  // enable OSD leveldb log file

// leveldb write buffer size
OPTION(leveldb_write_buffer_size, OPT_U64, 8 *1024*1024)
OPTION(leveldb_cache_size, OPT_U64, 128 *1024*1024) // leveldb cache size
OPTION(leveldb_block_size, OPT_U64, 0) // leveldb block size
OPTION(leveldb_bloom_size, OPT_INT, 0) // leveldb bloom bits per entry
OPTION(leveldb_max_open_files, OPT_INT, 0) // leveldb max open files
OPTION(leveldb_compression, OPT_BOOL, true) // leveldb uses compression
OPTION(leveldb_paranoid, OPT_BOOL, false) // leveldb paranoid flag
OPTION(leveldb_log, OPT_STR, "/dev/null")  // enable leveldb log file
OPTION(leveldb_compact_on_mount, OPT_BOOL, false)

OPTION(osd_client_op_priority, OPT_U32, 63)

// Max time to wait between notifying mon of shutdown and shutting down
OPTION(osd_mon_shutdown_timeout, OPT_TIME, 5s)

// OSD's maximum object size
OPTION(osd_max_object_size, OPT_U64, 100*1024L*1024L*1024L)
OPTION(osd_max_attr_size, OPT_U64, 0)

OPTION(osd_objectstore, OPT_STR, "filestore")  // ObjectStore backend type
// Override maintaining compatibility with older OSDs
// Set to true for testing.  Users should NOT set this.
OPTION(osd_debug_override_acting_compat, OPT_BOOL, false)

OPTION(osd_bench_small_size_max_iops, OPT_U32, 100) // 100 IOPS
OPTION(osd_bench_large_size_max_throughput, OPT_U64, 100 << 20) // 100 MB/s
// cap the block size at 64MB
OPTION(osd_bench_max_block_size, OPT_U64, 64 << 20)
// duration of 'osd bench', capped at 30s to avoid triggering timeouts
OPTION(osd_bench_duration, OPT_TIME, 30s)

OPTION(filestore_debug_disable_sharded_check, OPT_BOOL, false)

/// filestore wb throttle limits
OPTION(filestore_wbthrottle_enable, OPT_BOOL, true)
OPTION(filestore_wbthrottle_btrfs_bytes_start_flusher, OPT_U64, 41943040)
OPTION(filestore_wbthrottle_btrfs_bytes_hard_limit, OPT_U64, 419430400)
OPTION(filestore_wbthrottle_btrfs_ios_start_flusher, OPT_U64, 500)
OPTION(filestore_wbthrottle_btrfs_ios_hard_limit, OPT_U64, 5000)
OPTION(filestore_wbthrottle_btrfs_inodes_start_flusher, OPT_U64, 500)
OPTION(filestore_wbthrottle_xfs_bytes_start_flusher, OPT_U64, 41943040)
OPTION(filestore_wbthrottle_xfs_bytes_hard_limit, OPT_U64, 419430400)
OPTION(filestore_wbthrottle_xfs_ios_start_flusher, OPT_U64, 500)
OPTION(filestore_wbthrottle_xfs_ios_hard_limit, OPT_U64, 5000)
OPTION(filestore_wbthrottle_xfs_inodes_start_flusher, OPT_U64, 500)

/// These must be less than the fd limit
OPTION(filestore_wbthrottle_btrfs_inodes_hard_limit, OPT_U64, 5000)
OPTION(filestore_wbthrottle_xfs_inodes_hard_limit, OPT_U64, 5000)

// Tests index failure paths
OPTION(filestore_index_retry_probability, OPT_DOUBLE, 0)

// Allow object read error injection
OPTION(filestore_debug_inject_read_err, OPT_BOOL, false)

// Expensive debugging check on sync
OPTION(filestore_debug_omap_check, OPT_BOOL, 0)

// Use omap for xattrs for attrs over
// filestore_max_inline_xattr_size or
OPTION(filestore_max_inline_xattr_size, OPT_U32, 0)	//Override
OPTION(filestore_max_inline_xattr_size_xfs, OPT_U32, 65536)
OPTION(filestore_max_inline_xattr_size_btrfs, OPT_U32, 2048)
OPTION(filestore_max_inline_xattr_size_other, OPT_U32, 512)

// for more than filestore_max_inline_xattrs attrs
OPTION(filestore_max_inline_xattrs, OPT_U32, 0)	//Override
OPTION(filestore_max_inline_xattrs_xfs, OPT_U32, 10)
OPTION(filestore_max_inline_xattrs_btrfs, OPT_U32, 10)
OPTION(filestore_max_inline_xattrs_other, OPT_U32, 2)

OPTION(filestore_sloppy_crc, OPT_BOOL, false)	      // track sloppy crcs
OPTION(filestore_sloppy_crc_block_size, OPT_INT, 65536)

OPTION(filestore_max_alloc_hint_size, OPT_U64, 1ULL << 20) // bytes

OPTION(filestore_max_sync_interval, OPT_TIME, 5s)
OPTION(filestore_min_sync_interval, OPT_TIME, 10ms)
OPTION(filestore_btrfs_snap, OPT_BOOL, true)
OPTION(filestore_btrfs_clone_range, OPT_BOOL, true)
OPTION(filestore_zfs_snap, OPT_BOOL, false) // zfsonlinux is still unstable
OPTION(filestore_fsync_flushes_journal_data, OPT_BOOL, false)
OPTION(filestore_fiemap, OPT_BOOL, false)     // (try to) use fiemap
OPTION(filestore_journal_parallel, OPT_BOOL, false)
OPTION(filestore_journal_writeahead, OPT_BOOL, false)
OPTION(filestore_journal_trailing, OPT_BOOL, false)
OPTION(filestore_queue_max_ops, OPT_INT, 50)
OPTION(filestore_queue_max_bytes, OPT_INT, 100 << 20)
// this is ON TOP of filestore_queue_max_*
OPTION(filestore_queue_committing_max_ops, OPT_INT, 500)
OPTION(filestore_queue_committing_max_bytes, OPT_INT, 100 << 20) //  "
OPTION(filestore_op_threads, OPT_INT, 2)
OPTION(filestore_op_thread_timeout, OPT_TIME, 60s)
OPTION(filestore_op_thread_suicide_timeout, OPT_TIME, 180s)
OPTION(filestore_commit_timeout, OPT_TIME, 600s)
OPTION(filestore_fiemap_threshold, OPT_INT, 4096)
OPTION(filestore_merge_threshold, OPT_INT, 10)
OPTION(filestore_split_multiple, OPT_INT, 2)
// drop any new transactions on the floor
OPTION(filestore_blackhole, OPT_BOOL, false)
// FD lru size
OPTION(filestore_fd_cache_size, OPT_INT, 128)
// file onto which store transaction dumps
OPTION(filestore_dump_file, OPT_STR, "")
// inject a failure at the n'th opportunity
OPTION(filestore_kill_at, OPT_INT, 0)
// artificially stall in op queue thread
OPTION(filestore_inject_stall, OPT_TIME, 0s)
OPTION(filestore_fail_eio, OPT_BOOL, true)	 // fail/crash on EIO
OPTION(filestore_replica_fadvise, OPT_BOOL, true)
OPTION(filestore_debug_verify_split, OPT_BOOL, false)
OPTION(journal_dio, OPT_BOOL, true)
OPTION(journal_aio, OPT_BOOL, true)
OPTION(journal_force_aio, OPT_BOOL, false)

OPTION(keyvaluestore_queue_max_ops, OPT_INT, 50)
OPTION(keyvaluestore_queue_max_bytes, OPT_INT, 100 << 20)
// Expensive debugging check on sync
OPTION(keyvaluestore_debug_check_backend, OPT_BOOL, 0)
OPTION(keyvaluestore_op_threads, OPT_INT, 2)
OPTION(keyvaluestore_op_thread_timeout, OPT_INT, 60)
OPTION(keyvaluestore_op_thread_suicide_timeout, OPT_TIME, 3min)

OPTION(memstore_page_partitions, OPT_INT, 4)
OPTION(memstore_pages_per_stripe, OPT_INT, 16)

// max bytes to search ahead in journal searching for corruption
OPTION(journal_max_corrupt_search, OPT_U64, 10<<20)
OPTION(journal_block_align, OPT_BOOL, true)
OPTION(journal_write_header_frequency, OPT_U64, 0)
OPTION(journal_max_write_bytes, OPT_INT, 10 << 20)
OPTION(journal_max_write_entries, OPT_INT, 100)
OPTION(journal_queue_max_ops, OPT_INT, 300)
OPTION(journal_queue_max_bytes, OPT_INT, 32 << 20)
// align data payloads >= this.
OPTION(journal_align_min_size, OPT_INT, 64 << 10)
OPTION(journal_replay_from, OPT_INT, 0)
OPTION(journal_zero_on_create, OPT_BOOL, false)
// assume journal is not corrupt
OPTION(journal_ignore_corruption, OPT_BOOL, false)

// FragTreeIndex
OPTION(fragtreeindex_initial_split, OPT_INT, 4) // start with 2^N subdirs
OPTION(fragtreeindex_merge_threshold, OPT_INT, 256) // merge under N entries
OPTION(fragtreeindex_split_threshold, OPT_INT, 4096) // split over N entries
OPTION(fragtreeindex_split_bits, OPT_INT, 2) // split each subdir into 2^N
OPTION(fragtreeindex_migration_threads, OPT_INT, 2) // migration thread pool size

// how many seconds to wait for a response from the monitor before
// returning an error from a rados operation. 0 means on limit.
OPTION(rados_mon_op_timeout, OPT_TIME, 30s)
// how many seconds to wait for a response from osds before returning
// an error from a rados operation. 0 means no limit.
OPTION(rados_osd_op_timeout, OPT_TIME, 5min)

OPTION(nss_db_path, OPT_STR, "") // path to nss db


OPTION(rgw_max_chunk_size, OPT_INT, 512 * 1024)

OPTION(rgw_data, OPT_STR, "/var/lib/ceph/radosgw/$cluster-$id")
OPTION(rgw_enable_apis, OPT_STR, "s3, swift, swift_auth, admin")
OPTION(rgw_cache_enabled, OPT_BOOL, true)   // rgw cache enabled
OPTION(rgw_cache_lru_size, OPT_INT, 10000)   // num of entries in rgw cache
// path to unix domain socket, if not specified, rgw will not run as
// external fcgi
OPTION(rgw_socket_path, OPT_STR, "")
// host for radosgw, can be an IP, default is 0.0.0.0
OPTION(rgw_host, OPT_STR, "")
// port to listen, format as "8080" "5000", if not specified, rgw will
// not run external fcgi
OPTION(rgw_port, OPT_STR, "")
OPTION(rgw_dns_name, OPT_STR, "")
// alternative value for SCRIPT_URI if not set in request
OPTION(rgw_script_uri, OPT_STR, "")
// alternative value for REQUEST_URI if not set in request
OPTION(rgw_request_uri, OPT_STR,  "")
// the swift url, being published by the internal swift auth
OPTION(rgw_swift_url, OPT_STR, "")
// entry point for which a url is considered a swift url
OPTION(rgw_swift_url_prefix, OPT_STR, "swift")
// default URL to go and verify tokens for v1 auth (if not using
// internal swift auth)
OPTION(rgw_swift_auth_url, OPT_STR, "")
// entry point for which a url is considered a swift auth url
OPTION(rgw_swift_auth_entry, OPT_STR, "auth")
// tenant name to use for swift access
OPTION(rgw_swift_tenant_name, OPT_STR, "")
// url for keystone server
OPTION(rgw_keystone_url, OPT_STR, "")
// keystone admin token (shared secret)
OPTION(rgw_keystone_admin_token, OPT_STR, "")
// keystone admin user name
OPTION(rgw_keystone_admin_user, OPT_STR, "")
// keystone admin user password
OPTION(rgw_keystone_admin_password, OPT_STR, "")
// keystone admin user tenant
OPTION(rgw_keystone_admin_tenant, OPT_STR, "")
// roles required to serve requests
OPTION(rgw_keystone_accepted_roles, OPT_STR, "Member, admin")
// max number of entries in keystone token cache
OPTION(rgw_keystone_token_cache_size, OPT_INT, 10000)
// Time between tokens revocation check
OPTION(rgw_keystone_revocation_interval, OPT_TIME, 15min)
// should we try to use the internal credentials for s3?
OPTION(rgw_s3_auth_use_rados, OPT_BOOL, true)
// should we try to use keystone for s3?
OPTION(rgw_s3_auth_use_keystone, OPT_BOOL, false)
// entry point for which a url is considered an admin request
OPTION(rgw_admin_entry, OPT_STR, "admin")
OPTION(rgw_enforce_swift_acls, OPT_BOOL, true)
// time in seconds for swift token expiration
OPTION(rgw_swift_token_expiration, OPT_TIME, 24h)
// enable if 100-Continue works
OPTION(rgw_print_continue, OPT_BOOL, true)
// e.g. X-Forwarded-For, if you have a reverse proxy
OPTION(rgw_remote_addr_param, OPT_STR, "REMOTE_ADDR")
OPTION(rgw_op_thread_timeout, OPT_TIME, 10min)
OPTION(rgw_op_thread_suicide_timeout, OPT_TIME, 0ns)
OPTION(rgw_thread_pool_size, OPT_INT, 100)
OPTION(rgw_num_control_oids, OPT_INT, 8)

OPTION(rgw_zone, OPT_STR, "") // zone name
// vol where zone specific info is stored
OPTION(rgw_zone_root_vol, OPT_STR, ".rgw.root")
OPTION(rgw_region, OPT_STR, "") // region name
// vol where all region info is stored
OPTION(rgw_region_root_vol, OPT_STR, ".rgw.root")
// oid where default region info is stored
OPTION(rgw_default_region_info_oid, OPT_STR, "default.region")
OPTION(rgw_log_nonexistent_bucket, OPT_BOOL, false)
// man date to see codes (a subset are supported)
OPTION(rgw_log_object_name, OPT_STR, "%Y-%m-%d-%H-%i-%n")
OPTION(rgw_log_object_name_utc, OPT_BOOL, false)
OPTION(rgw_usage_max_shards, OPT_INT, 32)
OPTION(rgw_usage_max_user_shards, OPT_INT, 1)
// enable logging every rgw operation
OPTION(rgw_enable_ops_log, OPT_BOOL, false)
// enable logging bandwidth usage
OPTION(rgw_enable_usage_log, OPT_BOOL, false)
// whether ops log should go to rados
OPTION(rgw_ops_log_rados, OPT_BOOL, true)
// path to unix domain socket where ops log can go
OPTION(rgw_ops_log_socket_path, OPT_STR, "")
// max data backlog for ops log
OPTION(rgw_ops_log_data_backlog, OPT_INT, 5 << 20)
// threshold to flush pending log data
OPTION(rgw_usage_log_flush_threshold, OPT_INT, 1024)
// How often to flush pending log data
OPTION(rgw_usage_log_tick_interval, OPT_TIME, 30s)
// man date to see codes (a subset are supported)
OPTION(rgw_intent_log_object_name, OPT_STR, "%Y-%m-%d-%i-%n")
OPTION(rgw_intent_log_object_name_utc, OPT_BOOL, false)
OPTION(rgw_init_timeout, OPT_TIME, 5min)
OPTION(rgw_mime_types_file, OPT_STR, "/etc/mime.types")
OPTION(rgw_gc_max_objs, OPT_INT, 32)
// wait time before object may be handled by gc
OPTION(rgw_gc_obj_min_wait, OPT_TIME, 2h)
// total run time for a single gc processor work
OPTION(rgw_gc_processor_max_time, OPT_TIME, 1h)
// gc processor cycle time
OPTION(rgw_gc_processor_period, OPT_TIME, 1h)
// alternative success status response for create-oid (0 - default)
OPTION(rgw_s3_success_create_obj_status, OPT_INT, 0)
// should rgw try to resolve hostname as a dns cname record
OPTION(rgw_resolve_cname, OPT_BOOL, false)
OPTION(rgw_obj_stripe_size, OPT_INT, 4 << 20)
// list of extended attrs that can be set on objects (beyond the default)
OPTION(rgw_extended_http_attrs, OPT_STR, "")
// how long to wait for process to go down before exiting unconditionally
OPTION(rgw_exit_timeout, OPT_TIME, 2min)
// window size in bytes for single get oid request
OPTION(rgw_get_obj_window_size, OPT_INT, 16 << 20)
// max length of a single get oid rados op
OPTION(rgw_get_obj_max_req_size, OPT_INT, 4 << 20)
// enable relaxed bucket name rules for US region buckets
OPTION(rgw_relaxed_s3_bucket_names, OPT_BOOL, false)
// if the user has bucket perms, use those before key perms (recurse
// and full_control)
OPTION(rgw_defer_to_bucket_acls, OPT_STR, "")
// max buckets to retrieve in a single op when listing user buckets
OPTION(rgw_list_buckets_max_chunk, OPT_INT, 1000)
// max shards for metadata log
OPTION(rgw_md_log_max_shards, OPT_INT, 64)
// max shards for keeping inter-region copy progress info
OPTION(rgw_num_zone_opstate_shards, OPT_INT, 128)
// min time between opstate updates on a single upload (0 for
// disabling ratelimit)
OPTION(rgw_opstate_ratelimit_time, OPT_TIME, 30s)
OPTION(rgw_curl_wait_timeout, OPT_TIME, 1s) // timeout for certain curl calls
// should dump progress during long copy operations?
OPTION(rgw_copy_obj_progress, OPT_BOOL, true)
// min bytes between copy progress output
OPTION(rgw_copy_obj_progress_every_bytes, OPT_INT, 1024 * 1024)

// data log entries window (in seconds)
OPTION(rgw_data_log_window, OPT_INT, 30)
// number of in-memory entries to hold for data changes log
OPTION(rgw_data_log_changes_size, OPT_INT, 1000)
// number of objects to keep data changes log on
OPTION(rgw_data_log_num_shards, OPT_INT, 128)
OPTION(rgw_data_log_obj_prefix, OPT_STR, "data_log") //
OPTION(rgw_replica_log_obj_prefix, OPT_STR, "replica_log") //

// time for cached bucket stats to be cached within rgw instance
OPTION(rgw_bucket_quota_ttl, OPT_TIME, 10min)
// threshold from which we don't rely on cached info for quota decisions
OPTION(rgw_bucket_quota_soft_threshold, OPT_DOUBLE, 0.95)
// number of entries in bucket quota cache
OPTION(rgw_bucket_quota_cache_size, OPT_INT, 10000)

// Return the bucket name in the 'Bucket' response header
OPTION(rgw_expose_bucket, OPT_BOOL, false)

// alternative front ends
OPTION(rgw_frontends, OPT_STR, "")

// time period for accumulating modified buckets before syncing stats
OPTION(rgw_user_quota_bucket_sync_interval, OPT_TIME, 3min)
// time period for accumulating modified buckets before syncing entire
// user stats
OPTION(rgw_user_quota_sync_interval, OPT_TIME, 24h)
// whether stats for idle users be fully synced
OPTION(rgw_user_quota_sync_idle_users, OPT_BOOL, false)
// min time between two full stats syc for non-idle users
OPTION(rgw_user_quota_sync_wait_time, OPT_TIME, 24h)

// min size for each part (except for last one) in multipart upload
OPTION(rgw_multipart_min_part_size, OPT_INT, 5 * 1024 * 1024)

// This will be set to true when it is safe to start threads.
// Once it is true, it will never change.
OPTION(internal_safe_to_start_threads, OPT_BOOL, false)
