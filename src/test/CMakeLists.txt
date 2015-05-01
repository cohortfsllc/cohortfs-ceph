if(NOT ${WITH_MDS})
  add_definitions(-DNO_MDS)
endif(NOT ${WITH_MDS})

if(NOT ${WITH_CLIENT})
  add_definitions(-DNO_CLIENT)
endif(NOT ${WITH_CLIENT})

# test_timers
add_executable(test_timers
  TestTimers.cc
  )
target_link_libraries(test_timers global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_signal_handlers
add_executable(test_signal_handlers
  TestSignalHandlers.cc
  )
target_link_libraries(test_signal_handlers global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS})

# test_msgr
add_executable(test_msgr
  testmsgr.cc
  )
target_link_libraries(test_msgr global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_crypt
add_executable(test_crypt
  testcrypto.cc
  )
target_link_libraries(test_crypt
  global
  ${CRYPTO_LIBS}
  m
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  )

# test_rados
add_executable(test_rados
  osd/TestRados.cc
  osd/TestOpStat.cc
  osd/Object.cc 
  osd/RadosModel.cc
  )
target_link_libraries(test_rados
  librados
  global
  ${CMAKE_DL_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  )

# test_mutate
add_executable(test_mutate
  test_mutate.cc
  )
target_link_libraries(test_mutate global librados ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS})

# test_rewrite_latency
add_executable(test_rewrite_latency
  test_rewrite_latency.cc
  )
target_link_libraries(test_rewrite_latency common ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})

# streamtest
add_executable(streamtest
  streamtest.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
target_link_libraries(streamtest os global leveldb aio boost_filesystem
  boost_system ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_trans
add_executable(test_trans
  test_trans.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
target_link_libraries(test_trans os global leveldb aio boost_filesystem
  boost_system ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_keys
add_executable(test_keys
  testkeys.cc
  )
target_link_libraries(test_keys mon global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

## dencoder
set(dencoder_srcs
  encoding/ceph_dencoder.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  $<TARGET_OBJECTS:osdc_objs>
  )
if(0)
  list(APPEND dencoder_srcs
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_tools.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_gc.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_env.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_dencoder.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_json_enc.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_multi.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rados.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_bucket.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_acl.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_xml.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_acl_s3.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_quota.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_cors.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_cache.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_cors_s3.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_client_io.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_op.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_user.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_resolve.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_swift.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_s3.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_usage.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_user.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_bucket.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_http_client.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_swift.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_swift_auth.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_keystone.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_client.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_metadata.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_config.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_conn.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_log.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_metadata.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_opstate.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_rest_replica_log.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_common.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_multi_del.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_formats.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_acl_swift.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_policy_s3.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_auth_s3.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_usage.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_log.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_loadgen.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_civetweb.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_fcgi.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_replica_log.cc
    ${CMAKE_SOURCE_DIR}/src/civetweb/src/civetweb.c
    ${CMAKE_SOURCE_DIR}/src/civetweb/src/CivetServer.cpp
  )
endif(0)
set(dencoder_dependencies
  cls_lock_client
  cls_refcount_client
  cls_replica_log_client
  librados
  osd)
if(WITH_MDS)
  list(APPEND dencoder_dependencies mds)
endif(WITH_MDS)
list(APPEND dencoder_dependencies
  mon
  os
  global
  boost_filesystem
  boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS})
if(0)
  list(APPEND dencoder_dependencies
    cls_log_client
    cls_statelog_client
    cls_version_client
    cls_kvs
    cls_user_client
    curl
    expat
    fcgi
    resolv)
endif(0)

add_executable(ceph-dencoder ${dencoder_srcs}
)
target_link_libraries(ceph-dencoder ${dencoder_dependencies})

# get_command_descriptions
add_executable(get_command_descriptions
  common/get_command_descriptions.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  $<TARGET_OBJECTS:os_mon_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(get_command_descriptions
  mon
  global
  leveldb
  boost_regex
  boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

## Build tests
# These should all use explicit _CXXFLAGS so avoid basename conflicts

set(WITH_BUILD_TESTS ON)
if(${WITH_BUILD_TESTS})
set(build_libcommon_srcs
  buildtest_skeleton.cc
  )
add_executable(test_build_libcommon
  ${build_libcommon_srcs}
  $<TARGET_OBJECTS:common_objs>
  )
target_link_libraries(test_build_libcommon json_spirit  rt pthread global ${Boost_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})

set(build_librados_srcs
  buildtest_skeleton.cc
    )
add_executable(test_build_librados
  ${build_librados_srcs}
  $<TARGET_OBJECTS:librados_objs>
  $<TARGET_OBJECTS:osdc_objs>)
target_link_libraries(test_build_librados pthread ${CRYPTO_LIBS} ${EXTRALIBS} global osd global common cls_lock_client)
 
if(${WITH_RADOSGW})
  set(build_librgw_srcs
  buildtest_skeleton.cc
    )
  add_executable(test_build_librgw
  ${build_librgw_srcs}
    )
target_link_libraries(test_build_librgw librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_version_client
    cls_replica_log_client cls_user_client
    curl expat global fcgi resolv ${ALLOC_LIBS} pthread ${CRYPTO_LIBS} ${EXTRALIBS} global)
endif(${WITH_RADOSGW})
endif(${WITH_BUILD_TESTS})

## Benchmarks

# smalliobench
set(smalliobench_srcs
  bench/small_io_bench.cc
  bench/rados_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobench
  ${smalliobench_srcs}
  )
target_link_libraries(smalliobench librados boost_program_options global
  boost_filesystem ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# smalliobenchfs
set(smalliobenchfs_srcs
  bench/small_io_bench_fs.cc
  bench/testfilestore_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchfs
  ${smalliobenchfs_srcs}
  $<TARGET_OBJECTS:os_filestore_objs>
  )
target_link_libraries(smalliobenchfs librados boost_program_options os global
  leveldb aio boost_filesystem boost_system ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# smalliobenchdumb
set(smalliobenchdumb_srcs
  bench/small_io_bench_dumb.cc
  bench/dumb_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchdumb
  ${smalliobenchdumb_srcs}
  )
target_link_libraries(smalliobenchdumb librados boost_program_options os global
  boost_filesystem ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# smalliobenchrbd
set(smalliobenchrbd_srcs
  bench/small_io_bench_rbd.cc
  bench/rbd_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchrbd
  ${smalliobenchrbd_srcs}
  )
target_link_libraries(smalliobenchrbd librbd librados boost_program_options os
  global boost_filesystem  ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# tpbench
set(tpbench_srcs
  bench/tp_bench.cc
  bench/detailed_stat_collector.cc)
add_executable(tpbench
  ${tpbench_srcs}
  )
target_link_libraries(tpbench librados boost_filesystem boost_program_options
  os global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# omapbench
set(omapbench_srcs
  omap_bench.cc
  )
add_executable(omapbench
  ${omapbench_srcs}
  )
target_link_libraries(omapbench
  librados
  boost_program_options
  os
  global
  boost_filesystem
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  )

# kvstorebench
set(kvstorebench_srcs
  kv_store_bench.cc
  ${CMAKE_SOURCE_DIR}/src/key_value_store/kv_flat_btree_async.cc
  )
add_executable(kvstorebench
  ${kvstorebench_srcs}
  )
target_link_libraries(kvstorebench librados global boost_filesystem
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

## System tests

# systest
set(libsystest_srcs system/cross_process_sem.cc
  system/systest_runnable.cc
  system/systest_settings.cc
  system/st_rados_create_volume.cc
  system/st_rados_delete_volume.cc
  system/st_rados_list_objects.cc
  system/st_rados_watch.cc
  system/st_rados_notify.cc)
add_library(systest STATIC ${libsystest_srcs})

# test_rados_list_parallel
add_executable(test_rados_list_parallel
  system/rados_list_parallel.cc
  )
target_link_libraries(test_rados_list_parallel librados systest global pthread
  rt ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_rados_open_volumess_parallel
set(test_rados_open_volumes_parallel_srcs system/rados_open_volumes_parallel.cc)
add_executable(test_rados_open_volumes_parallel
  ${test_rados_open_volumes_parallel_srcs}
  )
target_link_libraries(test_rados_open_volumes_parallel librados systest global
  pthread rt ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_rados_delete_volumes_parallel
set(test_rados_delete_volumes_parallel_srcs
  system/rados_delete_volumes_parallel.cc
  system/st_rados_create_volume.cc
  system/st_rados_delete_volume.cc
  system/st_rados_list_objects.cc

  )
add_executable(test_rados_delete_volumes_parallel
  ${test_rados_delete_volumes_parallel_srcs}
  )
target_link_libraries(test_rados_delete_volumes_parallel librados systest global
  pthread rt ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# test_rados_watch_notify
set(test_rados_watch_notify_srcs
  system/rados_watch_notify.cc
  system/st_rados_create_volume.cc
  system/st_rados_delete_volume.cc
  system/st_rados_delete_objs.cc
  system/st_rados_watch.cc
  system/st_rados_notify.cc
  )
add_executable(test_rados_watch_notify
  ${test_rados_watch_notify_srcs}
  )
target_link_libraries(test_rados_watch_notify librados systest global
  pthread rt ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

# bench_log
set(bench_log_srcs
  bench_log.cc
  )
add_executable(bench_log
  ${bench_log_srcs}
  )
target_link_libraries(bench_log global pthread rt ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

#Make check starts here
add_custom_target(symlinks  COMMAND
  ln -sf ${CMAKE_SOURCE_DIR}/src/test/ ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-mon ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/ceph ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_SOURCE_DIR}/qa/ ${CMAKE_BINARY_DIR}/src/
  COMMENT "Symlinks have been created")
add_dependencies(check symlinks)

add_test(NAME ceph_argparse_py COMMAND python ${CMAKE_SOURCE_DIR}/src/test/pybind/test_ceph_argparse.py)
add_dependencies(check ceph_argparse_py)

add_test(NAME unittest_bufferlist_shell COMMAND bash ${CMAKE_SOURCE_DIR}/src/unittest_bufferlist.sh) 
add_dependencies(check unittest_bufferlist_shell)

add_test(NAME check_generated COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/encoding/check-generated.sh)
add_dependencies(check check_generated)

add_test(NAME misc COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/misc.sh)
add_dependencies(check misc)

add_test(NAME osd_erasure_code_profile COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/osd-erasure-code-profile.sh) 
add_dependencies(check osd_erasure_code_profile)

add_test(NAME mkfs COMMAND bash	${CMAKE_SOURCE_DIR}/src/test/mon/mkfs.sh)
add_dependencies(check mkfs)

add_test(NAME ceph_disk COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/ceph-disk.sh)
add_dependencies(check ceph_disk)

add_test(NAME mon_handle_forward COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/mon/mon-handle-forward.sh)
add_dependencies(check mon_handle_forward)

add_test(NAME vstart_wrapped_tests COMMAND bash ${CMAKE_SOURCE_DIR}/src/test/vstart_wrapped_tests.sh)
add_dependencies(check mon_handle_forward)

set(UNITTEST_LIBS gtest_main ${PTHREAD_LIBS})
set(UNITTEST_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${CMAKE_SOURCE_DIR}/src/gtest/include -I${CMAKE_BINARY_DIR}/src/gtest/include -fno-strict-aliasing")

if (WITH_CLIENT)
  list(APPEND UNITTEST_LIBS cephfs boost_system)
endif(WITH_CLIENT)

# unittest_encoding
set(unittest_encoding_srcs
  encoding.cc
  )
add_executable(unittest_encoding EXCLUDE_FROM_ALL
  ${unittest_encoding_srcs}
  )
add_test(unittest_encoding unittest_encoding)
add_dependencies(check unittest_encoding)
target_link_libraries(unittest_encoding librados global boost_filesystem
  pthread rt m ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_encoding
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_addrs
set(unittest_addrs_srcs
  test_addrs.cc
  )
add_executable(unittest_addrs EXCLUDE_FROM_ALL
  ${unittest_addrs_srcs}
  )
add_test(unittest_addrs unittest_addrs)
add_dependencies(check unittest_addrs)
target_link_libraries(unittest_addrs
  librados global boost_filesystem pthread rt m
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_addrs
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_histogram
add_executable(unittest_histogram EXCLUDE_FROM_ALL
  common/histogram.cc
  )
add_test(unittest_histogram unittest_histogram)
add_dependencies(check unittest_histogram)
target_link_libraries(unittest_histogram global
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_histogram
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_str_map
set(unittest_str_map_srcs
  common/test_str_map.cc
  )
add_executable(unittest_str_map EXCLUDE_FROM_ALL
  ${unittest_str_map_srcs}
  )
add_test(unittest_str_map unittest_str_map)
add_dependencies(check unittest_str_map)
target_link_libraries(unittest_str_map global
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_str_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sharedptr_registry
set(unittest_sharedptr_registry_srcs
  common/test_sharedptr_registry.cc
  )
add_executable(unittest_sharedptr_registry EXCLUDE_FROM_ALL
  ${unittest_sharedptr_registry_srcs}
  )
add_test(unittest_sharedptr_registry unittest_sharedptr_registry)
add_dependencies(check unittest_sharedptr_registry)
target_link_libraries(unittest_sharedptr_registry global
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sharedptr_registry
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sloppy_crc_map
set(unittest_sloppy_crc_map_srcs
  common/test_sloppy_crc_map.cc
  )
add_executable(unittest_sloppy_crc_map EXCLUDE_FROM_ALL
  ${unittest_sloppy_crc_map_srcs}
  )
add_test(unittest_sloppy_crc_map unittest_sloppy_crc_map)
add_dependencies(check unittest_sloppy_crc_map)
target_link_libraries(unittest_sloppy_crc_map global
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sloppy_crc_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_util
set(unittest_util_srcs
  common/test_util.cc
  ${CMAKE_SOURCE_DIR}/src/common/util.cc
  )
add_executable(unittest_util EXCLUDE_FROM_ALL
  ${unittest_util_srcs}
  )
add_test(unittest_util unittest_util)
add_dependencies(check unittest_util)
target_link_libraries(unittest_util
  global
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_util
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_osdmap
set(unittest_osdmap_srcs osd/TestOSDMap.cc)
add_executable(unittest_osdmap EXCLUDE_FROM_ALL
  ${unittest_osdmap_srcs}
  )
add_test(unittest_osdmap unittest_osdmap)
add_dependencies(check unittest_osdmap)
target_link_libraries(unittest_osdmap global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_osdmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_workqueue
set(unittest_workqueue_srcs test_workqueue.cc)
add_executable(unittest_workqueue EXCLUDE_FROM_ALL
  ${unittest_workqueue_srcs}
  )
add_test(unittest_workqueue unittest_workqueue)
add_dependencies(check unittest_workqueue)
target_link_libraries(unittest_workqueue global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_workqueue PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_prebufferedstreambuf
set(unittest_prebufferedstreambuf_srcs test_prebufferedstreambuf.cc)
add_executable(unittest_prebufferedstreambuf EXCLUDE_FROM_ALL
  ${unittest_prebufferedstreambuf_srcs}
  )
add_test(unittest_prebufferedstreambuf unittest_prebufferedstreambuf)
add_dependencies(check unittest_prebufferedstreambuf)
target_link_libraries(unittest_prebufferedstreambuf global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_prebufferedstreambuf PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_str_list
set(unittest_str_list_srcs test_str_list.cc)
add_executable(unittest_str_list EXCLUDE_FROM_ALL
  ${unittest_str_list_srcs}
  )
add_test(unittest_str_list unittest_str_list)
add_dependencies(check unittest_str_list)
target_link_libraries(unittest_str_list global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_str_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_log
set(unittest_log_srcs ${CMAKE_SOURCE_DIR}/src/log/test.cc)
add_executable(unittest_log EXCLUDE_FROM_ALL
  ${unittest_log_srcs}
  )
add_test(unittest_log unittest_log)
add_dependencies(check unittest_log)
target_link_libraries(unittest_log global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_throttle
set(unittest_throttle_srcs common/Throttle.cc)
add_executable(unittest_throttle EXCLUDE_FROM_ALL
  ${unittest_throttle_srcs}
  )
add_test(unittest_throttle unittest_throttle)
add_dependencies(check unittest_throttle)
target_link_libraries(unittest_throttle global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_throttle PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_base64
set(unittest_base64_srcs base64.cc)
add_executable(unittest_base64 EXCLUDE_FROM_ALL
  ${unittest_base64_srcs}
  )
add_test(unittest_base64 unittest_base64)
add_dependencies(check unittest_base64)
target_link_libraries(unittest_base64 global ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_base64 PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ceph_argparse
set(unittest_ceph_argparse_srcs ceph_argparse.cc)
add_executable(unittest_ceph_argparse EXCLUDE_FROM_ALL
  ${unittest_ceph_argparse_srcs}
  )
add_test(unittest_ceph_argparse unittest_ceph_argparse)
add_dependencies(check unittest_ceph_argparse)
target_link_libraries(unittest_ceph_argparse global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_argparse PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ceph_compatset
set(unittest_ceph_compatset_srcs ceph_compatset.cc)
add_executable(unittest_ceph_compatset EXCLUDE_FROM_ALL
  ${unittest_ceph_compatset_srcs}
  )
add_test(unittest_ceph_compatset unittest_ceph_compatset)
add_dependencies(check unittest_ceph_compatset)
target_link_libraries(unittest_ceph_compatset global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_compatset PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_osd_types
set(unittest_osd_types_srcs osd/types.cc)
add_executable(unittest_osd_types EXCLUDE_FROM_ALL
  ${unittest_osd_types_srcs}
  )
add_test(unittest_osd_types unittest_osd_types)
add_dependencies(check unittest_osd_types)
target_link_libraries(unittest_osd_types global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_osd_types PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_gather
set(unittest_gather_srcs gather.cc)
add_executable(unittest_gather EXCLUDE_FROM_ALL
  ${unittest_gather_srcs}
  )
add_test(unittest_gather unittest_gather)
add_dependencies(check unittest_gather)
target_link_libraries(unittest_gather global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_gather PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# run_cmd
set(unittest_run_cmd_srcs run_cmd.cc)
add_executable(unittest_run_cmd EXCLUDE_FROM_ALL
  ${unittest_run_cmd_srcs}
  )
add_test(unittest_run_cmd unittest_run_cmd)
add_dependencies(check unittest_run_cmd)
target_link_libraries(unittest_run_cmd global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_run_cmd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# signals
set(unittest_signals_srcs signals.cc)
add_executable(unittest_signals EXCLUDE_FROM_ALL
  ${unittest_signals_srcs}
  )
add_test(unittest_signals unittest_signals)
add_dependencies(check unittest_signals)
target_link_libraries(unittest_signals global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_signals PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_simple_spin
set(unittest_simple_spin_srcs simple_spin.cc)
add_executable(unittest_simple_spin EXCLUDE_FROM_ALL
  ${unittest_simple_spin_srcs}
  )
add_test(unittest_simple_spin unittest_simple_spin)
add_dependencies(check unittest_simple_spin)
target_link_libraries(unittest_simple_spin global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_simple_spin PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_librados
set(unittest_librados_srcs librados/librados.cc)
add_executable(unittest_librados EXCLUDE_FROM_ALL
  ${unittest_librados_srcs}
  )
add_test(unittest_librados unittest_librados)
add_dependencies(check unittest_librados)
target_link_libraries(unittest_librados
  librados global boost_filesystem
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_librados PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_bufferlist
set(unittest_bufferlist_srcs bufferlist.cc)
add_executable(unittest_bufferlist EXCLUDE_FROM_ALL
  ${unittest_bufferlist_srcs}
  )
add_test(unittest_bufferlist unittest_bufferlist)
add_dependencies(check unittest_bufferlist)
target_link_libraries(unittest_bufferlist global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_bufferlist PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crc32
set(unittest_crc32_srcs common/test_crc32c.cc)
add_executable(unittest_crc32 EXCLUDE_FROM_ALL
  ${unittest_crc32_srcs}
  )
add_test(unittest_crc32 unittest_crc32)
add_dependencies(check unittest_crc32)
target_link_libraries(unittest_crc32 global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_crc32 PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_arch
set(unittest_arch_srcs test_arch.cc)
add_executable(unittest_arch EXCLUDE_FROM_ALL
  ${unittest_arch_srcs}
  )
add_test(unittest_arch unittest_arch)
add_dependencies(check unittest_arch)
target_link_libraries(unittest_arch global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_arch PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_crypto_init
set(unittest_crypto_init_srcs crypto_init.cc)
add_executable(unittest_crypto_init EXCLUDE_FROM_ALL
  ${unittest_crypto_init_srcs}
  )
add_test(unittest_crypto_init unittest_crypto_init)
add_dependencies(check unittest_crypto_init)
target_link_libraries(unittest_crypto_init global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_crypto_init PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_admin_socket
set(unittest_admin_socket_srcs admin_socket.cc)
add_executable(unittest_admin_socket EXCLUDE_FROM_ALL
  ${unittest_admin_socket_srcs}
  )
add_test(unittest_admin_socket unittest_admin_socket)
add_dependencies(check unittest_admin_socket)
target_link_libraries(unittest_admin_socket global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_admin_socket PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ceph_crypto
set(unittest_ceph_crypto_srcs ceph_crypto.cc)
add_executable(unittest_ceph_crypto EXCLUDE_FROM_ALL
  ${unittest_ceph_crypto_srcs}
  )
add_test(unittest_ceph_crypto unittest_ceph_crypto)
add_dependencies(check unittest_ceph_crypto)
target_link_libraries(unittest_ceph_crypto global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_crypto PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_utf8
set(unittest_utf8_srcs utf8.cc)
add_executable(unittest_utf8 EXCLUDE_FROM_ALL
  ${unittest_utf8_srcs}
  )
add_test(unittest_utf8 unittest_utf8)
add_dependencies(check unittest_utf8)
target_link_libraries(unittest_utf8 global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_utf8 PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mime
set(unittest_mime_srcs mime.cc)
add_executable(unittest_mime EXCLUDE_FROM_ALL
  ${unittest_mime_srcs}
  )
add_test(unittest_mime unittest_mime)
add_dependencies(check unittest_mime)
target_link_libraries(unittest_mime global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_mime PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_escape
set(unittest_escape_srcs escape.cc)
add_executable(unittest_escape EXCLUDE_FROM_ALL
  ${unittest_escape_srcs}
  )
add_test(unittest_escape unittest_escape)
add_dependencies(check unittest_escape)
target_link_libraries(unittest_escape global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_escape PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_chain_xattr
set(unittest_chain_xattr_srcs
  objectstore/chain_xattr.cc
  )
add_executable(unittest_chain_xattr EXCLUDE_FROM_ALL
  ${unittest_chain_xattr_srcs}
  )
add_test(unittest_chain_xattr unittest_chain_xattr)
add_dependencies(check unittest_chain_xattr)
set_target_properties(unittest_chain_xattr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_chain_xattr
  os global boost_filesystem
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS}
  )

# unittest_flatindex
set(unittest_flatindex_srcs
  os/TestFlatIndex.cc
  )
add_executable(unittest_flatindex EXCLUDE_FROM_ALL
  ${unittest_flatindex_srcs}
  )
add_test(unittest_flatindex unittest_flatindex)
add_dependencies(check unittest_flatindex)
target_link_libraries(unittest_flatindex
  os global boost_filesystem
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_flatindex PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_strtol
set(unittest_strtol_srcs strtol.cc)
add_executable(unittest_strtol EXCLUDE_FROM_ALL
  ${unittest_strtol_srcs}
  )
add_test(unittest_strtol unittest_strtol)
add_dependencies(check unittest_strtol)
target_link_libraries(unittest_strtol global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_strtol PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_confutils
set(unittest_confutils_srcs confutils.cc)
add_executable(unittest_confutils EXCLUDE_FROM_ALL
  ${unittest_confutils_srcs}
  )
add_test(unittest_confutils unittest_confutils)
add_dependencies(check unittest_confutils)
target_link_libraries(unittest_confutils global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_confutils PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_config
set(unittest_config_srcs common/test_config.cc)
add_executable(unittest_config EXCLUDE_FROM_ALL
  ${unittest_config_srcs}
  )
add_test(unittest_config unittest_config)
add_dependencies(check unittest_config)
target_link_libraries(unittest_config global ${CMAKE_DL_LIBS} ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_context
set(unittest_context_srcs common/test_context.cc)
add_executable(unittest_context EXCLUDE_FROM_ALL
  ${unittest_context_srcs}
  )
add_test(unittest_context unittest_context)
add_dependencies(check unittest_context)
target_link_libraries(unittest_context global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_context PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_heartbeatmap
set(unittest_heartbeatmap_srcs heartbeat_map.cc)
add_executable(unittest_heartbeatmap EXCLUDE_FROM_ALL
  ${unittest_heartbeatmap_srcs}
  )
add_test(unittest_heartbeatmap unittest_heartbeatmap)
add_dependencies(check unittest_heartbeatmap)
target_link_libraries(unittest_heartbeatmap global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_heartbeatmap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

if(${WITH_RADOSGW})
  # unittest_formatter
  set(unittest_formatter_srcs formatter.cc
    ${CMAKE_SOURCE_DIR}/src/rgw/rgw_formats.cc)
  add_executable(unittest_formatter EXCLUDE_FROM_ALL
    ${unittest_formatter_srcs}
  )
add_test(unittest_formatter unittest_formatter)
add_dependencies(check unittest_formatter)
  target_link_libraries(unittest_formatter global ${CMAKE_DL_LIBS}
    ${ALLOC_LIBS} ${UNITTEST_LIBS})
  set_target_properties(unittest_formatter PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})

# unittest_libcephfs_config
if(${WITH_CLIENT})
set(unittest_libcephfs_config_srcs libcephfs_config.cc)
add_executable(unittest_libcephfs_config EXCLUDE_FROM_ALL
  ${unittest_libcephfs_config_srcs}
  )
add_test(unittest_libcephfs_config unittest_libcephfs_config)
add_dependencies(check unittest_libcephfs_config)
target_link_libraries(unittest_libcephfs_config
  common
  global
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_libcephfs_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
endif(${WITH_CLIENT})

# unittest_librados_config
set(unittest_librados_config_srcs librados/librados_config.cc)
add_executable(unittest_librados_config EXCLUDE_FROM_ALL
  ${unittest_librados_config_srcs}
  )
add_test(unittest_librados_config unittest_librados_config)
add_dependencies(check unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  global boost_filesystem
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS}
  )
set_target_properties(unittest_librados_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

if(0)
### XXX THIS DOES NOT WORK:
### XXX global depends on common which needs libxio
### XXX but cmake doesn't know that's a required order,
### XXX so (at least for me (serpent chroot for cohort2) this is coming
### XXX out ordered as "global libxio common" which does not work.
# unittest_daemon_config
set(unittest_daemon_config_srcs daemon_config.cc)
add_executable(unittest_daemon_config EXCLUDE_FROM_ALL
  ${unittest_daemon_config_srcs}
  )
target_link_libraries(unittest_daemon_config
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${EXTRALIBS}
  )
add_test(unittest_daemon_config unittest_daemon_config)
add_dependencies(check unittest_daemon_config)
set_target_properties(unittest_daemon_config PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
endif(0)

# unittest_mon_moncap
set(unittest_mon_moncap_srcs mon/moncap.cc)
add_executable(unittest_mon_moncap EXCLUDE_FROM_ALL
  ${unittest_mon_moncap_srcs}
  )
add_test(unittest_mon_moncap unittest_mon_moncap)
add_dependencies(check unittest_mon_moncap)
target_link_libraries(unittest_mon_moncap mon global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mon_moncap PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_ipaddr
add_executable(unittest_ipaddr EXCLUDE_FROM_ALL test_ipaddr.cc)
add_test(unittest_ipaddr unittest_ipaddr)
add_dependencies(check unittest_ipaddr)
target_link_libraries(unittest_ipaddr mon global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ipaddr PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_texttable
set(unittest_texttable_srcs
  test_texttable.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  )
add_executable(unittest_texttable EXCLUDE_FROM_ALL
  ${unittest_texttable_srcs}
  )
add_test(unittest_texttable unittest_texttable)
add_dependencies(check unittest_texttable)
target_link_libraries(unittest_texttable mon global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_texttable PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_on_exit
set(unittest_on_exit_srcs on_exit.cc)
add_executable(unittest_on_exit EXCLUDE_FROM_ALL
  ${unittest_on_exit_srcs}
  )
add_test(unittest_on_exit unittest_on_exit)
add_dependencies(check unittest_on_exit)
target_link_libraries(unittest_on_exit
  global
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${UNITTEST_LIBS})
set_target_properties(unittest_on_exit PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# make check ends

if(0)
  # test_cors
  set(test_cors_srcs test_cors.cc)
  add_executable(test_cors
    ${test_cors_srcs}
    )
  target_link_libraries(test_cors
    librados
    rgw_a
    global
    curl
    expat
    ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
  set_target_properties(test_cors PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_rgw_manifest
  set(test_rgw_manifest_srcs rgw/test_rgw_manifest.cc)
  add_executable(test_rgw_manifest
    ${test_rgw_manifest_srcs}
    )
  target_link_libraries(test_rgw_manifest
    rgw_a
    cls_rgw_client
    cls_lock_client
    cls_refcount_client
    cls_log_client
    cls_statelog_client
    cls_version_client
    cls_replica_log_client
    cls_kvs
    cls_user_client
    librados
    global
    curl
    expat
    ${CMAKE_DL_LIBS}
    ${ALLOC_LIBS}
    ${UNITTEST_LIBS}
    ${CRYPTO_LIBS}
    )
  set_target_properties(test_rgw_manifest PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_meta
  set(test_cls_rgw_meta_srcs test_rgw_admin_meta.cc)
  add_executable(test_cls_rgw_meta
    ${test_cls_rgw_meta_srcs}
    )
  target_link_libraries(test_cls_rgw_meta
    librados
    rgw_a
    global
    curl
    expat
    cls_version_client
    cls_log_client
    cls_statelog_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    boost_regex
    ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_cls_rgw_meta PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_log
  set(test_cls_rgw_log_srcs
    test_rgw_admin_log.cc
    )
  add_executable(test_cls_rgw_log
    ${test_cls_rgw_log_srcs}
    )
  target_link_libraries(test_cls_rgw_log
    librados
    rgw_a
    global
    curl
    expat
    cls_version_client
    cls_log_client
    cls_statelog_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    boost_regex
    ${CMAKE_DL_LIBS}
    ${ALLOC_LIBS}
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CRYPTO_LIBS}
    )
  set_target_properties(test_cls_rgw_log PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_opstate
  set(test_cls_rgw_opstate_srcs test_rgw_admin_opstate.cc)
  add_executable(test_cls_rgw_opstate
    ${test_cls_rgw_opstate_srcs}
    )
  target_link_libraries(test_cls_rgw_opstate
    rgw_a
    librados
    cls_version_client
    cls_log_client
    cls_statelog_client
    cls_refcount_client
    cls_rgw_client
    cls_user_client
    cls_lock_client
    global
    curl
    expat
    ${CMAKE_DL_LIBS}
    ${ALLOC_LIBS}
    ${UNITTEST_LIBS}
    ${CRYPTO_LIBS}
    ${EXTRALIBS}
    )
  set_target_properties(test_cls_rgw_opstate PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
endif(0)

# radostest
set(libradostest_srcs librados/test.cc librados/TestCase.cc)
add_library(radostest STATIC ${libradostest_srcs})
set_target_properties(radostest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# multi_stress_watch
add_executable(multi_stress_watch
  multi_stress_watch.cc
  )
target_link_libraries(multi_stress_watch librados global radostest
  ${CMAKE_DL_LIBS} ${ALLOC_LIBS})

add_executable(test_librbd
  librbd/test_librbd.cc
  )
set_target_properties(test_librbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librbd
  librbd
  librados
  ${UNITTEST_LIBS}
  global
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

#add_executable(test_librbd_fsx
#  librbd/fsx.cc
#  )
#target_link_libraries(test_librbd_fsx
#  librbd
#  librados
#  global
#  m
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  )

add_executable(test_cls_refcount
  cls_refcount/test_cls_refcount.cc
  )
set_target_properties(test_cls_refcount PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_refcount
  librados
  cls_refcount_client
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_version
  cls_version/test_cls_version.cc
  )
set_target_properties(test_cls_version PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_version
  librados
  cls_version_client
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_log
  cls_log/test_cls_log.cc
  )
set_target_properties(test_cls_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_log
  librados
  cls_log_client
  global
  radostest
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )

add_executable(test_cls_statelog
  cls_statelog/test_cls_statelog.cc
  )
set_target_properties(test_cls_statelog PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_statelog
  librados
  cls_statelog_client
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_replica_log
  cls_replica_log/test_cls_replica_log.cc
  )
set_target_properties(test_cls_replica_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_replica_log
  librados
  cls_replica_log_client
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_lock
  cls_lock/test_cls_lock.cc
  )
set_target_properties(test_cls_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_lock
  cls_lock
  librados
  global
  ${UNITTEST_LIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

add_executable(test_cls_hello
  cls_hello/test_cls_hello.cc
  )
set_target_properties(test_cls_hello PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_hello
  librados
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS}
  )

if(0)
  add_executable(test_cls_rgw
    cls_rgw/test_cls_rgw.cc
    )
  set_target_properties(test_cls_rgw PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_cls_rgw
    cls_rgw_client
    librados
    global
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${ALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    radostest)
endif(0)

add_executable(test_mon_workloadgen
  mon/test_mon_workloadgen.cc
  $<TARGET_OBJECTS:osdc_objs>
  )
target_link_libraries(test_mon_workloadgen
  os global boost_filesystem
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_rados_api_io
  librados/io.cc
  )
set_target_properties(test_rados_api_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_io
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

add_executable(test_rados_api_c_write_operations
  librados/c_write_operations.cc
  )
set_target_properties(test_rados_api_c_write_operations PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_write_operations
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest)

add_executable(test_rados_api_c_read_operations
  librados/c_read_operations.cc
  )
set_target_properties(test_rados_api_c_read_operations PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_read_operations
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

add_executable(test_rados_api_aio
  librados/aio.cc
  )
set_target_properties(test_rados_api_aio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_aio
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

add_executable(test_rados_api_list
  librados/list.cc
  )
set_target_properties(test_rados_api_list PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_list
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest)

add_executable(test_rados_api_volume
  librados/volume.cc
  )
set_target_properties(test_rados_api_volume PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS}
  )
target_link_libraries(test_rados_api_volume
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

add_executable(test_rados_api_watch_notify
  librados/watch_notify.cc
  )
set_target_properties(test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_watch_notify
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

add_executable(test_rados_api_cls
  librados/cls.cc
  )
set_target_properties(test_rados_api_cls PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cls
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

add_executable(test_rados_api_misc
  librados/misc.cc
  )
set_target_properties(test_rados_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_misc
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  boost_system
  radostest
  )

add_executable(test_rados_api_lock
  librados/lock.cc
  )
set_target_properties(test_rados_api_lock PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_lock
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  radostest
  )

if(${WITH_CLIENT})
  add_executable(test_libcephfs
    libcephfs/test.cc
    libcephfs/readdir_r_cb.cc
    libcephfs/caps.cc
    libcephfs/multiclient.cc
  )
  set_target_properties(test_libcephfs PROPERTIES COMPILE_FLAGS
    ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_libcephfs
    common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${ALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_CLIENT})

add_executable(test_objectstore
  objectstore/store_test.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
set_target_properties(test_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_objectstore
  os leveldb aio boost_filesystem boost_system
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_objectstore_workloadgen
  objectstore/workload_generator.cc
  objectstore/TestObjectStoreState.cc
  )
target_link_libraries(test_objectstore_workloadgen
  os global boost_filesystem
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filestore_idempotent
  objectstore/test_idempotent.cc
  objectstore/FileStoreTracker.cc
  common/ObjectContents.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
target_link_libraries(test_filestore_idempotent
  os global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filestore_idempotent_sequence
  objectstore/test_idempotent_sequence.cc
  objectstore/DeterministicOpSequence.cc
  objectstore/TestObjectStoreState.cc
  objectstore/FileStoreDiff.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
target_link_libraries(test_filestore_idempotent_sequence
  os global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_xattr_bench
  xattr_bench.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
set_target_properties(test_xattr_bench PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_xattr_bench
  os
  ${UNITTEST_LIBS}
  global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_filejournal
  test_filejournal.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
set_target_properties(test_filejournal PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_filejournal
  os
  ${UNITTEST_LIBS}
  global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_stress_watch
  test_stress_watch.cc
  )
set_target_properties(test_stress_watch PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_stress_watch
  librados
  global
  ${UNITTEST_LIBS}
  radostest
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_object_map
  ObjectMap/test_object_map.cc
  ObjectMap/KeyValueDBMemory.cc
  )
set_target_properties(test_object_map PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_object_map
  os boost_filesystem
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb_atomicity
  ObjectMap/test_keyvaluedb_atomicity.cc
  )
set_target_properties(test_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_atomicity
  os
  ${UNITTEST_LIBS}
  global
  boost_filesystem
  boost_system
  leveldb
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

add_executable(test_keyvaluedb_iterators
  ObjectMap/test_keyvaluedb_iterators.cc
  ObjectMap/KeyValueDBMemory.cc
  )
set_target_properties(test_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_iterators
  os boost_filesystem
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

if(${WITH_FUSE})
  add_executable(test_cfuse_cache_invalidate
    test_cfuse_cache_invalidate.cc
    )
  target_link_libraries(test_cfuse_cache_invalidate
    global
    os
    boost_filesystem
    ${EXTRALIBS}
    ${ALLOC_LIBS}
    ${CMAKE_DL_LIBS}
    )
endif(${WITH_FUSE})

add_executable(test_get_blkdev_size
  test_get_blkdev_size.cc
  )
target_link_libraries(test_get_blkdev_size
  common
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )

# the ones we added in except for the fio stuff?
add_executable(simple_server
  messenger/simple_server.cc
  messenger/simple_dispatcher.cc
  )
target_link_libraries(simple_server os global common boost_regex
  boost_filesystem ${ALLOC_LIBS})

add_executable(simple_client
  messenger/simple_client.cc
  messenger/simple_dispatcher.cc
  )
target_link_libraries(simple_client os global common boost_filesystem
  boost_regex ${ALLOC_LIBS})

if (HAVE_XIO)
  add_executable(xio_server
    messenger/xio_server.cc
    messenger/xio_dispatcher.cc
    )
  target_link_libraries(xio_server os global common boost_filesystem
    boost_regex ${ALLOC_LIBS} ${EXTRALIBS})

  add_executable(xio_client
    messenger/xio_client.cc
    messenger/xio_dispatcher.cc
    )
  target_link_libraries(xio_client os global common boost_filesystem
    boost_regex ${ALLOC_LIBS} ${EXTRALIBS})
endif(HAVE_XIO)

if (WITH_FIO)
  include_directories(${FIO_INC_DIR})
  add_library(fio_ceph_filestore SHARED fio_ceph_filestore.cc)
  set_target_properties(fio_ceph_filestore PROPERTIES COMPILE_FLAGS
    "-fpermissive -Wno-pointer-arith")
  target_link_libraries(fio_ceph_filestore os global ${CRYPTO_LIBS}
    ${EXTRA_LIBS})
  install(TARGETS fio_ceph_filestore DESTINATION lib)
endif (WITH_FIO)
# the ones we added in except for the fio stuff?
# osbench
add_executable(osbench objectstore_bench.cc)
target_link_libraries(osbench os global boost_filesystem ${EXTRA_LIBS})

# mmapbench
add_executable(mmapbench mmap_bench.cc)
target_link_libraries(mmapbench os global boost_filesystem)

# test_direct_messenger
add_executable(test_direct_messenger messenger/test_direct_messenger.cc)
target_link_libraries(test_direct_messenger global boost_system)

# test_libosd
add_executable(test_libosd test_libosd.c)
set_target_properties(test_libosd PROPERTIES LINKER_LANGUAGE C)
target_link_libraries(test_libosd PRIVATE osd os global
  boost_filesystem boost_system ${ALLOC_LIBS})

# test_libosd_remote
add_executable(test_libosd_remote test_libosd_remote.c)
set_target_properties(test_libosd_remote PROPERTIES LINKER_LANGUAGE C)
target_link_libraries(test_libosd_remote PRIVATE osd os global
  boost_filesystem boost_system ${ALLOC_LIBS})

# libosd_bench
add_executable(libosd_bench libosd_bench.cc)
target_link_libraries(libosd_bench PRIVATE osd common pthread)

# libosd_bench_async
add_executable(libosd_bench_async libosd_bench_async.cc)
target_link_libraries(libosd_bench_async PRIVATE osd common pthread)

# libosd_bench_async2
add_executable(libosd_bench_async2 libosd_bench_async2.cc)
target_link_libraries(libosd_bench_async2 PRIVATE osd common pthread)

# lttng_stream_test
if (HAVE_LTTNG)
  add_executable(lttng_stream_test lttng_stream_test.cc)
  target_link_libraries(lttng_stream_test common global)
endif(HAVE_LTTNG)

# test_threadpool
add_executable(test_threadpool test_threadpool.cc)
target_link_libraries(test_threadpool PRIVATE global ${UNITTEST_LIBS})
set_target_properties(test_threadpool PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# test_cohort_lru
add_executable(test_cohort_lru test_cohort_lru.cc)
target_link_libraries(test_cohort_lru PRIVATE global ${UNITTEST_LIBS})
set_target_properties(test_cohort_lru PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# test_assocvec
add_executable(test_assocvec test_assocvec.cc)
target_link_libraries(test_assocvec PRIVATE global ${UNITTEST_LIBS})
set_target_properties(test_assocvec PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# test_queues
add_executable(test_queues test_queues.cc)
target_link_libraries(test_queues PRIVATE global ${UNITTEST_LIBS})
set_target_properties(test_queues PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# test_fragtreeindex
add_executable(test_fragtreeindex test_fragtreeindex.cc)
target_link_libraries(test_fragtreeindex os_filestore global common ${UNITTEST_LIBS})
set_target_properties(test_fragtreeindex PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# test_zfsfragtreeindex
add_executable(test_zfsfragtreeindex test_zfsfragtreeindex.cc)
target_link_libraries(test_zfsfragtreeindex os_filestore global common ${UNITTEST_LIBS})
set_target_properties(test_zfsfragtreeindex PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
