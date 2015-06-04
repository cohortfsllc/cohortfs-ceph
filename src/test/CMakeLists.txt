if(NOT ${WITH_MDS})
  add_definitions(-DNO_MDS)
endif(NOT ${WITH_MDS})

if(NOT ${WITH_CLIENT})
  add_definitions(-DNO_CLIENT)
endif(NOT ${WITH_CLIENT})

#Make check starts here
add_custom_target(symlinks  COMMAND
  ln -sf ${CMAKE_SOURCE_DIR}/src/test/ ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-mon ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/ceph ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-authtool ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-conf ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_BINARY_DIR}/src/ceph-osd ${CMAKE_BINARY_DIR}/src/test/ &&
  ln -sf ${CMAKE_SOURCE_DIR}/src/ceph-disk ${CMAKE_BINARY_DIR}/src/test/ &&
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
  global boost_filesystem pthread rt m
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

# unittest_timers
add_executable(unittest_timers EXCLUDE_FROM_ALL
  TestTimers.cc
  )
add_test(unittest_timers unittest_timers)
add_dependencies(check unittest_timers)
target_link_libraries(unittest_timers global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(unittest_timers
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_signal_handlers
add_executable(unittest_signal_handlers EXCLUDE_FROM_ALL
  TestSignalHandlers.cc
  )
add_test(unittest_signal_handlers unittest_signal_handlers)
add_dependencies(check unittest_signal_handlers)
target_link_libraries(unittest_signal_handlers global ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS})
set_target_properties(unittest_timers
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_msgr
add_executable(unittest_msgr EXCLUDE_FROM_ALL
  testmsgr.cc
  )
add_test(unittest_msgr unittest_msgr)
add_dependencies(check unittest_msgr)
target_link_libraries(unittest_msgr global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(unittest_msgr
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_crypt
add_executable(unittest_crypt EXCLUDE_FROM_ALL
  testcrypto.cc
  )
add_test(unittest_crypt unittest_crypt)
add_dependencies(check unittest_crypt)
target_link_libraries(unittest_crypt
  global
  ${CRYPTO_LIBS}
  m
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  )
set_target_properties(unittest_crypt
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# streamtest
add_executable(streamtest EXCLUDE_FROM_ALL
  streamtest.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(streamtest streamtest)
add_dependencies(check streamtest)
target_link_libraries(streamtest os global leveldb aio boost_filesystem
  boost_system ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(streamtest
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_trans
add_executable(unittest_trans EXCLUDE_FROM_ALL
  test_trans.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(unittest_trans unittest_trans)
add_dependencies(check unittest_trans)
target_link_libraries(unittest_trans os global leveldb aio boost_filesystem
  boost_system ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(unittest_trans
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

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

add_executable(ceph-dencoder EXCLUDE_FROM_ALL 
  ${dencoder_srcs}) 
add_test(ceph-dencoder ceph-dencoder)
add_dependencies(check ceph-dencoder)
target_link_libraries(ceph-dencoder ${dencoder_dependencies} leveldb)
set_target_properties(ceph-dencoder
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# get_command_descriptions
add_executable(get_command_descriptions EXCLUDE_FROM_ALL
  common/get_command_descriptions.cc
  ${CMAKE_SOURCE_DIR}/src/common/TextTable.cc
  $<TARGET_OBJECTS:os_mon_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_test(get_command_descriptions get_command_descriptions)
add_dependencies(check get_command_descriptions)
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
set_target_properties(get_command_descriptions
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

## Build tests
# These should all use explicit _CXXFLAGS so avoid basename conflicts
set(WITH_BUILD_TESTS ON)
if(${WITH_BUILD_TESTS})

# build_libcommon
set(build_libcommon_srcs
  buildtest_skeleton.cc
  )
add_executable(test_build_libcommon EXCLUDE_FROM_ALL
  ${build_libcommon_srcs}
  $<TARGET_OBJECTS:common_objs>
  )
add_test(test_build_libcommon test_build_libcommon)
add_dependencies(check test_build_libcommon)
target_link_libraries(test_build_libcommon json_spirit  rt pthread global ${Boost_LIBRARIES} ${CRYPTO_LIBS} ${EXTRALIBS})
set_target_properties(test_build_libcommon
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# build_librgw
if(${WITH_RADOSGW})
  set(build_librgw_srcs
  buildtest_skeleton.cc
    )
  add_executable(test_build_librgw EXCLUDE_FROM_ALL
  ${build_librgw_srcs}
    )
add_test(test_build_librgw test_build_librgw)
add_dependencies(check test_build_librgw)
target_link_libraries(test_build_librgw
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_version_client
    cls_replica_log_client cls_user_client
    curl expat global fcgi resolv ${ALLOC_LIBS} pthread ${CRYPTO_LIBS} ${EXTRALIBS} global)
set_target_properties(test_build_librgw
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})
endif(${WITH_BUILD_TESTS})

# unittest_objectstore
add_executable(unittest_objectstore EXCLUDE_FROM_ALL
  objectstore/store_test.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(unittest_objectstore unittest_objectstore)
add_dependencies(check unittest_objectstore)
set_target_properties(unittest_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_objectstore
  os leveldb aio boost_filesystem boost_system
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_objectstore
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_objectstore_workloadgen
add_executable(unittest_objectstore_workloadgen EXCLUDE_FROM_ALL
  objectstore/workload_generator.cc
  objectstore/TestObjectStoreState.cc
  )
add_test(unittest_objectstore_workloadgen unittest_objectstore_workloadgen)
add_dependencies(check unittest_objectstore_workloadgen)
target_link_libraries(unittest_objectstore_workloadgen
  os global boost_filesystem leveldb
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_objectstore_workloadgen
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_filestore_idempotent
add_executable(unittest_filestore_idempotent EXCLUDE_FROM_ALL
  objectstore/test_idempotent.cc
  objectstore/FileStoreTracker.cc
  common/ObjectContents.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(unittest_filestore_idempotent unittest_filestore_idempotent)
add_dependencies(check unittest_filestore_idempotent)
target_link_libraries(unittest_filestore_idempotent
  os global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_filestore_idempotent
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_filestore_idempotent_sequence
add_executable(unittest_filestore_idempotent_sequence EXCLUDE_FROM_ALL
  objectstore/test_idempotent_sequence.cc
  objectstore/DeterministicOpSequence.cc
  objectstore/TestObjectStoreState.cc
  objectstore/FileStoreDiff.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(unittest_filestore_idempotent_sequence unittest_filestore_idempotent_sequence)
add_dependencies(check unittest_filestore_idempotent_sequence)
target_link_libraries(unittest_filestore_idempotent_sequence
  os global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_filestore_idempotent_sequence
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_xattr_bench
add_executable(unittest_xattr_bench EXCLUDE_FROM_ALL
  xattr_bench.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(unittest_xattr_bench unittest_xattr_bench)
add_dependencies(check unittest_xattr_bench)
target_link_libraries(unittest_xattr_bench
  os
  ${UNITTEST_LIBS}
  global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_xattr_bench PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_filejournal
add_executable(unittest_filejournal EXCLUDE_FROM_ALL
  test_filejournal.cc
  $<TARGET_OBJECTS:os_filestore_objs>
  )
add_test(unittest_filejournal unittest_filejournal)
add_dependencies(check unittest_filejournal)
set_target_properties(unittest_filejournal PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_filejournal
  os
  ${UNITTEST_LIBS}
  global leveldb aio boost_filesystem boost_system
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_filejournal
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_osbench
add_executable(unittest_osbench EXCLUDE_FROM_ALL
                objectstore_bench.cc)
add_test(unittest_osbench unittest_osbench)
add_dependencies(check unittest_osbench)
target_link_libraries(unittest_osbench os global boost_filesystem leveldb ${EXTRA_LIBS})
set_target_properties(unittest_osbench
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_mmapbench
add_executable(unittest_mmapbench EXCLUDE_FROM_ALL
                mmap_bench.cc)
add_test(unittest_mmapbench unittest_mmapbench)
add_dependencies(check unittest_mmapbench)
target_link_libraries(unittest_mmapbench os global boost_filesystem leveldb)
set_target_properties(unittest_mmapbench
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_direct_messenger
add_executable(unittest_direct_messenger EXCLUDE_FROM_ALL
                messenger/test_direct_messenger.cc)
add_test(unittest_direct_messenger unittest_direct_messenger)
add_dependencies(check unittest_direct_messenger)
target_link_libraries(unittest_direct_messenger global boost_system)
set_target_properties(unittest_direct_messenger
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_libosd
add_executable(unittest_libosd EXCLUDE_FROM_ALL 
                test_libosd.c)
add_test(unittest_libosd unittest_libosd)
add_dependencies(check unittest_libosd)
set_target_properties(unittest_libosd PROPERTIES LINKER_LANGUAGE C)
target_link_libraries(unittest_libosd PRIVATE osd os global
  boost_filesystem boost_system leveldb ${ALLOC_LIBS})

# unittest_libosd_remote
add_executable(unittest_libosd_remote EXCLUDE_FROM_ALL 
                test_libosd_remote.c)
add_test(unittest_libosd_remote unittest_libosd_remote)
add_dependencies(check unittest_libosd_remote)
set_target_properties(unittest_libosd_remote PROPERTIES LINKER_LANGUAGE C)
target_link_libraries(unittest_libosd_remote PRIVATE osd os global
  boost_filesystem boost_system leveldb ${ALLOC_LIBS})

# libosd_bench
add_executable(libosd_bench EXCLUDE_FROM_ALL libosd_bench.cc)
add_test(libosd_bench libosd_bench)
add_dependencies(check libosd_bench)
target_link_libraries(libosd_bench PRIVATE osd common pthread leveldb)
set_target_properties(libosd_bench
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# libosd_bench_async
add_executable(libosd_bench_async EXCLUDE_FROM_ALL libosd_bench_async.cc)
add_test(libosd_bench_async libosd_bench_async)
add_dependencies(check libosd_bench_async)
target_link_libraries(libosd_bench_async PRIVATE osd common pthread leveldb)
set_target_properties(libosd_bench_async
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# libosd_bench_async2
add_executable(libosd_bench_async2 EXCLUDE_FROM_ALL libosd_bench_async2.cc)
add_test(libosd_bench_async2 libosd_bench_async2)
add_dependencies(check libosd_bench_async2)
target_link_libraries(libosd_bench_async2 PRIVATE osd common pthread leveldb)
set_target_properties(libosd_bench_async2
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_libmds
add_executable(unittest_libmds test_libmds.c)
add_test(unittest_libmds unittest_libmds)
add_dependencies(check unittest_libmds)
target_link_libraries(unittest_libmds PRIVATE mds global leveldb ${ALLOC_LIBS})
set_target_properties(unittest_libmds PROPERTIES LINKER_LANGUAGE C)

# lttng_stream_test
if (HAVE_LTTNG)
  add_executable(lttng_stream_test EXCLUDE_FROM_ALL lttng_stream_test.cc)
  add_test(lttng_stream_test lttng_stream_test)
  add_dependencies(check lttng_stream_test)
  target_link_libraries(lttng_stream_test common global)
  set_target_properties(lttng_stream_test
    PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endif(HAVE_LTTNG)

# test_threadpool
add_executable(unittest_threadpool EXCLUDE_FROM_ALL test_threadpool.cc)
add_test(unittest_threadpool unittest_threadpool)
add_dependencies(check unittest_threadpool)
target_link_libraries(unittest_threadpool PRIVATE global ${UNITTEST_LIBS})
set_target_properties(unittest_threadpool PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_cohort_lru
add_executable(unittest_cohort_lru EXCLUDE_FROM_ALL test_cohort_lru.cc)
add_test(unittest_cohort_lru unittest_cohort_lru)
add_dependencies(check unittest_cohort_lru)
target_link_libraries(unittest_cohort_lru PRIVATE global ${UNITTEST_LIBS})
set_target_properties(unittest_cohort_lru PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_assocvec
add_executable(unittest_assocvec EXCLUDE_FROM_ALL test_assocvec.cc)
add_test(unittest_assocvec unittest_assocvec)
add_dependencies(check unittest_assocvec)
target_link_libraries(unittest_assocvec PRIVATE global ${UNITTEST_LIBS})
set_target_properties(unittest_assocvec PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_queues
add_executable(unittest_queues EXCLUDE_FROM_ALL test_queues.cc)
add_test(unittest_queues unittest_queues)
add_dependencies(check unittest_queues)
target_link_libraries(unittest_queues PRIVATE global ${UNITTEST_LIBS})
set_target_properties(unittest_queues PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_fragtreeindex
add_executable(unittest_fragtreeindex EXCLUDE_FROM_ALL test_fragtreeindex.cc)
add_test(unittest_fragtreeindex unittest_fragtreeindex)
add_dependencies(check unittest_fragtreeindex)
target_link_libraries(unittest_fragtreeindex os_filestore global common leveldb ${UNITTEST_LIBS})
set_target_properties(unittest_fragtreeindex PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_cohort_function
add_executable(unittest_cohort_function EXCLUDE_FROM_ALL test_function.cc)
add_test(unittest_cohort_function unittest_cohort_function)
add_dependencies(check unittest_cohort_function)
target_link_libraries(unittest_cohort_function ${UNITTEST_LIBS})
set_target_properties(unittest_cohort_function PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# smalliobenchrbd
set(smalliobenchrbd_srcs
  bench/small_io_bench_rbd.cc
  bench/rbd_backend.cc
  bench/detailed_stat_collector.cc
  bench/bencher.cc
  )
add_executable(smalliobenchrbd EXCLUDE_FROM_ALL
  ${smalliobenchrbd_srcs}
  )
add_test(smalliobenchrbd smalliobenchrbd)
add_dependencies(check smalliobenchrbd)
target_link_libraries(smalliobenchrbd librbd librados boost_program_options
  global boost_filesystem  ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(smalliobenchrbd
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# tpbench
set(tpbench_srcs
  bench/tp_bench.cc
  bench/detailed_stat_collector.cc)
add_executable(tpbench EXCLUDE_FROM_ALL
  ${tpbench_srcs}
  )
add_test(tpbench tpbench)
add_dependencies(check tpbench)
target_link_libraries(tpbench librados boost_filesystem boost_program_options
  global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(tpbench
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_librbd
add_executable(unittest_librbd EXCLUDE_FROM_ALL
  librbd/test_librbd.cc
  )
add_test(unittest_librbd unittest_librbd)
add_dependencies(check unittest_librbd)
target_link_libraries(unittest_librbd
  librbd
  librados
  ${UNITTEST_LIBS}
  global
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )
set_target_properties(unittest_librbd PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_mon_workloadgen
add_executable(unittest_mon_workloadgen
  mon/test_mon_workloadgen.cc
  $<TARGET_OBJECTS:osdc_objs>
  )
add_test(unittest_mon_workloadgen unittest_mon_workloadgen)
add_dependencies(check unittest_mon_workloadgen)
target_link_libraries(unittest_mon_workloadgen
  os global boost_filesystem leveldb
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_mon_workloadgen
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_get_blkdev_size
add_executable(unittest_get_blkdev_size EXCLUDE_FROM_ALL
  test_get_blkdev_size.cc
  )
add_test(unittest_get_blkdev_size unittest_get_blkdev_size)
add_dependencies(check unittest_get_blkdev_size)
target_link_libraries(unittest_get_blkdev_size
  common
  ${EXTRALIBS}
  ${ALLOC_LIBS}
  ${CMAKE_DL_LIBS}
  )
set_target_properties(unittest_get_blkdev_size
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_keyvaluedb_atomicity
add_executable(unittest_keyvaluedb_atomicity EXCLUDE_FROM_ALL
  ObjectMap/test_keyvaluedb_atomicity.cc
  )
add_test(unittest_keyvaluedb_atomicity unittest_keyvaluedb_atomicity)
add_dependencies(check unittest_keyvaluedb_atomicity)
set_target_properties(unittest_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_keyvaluedb_atomicity
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

# unittest_keyvaluedb_iterators
add_executable(unittest_keyvaluedb_iterators EXCLUDE_FROM_ALL
  ObjectMap/test_keyvaluedb_iterators.cc
  ObjectMap/KeyValueDBMemory.cc
  )
add_test(unittest_keyvaluedb_iterators unittest_keyvaluedb_iterators)
add_dependencies(check unittest_keyvaluedb_iterators)
set_target_properties(unittest_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(unittest_keyvaluedb_iterators
  boost_filesystem
  ${UNITTEST_LIBS}
  global
  os
  leveldb
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  )

if (HAVE_XIO)
  # xio_server
  add_executable(xio_server
    messenger/xio_server.cc
    messenger/xio_dispatcher.cc
    )
  add_test(xio_server xio_server)
  add_dependencies(check xio_server)
  target_link_libraries(xio_server os global common boost_filesystem
    boost_regex ${ALLOC_LIBS} ${EXTRALIBS})
  set_target_properties(xio_server
    PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

  # xio_client
  add_executable(xio_client
    messenger/xio_client.cc
    messenger/xio_dispatcher.cc
    )
  add_test(xio_client xio_client)
  add_dependencies(check xio_client)
  target_link_libraries(xio_client os global common boost_filesystem
    boost_regex ${ALLOC_LIBS} ${EXTRALIBS})
  set_target_properties(xio_client
    PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endif(HAVE_XIO)

# Tests that hang so are marked out for now
# simple_server
#add_executable(simple_server EXCLUDE_FROM_ALL
#  messenger/simple_server.cc
#  messenger/simple_dispatcher.cc
#  )
#add_test(simple_server simple_server)
#add_dependencies(check simple_server)
#target_link_libraries(simple_server global common boost_regex
#  boost_filesystem ${ALLOC_LIBS})
#set_target_properties(simple_server
#  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

#simple_client
#add_executable(simple_client
#  messenger/simple_client.cc
#  messenger/simple_dispatcher.cc
#  )
#add_test(simple_client simple_client)
#add_dependencies(check simple_client)
#target_link_libraries(simple_client global common boost_filesystem
#  boost_regex ${ALLOC_LIBS})
#set_target_properties(simple_client
#  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_keys not working commented out for now
#add_executable(unittest_keys
#  testkeys.cc
#  )
#add_test(unittest_keys unittest_keys)
#add_dependencies(check unittest_keys)
#target_link_libraries(unittest_keys mon global ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
#set_target_properties(unittest_keys
#  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})


# Don't know if following tests are necessary

# bench_log
set(bench_log_srcs
  bench_log.cc
  )
add_executable(bench_log EXCLUDE_FROM_ALL
  ${bench_log_srcs}
  )
add_test(bench_log bench_log)
add_dependencies(check bench_log)
target_link_libraries(bench_log global pthread rt ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
set_target_properties(bench_log
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

#these test don't compile
# unittest_cors
#set(unittest_cors_srcs test_cors.cc)
#add_executable(unittest_cors EXCLUDE_FROM_ALL
#  ${unittest_cors_srcs}
#  )
#add_test(unittest_cors unittest_cors)
#add_dependencies(check unittest_cors)
#target_link_libraries(unittest_cors
#  librados
#  #rgw_a
#  global
#  curl
#  expat
#  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS})
#set_target_properties(unittest_cors PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})

# test_rgw_manifest
#set(test_rgw_manifest_srcs rgw/test_rgw_manifest.cc)
##add_executable(unittest_rgw_manifest EXCLUDE_FROM_ALL
#  ${test_rgw_manifest_srcs}
#  )
#add_test(unittest_rgw_manifest unittest_rgw_manifest)
#add_dependencies(check unittest_rgw_manifest)
#target_link_libraries(unittest_rgw_manifest
#  rgw_a
#  cls_rgw_client
#  cls_lock_client
#  cls_refcount_client
#  cls_log_client
#  cls_statelog_client
#  cls_version_client
#  cls_replica_log_client
#  cls_kvs
#  cls_user_client
#  librados
#  global
#  curl
#  expat
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${UNITTEST_LIBS}
#  ${CRYPTO_LIBS}
#  )
#set_target_properties(unittest_rgw_manifest PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})

## test_cls_rgw_meta
#set(test_cls_rgw_meta_srcs test_rgw_admin_meta.cc)
#add_executable(unittest_cls_rgw_meta_srcs EXCLUDE_FROM_ALL
#  ${test_cls_rgw_meta_srcs}
#  )
#add_test(unittest_cls_rgw_meta_srcs unittest_cls_rgw_meta_srcs)
#add_dependencies(check unittest_cls_rgw_meta_srcs)
#target_link_libraries(unittest_cls_rgw_meta_srcs
#  librados
#  rgw_a
#  global
#  curl
#  expat
#  cls_version_client
#  cls_log_client
#  cls_statelog_client
#  cls_refcount_client
#  cls_rgw_client
#  cls_user_client
#  cls_lock_client
#  boost_regex
#  ${CMAKE_DL_LIBS} ${ALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
#set_target_properties(unittest_cls_rgw_meta_srcs PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})

# unittest_cls_rgw_log
#set(test_cls_rgw_log_srcs
#  test_rgw_admin_log.cc
#  )
#add_executable(unittest_cls_rgw_log EXCLUDE_FROM_ALL
#  ${test_cls_rgw_log_srcs}
#  )
#add_test(unittest_cls_rgw_log unittest_cls_rgw_log)
#add_dependencies(check unittest_cls_rgw_log)
#target_link_libraries(unittest_cls_rgw_log
#  librados
#  rgw_a
#  global
#  curl
#  expat
#  cls_version_client
#  cls_log_client
#  cls_statelog_client
#  cls_refcount_client
#  cls_rgw_client
#  cls_user_client
#  cls_lock_client
#  boost_regex
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${UNITTEST_LIBS}
#  ${EXTRALIBS}
#  ${CRYPTO_LIBS}
#  )
#set_target_properties(unittest_cls_rgw_log PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})

# unittest_cls_rgw_opstate
#set(test_cls_rgw_opstate_srcs test_rgw_admin_opstate.cc)
#add_executable(unittest_cls_rgw_opstate EXCLUDE_FROM_ALL
#  ${test_cls_rgw_opstate_srcs}
#  )
#add_test(unittest_cls_rgw_opstate unittest_cls_rgw_opstate)
#add_dependencies(check unittest_cls_rgw_opstate)
#target_link_libraries(unittest_cls_rgw_opstate
#  rgw_a
#  librados
#  cls_version_client
#  cls_log_client
#  cls_statelog_client
#  cls_refcount_client
#  cls_rgw_client
#  cls_user_client
#  cls_lock_client
#  global
#  curl
#  expat
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${UNITTEST_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  )
#set_target_properties(unittest_cls_rgw_opstate PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})

#unittest_cls_refcount
#add_executable(unittest_cls_refcount EXCLUDE_FROM_ALL
#  cls_refcount/test_cls_refcount.cc
#  )
#add_test(unittest_cls_refcount unittest_cls_refcount)
#add_dependencies(check unittest_cls_refcount)
#set_target_properties(unittest_cls_refcount PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_refcount
#  librados
#  cls_refcount_client
#  global
#  ${UNITTEST_LIBS}
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  radostest
#  )

#unittest_cls_version
#add_executable(unittest_cls_version EXCLUDE_FROM_ALL
#  cls_version/test_cls_version.cc
#  )
#add_test(unittest_cls_version unittest_cls_version)
#add_dependencies(check unittest_cls_version)
#set_target_properties(unittest_cls_version PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_version
#  librados
#  cls_version_client
#  global
#  ${UNITTEST_LIBS}
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  radostest
#  )

# unittest_cls_log
#add_executable(unittest_cls_log EXCLUDE_FROM_ALL
#  cls_log/test_cls_log.cc
#  )
#add_test(unittest_cls_log unittest_cls_log)
#add_dependencies(check unittest_cls_log)
#set_target_properties(unittest_cls_log PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_log
#  librados
#  cls_log_client
#  global
#  radostest
#  ${UNITTEST_LIBS}
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  )

# unittest_cls_statelog
#add_executable(unittest_cls_statelog EXCLUDE_FROM_ALL
#  cls_statelog/test_cls_statelog.cc
#  )
#add_test(unittest_cls_statelog unittest_cls_statelog)
#add_dependencies(check unittest_cls_statelog)
#set_target_properties(unittest_cls_statelog PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_statelog
#  librados
#  cls_statelog_client
#  global
#  ${UNITTEST_LIBS}
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  radostest
#  )

## unittest_cls_replica_log
#add_executable(unittest_cls_replica_log EXCLUDE_FROM_ALL
#  cls_replica_log/test_cls_replica_log.cc
#  )
#add_test(unittest_cls_replica_log unittest_cls_replica_log)
#add_dependencies(check unittest_cls_replica_log)
#set_target_properties(unittest_cls_replica_log PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_replica_log
#  librados
#  cls_replica_log_client
#  global
#  ${UNITTEST_LIBS}
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  radostest
#  )

# unittest_cls_lock
#add_executable(unittest_cls_lock EXCLUDE_FROM_ALL
#  cls_lock/test_cls_lock.cc
#  )
#add_test(unittest_cls_lock unittest_cls_lock)
#add_dependencies(check unittest_cls_lock)
#set_target_properties(unittest_cls_lock PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_lock
#  librados
#  global
#  ${UNITTEST_LIBS}
#  ${CMAKE_DL_LIBS}
#  ${ALLOC_LIBS}
#  ${CRYPTO_LIBS}
#  ${EXTRALIBS}
#  radostest
#  )

# unittest_cls_rgw
#add_executable(unittest_cls_rgw EXCLUDE_FROM_ALL
#  cls_rgw/test_cls_rgw.cc
#  )
#add_test(unittest_cls_rgw unittest_cls_rgw)
#add_dependencies(check unittest_cls_rgw)
#set_target_properties(unittest_cls_rgw PROPERTIES COMPILE_FLAGS
#  ${UNITTEST_CXX_FLAGS})
#target_link_libraries(unittest_cls_rgw
#  cls_rgw_client
#  librados
#  global
#  ${UNITTEST_LIBS}
#  ${EXTRALIBS}
#  ${ALLOC_LIBS}
#  ${CMAKE_DL_LIBS}
#  radostest)

# make check ends

# Libs 
# systest
set(libsystest_srcs system/cross_process_sem.cc
  system/systest_runnable.cc
  system/systest_settings.cc)
add_library(systest STATIC ${libsystest_srcs})

# fio_ceph_filestore
if (WITH_FIO)
  include_directories(${FIO_INC_DIR})
  add_library(fio_ceph_filestore SHARED fio_ceph_filestore.cc)
  set_target_properties(fio_ceph_filestore PROPERTIES COMPILE_FLAGS
    "-fpermissive -Wno-pointer-arith")
  target_link_libraries(fio_ceph_filestore os global ${CRYPTO_LIBS}
    ${EXTRA_LIBS})
  install(TARGETS fio_ceph_filestore DESTINATION lib)
endif (WITH_FIO)

# test_mcas_skiplist
add_executable(test_mcas_skiplist test_mcas_skiplist.cc)
target_link_libraries(test_mcas_skiplist ${UNITTEST_LIBS} ${MCAS_LIBRARIES})
set_target_properties(test_mcas_skiplist PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
