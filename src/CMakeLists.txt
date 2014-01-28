add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(AM_COMMON_FLAGS "-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_definitions("${AM_COMMON_FLAGS}")

set(EXTRALIBS uuid rt)
if(${WITH_PROFILER})
  list(APPEND EXTRALIBS profiler)
endif(${WITH_PROFILER})

set(CRYPTO_LIBS cryptopp)

if(${HAVE_ATOMIC_OPS})
  list(APPEND EXTRALIBS atomic_ops)
endif(${HAVE_ATOMIC_OPS})

set(bindir ${CMAKE_INSTALL_PREFIX}/bin)
set(sbindir ${CMAKE_INSTALL_PREFIX}/sbin)
set(libdir ${CMAKE_INSTALL_PREFIX}/lib)
set(sysconfdir ${CMAKE_INSTALL_PREFIX}/etc)
set(pkgdatadir ${CMAKE_INSTALL_PREFIX}/share)
set(prefix ${CMAKE_INSTALL_PREFIX})
set(GCOV_PREFIX_STRIP 4)

configure_file(
${CMAKE_SOURCE_DIR}/src/init-ceph.in.cmake
${CMAKE_BINARY_DIR}/init-ceph
@ONLY
)

#add_custom_target(init-ceph SOURCES init-ceph)

configure_file(
${CMAKE_SOURCE_DIR}/src/mkcephfs.in.cmake
${CMAKE_BINARY_DIR}/mkcephfs
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-coverage.in.cmake
${CMAKE_BINARY_DIR}/ceph-coverage
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in.cmake
${CMAKE_BINARY_DIR}/ceph-debugpack
@ONLY
)

add_custom_target(shell_scripts ALL
COMMAND chmod 755 ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph ${CMAKE_BINARY_DIR}/ceph-debugpack ${CMAKE_BINARY_DIR}/ceph-coverage
DEPENDS ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph
COMMENT "Shell Scripts have permission")

add_subdirectory(rgw)
add_subdirectory(os)
add_subdirectory(osd)
if(${WITH_MDS})
  add_subdirectory(mds)
endif(${WITH_MDS})
if(${WITH_CLIENT})
  add_subdirectory(client)
endif(${WITH_CLIENT})

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

include_directories(".")

##########################################

# osdc
set(osdc_files
	osdc/Objecter.cc 
	osdc/ObjectCacher.cc 
	osdc/Filer.cc 
	osdc/Striper.cc
)

add_library(osdc ${CEPH_SHARED} ${osdc_files})

# mon
set (no_fun_mon_files
	mon/PGMonitor.cc 
	mon/PGMap.cc 
)
set (cohort_mon_files
	mon/Monitor.cc 
	mon/CohortOSDMonitor.cc
)

set(lib_mon_srcs
	auth/cephx/CephxKeyServer.cc 
	auth/cephx/CephxServiceHandler.cc 
#X	auth/cephx/CephxSessionHandler.cc 
	auth/AuthServiceHandler.cc 
#X	auth/AuthSessionHandler.cc 
	mon/Paxos.cc 
	mon/PaxosService.cc 
	mon/OSDMonitor.cc 
#X	mon/MDSMonitor.cc 
	mon/MonmapMonitor.cc 
	mon/LogMonitor.cc 
	mon/AuthMonitor.cc 
	mon/Elector.cc 
	mon/MonitorStore.cc 
#X	os/LevelDBStore.cc 
	mon/HealthMonitor.cc 
	mon/DataHealthService.cc 
	mon/ConfigKeyService.cc 
#X	common/util.cc
# ${no_fun_mon_files}
	${cohort_mon_files}
)

add_library(mon ${CEPH_SHARED} ${lib_mon_srcs})

# global
set(libglobal_srcs
	global/global_init.cc 
	global/pidfile.cc 
	global/signal_handler.cc
)
set(global_common_files
	global/global_context.cc 
)
add_library(global_common_objs OBJECT ${global_common_files})
add_library(global ${CEPH_SHARED} ${libglobal_srcs} $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(global PROPERTIES OUTPUT_NAME ceph-global VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

# common
set (crush_files
	crush/builder.c
	crush/mapper.c
	crush/crush.c
	crush/hash.c
	crush/CrushWrapper.cc
	crush/CrushCompiler.cc
	crush/CrushTester.cc
)
#set (cohort_osd_files
#	cohort/CohortOSD.cc
#	cohort/CohortOSD.h
#	cohort/OSDVol.cc
#	cohort/OSDVol.h
#	cohort/VolLog.cc
#	cohort/VolLog.h
#)
#set (cohort_librgw_files
#	cohort/CohortOSDMap.cc
#	cohort/CohortOSDMap.h
#)
set (cohort_common_files
	cohort/CohortOSDMap.cc
	cohort/CohortPlaceSystem.cc
	cohort/CohortPlaceSystem.h
	cohort/CohortVolume.cc
	cohort/CohortVolume.h
	cohort/erasure.cc
	cohort/erasure.h
	vol/Volume.cc
	vol/Volume.h
	osd/PlaceSystem.cc
)

set (mds_files)
if (${WITH_MDS})
	list(APPEND mds_files
		../mds/MDSMap.cc
		../mds/inode_backtrace.cc
		../mds/mdstypes.cc
	)
endif (${WITH_MDS})


add_custom_target(check_version ALL ${CMAKE_SOURCE_DIR}/src/check_version ${CMAKE_SOURCE_DIR}/src/.git_version
COMMENT "ceph_ver.h is up to date")

include_directories("${CMAKE_BINARY_DIR}/src")

add_custom_command(
PRE_BUILD
OUTPUT ${CMAKE_SOURCE_DIR}/src/.git_version
COMMAND ${CMAKE_SOURCE_DIR}/src/check_version ${CMAKE_SOURCE_DIR}/src/.git_version
DEPENDS ${CMAKE_SOURCE_DIR}/src/check_version
)

add_custom_command(
PRE_BUILD
OUTPUT ${CMAKE_BINARY_DIR}/src/ceph_ver.h
COMMAND ${CMAKE_SOURCE_DIR}/src/make_version ${CMAKE_SOURCE_DIR}/src/.git_version ${CMAKE_BINARY_DIR}/src/ceph_ver.h
DEPENDS ${CMAKE_SOURCE_DIR}/src/make_version ${CMAKE_SOURCE_DIR}/src/.git_version
)

# this list ommits the ceph_ver.c file
set (libcommon_files
	${CMAKE_BINARY_DIR}/src/ceph_ver.h
	ceph_ver.c
	auth/AuthAuthorizeHandler.cc
	auth/AuthClientHandler.cc
	auth/AuthSessionHandler.cc
	auth/AuthMethodList.cc
	auth/cephx/CephxAuthorizeHandler.cc
	auth/cephx/CephxClientHandler.cc
	auth/cephx/CephxProtocol.cc
	auth/cephx/CephxSessionHandler.cc
	auth/none/AuthNoneAuthorizeHandler.cc
	auth/unknown/AuthUnknownAuthorizeHandler.cc
	auth/Crypto.cc
	auth/KeyRing.cc
	auth/RotatingKeyRing.cc
	common/DecayCounter.cc
	common/LogClient.cc
	common/LogEntry.cc
	common/PrebufferedStreambuf.cc
	common/BackTrace.cc
	common/perf_counters.cc
	common/Mutex.cc
	common/OutputDataSocket.cc
	common/admin_socket.cc
	common/admin_socket_client.cc
	common/cmdparse.cc
	common/escape.c
	common/Clock.cc
	common/Throttle.cc
	common/Timer.cc
	common/Finisher.cc
	common/environment.cc
	common/sctp_crc32.c
	common/assert.cc
        common/run_cmd.cc
	common/WorkQueue.cc
	common/ConfUtils.cc
	common/MemoryModel.cc
	common/armor.c
	common/fd.cc
	common/xattr.c
	common/safe_io.c
	common/snap_types.cc
	common/str_list.cc
	common/errno.cc
	json_spirit/json_spirit_reader.cpp
	json_spirit/json_spirit_writer.cpp
	json_spirit/json_spirit_value.cpp
	log/Log.cc
	log/SubsystemMap.cc
	mon/MonCap.cc
	mon/MonClient.cc
	mon/MonMap.cc
	msg/Accepter.cc
	msg/DispatchQueue.cc
	msg/Message.cc
	common/RefCountedObj.cc
	msg/Messenger.cc
	msg/Pipe.cc
	msg/SimpleMessenger.cc
	msg/msg_types.cc
	os/hobject.cc
	osd/OSDMap.cc
	osd/osd_types.cc
#	${mds_files}
	common/blkdev.cc
	common/common_init.cc
	common/pipe.c
	common/ceph_argparse.cc
	common/ceph_context.cc
	common/buffer.cc
	common/code_environment.cc
	common/dout.cc
	common/signal.cc
	common/simple_spin.cc
	common/Thread.cc
	common/Formatter.cc
	common/HeartbeatMap.cc
	include/ceph_fs.cc
	include/ceph_hash.cc
	include/ceph_strings.cc
	include/ceph_frag.cc
	common/config.cc
	common/utf8.c
	common/mime.c
	common/strtol.cc
	common/page.cc
	common/lockdep.cc
	common/version.cc
	common/hex.cc
	common/entity_name.cc
	common/ceph_crypto.cc
	common/ceph_crypto_cms.cc
	common/ceph_json.cc
	common/ipaddr.cc
	common/pick_address.cc
	include/addr_parsing.c
#	${crush_files}
	${cohort_common_files}
)


if (${WITH_PROFILER})
  list(APPEND libcommon_files perfglue/cpu_profiler.cc)
else ()
  list(APPEND libcommon_files perfglue/disabled_stubs.cc)
endif (${WITH_PROFILER})

if(${ENABLE_SHARED})
  list(APPEND libcommon_files $<TARGET_OBJECTS:global_common_objs>)
endif(${ENABLE_SHARED})

add_library (common ${CEPH_SHARED} ${libcommon_files})

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c ${CMAKE_SOURCE_DIR}/src/common/version.cc ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/ceph_ver.h)

add_dependencies(common check_version)

if (${WITH_PROFILER})
  target_link_libraries(common profiler)
endif(${WITH_PROFILER})

target_link_libraries(common ${CRYPTO_LIBS} rt ${Boost_LIBRARIES})

# cls/lock
set(cls_lock_files
	cls/lock/cls_lock_client.cc  
	cls/lock/cls_lock_types.cc 
	cls/lock/cls_lock_ops.cc
)
add_library(cls_lock_objs OBJECT ${cls_lock_files})
add_library(cls_lock ${CEPH_SHARED} $<TARGET_OBJECTS:cls_lock_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_lock PROPERTIES OUTPUT_NAME cls_lock VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#monitor
set(ceph_mon_srcs
	ceph_mon.cc
	common/TextTable.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-mon ${ceph_mon_srcs})

target_link_libraries(ceph-mon mon boost_thread common os global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# osd
set(ceph_osd_srcs
	ceph_osd.cc
	objclass/class_debug.cc
	objclass/class_api.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-osd ${ceph_osd_srcs})

target_link_libraries(ceph-osd osd os global ${TCMALLOC_LIBS})

if(${WITH_MDS})
#mds
set(ceph_mds_srcs
	ceph_mds.cc
	${TCMALLOC_srcs}
)
add_executable(ceph-mds ${ceph_mds_srcs})

target_link_libraries(ceph-mds mds osdc ${CMAKE_DL_LIBS} global ${TCMALLOC_LIBS} boost_thread)
endif(${WITH_MDS})

# admin tools

#cephfs
set(cephfs_srcs
	cephfs.cc
)

add_executable(cephfs ${cephfs_srcs})

target_link_libraries(cephfs common ${EXTRALIBS})

# synthetic client
set(ceph_syn_srcs
	ceph_syn.cc
	client/SyntheticClient.cc
)

if(${WITH_CLIENT})
  add_executable(ceph-syn ${ceph_syn_srcs})
  target_link_libraries(ceph-syn client global)
endif(${WITH_CLIENT})

set(librados_srcs
	librados/librados.cc 
	librados/RadosClient.cc
	librados/IoCtxImpl.cc 
)

add_library(librados ${CEPH_SHARED} ${librados_srcs} $<TARGET_OBJECTS:cls_lock_objs>)
add_dependencies(librados osdc)
target_link_libraries(librados osdc common ${CRYPTO_LIBS} ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(librados PROPERTIES VERSION 2.0.0 SOVERSION 2)
endif(${ENABLE_SHARED})

#rados
set(rados_srcs
	rados.cc
	rados_import.cc
	rados_export.cc
	rados_sync.cc
	common/obj_bencher.cc
)
add_executable(rados ${rados_srcs})
target_link_libraries(rados librados global ${CMAKE_DL_LIBS})

if(${WITH_RBD})
#rbd
set(rbd_srcs
        librbd/librbd.cc
        librbd/AioCompletion.cc
        librbd/AioRequest.cc
        cls/rbd/cls_rbd_client.cc
        librbd/ImageCtx.cc
        librbd/internal.cc
        librbd/LibrbdWriteback.cc
        librbd/WatchCtx.cc
        osdc/ObjectCacher.cc
        osdc/Striper.cc
        librados/snap_set_diff.cc
        common/util.cc
)
add_library(rbd ${CEPH_SHARED} ${rbd_srcs} $<TARGET_OBJECTS:cls_lock_objs>)
target_link_libraries(rbd ${CMAKE_DL_LIBS})
if(${ENABLE_SHARED})
  set_target_properties(rbd PROPERTIES VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})
endif(${WITH_RBD})

#radosgw worry about option logic at a different time
if(${WITH_RADOSGW})
set(radosgw_srcs
	rgw/rgw_resolve.cc
        rgw/rgw_rest.cc
        rgw/rgw_rest_swift.cc
        rgw/rgw_rest_s3.cc
        rgw/rgw_rest_usage.cc
        rgw/rgw_rest_user.cc
        rgw/rgw_rest_bucket.cc
        rgw/rgw_http_client.cc
        rgw/rgw_swift.cc
	rgw/rgw_swift_auth.cc
	rgw/rgw_main.cc
)

add_executable(radosgw ${radosgw_srcs})

target_link_libraries(radosgw librgw_a librados libcls_rgw_client libcls_lock_client libcls_refcount_client curl expat global fcgi resolv)
install(TARGETS radosgw DESTINATION bin)
endif(${WITH_RADOSGW})

#librados_config_srcs
set(librados_config_srcs
	librados-config.cc
)

add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados global ${CMAKE_DL_LIBS})


#ceph-conf
set(ceph_conf_srcs
	ceph_conf.cc
)

add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)

#monmaptool
set(monmaptool_srcs
	monmaptool.cc
)

add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)

#crushtool
set(crushtool_srcs
	crushtool.cc
)

if (${WITH_CLIENT})
  add_executable(crushtool ${crushtool_srcs})
  target_link_libraries(crushtool global)
endif (${WITH_CLIENT})

#osdmaptool
set(osdomaptool_srcs
osdmaptool.cc
)

add_executable(osdmaptool  ${osdomaptool_srcs})
target_link_libraries(osdmaptool global ${CMAKE_DL_LIBS})

#ceph-authtool
set(ceph_authtool_srcs
	ceph_authtool.cc
)

add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global)

#ceph-fuse
if(WITH_FUSE)
set(ceph_fuse_srcs
ceph_fuse.cc
client/fuse_ll.cc
)

add_executable(ceph-fuse ${ceph_fuse_srcs})
target_link_libraries(ceph-fuse fuse client global)
set(ceph_fuse_bin "ceph-fuse")
endif(WITH_FUSE)

#ceph_mon_store_converter
set(ceph_mon_store_converter_srcs
mon_store_converter.cc
mon/MonitorStore.cc
)

add_executable(ceph_mon_store_converter ${ceph_mon_store_converter_srcs})
target_link_libraries(ceph_mon_store_converter os global)

#kvs
if(${WITH_KVS})
set(kvs_srcs
	key_value_store/cls_kvs.cc
)

add_library(cls_kvs ${CEPH_SHARED} ${kvs_srcs})

endif(${WITH_KVS})

set (ceph_mds_bin)
if (${WITH_MDS})
       list(APPEND ceph_mds_bin ceph-mds)
endif (${WITH_MDS})
set (ceph_client_bin)
if (${WITH_CLIENT})
       list(APPEND ceph_client_bin ceph-syn crushtool)
endif (${WITH_CLIENT})


# add the install targets
install(TARGETS
  ceph-mon
  ceph-osd
  ${ceph_mds_bin}
  cephfs
  ${ceph_fuse_bin}
  ${ceph_client_bin}
  rados
  librados-config
  ceph-conf
  monmaptool
  osdmaptool
  ceph-authtool
  ceph_mon_store_converter
  DESTINATION bin)

install(PROGRAMS
  ${CMAKE_BINARY_DIR}/init-ceph
  ${CMAKE_BINARY_DIR}/mkcephfs
  ${CMAKE_SOURCE_DIR}/doc/start/ceph.conf
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph_common.sh
  DESTINATION bin)
