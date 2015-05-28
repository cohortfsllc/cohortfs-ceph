include(GetGitRevisionDescription)

# Build types
set(CMAKE_C_FLAGS_DEBUG "-O0 -g3 -gdwarf-4")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3 -gdwarf-4 -DWITH_SQUEAKY_CLEAN_VALGRIND")

set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG -g0 -DUSE_CEPH_ASSERT")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -g0 -DUSE_CEPH_ASSERT")

set(CMAKE_C_FLAGS_MINSIZEREL "-Os -DNDEBUG -g0 -DUSE_CEPH_ASSERT")
set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -DNDEBUG -g0 -DUSE_CEPH_ASSERT")

set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O -g" -DUSE_CEPH_ASSERT)
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O -g" -DUSE_CEPH_ASSERT)

add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")
add_definitions("-DCEPH_PKGLIBDIR=\"${CMAKE_INSTALL_PREFIX}/lib\"")
add_definitions("-D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set(CLIKE_COMMON_FLAGS "-Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -Wno-vla -fno-strict-aliasing -fsigned-char -fno-omit-frame-pointer -fPIC")

# sort out which allocator to use
if(${ALLOCATOR} STREQUAL "jemalloc")
  find_package(JeMalloc)
  if(JEMALLOC_FOUND)
    set(ALLOC_LIBS ${JEMALLOC_LIBRARIES})
    set(CMAKE_CLIKE_FLAGS "${CMAKE_CLIKE_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  else(JEMALLOC_FOUND)
    message(WARNING "jemalloc not found, falling back to libc")
    set(ALLOCATOR "libc")
  endif(JEMALLOC_FOUND)
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
elseif(${ALLOCATOR} STREQUAL "tcmalloc")
  find_package(tcmalloc)
  if(Tcmalloc_FOUND)
    set(ALLOC_LIBS ${Tcmalloc_LIBRARIES})
    set(CMAKE_CLIKE_FLAGS "${CMAKE_CLIKE_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
    set(TCMALLOC_srcs perfglue/heap_profiler.cc)
  else(Tcmalloc_FOUND)
    message(WARNING "tcmalloc not found, falling back to libc")
    set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
    set(ALLOCATOR "libc")
  endif(Tcmalloc_FOUND)
else()
  if(NOT ${ALLOCATOR} STREQUAL "libc")
    message(SEND_ERROR "${ALLOCATOR} is not a valid option. Valid allocators are: jemalloc|tcmalloc|libc")
  endif()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif()

if(${WITH_SQUEAKY_CLEAN_VALGRIND})
  set(CLIKE_COMMON_FLAGS "${CLIKE_COMMON_FLAGS} -DSQUEAKY_CLEAN_VALGRIND=1")
endif(${WITH_SQUEAKY_CLEAN_VALGRIND})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CLIKE_COMMON_FLAGS} -std=c11")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CLIKE_COMMON_FLAGS} -std=c++14 -Wnon-virtual-dtor -Wno-invalid-offsetof")

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wno-unused-private-field -Wno-mismatched-tags")
endif()

set(EXTRALIBS rt dl boost_system)
if(${WITH_PROFILER})
  list(APPEND EXTRALIBS profiler)
endif(${WITH_PROFILER})


if(USE_LIBCXX)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
    set(LIBCXX_LINKER_FLAGS "")
    list(APPEND EXTRALIBS -lsupc++)
    if(DEFINED LIBCXX_ROOT)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isystem ${LIBCXX_ROOT}/include")
      set(LIBCXX_LINKER_FLAGS "${LIBCXX_LINKER_FLAGS} -L${LIBCXX_ROOT}/lib")
    endif(DEFINED LIBCXX_ROOT)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LIBCXX_LINKER_FLAGS}")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${LIBCXX_LINKER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${LIBCXX_LINKER_FLAGS}")
  else()
    message(FATAL_ERROR "We don't support using libc++ with g++.")
  endif()
endif()

if (HAVE_XIO)
  include_directories(${Xio_INCLUDE_DIR})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${Xio_INCLUDE_DIR}")
  list(APPEND EXTRALIBS ${Xio_LIBRARY} ibverbs rdmacm pthread rt)
endif(HAVE_XIO)


if (HAVE_LTTNG)
  list(APPEND EXTRALIBS ${LTTNG_UST_LIBRARY})
endif(HAVE_LTTNG)

set(CRYPTO_LIBS cryptopp)

set(GCOV_PREFIX_STRIP 4)

get_git_head_revision(GIT_REFSPEC CEPH_GIT_VER)
git_describe(CEPH_GIT_NICE_VER --always)

# Python stuff
find_package(PythonInterp 2 QUIET)
if(NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Python 2 interpreter not found.")
endif(NOT PYTHONINTERP_FOUND)

# if CMAKE_INSTALL_PREFIX is an empty string, must replace
# it with "/" to make PYTHON_INSTALL_TEMPLATE an absolute path to be
# consistent with all other installation paths.
if(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "${CMAKE_INSTALL_PREFIX}")
else(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "/")
endif(CMAKE_INSTALL_PREFIX)

execute_process(
  COMMAND
  ${PYTHON_EXECUTABLE} -c "from distutils import sysconfig; print sysconfig.get_python_lib(1,0,prefix='${PYTHON_INSTALL_TEMPLATE}')"
  OUTPUT_VARIABLE PYTHON_INSTDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
COMMAND
${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/src/ceph.setpath.py ${PYTHON_INSTDIR} ${CMAKE_INSTALL_PREFIX}/bin
OUTPUT_VARIABLE CEPH_PYTHON_SET_PATH)

include_directories(".")
#include_directories("./include")

if(HAVE_LTTNG)
  add_subdirectory(blkin)
  include_directories(blkin/blkin-lib)
  include_directories(blkin)
endif(HAVE_LTTNG)

# tcmalloc heap profiler
set(heap_profiler_files ${TCMALLOC_srcs})
add_library(heap_profiler_objs OBJECT ${heap_profiler_files})

set(LIBEDIT_LIBS edit)

# Common infrastructure
configure_file(
  ${CMAKE_SOURCE_DIR}/src/ceph_ver.h.in.cmake
  ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
  @ONLY)

set(arch_files
  arch/intel.c
  arch/neon.c
  arch/probe.cc
  )

set(auth_files
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
  )

if (HAVE_XIO)
  set (xiomsg_files
    msg/QueueStrategy.cc
    msg/XioConnection.cc
    msg/XioMessenger.cc
    msg/XioMsg.cc
    msg/XioPool.cc
    msg/XioPortal.cc
    )
endif (HAVE_XIO)

set(mds_files
  mds/MDSMap.cc
  )

add_subdirectory(json_spirit)

set(libcommon_files
  common/DecayCounter.cc
  common/LogClient.cc
  common/LogEntry.cc
  common/PrebufferedStreambuf.cc
  common/cohort_error.cc
  common/BackTrace.cc
  common/OutputDataSocket.cc
  common/admin_socket.cc
  common/admin_socket_client.cc
  common/cmdparse.cc
  common/escape.c
  common/Throttle.cc
  common/Finisher.cc
  common/environment.cc
  common/sctp_crc32.c
  common/crc32c.cc
  common/crc32c_intel_baseline.c
  common/crc32c_intel_fast.c
  common/assert.cc
  common/run_cmd.cc
  common/WorkQueue.cc
  common/ConfUtils.cc
  common/MemoryModel.cc
  common/fd.cc
  common/xattr.c
  common/str_list.cc
  common/str_map.cc
  common/errno.cc
  common/util.cc
  common/SloppyCRCMap.cc
  common/cohort_lru.cc
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
  msg/PipeConnection.cc
  msg/SimpleMessenger.cc
  msg/DirectMessenger.cc
  msg/msg_types.cc
  common/oid.cc
  osd/OSDMap.cc
  common/histogram.cc
  osd/osd_types.cc
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
  common/ceph_fs.cc
  common/ceph_hash.cc
  common/ceph_strings.cc
  common/ceph_frag.cc
  common/config.cc
  common/utf8.c
  common/mime.c
  common/strtol.cc
  common/page.cc
  common/version.cc
  common/hex.cc
  common/entity_name.cc
  common/ceph_crypto.cc
  common/ceph_crypto_cms.cc
  common/ceph_json.cc
  common/ipaddr.cc
  common/pick_address.cc
  common/address_helper.cc
  common/linux_version.c
  xxHash-r39/xxhash.c
  vol/Volume.cc
  placer/Placer.cc
  placer/ErasureCPlacer.cc
  placer/StripedPlacer.cc
  common/ThreadPool.cc
  ${arch_files}
  ${auth_files}
  ${xiomsg_files}
  ${mds_files})
set(common_mountcephfs_files
  common/armor.c
  common/safe_io.c
  common/addr_parsing.c
  )
add_library(common_mountcephfs_objs OBJECT ${common_mountcephfs_files})

if(${WITH_PROFILER})
  list(APPEND libcommon_files perfglue/cpu_profiler.cc)
else()
  list(APPEND libcommon_files perfglue/disabled_stubs.cc)
endif(${WITH_PROFILER})

add_library(common_objs OBJECT ${libcommon_files})
set(libcommon_objs $<TARGET_OBJECTS:common_objs>
  $<TARGET_OBJECTS:common_mountcephfs_objs>)

add_library(common ${CEPH_SHARED} ${libcommon_objs})
if(${ENABLE_SHARED})
  set_target_properties(common PROPERTIES OUTPUT_NAME ceph-common
    VERSION "1.0.0" SOVERSION "1" LINK_FLAGS "-Wl,-export-dynamic")
endif(${ENABLE_SHARED})
install(TARGETS common DESTINATION lib)

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/common/version.cc
  ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc
  APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if(${WITH_PROFILER})
  target_link_libraries(common PRIVATE profiler)
endif(${WITH_PROFILER})
if(HAVE_LTTNG)
  target_link_libraries(common PRIVATE blkin)
endif(HAVE_LTTNG)

target_link_libraries(common PRIVATE erasure_code json_spirit ${CRYPTO_LIBS} rt ${Boost_LIBRARIES} ${EXTRALIBS})

set(libglobal_srcs
  global/global_init.cc
  global/pidfile.cc
  global/signal_handler.cc
  )
add_library(global STATIC
  ${libglobal_srcs})
target_link_libraries(global PRIVATE common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(global PROPERTIES OUTPUT_NAME ceph-global VERSION "1.0.0" SOVERSION "1"
    LINK_FLAGS "-Wl,-export-dynamic")
endif(${ENABLE_SHARED})

# rados object classes
add_subdirectory(cls)

# RADOS client/library
set(osdc_files
  osdc/Objecter.cc
  messages/MOSDOpReply.cc
  osdc/RadosClient.cc
  )
add_library(osdc_objs OBJECT ${osdc_files})

add_library(librados ${CEPH_SHARED} 
  $<TARGET_OBJECTS:osdc_objs>
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
target_link_libraries(librados PRIVATE global common cls_lock_client
  ${CRYPTO_LIBS} ${EXTRALIBS} ${ALLOC_LIBS})
if(${ENABLE_SHARED})
  set_target_properties(librados PROPERTIES OUTPUT_NAME rados VERSION 2.0.0 SOVERSION 2
    LINK_FLAGS "-Wl,-export-dynamic")
endif(${ENABLE_SHARED})
install(FILES include/buffer.h include/page.h include/crc32c.h
  DESTINATION include/rados)
install(TARGETS librados DESTINATION lib)

set(rados_srcs
  tools/rados/rados.cc
  common/obj_bencher.cc)
add_executable(rados
  ${rados_srcs}
  )
target_link_libraries(rados librados global cls_lock_client boost_filesystem ${CMAKE_DL_LIBS})

install(TARGETS rados DESTINATION bin)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/
  DESTINATION ${PYTHON_INSTDIR})

# Monitor
set(lib_mon_srcs
  auth/cephx/CephxKeyServer.cc
  auth/cephx/CephxServiceHandler.cc
  auth/AuthServiceHandler.cc
  ${osd_mon_files}
  mon/Paxos.cc
  mon/PaxosService.cc
  mon/OSDMonitor.cc
  mon/MDSMonitor.cc
  mon/MonmapMonitor.cc
  mon/LogMonitor.cc
  mon/AuthMonitor.cc
  mon/Elector.cc
  mon/MonitorStore.cc
  mon/HealthMonitor.cc
  ${os_mon_files}
  mon/DataHealthService.cc
  mon/ConfigKeyService.cc
  mon/MessageFactory.cc
  )
set(monitorstore_src
  mon/MonitorStore.cc
  )
add_library(monitorstore_obj OBJECT ${monitorstore_src})
add_library(mon STATIC ${lib_mon_srcs} $<TARGET_OBJECTS:os_mon_objs>
  $<TARGET_OBJECTS:osd_mon_objs> $<TARGET_OBJECTS:monitorstore_obj>)
set(ceph_mon_srcs ceph_mon.cc common/TextTable.cc)
add_executable(ceph-mon ${ceph_mon_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(ceph-mon mon boost_regex boost_system boost_thread
  common global leveldb boost_filesystem ${EXTRALIBS} ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS})
install(TARGETS ceph-mon DESTINATION bin)

# OSD

set(libos_srcs
  os/chain_xattr.cc
  os/ObjectStore.cc
  os/Transaction.cc
  os/Factory.cc
  os/JournalingObjectStore.cc
  os/LevelDBStore.cc
  os/DBObjectMap.cc
  os/GenericObjectMap.cc
  )
set(os_mon_files
  os/LevelDBStore.cc
  )
add_library(os_mon_objs OBJECT ${os_mon_files})
add_library(os ${CEPH_SHARED} ${libos_srcs})
target_link_libraries(os PRIVATE snappy boost_filesystem leveldb)
if(${ENABLE_SHARED})
  set_target_properties(os PROPERTIES OUTPUT_NAME ceph-os
    VERSION "1.0.0" SOVERSION "1" LINK_FLAGS "-Wl,-export-dynamic")
endif(${ENABLE_SHARED})
install(TARGETS os DESTINATION lib)

# objectstore modules
# os filestore
set(os_filestore_files
  os/FileJournal.cc
  os/FileStore.cc
  os/GenericFileStoreBackend.cc
  os/BtrfsFileStoreBackend.cc
  os/ZFSFileStoreBackend.cc
  os/FragTreeIndex.cc
)

add_library(os_filestore_objs OBJECT ${os_filestore_files})
add_library(os_filestore SHARED $<TARGET_OBJECTS:os_filestore_objs>)
if(${HAVE_LIBAIO})
  target_link_libraries(os_filestore aio)
endif(${HAVE_LIBAIO})
target_link_libraries(os_filestore common os)
set_target_properties(os_filestore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_filestore DESTINATION lib/os-modules)

#os kvstore
set(os_kvstore_files
  os/KeyValueStore.cc
)

add_library(os_kvstore_objs OBJECT ${os_kvstore_files})
add_library(os_kvstore SHARED $<TARGET_OBJECTS:os_kvstore_objs>)
target_link_libraries(os_kvstore common os)
set_target_properties(os_kvstore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_kvstore DESTINATION lib/os-modules)

#os memstore
set(os_memstore_files
  os/MemStore.cc
)

add_library(os_memstore_objs OBJECT ${os_memstore_files})
add_library(os_memstore SHARED $<TARGET_OBJECTS:os_memstore_objs>)
target_link_libraries(os_memstore common os)
set_target_properties(os_memstore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_memstore DESTINATION lib/os-modules)

# RADOS classes
set(cls_references_files objclass/class_api.cc)
add_library(cls_references_objs OBJECT ${cls_references_files})

set(libosd_srcs
  libosd/libosd.cc
  libosd/remote.cc
  libosd/Context.cc
  libosd/Objecter.cc
  libosd/Dispatcher.cc
  libosd/Messengers.cc
  )
set(osd_srcs
  osd/ClassHandler.cc
  osd/OpRequest.cc
  osd/OSD.cc
  osd/OSDVol.cc
  osd/Watch.cc
  ${libosd_srcs}
  $<TARGET_OBJECTS:cls_references_objs>
  $<TARGET_OBJECTS:osdc_objs>
  )
set(osd_mon_files mon/Monitor.cc)
add_library(osd_mon_objs OBJECT ${osd_mon_files})
add_library(osd ${CEPH_SHARED} ${osd_srcs})
if(${ENABLE_SHARED})
  set_target_properties(osd PROPERTIES OUTPUT_NAME ceph-osd VERSION "1.0.0"
    SOVERSION "1" LINK_FLAGS "-Wl,-export-dynamic")
  install(TARGETS osd DESTINATION lib)
  install(FILES libosd/ceph_osd.h libosd/ceph_osd_remote.h DESTINATION include)
endif(${ENABLE_SHARED})
target_link_libraries(osd PRIVATE common os global dl leveldb ${EXTRALIBS})

set(ceph_osd_srcs ceph_osd.cc objclass/class_api.cc)
add_executable(ceph-osd ${ceph_osd_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(ceph-osd osd os global
  boost_system boost_filesystem leveldb ${ALLOC_LIBS})
install(TARGETS ceph-osd DESTINATION bin)

# MDS
if(WITH_LIBMDS)
  set(mds_srcs
    mds/libmds.cc
    mds/MDS.cc
    mds/MessageFactory.cc
    $<TARGET_OBJECTS:osdc_objs>
    )
  add_library(mds ${CEPH_SHARED} ${mds_srcs})
  if(${ENABLE_SHARED})
    set_target_properties(mds PROPERTIES OUTPUT_NAME ceph-mds VERSION "1.0.0"
      SOVERSION "1" LINK_FLAGS "-Wl,-export-dynamic")
    install(TARGETS mds DESTINATION lib)
  endif(${ENABLE_SHARED})
  target_link_libraries(mds PRIVATE common os dl ${MCAS_LIBRARIES} ${EXTRALIBS})
  install(FILES
	mds/MDS.h include/ceph_time.h mds/ceph_mds.h
	DESTINATION include/cephfs)
endif(WITH_LIBMDS)

add_subdirectory(erasure-code)

# Support/Tools

set(cephfs_srcs
  cephfs.cc
  )
add_executable(cephfstool ${cephfs_srcs})
target_link_libraries(cephfstool common ${EXTRALIBS})
set_target_properties(cephfstool PROPERTIES OUTPUT_NAME cephfs)
install(TARGETS cephfstool DESTINATION bin)

set(ceph_conf_srcs
  tools/ceph_conf.cc
  )
add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)
install(TARGETS ceph-conf DESTINATION bin)

set(monmaptool_srcs
  tools/monmaptool.cc
  )
add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)
install(TARGETS monmaptool DESTINATION bin)

set(osdomaptool_srcs
  tools/osdmaptool.cc
  )
add_executable(osdmaptool  ${osdomaptool_srcs})
target_link_libraries(osdmaptool global ${CMAKE_DL_LIBS})
install(TARGETS osdmaptool DESTINATION bin)

set(ceph_authtool_srcs
  tools/ceph_authtool.cc
  )
add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global ${EXTRALIBS})
install(TARGETS ceph-authtool DESTINATION bin)

set(ceph_babeltrace_wrapper_srcs
  tools/ceph_babeltrace_wrapper.cc
  )
add_executable(ceph_babeltrace_wrapper ${ceph_babeltrace_wrapper_srcs})
target_link_libraries(ceph_babeltrace_wrapper common ${EXTRALIBS})
install(TARGETS ceph_babeltrace_wrapper DESTINATION bin)

set(ceph_mon_store_converter_srcs
  tools/mon_store_converter.cc
  )
add_executable(ceph_mon_store_converter ${ceph_mon_store_converter_srcs}
  $<TARGET_OBJECTS:monitorstore_obj>)
target_link_libraries(ceph_mon_store_converter global os
  boost_filesystem boost_system leveldb)
install(TARGETS ceph_mon_store_converter DESTINATION bin)

if(WITH_BABELTRACE_CEPH)
  set(babeltrace_ceph_srcs
    tools/ceph_log_plugin.cc
  )
  add_executable(babeltrace_ceph ${babeltrace_ceph_srcs})
  target_link_libraries(babeltrace_ceph glib-2.0 babeltrace babeltrace-ctf)
  install(TARGETS babeltrace_ceph DESTINATION bin)
endif(WITH_BABELTRACE_CEPH)

configure_file(${CMAKE_SOURCE_DIR}/src/mkcephfs.in
  ${CMAKE_BINARY_DIR}/mkcephfs @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
  ${CMAKE_BINARY_DIR}/ceph-coverage @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
  ${CMAKE_BINARY_DIR}/ceph-debugpack @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/src/ceph.in.cmake
  ${CMAKE_BINARY_DIR}/ceph @ONLY)

add_custom_target(shell_scripts ALL
  COMMAND chmod 755 ${CMAKE_BINARY_DIR}/mkcephfs
  ${CMAKE_BINARY_DIR}/init-ceph ${CMAKE_BINARY_DIR}/ceph-debugpack
  ${CMAKE_BINARY_DIR}/ceph-coverage ${CMAKE_BINARY_DIR}/ceph
  DEPENDS ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph
  ${CMAKE_BINARY_DIR}/ceph-debugpack ${CMAKE_BINARY_DIR}/ceph-coverage
  ${CMAKE_BINARY_DIR}/ceph COMMENT "Shell Scripts have permission")

install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/src/ceph-run
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph-clsinfo
  # This is rather bostumous, it would be better if configured scripts were
  # treated as targets in their own right. Will require some poking at
  # CMake to work properly. Same goes for the other configured scripts.
  ${CMAKE_BINARY_DIR}/ceph-debugpack
  ${CMAKE_BINARY_DIR}/ceph-coverage
  ${CMAKE_BINARY_DIR}/ceph
  DESTINATION bin)
install(TARGETS
  cephfstool
  ceph-conf
  ceph-authtool
  ceph_mon_store_converter
  DESTINATION bin)

configure_file(${CMAKE_SOURCE_DIR}/src/init-ceph.in
  ${CMAKE_BINARY_DIR}/init-ceph @ONLY)

install(PROGRAMS ${CMAKE_BINARY_DIR}/init-ceph DESTINATION bin)
install(FILES ${CMAKE_SOURCE_DIR}/doc/start/ceph.conf
  DESTINATION ${sysconfdir}/ceph/ RENAME ceph.conf.example)

install(PROGRAMS ${CMAKE_SOURCE_DIR}/src/ceph_common.sh
  DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/ceph)

install(PROGRAMS ${CMAKE_SOURCE_DIR}/src/ceph-create-keys
  ${CMAKE_SOURCE_DIR}/src/ceph-disk ${CMAKE_SOURCE_DIR}/src/ceph-disk-activate
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-prepare ${CMAKE_SOURCE_DIR}/src/ceph-disk-udev
  ${CMAKE_BINARY_DIR}/mkcephfs DESTINATION sbin)

if(${WITH_RBD})
  set(librbd_srcs librbd/Image.cc)
  add_library(librbd ${CEPH_SHARED} ${librbd_srcs})
  target_link_libraries(librbd PRIVATE ${CMAKE_DL_LIBS})
  if(${ENABLE_SHARED})
    set_target_properties(librbd PROPERTIES VERSION "1.0.0" SOVERSION "1" OUTPUT_NAME rbd
      LINK_FLAGS "-Wl,-export-dynamic")
  endif(${ENABLE_SHARED})
  install(TARGETS librados librbd DESTINATION lib)
  set(rbd_srcs rbd.cc common/TextTable.cc)
  set(rbd_mountcephfs_files common/secret.c)
  add_library(rbd_mountcephfs_objs OBJECT ${rbd_mountcephfs_files})
  add_executable(rbd ${rbd_srcs}
    $<TARGET_OBJECTS:rbd_mountcephfs_objs>
    $<TARGET_OBJECTS:heap_profiler_objs>)
  set_target_properties(rbd PROPERTIES OUTPUT_NAME rbd)
  target_link_libraries(rbd global librbd librados common
    boost_filesystem keyutils ${CMAKE_DL_LIBS} ${ALLOC_LIBS})
  install(TARGETS rbd DESTINATION bin)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/src/ceph-rbdnamer
    DESTINATION bin)
endif(${WITH_RBD})

# RadosGW
if(${WITH_KVS})
  set(kvs_srcs key_value_store/cls_kvs.cc)
  add_library(cls_kvs SHARED ${kvs_srcs})
  target_link_libraries(cls_kvs osd)
  set_target_properties(cls_kvs PROPERTIES VERSION "1.0.0" SOVERSION "1")
  install(TARGETS cls_kvs DESTINATION lib/rados-classes)
endif(${WITH_KVS})

if(${WITH_RADOSGW})
  include_directories("civetweb/include")
  set(radosgw_srcs rgw/rgw_tools.cc rgw/rgw_gc.cc rgw/rgw_env.cc
    rgw/rgw_dencoder.cc rgw/rgw_json_enc.cc rgw/rgw_multi.cc
    rgw/rgw_rados.cc rgw/rgw_bucket.cc rgw/rgw_acl.cc rgw/rgw_xml.cc
    rgw/rgw_acl_s3.cc rgw/rgw_quota.cc rgw/rgw_cors.cc
    rgw/rgw_cache.cc rgw/rgw_cors_s3.cc rgw/rgw_client_io.cc
    rgw/rgw_op.cc rgw/rgw_user.cc rgw/rgw_resolve.cc rgw/rgw_rest.cc
    rgw/rgw_rest_swift.cc rgw/rgw_rest_s3.cc rgw/rgw_rest_usage.cc
    rgw/rgw_rest_user.cc rgw/rgw_rest_bucket.cc rgw/rgw_http_client.cc
    rgw/rgw_swift.cc rgw/rgw_swift_auth.cc rgw/rgw_main.cc
    rgw/rgw_keystone.cc rgw/rgw_rest_client.cc rgw/rgw_metadata.cc
    rgw/rgw_rest_config.cc rgw/rgw_rest_conn.cc rgw/rgw_rest_log.cc
    rgw/rgw_rest_metadata.cc rgw/rgw_rest_opstate.cc
    rgw/rgw_rest_replica_log.cc rgw/rgw_common.cc rgw/rgw_multi_del.cc
    rgw/rgw_formats.cc rgw/rgw_acl_swift.cc rgw/rgw_policy_s3.cc
    rgw/rgw_auth_s3.cc rgw/rgw_usage.cc rgw/rgw_log.cc
    rgw/rgw_loadgen.cc rgw/rgw_civetweb.cc rgw/rgw_fcgi.cc
    rgw/rgw_replica_log.cc civetweb/src/civetweb.c civetweb/src/CivetServer.cpp)
  add_library(radosgw_objs OBJECT ${radosgw_srcs})
  add_executable(radosgw $<TARGET_OBJECTS:radosgw_objs> $<TARGET_OBJECTS:heap_profiler_objs>)
  target_link_libraries(radosgw librados
    cls_rgw_client cls_lock_client cls_refcount_client
    cls_log_client cls_statelog_client cls_version_client
    cls_replica_log_client cls_user_client curl expat global fcgi resolv
    ${ALLOC_LIBS})
  install(TARGETS radosgw DESTINATION bin)
endif(${WITH_RADOSGW})

if (WITH_TESTS)
  add_subdirectory(gtest)
  add_subdirectory(test)
endif (WITH_TESTS)
