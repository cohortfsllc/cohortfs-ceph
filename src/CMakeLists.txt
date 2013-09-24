
add_definitions("-D__linux__ -DHAVE_CONFIG_H -D__CEPH__ -DUSE_CRYPTOPP -DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set_property(
   DIRECTORY
   PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H __CEPH__ __linux__ USE_CRYPTOPP CEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} _FILE_OFFSET_BITS=64 _REENTRANT _THREAD_SAFE __STDC_FORMAT_MACROS _GNU_SOURCE
   )

set(CMAKE_CXX_FLAGS " -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

set(EXTRALIBS uuid rt profiler)

set(CRYPTO_LIBS cryptopp)

if (${HAVE_ATOMIC_OPS})
  set(EXTRALIBS
	atomic_ops
	${EXTRALIBS})
endif(${HAVE_ATOMIC_OPS})

add_subdirectory(common)
add_subdirectory(osdc)
add_subdirectory(cls/lock)
add_subdirectory(librados)
add_subdirectory(mon)
add_subdirectory(os)
add_subdirectory(global)
add_subdirectory(osd)
add_subdirectory(mds)
add_subdirectory(client)

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

include_directories(".")

#monitor
set(ceph_mon_srcs 
	ceph_mon.cc
	common/TextTable.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-mon ${ceph_mon_srcs})
cotire(ceph-mon)

target_link_libraries(ceph-mon libmon boost_thread libcommon libos libglobal ${TCMALLOC_LIBS})

# osd
set(ceph_osd_srcs
	ceph_osd.cc 
	objclass/class_debug.cc 
	objclass/class_api.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-osd ${ceph_osd_srcs})
cotire(ceph-osd)

target_link_libraries(ceph-osd libosd libos libglobal ${TCMALLOC_LIBS})

#mds
set(ceph_mds_srcs
	ceph_mds.cc
	${TCMALLOC_srcs}
)
add_executable(ceph-mds ${ceph_mds_srcs})
cotire(ceph-mds)

target_link_libraries(ceph-mds libmds libosdc ${CMAKE_DL_LIBS} libglobal ${TCMALLOC_LIBS} boost_thread)

# admin tools

#ceph-conf
set(ceph_conf_srcs
	ceph_conf.cc
)

add_executable(ceph-conf ${ceph_conf_srcs})
cotire(ceph-conf)
target_link_libraries(ceph-conf libglobal)

#ceph-authtool
set(ceph_authtool_srcs
	ceph_authtool.cc
)

add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool libglobal)

#cephfs
set(cephfs_srcs 
	cephfs.cc
)

add_executable(cephfs ${cephfs_srcs})
cotire(cephfs)

target_link_libraries(cephfs libcommon)

# synthetic client
set(ceph_syn_srcs
	ceph_syn.cc 
	client/SyntheticClient.cc
)

add_executable(ceph-syn ${ceph_syn_srcs})
cotire(ceph-syn)
target_link_libraries(ceph-syn libclient libglobal)

#rados
set(rados_srcs
	rados.cc 
	rados_import.cc 
	rados_export.cc 
	rados_sync.cc 
	common/obj_bencher.cc
)
add_executable(rados ${rados_srcs})
cotire(rados)
target_link_libraries(rados cls_lock librados libglobal)

#librados_config_srcs
set(librados_config_srcs
	librados-config.cc 
)

add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados libglobal)

#ceph_osdmap_tool
set(ceph_osdomap_tool_srcs 
	tools/ceph-osdomap-tool.cc 
	os/LevelDBStore.cc
)

add_executable(ceph-osdomap-tool ${ceph_osdomap_tool_srcs})
cotire(ceph-osdomap-tool)
target_link_libraries(ceph-osdomap-tool libos libglobal -lboost_program_options)
# has these: ${UNITTEST_CXXFLAGS}

#set(ceph_monstore_tool_srcs
#	tools/ceph-monstore-tool.cc
#	os/LevelDBStore.cc
#)

#add_executable(ceph-monstore-tool ${ceph_monstore_tool_srcs})
#ceph_monstore_tool_LDFLAGS = ${AM_LDFLAGS}
#target_link_libraries(ceph-monstore-tool libos libglobal -lboost_program_options)
#has these too: ${UNITTEST_CXXFLAGS}


#monmaptool_SOURCES = monmaptool.cc
#monmaptool_LDADD = $(LIBGLOBAL_LDA)
#crushtool_SOURCES = crushtool.cc
#crushtool_LDADD = $(LIBGLOBAL_LDA)
#osdmaptool_SOURCES = osdmaptool.cc
#osdmaptool_LDADD = $(LIBGLOBAL_LDA)
#bin_PROGRAMS += monmaptool crushtool osdmaptool
#ceph-filestore-dump
#set(ceph_filestore_dump_srcs 
#	tools/ceph-filestore-dump.cc 
#	objclass/class_debug.cc 
#	objclass/class_api.cc
#	perfglue/disabled_heap_profiler.cc
#)

#add_executable(ceph-filestore-dump ${ceph_filestore_dump_srcs})

#target_link_libraries(ceph-filestore-dump libosd libos libglobal boost_program_options)

#if(${LINUX})
#target_link_libraries(ceph-filestore-dump dl)
#endif(${LINUX})

message( STATUS "src cmake is here" )

