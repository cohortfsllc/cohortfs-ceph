message( STATUS "libmon is here" )

set(lib_mon_srcs
	../auth/cephx/CephxKeyServer.cc 
	../auth/cephx/CephxServiceHandler.cc 
	../auth/cephx/CephxSessionHandler.cc 
	../auth/AuthServiceHandler.cc 
	../auth/AuthSessionHandler.cc 
	../mon/Monitor.cc 
	../mon/Paxos.cc 
	../mon/PaxosService.cc 
	../mon/OSDMonitor.cc 
	../mon/MDSMonitor.cc 
	../mon/MonmapMonitor.cc 
	../mon/PGMonitor.cc 
	../mon/PGMap.cc 
	../mon/LogMonitor.cc 
	../mon/AuthMonitor.cc 
	../mon/Elector.cc 
	../mon/MonitorStore.cc 
	../os/LevelDBStore.cc 
	../mon/HealthMonitor.cc 
	../mon/DataHealthService.cc 
	../mon/ConfigKeyService.cc 
	../common/util.cc
)

include_directories("..")

add_definitions("-D__linux__ -DHAVE_CONFIG_H -D__CEPH__ -DUSE_CRYPTOPP -DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set_property(
   DIRECTORY
   PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H __CEPH__ __linux__ USE_CRYPTOPP CEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} _FILE_OFFSET_BITS=64 _REENTRANT _THREAD_SAFE __STDC_FORMAT_MACROS _GNU_SOURCE
   )

set(CMAKE_CXX_FLAGS " -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_library(libmon ${lib_mon_srcs})
cotire(libmon)


