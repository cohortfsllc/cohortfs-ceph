set(osd_srcs
	../osd/PG.cc 
	../osd/PGLog.cc 
	../osd/ReplicatedPG.cc 
	../osd/Ager.cc 
	../osd/OSD.cc 
	../osd/OSDCap.cc 
	../osd/Watch.cc 
	../osd/ClassHandler.cc 
	../osd/OpRequest.cc 
	../osd/SnapMapper.cc
)

include_directories("..")


add_definitions("-D__linux__ -DHAVE_CONFIG_H -D__CEPH__ -DUSE_CRYPTOPP -DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set_property(
   DIRECTORY
   PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H __CEPH__ __linux__ USE_CRYPTOPP CEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} _FILE_OFFSET_BITS=64 _REENTRANT _THREAD_SAFE __STDC_FORMAT_MACROS _GNU_SOURCE
   )

set(CMAKE_CXX_FLAGS " -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_library(libosd ${osd_srcs})
cotire(libosd)

target_link_libraries(libosd dl leveldb)

message( STATUS "osd is here" )
