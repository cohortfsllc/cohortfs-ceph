set(mds_srcs
	../mds/Anchor.cc 
	../mds/Capability.cc 
	../mds/Dumper.cc 
	../mds/Resetter.cc 
	../mds/MDS.cc
	../mds/flock.cc
	../mds/locks.c 
	../mds/journal.cc 
	../mds/Server.cc 
	../mds/Mutation.cc 
	../mds/MDCache.cc 
	../mds/Locker.cc 
	../mds/Migrator.cc 
	../mds/MDBalancer.cc 
	../mds/CDentry.cc 
	../mds/CDir.cc 
	../mds/CInode.cc 
	../mds/LogEvent.cc 
	../mds/MDSTable.cc
	../mds/InoTable.cc 
	../mds/MDSTableClient.cc 
	../mds/MDSTableServer.cc 
	../mds/AnchorServer.cc 
	../mds/AnchorClient.cc
	../mds/SnapRealm.cc 
	../mds/SnapServer.cc 
	../mds/snap.cc 
	../mds/SessionMap.cc 
	../mds/MDLog.cc
	../osdc/Journaler.cc
)

include_directories("..")

add_definitions("-D__linux__ -DHAVE_CONFIG_H -D__CEPH__ -DUSE_CRYPTOPP -DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set_property(
   DIRECTORY
   PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H __CEPH__ __linux__ USE_CRYPTOPP CEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} _FILE_OFFSET_BITS=64 _REENTRANT _THREAD_SAFE __STDC_FORMAT_MACROS _GNU_SOURCE
   )

set(CMAKE_CXX_FLAGS " -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_library(libmds ${mds_srcs})
cotire(libmds)

message( STATUS "libmds is here" )
