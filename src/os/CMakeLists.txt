message( STATUS "libos is here" )

set(libos_srcs
	../os/FileJournal.cc 
	../os/FileStore.cc 
	../os/chain_xattr.cc 
	../os/ObjectStore.cc 
	../os/JournalingObjectStore.cc 
	../os/LFNIndex.cc 
	../os/HashIndex.cc 
	../os/IndexManager.cc 
	../os/FlatIndex.cc 
	../os/DBObjectMap.cc 
	../os/LevelDBStore.cc 
	../os/WBThrottle.cc
)

include_directories("..")

add_definitions("-D__linux__ -DHAVE_CONFIG_H -D__CEPH__ -DUSE_CRYPTOPP -DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set_property(
   DIRECTORY
   PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H __CEPH__ __linux__ USE_CRYPTOPP CEPH_LIBDIR=${LIBRARY_OUTPUT_PATH} _FILE_OFFSET_BITS=64 _REENTRANT _THREAD_SAFE __STDC_FORMAT_MACROS _GNU_SOURCE
   )

set(CMAKE_CXX_FLAGS " -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_library(libos ${libos_srcs})
cotire(libos)

if(${HAVE_LIBAIO})
target_link_libraries(libos aio)
endif(${HAVE_LIBAIO})

target_link_libraries(libos leveldb snappy)
