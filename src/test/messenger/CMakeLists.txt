add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(AM_COMMON_FLAGS "-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "-Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_definitions("${AM_COMMON_FLAGS}")

set(EXTRALIBS uuid rt profiler)

set(CRYPTO_LIBS cryptopp)

if (${HAVE_ATOMIC_OPS})
  set(EXTRALIBS
	atomic_ops
	${EXTRALIBS})
endif(${HAVE_ATOMIC_OPS})

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs ${PROJECT_SOURCE_DIR}/src/perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs ${PROJECT_SOURCE_DIR}/src/perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

include_directories("."
  "${PROJECT_SOURCE_DIR}/src/")

set(simple_server_srcs
	simple_server.cc
	${TCMALLOC_srcs}
)

add_executable(simple_server ${simple_server_srcs})

target_link_libraries(simple_server os global ${TCMALLOC_LIBS})

set(simple_client_srcs
	simple_client.cc
	${TCMALLOC_srcs}
)
add_executable(simple_client ${simple_client_srcs})

target_link_libraries(simple_client os global ${TCMALLOC_LIBS})
