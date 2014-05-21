
set(rados_srcs
	librados.cc 
	RadosClient.cc
	IoCtxImpl.cc 
	snap_set_diff.cc
)

add_library(rados SHARED ${rados_srcs})

add_dependencies(rados cls_lock osdc)

target_link_libraries(rados osdc cls_lock common ${CRYPTO_LIBS} ${EXTRALIBS})

set_target_properties(rados PROPERTIES VERSION 2.0.0 SOVERSION 1)

install(TARGETS rados DESTINATION lib)
