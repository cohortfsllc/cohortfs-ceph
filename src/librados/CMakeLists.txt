
set(librados_srcs
	../librados/librados.cc 
	../librados/RadosClient.cc
	../librados/IoCtxImpl.cc 
)

include_directories("..")

add_library(librados ${librados_srcs})

add_dependencies(librados cls_lock_static osdc)

target_link_libraries(librados osdc cls_lock_static common ${CRYPTO_LIBS} ${EXTRALIBS})

set_target_properties(librados PROPERTIES VERSION 2.0.0 SOVERSION 2)


