set(rbd_srcs
	librbd.cc
	AioCompletion.cc
	AioRequest.cc
	ImageCtx.cc
	internal.cc
	LibrbdWriteback.cc
	WatchCtx.cc
)
add_library(rbd SHARED ${rbd_srcs})
target_link_libraries(rbd rados osdc cls_rbd_client cls_lock_client ${EXTRALIBS})
set_target_properties(rbd PROPERTIES VERSION 0.1.8 SOVERSION 1)
install(TARGETS rbd DESTINATION lib)
install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/include/rbd" DESTINATION include)
