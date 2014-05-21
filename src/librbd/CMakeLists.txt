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
add_dependencies(rbd rados osdc)
target_link_libraries(rbd rados osdc cls_rbd ${EXTRALIBS})
set_target_properties(rbd PROPERTIES VERSION 0.1.8 SOVERSION 1)
install(TARGETS rbd DESTINATION lib)
