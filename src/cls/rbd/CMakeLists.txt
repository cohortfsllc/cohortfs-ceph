set(cls_rbd_srcs
	cls_rbd.cc
)
add_library(cls_rbd SHARED ${cls_rbd_srcs})
install(TARGETS cls_rbd DESTINATION "lib/rados-classes")

set(cls_rbd_client_srcs
	cls_rbd_client.cc
)
add_library(cls_rbd_client ${cls_rbd_client_srcs})
