set(cls_statelog_srcs
	cls_statelog.cc
)
add_library(cls_statelog SHARED ${cls_statelog_srcs})
install(TARGETS cls_statelog DESTINATION "lib/rados-classes")

set(cls_statelog_client_srcs
	cls_statelog_client.cc
)
add_library(cls_statelog_client ${cls_statelog_client_srcs})
