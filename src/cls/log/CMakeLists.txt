set(cls_log_srcs
	cls_log.cc
)
add_library(cls_log SHARED ${cls_log_srcs})
install(TARGETS cls_log DESTINATION "lib/rados-classes")

set(cls_log_client_srcs
	cls_log_client.cc
)
add_library(cls_log_client ${cls_log_client_srcs})
