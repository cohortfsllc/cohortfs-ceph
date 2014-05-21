set(cls_replica_log_srcs
	cls_replica_log.cc
)
add_library(cls_replica_log SHARED ${cls_replica_log_srcs})
install(TARGETS cls_replica_log DESTINATION "lib/rados-classes")

set(cls_replica_log_client_srcs
	cls_replica_log_client.cc
	cls_replica_log_ops.cc
	cls_replica_log_types.cc
)
add_library(cls_replica_log_client ${cls_replica_log_client_srcs})
