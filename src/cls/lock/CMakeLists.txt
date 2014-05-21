set(cls_lock_srcs
	cls_lock.cc
)
add_library(cls_lock SHARED ${cls_lock_srcs})
install(TARGETS cls_lock DESTINATION "lib/rados-classes")

set(cls_lock_client_srcs
	cls_lock_client.cc  
	cls_lock_types.cc
	cls_lock_ops.cc
)
add_library(cls_lock_client ${cls_lock_client_srcs})
