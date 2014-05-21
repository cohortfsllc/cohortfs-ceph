set(cls_user_srcs
	cls_user.cc
)
add_library(cls_user SHARED ${cls_user_srcs})
install(TARGETS cls_user DESTINATION "lib/rados-classes")

set(cls_user_client_srcs
	cls_user_client.cc
	cls_user_ops.cc
	cls_user_types.cc
)
add_library(cls_user_client ${cls_user_client_srcs})
