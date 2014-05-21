set(cls_version_srcs
	cls_version.cc
)
add_library(cls_version SHARED ${cls_version_srcs})
install(TARGETS cls_version DESTINATION "lib/rados-classes")

set(cls_version_client_srcs
	cls_version_client.cc
	cls_version_types.cc
)
add_library(cls_version_client ${cls_version_client_srcs})
