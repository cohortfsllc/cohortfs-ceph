set(cls_rgw_srcs
	cls_rgw.cc
	cls_rgw_ops.cc
	cls_rgw_types.cc
	${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc
)
add_library(cls_rgw SHARED ${cls_rgw_srcs})
target_link_libraries(cls_rgw json_spirit)
install(TARGETS cls_rgw DESTINATION "lib/rados-classes")

set(cls_rgw_client_srcs
	cls_rgw_client.cc
	cls_rgw_ops.cc
	cls_rgw_types.cc
)
add_library(cls_rgw_client ${cls_rgw_client_srcs})
