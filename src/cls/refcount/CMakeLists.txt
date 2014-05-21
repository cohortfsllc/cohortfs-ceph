set(cls_refcount_srcs
	cls_refcount.cc
	cls_refcount_ops.cc
	${CMAKE_SOURCE_DIR}/src/common/ceph_json.cc
)

add_library(cls_refcount SHARED ${cls_refcount_srcs})
target_link_libraries(cls_refcount json_spirit)
install(TARGETS cls_refcount DESTINATION "lib/rados-classes")

set(cls_refcount_client_srcs
	cls_refcount_client.cc
	cls_refcount_ops.cc
)
add_library(cls_refcount_client ${cls_refcount_client_srcs})
