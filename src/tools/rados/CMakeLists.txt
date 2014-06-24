set(radostool_srcs
	rados.cc 
	rados_import.cc 
	rados_export.cc 
	rados_sync.cc 
	${CMAKE_SOURCE_DIR}/src/common/obj_bencher.cc
)
add_executable(radostool ${radostool_srcs})
target_link_libraries(radostool cls_lock_client rados global)
set_target_properties(radostool PROPERTIES OUTPUT_NAME rados)

install(TARGETS radostool DESTINATION bin)

