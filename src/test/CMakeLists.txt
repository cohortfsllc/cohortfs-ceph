# Just run -my- tests
add_subdirectory(messenger)

if (WITH_FIO)
	include_directories(${FIO_INC_DIR})
	add_library(fio_ceph_filestore SHARED fio_ceph_filestore.cc)
	target_link_libraries(fio_ceph_filestore os global ${CRYPTO_LIBS} ${EXTRA_LIBS})
	install(TARGETS fio_ceph_filestore DESTINATION lib)
endif (WITH_FIO)

# osbench
add_executable(osbench objectstore_bench.cc)
target_link_libraries(osbench os global)
set_target_properties(osbench PROPERTIES COMPILE_FLAGS "-std=gnu++11")
