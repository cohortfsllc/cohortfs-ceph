# Just run -my- tests
add_subdirectory(messenger)

if (WITH_FIO)
	include_directories(${FIO_INCLUDE_DIR})
	add_library(fio_ceph_filestore SHARED fio_ceph_filestore.cc)
	target_link_libraries(
	  fio_ceph_filestore os global ${CRYPTO_LIBS} ${EXTRA_LIBS})
	install(TARGETS fio_ceph_filestore DESTINATION lib)

	if (WITH_CDS)
	  add_library(fio_cds_filestore SHARED fio_cds_filestore.cc)
	  target_link_libraries(
	    fio_cds_filestore os global ${CRYPTO_LIBS} ${EXTRA_LIBS})
	  install(TARGETS fio_cds_filestore DESTINATION lib)
	endif (WITH_CDS)

endif (WITH_FIO)

# osbench
add_executable(osbench objectstore_bench.cc)
target_link_libraries(osbench os global)
set_target_properties(osbench PROPERTIES COMPILE_FLAGS "-std=gnu++11")

# mmapbench
add_executable(mmapbench mmap_bench.cc)
target_link_libraries(mmapbench os global)
set_target_properties(mmapbench PROPERTIES COMPILE_FLAGS "-std=gnu++11")

# pagesetbench
add_executable(pagesetbench pageset_bench.cc)
target_link_libraries(pagesetbench os global)
set_target_properties(pagesetbench PROPERTIES COMPILE_FLAGS "-std=gnu++11")
