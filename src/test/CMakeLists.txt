# Just run -my- tests
add_subdirectory(messenger)

if (WITH_FIO)
	include_directories(${FIO_INCLUDE_DIR})
	add_library(fio_ceph_filestore SHARED fio_ceph_filestore.cc)
	target_link_libraries(fio_ceph_filestore os global ${CRYPTO_LIBS} ${EXTRA_LIBS})
	install(TARGETS fio_ceph_filestore DESTINATION lib)
endif (WITH_FIO)
