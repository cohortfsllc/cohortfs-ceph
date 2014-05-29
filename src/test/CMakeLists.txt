if (WITH_FIO)
	include_directories(${FIO_INC_DIR})
	add_library(fio_ceph_filestore SHARED fio_ceph_filestore.cc)
	target_link_libraries(
	  fio_ceph_filestore os global ${CRYPTO_LIBS} ${EXTRA_LIBS})
	install(TARGETS fio_ceph_filestore DESTINATION lib)

	if (WITH_CDS)
	  include_directories(${CDS_INC_DIR})
	  add_library(fio_cds_filestore SHARED fio_cds_filestore.cc)
	  target_link_libraries(
	    fio_cds_filestore os global ${CRYPTO_LIBS} ${EXTRA_LIBS})
	  install(TARGETS fio_cds_filestore DESTINATION lib)
	endif (WITH_CDS)

endif (WITH_FIO)
