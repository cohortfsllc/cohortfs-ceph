
set(libglobal_srcs
	global_context.cc 
	global_init.cc 
	pidfile.cc 
	signal_handler.cc
)

add_library(global ${libglobal_srcs})
if (WITH_CDS)
	target_link_libraries(global ${CDS_LIBS})
endif (WITH_CDS)


target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})


