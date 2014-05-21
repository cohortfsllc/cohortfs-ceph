
set(libglobal_srcs
	global_context.cc 
	global_init.cc 
	pidfile.cc 
	signal_handler.cc
)

add_library(global ${libglobal_srcs})


target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})


