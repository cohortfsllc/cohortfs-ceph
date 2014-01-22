set(LIBEDIT_LIBS edit)

set(no_fun_client_files
	../client/barrier.cc
)

set(libclient_srcs
	../client/Client.cc 
	../client/Inode.cc 
	../client/Dentry.cc 
	../client/MetaRequest.cc 
	../client/ClientSnapRealm.cc 
	../client/MetaSession.cc 
	../client/Trace.cc
# ${no_fun_client_files}
)

include_directories("..")

add_library(client ${libclient_srcs})

target_link_libraries(client osdc ${LIBEDIT_LIBS})
