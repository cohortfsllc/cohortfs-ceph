
set(LIBEDIT_LIBS edit)

set(libclient_srcs
	barrier.cc
	Capability.cc
	Client.cc 
	ClientSnapRealm.cc 
	Dentry.cc 
	DirRegMap.cc
	DirStripe.cc
	Inode.cc 
	InodeCache.cc
	MDSRegMap.cc
	MetaRequest.cc 
	MetaSession.cc 
	ReaddirWorker.cc 
	SnapRealmMap.cc
	Trace.cc
)

add_library(client ${libclient_srcs})

target_link_libraries(client osdc ${LIBEDIT_LIBS} ${CITYHASH_LIBRARIES})

# libcephfs
if (WITH_LIBCEPHFS)
	set(libcephfs_srcs ../libcephfs.cc)
	add_library(cephfs SHARED ${libcephfs_srcs})
	target_link_libraries(cephfs client global)
	install(TARGETS cephfs DESTINATION lib)
	install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/include/cephfs" DESTINATION include)
endif (WITH_LIBCEPHFS)
