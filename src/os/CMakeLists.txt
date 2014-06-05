set(libos_srcs
	FileJournal.cc 
	FileStore.cc 
	chain_xattr.cc 
	ObjectStore.cc 
	JournalingObjectStore.cc
	KeyValueStore.cc
	MemStore.cc
	LFNIndex.cc 
	HashIndex.cc 
	IndexManager.cc 
	FlatIndex.cc
	GenericObjectMap.cc
	DBObjectMap.cc 
	LevelDBStore.cc 
	WBThrottle.cc
	GenericFileStoreBackend.cc
	BtrfsFileStoreBackend.cc
	ZFSFileStoreBackend.cc
)
if (WITH_CDS)
	list(APPEND libos_srcs CDSMemStore.cc)
endif (WITH_CDS)

add_library(os ${libos_srcs})

if(${HAVE_LIBAIO})
target_link_libraries(os aio)
endif(${HAVE_LIBAIO})

target_link_libraries(os leveldb snappy)
