# os filestore
set(os_filestore_files
  FileJournal.cc
  FileStore.cc
  GenericFileStoreBackend.cc
  BtrfsFileStoreBackend.cc
  ZFSFileStoreBackend.cc
  FragTreeIndex.cc
)

add_library(os_filestore_objs OBJECT ${os_filestore_files})
add_library(os_filestore SHARED $<TARGET_OBJECTS:os_filestore_objs>)
if(${HAVE_LIBAIO})
  target_link_libraries(os_filestore aio)
endif(${HAVE_LIBAIO})
target_link_libraries(os_filestore common os)
set_target_properties(os_filestore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_filestore DESTINATION lib/os-modules)
