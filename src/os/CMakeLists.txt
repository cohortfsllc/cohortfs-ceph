
set(libos_srcs
  chain_xattr.cc
  ObjectStore.cc
  Transaction.cc
  Factory.cc
  JournalingObjectStore.cc
  kv/LevelDBStore.cc
  DBObjectMap.cc
  GenericObjectMap.cc
  )
set(os_mon_files
  kv/LevelDBStore.cc
  )
add_library(os_mon_objs OBJECT ${os_mon_files})
add_library(os ${CEPH_SHARED} ${libos_srcs})
target_link_libraries(os PRIVATE leveldb snappy boost_filesystem)
if(${ENABLE_SHARED})
  set_target_properties(os PROPERTIES OUTPUT_NAME ceph-os
    VERSION "1.0.0" SOVERSION "1" LINK_FLAGS "-Wl,-export-dynamic")
endif(${ENABLE_SHARED})
install(TARGETS os DESTINATION lib)

# objectstore modules
add_subdirectory(file)
add_subdirectory(kv)
add_subdirectory(zfs)
add_subdirectory(mem)
