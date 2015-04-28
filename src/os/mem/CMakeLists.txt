#os memstore
set(os_memstore_files
  MemStore.cc
)

add_library(os_memstore_objs OBJECT ${os_memstore_files})
add_library(os_memstore SHARED $<TARGET_OBJECTS:os_memstore_objs>)
target_link_libraries(os_memstore common os)
set_target_properties(os_memstore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_memstore DESTINATION lib/os-modules)
