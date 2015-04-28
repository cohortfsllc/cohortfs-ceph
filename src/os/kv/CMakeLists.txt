#os kvstore
set(os_kvstore_files
  KeyValueStore.cc
)

add_library(os_kvstore_objs OBJECT ${os_kvstore_files})
add_library(os_kvstore SHARED $<TARGET_OBJECTS:os_kvstore_objs>)
target_link_libraries(os_kvstore common os)
set_target_properties(os_kvstore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_kvstore DESTINATION lib/os-modules)
