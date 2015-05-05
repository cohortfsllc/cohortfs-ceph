#os zfstore
set(os_zfstore_files
  ZFStore.cc
  FragTreeIndex.cc
)

add_library(os_zfstore_objs OBJECT ${os_zfstore_files})
add_library(os_zfstore SHARED $<TARGET_OBJECTS:os_zfstore_objs>)
target_link_libraries(os_zfstore common os ${ZFS_WRAP})
set_target_properties(os_zfstore PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS os_zfstore DESTINATION lib/os-modules)
