
#PYTHON_ADD_MODULE(pyrados rados.cc)

# Define the wrapper library that wraps our library
add_library( pyrados SHARED rados.cc )
target_link_libraries( pyrados ${Boost_LIBRARIES} librados global )
# don't prepend wrapper library name with lib
set_target_properties( pyrados PROPERTIES PREFIX "" )
install(TARGETS pyrados DESTINATION ${PYTHON_INSTDIR})
