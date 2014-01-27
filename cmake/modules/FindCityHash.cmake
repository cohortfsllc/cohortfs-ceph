# Try to find CityHash
# Once done, this will define
#
# CITYHASH_FOUND - system has CityHash
# CITYHASH_INCLUDE_DIR - the CityHash include directories
# CITYHASH_LIBRARIES - link these to use CityHash
 
if(CITYHASH_INCLUDE_DIR AND CITYHASH_LIBRARIES)
set(CITYHASH_FIND_QUIETLY TRUE)
endif(CITYHASH_INCLUDE_DIR AND CITYHASH_LIBRARIES)
 
#INCLUDE(CheckCXXSymbolExists)
 
# include dir
find_path(CITYHASH_INCLUDE_DIR city.h)
 
# finally the library itself
find_library(LIBCITYHASH NAMES cityhash)
set(CITYHASH_LIBRARIES ${LIBCITYHASH})
 
# handle the QUIETLY and REQUIRED arguments and set CITYHASH_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CITYHASH DEFAULT_MSG CITYHASH_LIBRARIES CITYHASH_INCLUDE_DIR)
 
mark_as_advanced(CITYHASH_LIBRARIES CITYHASH_INCLUDE_DIR)
