# - Find LevelDB
#
# LEVELDB_INCLUDE - Where to find leveldb/db.h
# LEVELDB_LIBS - List of libraries when using LevelDB.
# LEVELDB_FOUND - True if LevelDB found.

get_filename_component(module_file_path ${CMAKE_CURRENT_LIST_FILE} PATH)

# Look for the header file.
#	2 find_paths: search order w/o NO_DEFAULT_PATH not good.
#	NO_SYSTEM_ENVIRONMENT_PATH -- don't look in $PATH or $LIB .
#	( set elsewhere: CMAKE_FIND_NO_INSTALL_PREFIX -- don't look where we install. )
if(DEFINED "ENV{LEVELDB_ROOT}")
find_path(LEVELDB_INCLUDE NAMES leveldb/db.h PATHS $ENV{LEVELDB_ROOT}/include DOC "Path in which the file leveldb/db.h is located." NO_DEFAULT_PATH )
endif()
find_path(LEVELDB_INCLUDE NAMES leveldb/db.h PATHS /opt/local/include DOC "Path in which the file leveldb/db.h is located." NO_DEFAULT_PATH )
find_path(LEVELDB_INCLUDE NAMES leveldb/db.h NO_SYSTEM_ENVIRONMENT_PATH )
mark_as_advanced(LEVELDB_INCLUDE)

# Look for the library.
# Does this work on UNIX systems? (LINUX)
if(DEFINED "ENV{LEVELDB_ROOT}")
find_library(LEVELDB_LIBS NAMES leveldb PATHS $ENV{LEVELDB_ROOT}/lib DOC "Path to leveldb library." NO_DEFAULT_PATH )
endif()
find_library(LEVELDB_LIBS NAMES leveldb DOC "Path to leveldb library." )
mark_as_advanced(LEVELDB_LIBS)

# Copy the results to the output variables.
if (LEVELDB_INCLUDE AND LEVELDB_LIBS)
  message(STATUS "Found leveldb in ${LEVELDB_INCLUDE} ${LEVELDB_LIBS}")
  set(LEVELDB_FOUND 1)
  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_LIBRARY ${LEVELDB_LIBS} pthread)
  set(CMAKE_REQUIRED_INCLUDES ${LEVELDB_INCLUDE})
 else ()
   set(LEVELDB_FOUND 0)
 endif ()

 # Report the results.
 if (NOT LEVELDB_FOUND)
   set(LEVELDB_DIR_MESSAGE "LEVELDB was not found. Make sure LEVELDB_LIBS and LEVELDB_INCLUDE are set.")
   if (LEVELDB_FIND_REQUIRED)
     message(FATAL_ERROR "${LEVELDB_DIR_MESSAGE}")
   elseif (NOT LEVELDB_FIND_QUIETLY)
     message(STATUS "${LEVELDB_DIR_MESSAGE}")
   endif ()
 endif ()
