# - Find fio
#
# FIO_INC_DIR - where to find fio.h
# FIO_FOUND - True if found.

message(STATUS "fio prefix is ${FIO_PREFIX}")

find_path(FIO_INC_DIR
  NAMES
  fio.h
  PATHS
  /usr/include
  ${FIO_PREFIX}
  DOC "Path to fio."
)

if (FIO_INC_DIR)
  set(FIO_FOUND TRUE)
else ()
  set(FIO_INC_DIR "")
  set(FIO_FOUND FALSE)
endif ()

if (FIO_FOUND)
  message(STATUS "Found fio: ${FIO_INC_DIR}")
else ()
  message(STATUS "Failed to find fio.h")
  if (FIO_FIND_REQUIRED)
    message(FATAL_ERROR "Missing required fio.h")
  endif ()
endif ()

mark_as_advanced(
  FIO_INC_DIR
  )
