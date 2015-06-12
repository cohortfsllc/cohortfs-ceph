# - Find mcas
#
#  MCAS_INCLUDE_DIRS   - where to find mcas/mcas.h, etc.
#  MCAS_LIBRARIES      - List of libraries when using mcas.
#  MCAS_FOUND          - True if mcas found.

find_path(MCAS_INCLUDE_DIR NAMES mcas/mcas.h)
mark_as_advanced(MCAS_INCLUDE_DIR)

find_library(MCAS_LIBRARY NAMES mcas)
mark_as_advanced(MCAS_LIBRARY)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(mcas
                                  REQUIRED_VARS MCAS_LIBRARY MCAS_INCLUDE_DIR)

if(mcas_FOUND)
  set(MCAS_FOUND 1)
endif()
if(MCAS_FOUND)
  set(MCAS_LIBRARIES ${MCAS_LIBRARY})
  set(MCAS_INCLUDE_DIRS ${MCAS_INCLUDE_DIR})
endif()
