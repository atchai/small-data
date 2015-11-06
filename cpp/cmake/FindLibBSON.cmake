# libbson
#  LIBBSON_FOUND
#  LIBBSON_INCLUDE_DIRS
#  LIBBSON_LIBRARIES
#  LIBBSON_DEFINITIONS

find_package(PkgConfig)
pkg_check_modules(PC_LIBBSON QUIET libbson)
set(LIBBSON_DEFINITIONS ${PC_LIBBSON_CFLAGS_OTHER})

find_path(LIBBSON_INCLUDE_DIR bson.h
    HINTS ${PC_LIBBSON_INCLUDEDIR} ${PC_LIBBSON_INCLUDE_DIRS}
    PATH_SUFFIXES libbson-1.0)

find_library(LIBBSON_LIBRARY NAMES libbson-1.0.dylib
    HINTS ${PC_LIBBSON_LIBDIR} ${PC_LIBBSON_LIBRARY_DIRS})

set(LIBBSON_LIBRARIES ${LIBBSON_LIBRARY})
set(LIBBSON_INCLUDE_DIRS ${LIBBSON_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibBSON DEFAULT_MSG
    LIBBSON_LIBRARY LIBBSON_INCLUDE_DIR)

mark_as_advanced(LIBBSON_INCLUDE_DIR LIBBSON_LIBRARY)