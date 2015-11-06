cmake_minimum_required(VERSION 3.3)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-Wall -std=c++11")

find_package(LibBSON REQUIRED)
include_directories(${LIBBSON_INCLUDE_DIR})

add_executable(smalldata
               src/main.cpp
               src/Log.cpp
               src/Users.cpp
               src/Terms.cpp
               src/DistinctiveTerms.cpp)
target_link_libraries(smalldata ${LIBBSON_LIBRARIES})
