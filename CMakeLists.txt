cmake_minimum_required(VERSION 2.9)
project(PeterDB)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_COMPILER "g++")

if (CMAKE_BUILD_TYPE MATCHES Debug)
    add_definitions(-DDEBUG=1)
endif ()

set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}")

include(ExternalProject)

ExternalProject_Add(googletest
        GIT_REPOSITORY    https://github.com/google/googletest.git
        GIT_TAG           release-1.11.0
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}
        )

ExternalProject_Add(googlelog
        GIT_REPOSITORY https://github.com/google/glog.git
        GIT_TAG           v0.5.0
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DWITH_GFLAGS=OFF
        )

include_directories("${CMAKE_BINARY_DIR}/include")
link_directories("${CMAKE_BINARY_DIR}/lib")

add_subdirectory(src)

option(PACKAGE_TESTS "Build the tests" ON)
if (PACKAGE_TESTS)
    enable_testing()
    include(GoogleTest)
    add_subdirectory(test)
endif ()
