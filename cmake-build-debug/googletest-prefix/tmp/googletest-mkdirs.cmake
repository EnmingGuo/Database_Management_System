# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src/googletest"
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src/googletest-build"
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix"
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/tmp"
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src/googletest-stamp"
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src"
  "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src/googletest-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src/googletest-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/googletest-prefix/src/googletest-stamp${cfgdir}") # cfgdir has leading slash
endif()
