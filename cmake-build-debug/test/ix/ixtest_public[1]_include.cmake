if(EXISTS "/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/test/ix/ixtest_public[1]_tests.cmake")
  include("/Users/guoemmett/Desktop/cs222-winter23-cutecutelovelydoggy/cmake-build-debug/test/ix/ixtest_public[1]_tests.cmake")
else()
  add_test(ixtest_public_NOT_BUILT ixtest_public_NOT_BUILT)
endif()
