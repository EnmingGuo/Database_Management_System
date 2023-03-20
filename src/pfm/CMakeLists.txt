add_library(pfm pfm.cc)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)