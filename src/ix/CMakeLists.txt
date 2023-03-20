add_library(ix ix.cc)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)