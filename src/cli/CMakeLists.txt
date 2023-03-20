add_library(cli cli.cc)

add_dependencies(qe ix rm googlelog )
target_link_libraries(cli qe ix rm glog readline)