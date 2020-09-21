cmake_minimum_required(VERSION 3.14)
project(rocksdb-robust)
set(CMAKE_CXX_STANDARD 11)

include_directories(extern)

# RocksDB
add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/extern/rocksdb/librocksdb.a)
include_directories(${CMAKE_SOURCE_DIR}/extern/rocksdb/include)


add_executable(exp_robust ${CMAKE_SOURCE_DIR}/experiment/exp_robust.cc)
target_link_libraries(exp_robust rocksdb)
