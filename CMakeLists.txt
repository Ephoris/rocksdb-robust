cmake_minimum_required(VERSION 3.14)
project(rocksdb-robust)
set(CMAKE_CXX_STANDARD 11)

include_directories(extern)

if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
# Update submodules as needed
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

# RocksDB
add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/extern/rocksdb/librocksdb.a)
include_directories(${CMAKE_SOURCE_DIR}/extern/rocksdb/include)


add_executable(exp_robust ${CMAKE_SOURCE_DIR}/experiment/exp_robust.cc)
target_link_libraries(exp_robust rocksdb)
