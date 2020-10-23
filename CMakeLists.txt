cmake_minimum_required(VERSION 3.17)
project(rocksdb-robust)
set(CMAKE_CXX_STANDARD 11)

# =========================================================================================================
# Compile Options
# =========================================================================================================
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options("-Wall" "-Wextra" "-Werror")

option(DEBUG "Debug mode on or off" OFF)
if(${DEBUG})
    message("DEBUG: Compiled with debug flags")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG")
endif()


# =========================================================================================================
# RocksDB
# =========================================================================================================
include_directories(extern)
execute_process(COMMAND git submodule update --init --recursive
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

if(NOT EXISTS ${CMAKE_SOURCE_DIR}/extern/rocksdb/librocksdb.a)
    # message( FATAL_ERROR "RocksDB not compiled. Please go to extern/rocksdb and run `make static_lib`" )
    execute_process(COMMAND make DISABLE_WARNINGS_AS_ERROR=True -j ${ROCKSDB_BUILD_THREADS} static_lib
                    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/rocksdb)
endif()

add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/extern/rocksdb/librocksdb.a)
include_directories(${CMAKE_SOURCE_DIR}/extern/rocksdb/include)


# =========================================================================================================
# Grab required packages
# =========================================================================================================
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(LZ4 REQUIRED)


# =========================================================================================================
# module source files
# =========================================================================================================
include_directories(${CMAKE_SOURCE_DIR}/src)

set(monkey_src
    ${CMAKE_SOURCE_DIR}/src/monkey/monkey_filter_policy.cpp
)
add_library(monkey ${monkey_src})

# set(common_src
# )
# add_library(common ${common_src})


# =========================================================================================================
# Tools
# =========================================================================================================
set(tools_infra_src
    ${CMAKE_SOURCE_DIR}/tools/infrastructure/bulk_loader.cpp
    ${CMAKE_SOURCE_DIR}/tools/infrastructure/data_generator.cpp
)
add_library(tools ${tools_infra_src})

# Tools executables
add_executable(db_builder ${CMAKE_SOURCE_DIR}/tools/db_builder.cpp)
target_link_libraries(db_builder rocksdb monkey tools ZLIB::ZLIB BZip2::BZip2 LZ4::LZ4)

add_executable(db_execute ${CMAKE_SOURCE_DIR}/tools/db_execute.cpp)
target_link_libraries(db_execute rocksdb monkey tools ZLIB::ZLIB BZip2::BZip2 LZ4::LZ4)
