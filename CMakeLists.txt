cmake_minimum_required(VERSION 3.16)
project(rocksdb-robust)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
message("C : ${CMAKE_C_COMPILER}")
message("CXX : ${CMAKE_CXX_COMPILER}")

# =====================================================================================================================
# Compile Options
# =====================================================================================================================
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Add our warnings
add_compile_options("-Wall" "-Wextra")

add_compile_options("-fstack-protector-strong"
                    "-fexceptions"
                    "-fasynchronous-unwind-tables"
                    "-ftrapv")

option(DEBUG "Debug mode on or off" OFF)
if(${DEBUG})
    message("DEBUG: Compiled with debug flags")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG")
    add_compile_options("-O0" "-g3" "-ggdb")
else()
    add_compile_options("-O2")
endif()


# =====================================================================================================================
# RocksDB
# =====================================================================================================================
include_directories(extern)

# if(NOT EXISTS ${CMAKE_SOURCE_DIR}/extern/rocksdb/librocksdb.a)
#     message( FATAL_ERROR "RocksDB not compiled. Please go to extern/rocksdb and run `make static_lib`" )
#     execute_process(COMMAND make DISABLE_WARNINGS_AS_ERROR=True -j ${ROCKSDB_BUILD_THREADS} static_lib
#                    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/extern/rocksdb)
# endif()

add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/extern/rocksdb/librocksdb.a)
include_directories(${CMAKE_SOURCE_DIR}/extern/rocksdb/include)


# =====================================================================================================================
# Grab required packages
# =====================================================================================================================
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Threads REQUIRED)
find_package(zstd REQUIRED)
find_package(snappy REQUIRED)
target_link_libraries(rocksdb
    INTERFACE BZip2::BZip2 LZ4::LZ4 Threads::Threads ZLIB::ZLIB zstd::zstd snappy::snappy ${CMAKE_DL_LIBS})


# =====================================================================================================================
# module source files
# =====================================================================================================================
include_directories(${CMAKE_SOURCE_DIR}/src)

file(GLOB_RECURSE
        tmpdb_src
        CONFIGURE_DEPENDS
        ${CMAKE_SOURCE_DIR}/src/tmpdb/*.cpp
        ${CMAKE_SOURCE_DIR}/src/tmpdb/*.hpp
)
add_library(tmpdb ${tmpdb_src})
target_link_libraries(tmpdb INTERFACE rocksdb)


# =====================================================================================================================
# Tools
# =====================================================================================================================

file(GLOB_RECURSE
        tools_src
        CONFIGURE_DEPENDS
        ${CMAKE_SOURCE_DIR}/tools/infrastructure/*.cpp
        ${CMAKE_SOURCE_DIR}/tools/infrastructure/*.hpp
)
add_library(tools ${tools_src})

# Tools executables
add_executable(db_builder ${CMAKE_SOURCE_DIR}/tools/db_builder.cpp)
target_link_libraries(db_builder tmpdb tools) 

add_executable(db_execute ${CMAKE_SOURCE_DIR}/tools/db_execute.cpp)
target_link_libraries(db_execute tmpdb tools)
