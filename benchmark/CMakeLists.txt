cmake_minimum_required(VERSION 3.28)
project(rocksdb_memtable_switching)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")

#add_subdirectory("${PROJECT_SOURCE_DIR}/../rocksdb" "${PROJECT_SOURCE_DIR}/../rocksdb/build")
#include_directories("${PROJECT_SOURCE_DIR}/../rocksdb/include")
include_directories("${PROJECT_SOURCE_DIR}/../third_party")
add_subdirectory("${PROJECT_SOURCE_DIR}/../rocksdb" "${CMAKE_CURRENT_BINARY_DIR}/rocksdb")

#set(ROCKSDB_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/../rocksdb/include)
#set(ROCKSDB_LIB ${PROJECT_SOURCE_DIR}/../rocksdb/build/librocksdb.a)

find_package(cppzmq REQUIRED)

add_executable(main
        main.cc
        cfg.cc
        cfg.h
)
target_compile_options(main PRIVATE -Wall -Wextra -O2)
target_link_libraries(main PRIVATE rocksdb cppzmq)
