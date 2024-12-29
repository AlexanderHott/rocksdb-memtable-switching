cmake_minimum_required(VERSION 3.28)
project(rocksdb_memtable_switching)

add_subdirectory("${PROJECT_SOURCE_DIR}/../rocksdb" "${PROJECT_SOURCE_DIR}/../rocksdb/build")
include_directories("${PROJECT_SOURCE_DIR}/../rocksdb/include")

set(ROCKSDB_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/../rocksdb/include)
set(ROCKSDB_LIB ${PROJECT_SOURCE_DIR}/../rocksdb/build/librocksdb.a)

find_package(cppzmq)

add_executable(main main.cc db_env.cc db_env.h config_options.h
        env.cc
        env.h
        json.hpp
)
target_include_directories(main PRIVATE ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(main PRIVATE ${ROCKSDB_LIB} cppzmq)
