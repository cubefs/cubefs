cmake_minimum_required (VERSION 3.5)
project(blobstore)

option(COVERAGE "enable coverage" OFF)
option(BUILD_TESTS "build tests" ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
IF (COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -fprofile-arcs -ftest-coverage")
ELSE()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g")
ENDIF()
find_package(Seastar REQUIRED)
find_package(Protobuf 3 REQUIRED)
find_package(spdlog REQUIRED)
find_package(PkgConfig)
pkg_check_modules(ISAL REQUIRED libisal)
pkg_check_modules(SPDK spdk_nvme spdk_env_dpdk)
if (SPDK_FOUND)
    pkg_check_modules(OPENSSL REQUIRED openssl)
endif()
message(STATUS "SPDK libs:" SPDK_LIBRARIES)

FILE(GLOB common_srcs ${PROJECT_SOURCE_DIR}/common/*.cc)
FILE(GLOB net_srcs ${PROJECT_SOURCE_DIR}/common/net/*.cc)
FILE(GLOB proto_files ${PROJECT_SOURCE_DIR}/common/proto/*.proto)
FOREACH(proto_file ${proto_files})
    STRING(REGEX REPLACE "[^/]proto" "" proto_file_name ${proto_file})
    LIST(APPEND proto_srcs "${proto_file_name}.pb.cc")
    ADD_CUSTOM_COMMAND(
        OUTPUT "${proto_file_name}.pb.h" "${proto_file_name}.pb.cc"
        COMMAND protoc --proto_path=${PROJECT_SOURCE_DIR}/common/proto --cpp_out=${PROJECT_SOURCE_DIR}/common/proto/ ${proto_file}
        DEPENDS ${proto_file}
    )
ENDFOREACH(proto_file)

include_directories(${PROJECT_SOURCE_DIR})
add_definitions(-DSPDLOG_FMT_EXTERNAL)
if (${SPDK_FOUND})
    add_definitions(-DHAS_SPDK)
endif()

add_library(COMMON_OBJS OBJECT ${common_srcs})
target_link_libraries(COMMON_OBJS Seastar::seastar)

add_library(PROTO_OBJS OBJECT ${proto_srcs})
target_link_libraries(PROTO_OBJS Seastar::seastar)

add_library(NET_OBJS OBJECT ${net_srcs})
target_link_libraries(NET_OBJS Seastar::seastar)

add_subdirectory(blobnode)
add_subdirectory(demos)
if (BUILD_TESTS)
    add_subdirectory(tests)
endif()
