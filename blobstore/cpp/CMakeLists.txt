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
message(STATUS "SPDK libs: " ${SPDK_LIBRARIES})

file(GLOB_RECURSE COMMON_SRC ${PROJECT_SOURCE_DIR}/common/*.cc)

file(GLOB PROTO_FILES ${PROJECT_SOURCE_DIR}/common/proto/*.proto)
set(PROTO_SRC "")

foreach(PROTO_FILE ${PROTO_FILES})
    string(REGEX REPLACE "[^/]proto" "" PROTO_FILE_NAME ${PROTO_FILE})
    list(APPEND PROTO_SRC "${PROTO_FILE_NAME}.pb.cc")
    add_custom_command(
        OUTPUT "${PROTO_FILE_NAME}.pb.h" "${PROTO_FILE_NAME}.pb.cc"
        COMMAND protoc --proto_path=${PROJECT_SOURCE_DIR}/common/proto --cpp_out=${PROJECT_SOURCE_DIR}/common/proto/ ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
    )
endforeach()

include_directories(${PROJECT_SOURCE_DIR})
add_definitions(-DSPDLOG_FMT_EXTERNAL)
if (${SPDK_FOUND})
    add_definitions(-DHAS_SPDK)
endif()

add_library(COMMON_OBJS OBJECT ${COMMON_SRC} ${PROTO_SRC})
target_link_libraries(COMMON_OBJS Seastar::seastar)

add_subdirectory(blobnode)
add_subdirectory(demos)
if (BUILD_TESTS)
    add_subdirectory(tests)
endif()
