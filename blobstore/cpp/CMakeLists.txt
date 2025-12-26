cmake_minimum_required (VERSION 3.5)
project(blobstore)

option(COVERAGE "enable coverage" OFF)
option(BUILD_TESTS "build tests" ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -fprofile-arcs -ftest-coverage")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g")
endif()
find_package(Seastar REQUIRED)
find_package(Protobuf 3 REQUIRED)
find_package(spdlog REQUIRED)
find_package(PkgConfig)
pkg_check_modules(ISAL REQUIRED libisal)
pkg_check_modules(SPDK spdk_nvme spdk_env_dpdk)
if(SPDK_FOUND)
    pkg_check_modules(OPENSSL REQUIRED openssl)
    message(STATUS "SPDK libs: " ${SPDK_LIBRARIES})
endif()


# Function to generate proto files for a directory
# Usage: generate_proto_files(<output_var> <proto_dir>)
function(generate_proto_files output_var proto_dir)
    set(generated_files "")

    file(GLOB PROTO_FILES_IN_DIR ${proto_dir}/*.proto)
    message(STATUS "PROTO_DIR: ${proto_dir}, FILES: ${PROTO_FILES_IN_DIR}")

    foreach(proto_file ${PROTO_FILES_IN_DIR})
        get_filename_component(proto_name ${proto_file} NAME_WE)
        set(proto_cc "${proto_dir}/${proto_name}.pb.cc")
        set(proto_h "${proto_dir}/${proto_name}.pb.h")

        add_custom_command(
            OUTPUT ${proto_h} ${proto_cc}
            COMMAND protoc --proto_path=${proto_dir} --cpp_out=${proto_dir}/ ${proto_file}
            DEPENDS ${proto_file}
            COMMENT "Generating protobuf files from ${proto_dir}/${proto_name}.proto"
        )
        list(APPEND generated_files ${proto_cc})
    endforeach()

    set(${output_var} ${generated_files} PARENT_SCOPE)
endfunction()


generate_proto_files(COMMON_PROTO_FILES ${PROJECT_SOURCE_DIR}/common/proto)

file(GLOB_RECURSE COMMON_SRC ${PROJECT_SOURCE_DIR}/common/*.cc)
# Exclude existing .pb.cc files from COMMON_SRC to avoid duplicates
list(FILTER COMMON_SRC EXCLUDE REGEX ".*\\.pb\\.cc$")

include_directories(${PROJECT_SOURCE_DIR})
add_definitions(-DSPDLOG_FMT_EXTERNAL)

# TODO
# if (SPDK_FOUND)
if (SPDK_FOUND_NOT)
    add_definitions(-DHAS_SPDK)
endif()

add_library(COMMON_OBJS OBJECT ${COMMON_SRC} ${COMMON_PROTO_FILES})
target_link_libraries(COMMON_OBJS protobuf::libprotobuf Seastar::seastar)

add_subdirectory(blobnode)
add_subdirectory(demos)
if (BUILD_TESTS)
    add_subdirectory(tests)
endif()
