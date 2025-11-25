generate_proto_files(BLOBNODE_PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/proto)

file(GLOB_RECURSE BLOBNODE_SRC CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
list(REMOVE_ITEM BLOBNODE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/main.cc")
list(FILTER BLOBNODE_SRC EXCLUDE REGEX ".*\\.pb\\.cc$")

add_library(BLOBNODE_OBJS OBJECT ${BLOBNODE_SRC} ${BLOBNODE_PROTO_FILES})
target_link_libraries(BLOBNODE_OBJS protobuf::libprotobuf Seastar::seastar)

add_executable(blobnode main.cc)
target_link_libraries(blobnode $<TARGET_OBJECTS:COMMON_OBJS> $<TARGET_OBJECTS:BLOBNODE_OBJS> protobuf::libprotobuf ${SPDK_LIBRARIES} ${ISAL_LIBRARIES} ${OPENSSL_LIBRARIES} numa uuid cares Seastar::seastar)

