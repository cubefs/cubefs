generate_proto_files(DEMO_PROTO_FILE  ${CMAKE_CURRENT_SOURCE_DIR}/proto)

# Create demo proto library
add_library(DEMO_PROTO_OBJS OBJECT ${DEMO_PROTO_FILE})
target_link_libraries(DEMO_PROTO_OBJS protobuf::libprotobuf Seastar::seastar)
target_include_directories(DEMO_PROTO_OBJS PUBLIC ${PROJECT_SOURCE_DIR})

add_executable(rpc_server_demo rpc_server_demo.cc)
target_link_libraries(rpc_server_demo
    $<TARGET_OBJECTS:COMMON_OBJS>
    protobuf::libprotobuf
    $<TARGET_OBJECTS:DEMO_PROTO_OBJS>
    isal
    Seastar::seastar
)

add_executable(rpc_client_demo rpc_client_demo.cc)
target_link_libraries(rpc_client_demo
    $<TARGET_OBJECTS:COMMON_OBJS>
    protobuf::libprotobuf
    $<TARGET_OBJECTS:DEMO_PROTO_OBJS>
    isal
    Seastar::seastar
)
