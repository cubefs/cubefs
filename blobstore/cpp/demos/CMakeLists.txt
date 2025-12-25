add_executable (rpc_server_demo rpc_server_demo.cc)
target_link_libraries(rpc_server_demo $<TARGET_OBJECTS:COMMON_OBJS> protobuf::libprotobuf isal Seastar::seastar)

add_executable (rpc_client_demo rpc_client_demo.cc)
target_link_libraries(rpc_client_demo $<TARGET_OBJECTS:COMMON_OBJS> protobuf::libprotobuf isal Seastar::seastar)
