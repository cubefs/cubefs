find_package(benchmark QUIET)
if (NOT benchmark_FOUND)
    message(WARNING "Google Benchmark not found. Install it with: git clone https://github.com/google/benchmark.git")
    message(WARNING "Or use: apt-get install libbenchmark-dev (on Ubuntu/Debian)")
endif()

find_package(Boost REQUIRED COMPONENTS unit_test_framework)
enable_testing()

add_executable(trace_test trace_test.cc)
target_link_libraries(trace_test
    PRIVATE
        $<TARGET_OBJECTS:COMMON_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        Seastar::seastar_testing
        Boost::unit_test_framework
)
target_compile_definitions(trace_test PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME TraceTest COMMAND trace_test -- -c 1)

if (benchmark_FOUND)
    add_executable(trace_bench trace_bench.cc)
    target_link_libraries(trace_bench
        PRIVATE
            $<TARGET_OBJECTS:COMMON_OBJS>
            protobuf::libprotobuf
            Seastar::seastar
            benchmark::benchmark
            benchmark::benchmark_main
    )
endif()

add_executable(net_server_router_test net_server_router_test.cc mock_stream.h)
target_link_libraries(net_server_router_test
    PRIVATE
        $<TARGET_OBJECTS:COMMON_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        Seastar::seastar_testing
        Boost::unit_test_framework
        isal
        pthread
)
target_compile_definitions(net_server_router_test PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME NetServerRouterTest COMMAND net_server_router_test -- -c 1)

add_executable(net_rpc_stream_test net_rpc_stream_test.cc mock_stream.h)
target_link_libraries(net_rpc_stream_test
    PRIVATE
        $<TARGET_OBJECTS:COMMON_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        Seastar::seastar_testing
        Boost::unit_test_framework
        isal
        pthread
)
target_compile_definitions(net_rpc_stream_test PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME NetRpcStreamTest COMMAND net_rpc_stream_test -- -c 1)
