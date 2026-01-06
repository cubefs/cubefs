find_package(benchmark QUIET)
if (NOT benchmark_FOUND)
    message(WARNING "Google Benchmark not found. Install it with: git clone https://github.com/google/benchmark.git")
    message(WARNING "Or use: apt-get install libbenchmark-dev (on Ubuntu/Debian)")
endif()

find_package(Boost REQUIRED COMPONENTS unit_test_framework)
enable_testing()

add_executable(test_common common_test.cc)
target_link_libraries(test_common
    PRIVATE
        $<TARGET_OBJECTS:COMMON_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        Seastar::seastar_testing
        Boost::unit_test_framework
)
target_compile_definitions(test_common PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME CommonTest COMMAND test_common -- -c 1)

add_executable(test_common_trace common_trace_test.cc)
target_link_libraries(test_common_trace
    PRIVATE
        $<TARGET_OBJECTS:COMMON_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        Seastar::seastar_testing
        Boost::unit_test_framework
)
target_compile_definitions(test_common_trace PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME CommonTraceTest COMMAND test_common_trace -- -c 1)

if (benchmark_FOUND)
    add_executable(bench_common_trace common_trace_bench.cc)
    target_link_libraries(bench_common_trace
        PRIVATE
            $<TARGET_OBJECTS:COMMON_OBJS>
            protobuf::libprotobuf
            Seastar::seastar
            benchmark::benchmark
            benchmark::benchmark_main
    )

    add_executable(store_allocator_bench store_allocator_bench.cc)
    target_link_libraries(store_allocator_bench
        PRIVATE
            $<TARGET_OBJECTS:COMMON_OBJS>
            $<TARGET_OBJECTS:BLOBNODE_OBJS>
            protobuf::libprotobuf
            Seastar::seastar
            isal
            benchmark::benchmark
            benchmark::benchmark_main
    )
endif()

add_executable(net_server_router_test net_server_router_test.cc net_mock_stream.h)
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

add_executable(net_rpc_stream_test net_rpc_stream_test.cc net_mock_stream.h)
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

add_executable(test_store_journal store_journal_test.cc)
target_link_libraries(test_store_journal
        $<TARGET_OBJECTS:COMMON_OBJS>
        $<TARGET_OBJECTS:BLOBNODE_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        isal
        Seastar::seastar_testing
        Boost::unit_test_framework
)
target_compile_definitions(test_store_journal PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME StoreJournalTest COMMAND test_store_journal -- -c 1)


add_executable(test_store_allocator store_allocator_test.cc)
target_link_libraries(test_store_allocator
        $<TARGET_OBJECTS:COMMON_OBJS>
        $<TARGET_OBJECTS:BLOBNODE_OBJS>
        protobuf::libprotobuf
        Seastar::seastar
        isal
        Seastar::seastar_testing
        Boost::unit_test_framework
)
target_compile_definitions(test_store_allocator PRIVATE SEASTAR_TESTING_MAIN)
add_test(NAME StoreAllocatorTest COMMAND test_store_allocator -- -c 1)
