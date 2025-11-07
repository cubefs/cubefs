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
            Seastar::seastar
            benchmark::benchmark
            benchmark::benchmark_main
    )
endif()
