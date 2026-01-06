#include <benchmark/benchmark.h>

#include <chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <string>
#include <vector>

#include "common/trace.h"

namespace blobstore {

// Benchmark for Trace construction
static void BM_TraceConstruction(benchmark::State& state) {
    for (auto _ : state) {
        Trace trace;
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceConstruction);

// Benchmark for Trace construction with custom trace_id
static void BM_TraceConstructionWithID(benchmark::State& state) {
    std::string trace_id = "test-trace-id-12345";
    for (auto _ : state) {
        Trace trace(trace_id);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceConstructionWithID);

// Benchmark for TraceString() access
static void BM_TraceStringAccess(benchmark::State& state) {
    Trace trace;
    trace.Append("op1", std::chrono::microseconds(100));
    trace.Append("op2", std::chrono::microseconds(200));
    trace.Append("key1", "value1");

    for (auto _ : state) {
        const std::string& str = trace.TraceString();
        const char* ptr = str.data();
        benchmark::DoNotOptimize(ptr);
    }
}
BENCHMARK(BM_TraceStringAccess);

// Benchmark for TraceID() access
static void BM_TraceIDAccess(benchmark::State& state) {
    Trace trace;
    for (auto _ : state) {
        const std::string& id = trace.TraceID();
        const char* ptr = id.data();
        benchmark::DoNotOptimize(ptr);
    }
}
BENCHMARK(BM_TraceIDAccess);

// Benchmark for Append with duration
static void BM_TraceAppendDuration(benchmark::State& state) {
    auto duration = std::chrono::microseconds(1234);
    for (auto _ : state) {
        Trace trace;
        trace.Append("test_operation", duration);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceAppendDuration);

// Benchmark for Append with time_point
static void BM_TraceAppendTimePoint(benchmark::State& state) {
    auto start = seastar::lowres_clock::now();
    for (auto _ : state) {
        Trace trace;
        trace.Append("test_operation", start);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceAppendTimePoint);

// Benchmark for Append with string value
static void BM_TraceAppendString(benchmark::State& state) {
    std::string value = "test_value_12345";
    for (auto _ : state) {
        Trace trace;
        trace.Append("test_key", value);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceAppendString);

// Benchmark for Append with negative duration (no duration case)
static void BM_TraceAppendNoDuration(benchmark::State& state) {
    for (auto _ : state) {
        Trace trace;
        trace.Append("test_operation");
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceAppendNoDuration);

// Benchmark for std time point
static void BM_TraceTimePointStd(benchmark::State& state) {
    for (auto _ : state) {
        Trace trace;
        auto start = std::chrono::steady_clock::now();
        trace.Append("std", start);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceTimePointStd);

// Benchmark for seastar time point
static void BM_TraceTimePointSeastar(benchmark::State& state) {
    for (auto _ : state) {
        Trace trace;
        auto start = seastar::lowres_clock::now();
        trace.Append("sst", start);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceTimePointSeastar);

// Benchmark for multiple Append operations
static void BM_TraceMultipleAppends(benchmark::State& state) {
    auto duration1 = std::chrono::microseconds(100);
    auto duration2 = std::chrono::microseconds(200);
    auto duration3 = std::chrono::microseconds(300);
    std::string value = "test_value";

    for (auto _ : state) {
        Trace trace;
        trace.Append("op1", duration1);
        trace.Append("op2", duration2);
        trace.Append("op3", duration3);
        trace.Append("key1", value);
        benchmark::DoNotOptimize(trace);
    }
}
BENCHMARK(BM_TraceMultipleAppends);

// Benchmark for many Append operations (stress test)
static void BM_TraceManyAppends(benchmark::State& state) {
    std::vector<std::string> names;
    std::vector<seastar::lowres_clock::duration> durations;

    // Pre-generate test data
    for (int i = 0; i < state.range(0); ++i) {
        names.push_back("operation_" + std::to_string(i));
        durations.push_back(std::chrono::microseconds(100 + i));
    }

    for (auto _ : state) {
        Trace local_trace;
        for (size_t i = 0; i < names.size(); ++i) {
            local_trace.Append(names[i], durations[i]);
        }
        benchmark::DoNotOptimize(local_trace);
    }
}
BENCHMARK(BM_TraceManyAppends)->Range(10, 1000);

}  // namespace blobstore

int main(int argc, char** argv) {
    seastar::app_template app;

    benchmark::Initialize(&argc, argv);

    app.run(argc, argv, [] {
        benchmark::RunSpecifiedBenchmarks();

        return seastar::make_ready_future<int>(0);
    });

    return 0;
}
