#include <benchmark/benchmark.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <vector>

#include "blobnode/store/slice_allocator.h"

using blobstore::blobnode::SliceAllocator;
using blobstore::blobnode::SliceIndex;

namespace blobstore {
namespace blobnode {

constexpr size_t min = 1 << 16;
constexpr size_t mid = 1 << 20;
constexpr size_t max = 1 << 24;

// Benchmark for SliceAllocator construction
static void BM_SliceAllocatorConstruction(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    for (auto _ : state) {
        SliceAllocator allocator(total_slices, false);
        benchmark::DoNotOptimize(allocator);
    }
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorConstruction)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for SliceAllocator construction with all_free
static void BM_SliceAllocatorConstructionWithInit(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    for (auto _ : state) {
        SliceAllocator allocator(total_slices, true);
        benchmark::DoNotOptimize(allocator);
    }
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorConstructionWithInit)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for Alloc operation
static void BM_SliceAllocatorAlloc(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    SliceAllocator allocator(total_slices, true);

    for (auto _ : state) {
        auto s = allocator.Alloc();
        if (s) {
            benchmark::DoNotOptimize(s.Value());
        }
    }
    state.SetItemsProcessed(state.iterations());
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorAlloc)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for Free operation
static void BM_SliceAllocatorFree(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    SliceAllocator allocator(total_slices, false);

    // Pre-allocate some slices to free
    std::vector<SliceIndex> indices;
    indices.reserve(1000);
    for (int i = 0; i < 1000 && i < total_slices; ++i) {
        indices.push_back(static_cast<SliceIndex>(i));
    }

    size_t index = 0;
    for (auto _ : state) {
        SliceIndex idx = indices[index % indices.size()];
        auto s = allocator.Free(idx);
        benchmark::DoNotOptimize(s);
        ++index;
    }
    state.SetItemsProcessed(state.iterations());
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorFree)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for Alloc-Free pattern (mixed workload)
static void BM_SliceAllocatorAllocFree(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    SliceAllocator allocator(total_slices, true);

    std::vector<SliceIndex> allocated;
    allocated.reserve(1000);

    for (auto _ : state) {
        // Alloc
        auto s1 = allocator.Alloc();
        if (s1.OK()) {
            allocated.push_back(s1.Value());
        }

        // Free one if we have any
        if (!allocated.empty()) {
            SliceIndex idx = allocated.back();
            allocated.pop_back();
            auto s2 = allocator.Free(idx);
            benchmark::DoNotOptimize(s2);
        }
    }
    state.SetItemsProcessed(state.iterations() * 2);  // Alloc + Free
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorAllocFree)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for sequential Alloc (stress test)
static void BM_SliceAllocatorSequentialAlloc(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    SliceAllocator allocator(total_slices, true);

    std::vector<SliceIndex> allocated;
    allocated.reserve(total_slices);

    int64_t count = 0;
    for (auto _ : state) {
        auto s = allocator.Alloc();
        if (s) {
            allocated.push_back(s.Value());
            ++count;
            benchmark::DoNotOptimize(s.Value());
        } else {
            // Reset: free all allocated slices
            for (auto idx : allocated) {
                allocator.Free(idx);
            }
            allocated.clear();
        }
    }
    state.SetItemsProcessed(count);
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorSequentialAlloc)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for sequential Free (stress test)
static void BM_SliceAllocatorSequentialFree(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    SliceAllocator allocator(total_slices, false);

    // Pre-allocate indices to free
    std::vector<SliceIndex> indices;
    indices.reserve(total_slices);
    for (SliceIndex i = 0; i < total_slices; ++i) {
        indices.push_back(i);
    }

    size_t index = 0;
    for (auto _ : state) {
        SliceIndex idx = indices[index % indices.size()];
        auto s = allocator.Free(idx);
        benchmark::DoNotOptimize(s);
        ++index;
    }
    state.SetItemsProcessed(state.iterations());
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorSequentialFree)->Arg(min)->Arg(mid)->Arg(max);

// Benchmark for random Alloc-Free pattern
static void BM_SliceAllocatorRandomPattern(benchmark::State& state) {
    SliceIndex total_slices = static_cast<SliceIndex>(state.range(0));
    SliceAllocator allocator(total_slices, true);

    std::vector<SliceIndex> allocated;
    allocated.reserve(10000);

    // Simple linear congruential generator for pseudo-randomness
    uint64_t seed = 12345;
    auto rand = [&seed, total_slices]() -> SliceIndex {
        seed = seed * 1103515245 + 12345;
        return static_cast<SliceIndex>(seed % total_slices);
    };

    for (auto _ : state) {
        // Randomly decide to alloc or free
        bool do_alloc = (rand() % 2 == 0) || allocated.empty();

        if (do_alloc) {
            auto s = allocator.Alloc();
            if (s) {
                allocated.push_back(s.Value());
            }
        } else {
            // Free a random allocated index
            size_t idx = rand() % allocated.size();
            SliceIndex to_free = allocated[idx];
            allocated[idx] = allocated.back();
            allocated.pop_back();
            auto s = allocator.Free(to_free);
            benchmark::DoNotOptimize(s);
        }
    }
    state.SetItemsProcessed(state.iterations());
    state.SetComplexityN(total_slices);
}
BENCHMARK(BM_SliceAllocatorRandomPattern)->Arg(min)->Arg(mid)->Arg(max);

}  // namespace blobnode
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
