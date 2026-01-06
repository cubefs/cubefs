#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <unordered_set>
#include <vector>

#include "blobnode/store/slice_allocator.h"

using blobstore::blobnode::SliceAllocator;
using blobstore::blobnode::SliceIndex;

SEASTAR_TEST_CASE(test_store_allocator_basic_alloc_free) {
    constexpr SliceIndex total_slices = 1000;
    SliceAllocator allocator(total_slices);
    for (SliceIndex i = 0; i < total_slices; ++i) {
        BOOST_REQUIRE(allocator.Free(i));
    }

    auto s1 = allocator.Alloc();
    BOOST_REQUIRE(s1);
    SliceIndex idx1 = s1.Value();
    BOOST_REQUIRE(idx1 < total_slices);

    BOOST_REQUIRE(allocator.Free(idx1));
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_store_allocator_alloc_all) {
    constexpr SliceIndex total_slices = 10000;
    SliceAllocator allocator(total_slices, true);

    std::unordered_set<SliceIndex> allocated;
    for (SliceIndex i = 0; i < total_slices; ++i) {
        auto s = allocator.Alloc();
        BOOST_REQUIRE(s);
        SliceIndex idx = s.Value();
        BOOST_REQUIRE(idx < total_slices);
        BOOST_REQUIRE(allocated.find(idx) == allocated.end());
        allocated.insert(idx);
    }

    auto s = allocator.Alloc();
    BOOST_REQUIRE(!s);
    BOOST_REQUIRE(s.Code() == blobstore::ErrCode::ErrInvalid);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_store_allocator_free_invalid_index) {
    constexpr SliceIndex total_slices = 100;
    SliceAllocator allocator(total_slices, true);
    BOOST_REQUIRE(!allocator.Free(total_slices));
    BOOST_REQUIRE(!allocator.Free(total_slices + 1000));
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_store_allocator_small_total) {
    constexpr SliceIndex total_slices = 1;
    SliceAllocator allocator(total_slices, true);

    auto s = allocator.Alloc();
    BOOST_REQUIRE(s);
    BOOST_REQUIRE_EQUAL(s.Value(), 0);

    BOOST_REQUIRE(!allocator.Alloc());
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_store_allocator_full) {
    constexpr SliceIndex total_slices = 4096 * 64 * 16;
    SliceAllocator allocator(total_slices, true);

    std::unordered_set<SliceIndex> allocated;
    for (SliceIndex i = 0; i < total_slices; ++i) {
        auto s = allocator.Alloc();
        BOOST_REQUIRE(s);
        SliceIndex idx = s.Value();
        BOOST_REQUIRE(idx < total_slices);
        BOOST_REQUIRE(allocated.find(idx) == allocated.end());
        allocated.insert(idx);
    }

    BOOST_REQUIRE_EQUAL(allocated.size(), total_slices);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_store_allocator_alloc_free_pattern) {
    constexpr SliceIndex total_slices = 100;
    SliceAllocator allocator(total_slices, true);

    std::vector<SliceIndex> allocated;
    for (SliceIndex i = 0; i < total_slices; ++i) {
        auto s = allocator.Alloc();
        BOOST_REQUIRE(s);
        allocated.push_back(s.Value());
    }

    for (size_t i = 0; i < allocated.size() / 2; ++i) {
        BOOST_REQUIRE(allocator.Free(allocated[i]));
    }
    for (SliceIndex i = 0; i < total_slices / 2; ++i) {
        BOOST_REQUIRE(allocator.Alloc());
    }
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_store_allocator_free_twice) {
    constexpr SliceIndex total_slices = 1;
    SliceAllocator allocator(total_slices, true);

    auto s = allocator.Alloc();
    BOOST_REQUIRE(s);
    SliceIndex idx = s.Value();

    BOOST_REQUIRE(allocator.Free(idx));
    BOOST_REQUIRE(allocator.Free(idx));
    BOOST_REQUIRE(allocator.Free(idx));

    s = allocator.Alloc();
    BOOST_REQUIRE(s);
    BOOST_REQUIRE(s.Value() == idx);
    return seastar::make_ready_future<>();
}
