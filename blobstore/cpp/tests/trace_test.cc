#include "common/trace.h"

#include <boost/test/unit_test.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/test_case.hh>

using blobstore::Trace;

SEASTAR_TEST_CASE(test_trace_default_construction) {
    Trace trace;
    BOOST_REQUIRE(!trace.TraceID().empty());
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_trace_custom_id_construction) {
    Trace trace("custom-trace-id");
    BOOST_REQUIRE_EQUAL(trace.TraceID(), "custom-trace-id");
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_trace_append_time_point) {
    auto start = seastar::lowres_clock::now();
    Trace trace;
    trace.Append("test_operation", start);

    BOOST_REQUIRE(!trace.TraceString().empty());
    BOOST_REQUIRE(!trace.TraceID().empty());
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_trace_append_duration) {
    Trace trace;
    auto duration = seastar::lowres_clock::duration(std::chrono::milliseconds(100));
    trace.Append("op_with_duration", duration);

    BOOST_REQUIRE(!trace.TraceString().empty());
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_trace_multiple_appends) {
    Trace trace;

    auto start1 = seastar::lowres_clock::now();
    co_await seastar::sleep(std::chrono::microseconds(100));
    trace.Append("op1", start1);

    auto start2 = seastar::lowres_clock::now();
    co_await seastar::sleep(std::chrono::microseconds(200));
    trace.Append("op2", start2);
    trace.Append("key1", "value1");

    BOOST_REQUIRE(!trace.TraceString().empty());
    co_return;
}
