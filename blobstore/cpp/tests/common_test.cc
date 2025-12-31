#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>

#include "common/types.h"

using blobstore::ChunkID;
using blobstore::Vuid;

SEASTAR_TEST_CASE(test_common_chunk_id_default_construction) {
    ChunkID id;
    BOOST_REQUIRE_EQUAL(id.GetVuid(), 0);
    BOOST_REQUIRE_EQUAL(id.GetTimestamp(), 0);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_new) {
    Vuid vuid = 12345678;
    ChunkID id = ChunkID::New(vuid);
    BOOST_REQUIRE_EQUAL(id.GetVuid(), vuid);
    BOOST_REQUIRE(id.GetTimestamp() > 0);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_string_roundtrip) {
    Vuid vuid = 0x123456789ABCDEF0;
    ChunkID id = ChunkID::New(vuid);

    std::string str = id.String();
    BOOST_REQUIRE_EQUAL(str.size(), ChunkID::kEncodeLen);
    BOOST_REQUIRE_EQUAL(str[16], '-');

    ChunkID id2;
    BOOST_REQUIRE(id2.From(str));
    BOOST_REQUIRE_EQUAL(id.GetVuid(), id2.GetVuid());
    BOOST_REQUIRE_EQUAL(id.GetTimestamp(), id2.GetTimestamp());
    BOOST_REQUIRE(id == id2);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_from_invalid_string) {
    ChunkID id;
    BOOST_REQUIRE(!id.From("123"));
    BOOST_REQUIRE(!id.From("0123456789abcdef+0123456789abcdef"));
    BOOST_REQUIRE(!id.From("ghijklmnopqrstuv-0123456789abcdef"));
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_comparison) {
    ChunkID id1;
    ChunkID id2;
    BOOST_REQUIRE(id1 == id2);

    ChunkID id3 = ChunkID::New(100);
    ChunkID id4 = ChunkID::New(200);

    BOOST_REQUIRE(id3 != id4);
    BOOST_REQUIRE(id3 < id4);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_hash) {
    ChunkID id1 = ChunkID::New(12345);
    ChunkID id2 = ChunkID::New(12345);

    std::hash<ChunkID> hasher;
    BOOST_REQUIRE(hasher(id1) != hasher(id2) || id1 == id2);

    ChunkID id3 = id1;
    BOOST_REQUIRE_EQUAL(hasher(id1), hasher(id3));
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_constants) {
    BOOST_REQUIRE_EQUAL(ChunkID::kLength, 16);
    BOOST_REQUIRE_EQUAL(ChunkID::kVuidLen, 8);
    BOOST_REQUIRE_EQUAL(ChunkID::kTimestampLen, 8);
    BOOST_REQUIRE_EQUAL(ChunkID::kEncodeLen, 33);
    return seastar::make_ready_future<>();
}

SEASTAR_TEST_CASE(test_common_chunk_id_in_unordered_map) {
    std::unordered_map<ChunkID, int> map;

    ChunkID id1 = ChunkID::New(100);
    ChunkID id2 = ChunkID::New(200);
    map[id1] = 1;
    map[id2] = 2;

    BOOST_REQUIRE_EQUAL(map.size(), 2);
    BOOST_REQUIRE_EQUAL(map[id1], 1);
    BOOST_REQUIRE_EQUAL(map[id2], 2);

    map[id1] = 10;
    BOOST_REQUIRE_EQUAL(map.size(), 2);
    BOOST_REQUIRE_EQUAL(map[id1], 10);
    return seastar::make_ready_future<>();
}
