#pragma once

#include <iostream>

#include "google/protobuf/io/coded_stream.h"

// C++20 Concepts

namespace blobstore {

// -- protobuf message --
using ZeroCopyOutputStream = ::google::protobuf::io::ZeroCopyOutputStream;
using ZeroCopyInputStream = ::google::protobuf::io::ZeroCopyInputStream;

template <typename T>
concept ProtobufMessageSerializable = requires(const T& message, ZeroCopyOutputStream* output,
                                               std::ostream* ostream, char* data, size_t size) {
    { message.ByteSizeLong() } -> std::convertible_to<size_t>;
    { message.SerializeToZeroCopyStream(output) } -> std::convertible_to<bool>;
    { message.SerializeToOstream(ostream) } -> std::convertible_to<bool>;
    { message.SerializeToArray(data, size) } -> std::convertible_to<bool>;
};

template <typename T>
concept ProtobufMessageDeserializable = requires(
    T& message, ZeroCopyInputStream* input, std::istream* istream, const char* data, size_t size) {
    { message.ParseFromZeroCopyStream(input) } -> std::convertible_to<bool>;
    { message.ParseFromIstream(istream) } -> std::convertible_to<bool>;
    { message.ParseFromArray(data, size) } -> std::convertible_to<bool>;
};

template <typename T>
concept ProtobufMessageSerdes = ProtobufMessageSerializable<T> || ProtobufMessageDeserializable<T>;
// -- protobuf message --

}  // namespace blobstore
