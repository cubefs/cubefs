#pragma once

#include <fmt/format.h>

#include <unordered_map>

#include "common/net/rpc_server.h"

enum class RoutePathIndex : blobstore::net::RpcServer::RouterIndex {
    Ping = 1,
    Kick = 2,
    Error = 3,
    Middle = 100,
    NotFound = 404,

    Stream = 1000,
};

inline blobstore::net::RpcServer::RouterIndex operator+(RoutePathIndex path_index) {
    return static_cast<blobstore::net::RpcServer::RouterIndex>(path_index);
}

static const std::unordered_map<RoutePathIndex, std::string> kRoutePathString = {
    {RoutePathIndex::Ping, "/ping"},         {RoutePathIndex::Kick, "/kick"},
    {RoutePathIndex::Error, "/error"},       {RoutePathIndex::Middle, "/middle"},
    {RoutePathIndex::NotFound, "/notfound"}, {RoutePathIndex::Stream, "/stream"},
};
