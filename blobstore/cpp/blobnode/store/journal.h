// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#pragma once

#include <seastar/core/queue.hh>
#include <seastar/core/seastar.hh>

#include "blobnode/store/proto.h"

namespace blobstore {
namespace blobnode {

class LogHandler;

struct LogEntry {};

class LogHandler {
    uint32_t idx_;
    std::vector<LogHandler> lhs_;
    LogHeaderVer latest_ver_;
    seastar::queue<LogEntry> queue;

   public:
};

}  // namespace blobnode
}  // namespace blobstore
