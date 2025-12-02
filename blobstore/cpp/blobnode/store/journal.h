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

#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <unordered_set>
#include <vector>

#include "blobnode/device/device.h"
#include "common/byteorder.h"
#include "common/const.h"
#include "common/crc.h"
#include "common/status.h"
#include "common/util.h"
#include "proto.h"

namespace blobstore {
namespace blobnode {

// Fixed journal record size: 4096 bytes (1 page)
constexpr size_t kJournalRecordSize = 4096;
constexpr size_t kJournalRecordHeaderSize = 2;
constexpr size_t kJournalRecordFooterSize = 8;
const constexpr size_t kJournalRecordMaxDataSize =
    kJournalRecordSize - kJournalRecordHeaderSize - kJournalRecordFooterSize - kCrcBytes;

constexpr size_t kJournalHeaderSize = 4096;
constexpr size_t kJournalHeaderCrcBlockSize = 512;
constexpr uint64_t kInvalidJournalVersion = 0;

class BaseJournalEntry {
   public:
    virtual ~BaseJournalEntry() = default;
    virtual Status<> MarshalTo(char *buffer) = 0;
    virtual Status<> UnmarshalFrom(const char *buffer, size_t size) = 0;
    virtual uint32_t ID() = 0;
    virtual uint8_t Size() = 0;
    virtual JournalRecordType Type() = 0;
};

using BaseJournalEntryPtr = seastar::shared_ptr<BaseJournalEntry>;

// Single record
struct JournalEntry {
    BaseJournalEntryPtr data_;
    seastar::promise<Status<>> pr;

    explicit JournalEntry(BaseJournalEntryPtr d) : data_(std::move(d)) {}

    FutureStatus<> GetFuture() { return pr.get_future(); }
};

using JournalEntryPtr = seastar::lw_shared_ptr<JournalEntry>;

// Journal Record structure, version and crc should in one sector
// Disk layout: [size(2)][data+padding(4079)][version(8)][CRC(4)] = 4096 bytes
// |------------|------------|-------------|
// |     size   |  payload   | ver + CRC32 |
// |------------|------------|-------------|
// payload layout(JournalEntry)
// |------------|------------|---------|
// | type(1)    | size(1)    |  data   |
// |------------|------------|---------|
class JournalRecord {
   public:
    JournalRecord(Buffer buf, const JournalHeaderVer ver)
        : ver_(ver), data_size_(0), data_(std::move(buf)) {
        entries_.reserve(32);
    }

    // Add entry to record
    bool AddEntry(JournalEntryPtr entry);
    // write header and crc
    FutureStatus<> Write(Device *device_, const uint64_t offset);

    // Check if empty (directly check data)
    bool IsEmpty() const { return entries_.empty(); }
    JournalHeaderVer GetVersion() const { return ver_; }

    // reset entry
    void Reset() {
        ver_ += 1;
        data_size_ = 0;
    }
    // Notify all records that flush is complete
    void Notify(const Status<> &status) {
        for (auto &r : entries_) {
            Status<> s = status;
            r->pr.set_value(std::move(s));
        }
        entries_.clear();
    }
    const std::vector<JournalEntryPtr> &GetEntries() const { return entries_; }

   private:
    JournalHeaderVer ver_;  // 8 bytes: version number
    uint16_t data_size_;    // 2 bytes: actual data length

    Buffer data_;
    std::vector<JournalEntryPtr>
        entries_;  // entry list, used to notify Append operations completed
};

using JournalRecordPtr = seastar::lw_shared_ptr<JournalRecord>;

// ==================== Arena configuration ====================

struct JournalArenaConfig {
    uint64_t arena_offset;
    uint64_t arena_size;
    uint64_t header_size;
    uint64_t replay_batch_size;
};

using CheckpointCallBack =
    std::function<FutureStatus<>(const std::map<uint64_t, BaseJournalEntryPtr> &)>;
// |-----------|---------|----------|---------|
// |  version  | flag    | payload  |  crc32  |
// |-----------|---------|----------|---------|
struct JournalHeader {
    uint64_t version;
    JournalHeaderFlag flag;

    // if header size > kJournalHeaderCrcBlockSize bytes, each kJournalHeaderCrcBlockSize byte
    // should reserve kCrcBytes byte to store the crc32 at end
    Status<> MarshalTo(char *buffer, const size_t buffer_size) const {
        Status<> s;
        if (buffer_size < Size()) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("journal: buffer too small for header");
            return s;
        }
        char *ptr = buffer;
        blobstore::BigEndian::PutUint64(ptr, version);
        ptr += sizeof(version);
        std::memcpy(ptr, &flag, sizeof(flag));
        return s;
    }

    // if header size > kJournalHeaderCrcBlockSize bytes, each kJournalHeaderCrcBlockSize byte
    // should skip kCrcBytes byte
    Status<> UnmarshalFrom(const char *buffer, const size_t buffer_size) {
        Status<> s;
        if (buffer_size < Size()) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("journal: buffer too small for header");
            return s;
        }
        const char *ptr = buffer;
        version = blobstore::BigEndian::Uint64(ptr);
        ptr += sizeof(version);
        std::memcpy(&flag, ptr, sizeof(flag));
        return s;
    }
    size_t Size() const { return sizeof(version) + sizeof(flag); }
};

class JournalArena {
   public:
    JournalArena(const JournalArenaConfig &config, Device *device);

    FutureStatus<> LoadHeader();

    FutureStatus<> WriteEntry(JournalRecordPtr record);

    FutureStatus<JournalHeaderVer> Replay(
        const std::function<FutureStatus<>(JournalRecordType, const char *, size_t)> &apply_fn);

    FutureStatus<> UpdateHeader(const JournalHeader header);

    // Get arena configuration
    const JournalArenaConfig &GetConfig() const { return config_; }

    const JournalHeader &GetHeader() const { return header_; }

    void UpdateOffset() { offset_ += kJournalRecordSize; }

   private:
    FutureStatus<bool> ReplayBatch(
        const char *buffer, size_t batch_size,
        const std::function<FutureStatus<>(JournalRecordType, const char *, size_t)> &apply_fn);

    JournalArenaConfig config_;
    Device *device_;
    JournalHeader header_;
    uint64_t offset_;
    JournalHeaderVer max_record_version_;

    seastar::gate gate_;
};

struct JournalConfig {
    uint64_t start_offset = 0;
    uint64_t journal_arena_size = 0;
};

class Journal;
using JournalPtr = std::unique_ptr<Journal>;

class Journal {
   public:
    static FutureStatus<> Format(Device *device, const JournalConfig &cfg);
    static FutureStatus<JournalPtr> Create(
        const JournalConfig &cfg, Device *device,
        std::map<JournalRecordType, CheckpointCallBack> &callbacks);
    FutureStatus<> Append(BaseJournalEntryPtr data);
    FutureStatus<> Replay(
        const std::function<FutureStatus<>(JournalRecordType, const char *, size_t)> &apply_fn,
        const std::function<FutureStatus<>()> &flush_fn);
    FutureStatus<> Stop();

   private:
    Journal() {}
    FutureStatus<> LoadHeader();
    void StartBackgroundTasks();
    seastar::future<> FlushLoop();
    seastar::future<> FlushCheckpointLoop();
    FutureStatus<> WriteEntry();
    void HandleIOError(Status<> &);

   private:
    Device *device_;
    std::unique_ptr<JournalArena> lgs_[2];
    size_t write_idx_ = 0;
    std::unordered_set<JournalRecordType> supported_types_;
    std::unique_ptr<seastar::queue<JournalEntryPtr>> entry_queue_;
    JournalRecordPtr current_write_record_;
    JournalHeaderVer next_journal_version_ = 1;
    bool arena_broken_ = false;

    // Background tasks
    seastar::future<> flush_task_ = seastar::make_ready_future<>();
    seastar::future<> checkpoint_task_ = seastar::make_ready_future<>();

    // Append synchronous waiting mechanism (based on record generation)
    seastar::condition_variable checkpoint_cv_;
    seastar::condition_variable checkpoint_done_cv_;
    seastar::condition_variable flush_cv_;
    // Callback functions
    CheckpointCallBack *checkpoint_cbs_[static_cast<uint8_t>(JournalRecordType::Max)]{};
    // for increment checkpoint
    std::map<uint64_t, BaseJournalEntryPtr>
        dirty_records_[static_cast<uint8_t>(JournalRecordType::Max)][2] = {};

    seastar::gate gate_;
};

}  // namespace blobnode
}  // namespace blobstore
