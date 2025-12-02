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

#include <algorithm>
#include <atomic>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/program_options.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/log.hh>
#include <vector>

#include "blobnode/device/device.h"
#include "blobnode/store/journal.h"

namespace blobstore {
namespace blobnode {

// Mock Device for testing
class MockDevice : public Device {
   public:
    MockDevice()
        : name_("mock_device"),
          storage_(256 * 1024 * 1024, 0),  // Pre-allocate 256MB
          capacity_(256 * 1024 * 1024),
          sector_size_(512) {}

    const std::string& Name() const override { return name_; }

    size_t Capacity() const override { return capacity_; }

    uint32_t SectorSize() const override { return sector_size_; }

    Buffer Alloc(size_t n) override { return Buffer::aligned(sector_size_, n); }

    seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) override {
        // Simulate disk failure
        if (disk_failed_) {
            Status<> s;
            s.SetCode(ErrCode::ErrEIO).SetReason("disk I/O error");
            return seastar::make_ready_future<Status<>>(s);
        }

        // Trigger failure after certain number of writes
        if (fail_after_writes_ > 0 && write_count_ >= fail_after_writes_) {
            disk_failed_ = true;
            Status<> s;
            s.SetCode(ErrCode::ErrEIO).SetReason("disk I/O error");
            return seastar::make_ready_future<Status<>>(s);
        }

        if (pos + len > storage_.size()) {
            Status<> s;
            s.SetCode(ErrCode::ErrInvalid).SetReason("write out of bounds");
            return seastar::make_ready_future<Status<>>(s);
        }
        std::memcpy(storage_.data() + pos, b, len);
        write_count_++;
        total_write_bytes_ += len;
        return seastar::make_ready_future<Status<>>(Status<>());
    }

    seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iovs) override {
        Status<> s;
        s.SetCode(ErrCode::ErrInvalid).SetReason("not implemented");
        return seastar::make_ready_future<Status<>>(s);
    }

    seastar::future<Status<>> Write(uint64_t pos, std::vector<Buffer> buffers) override {
        Status<> s;
        s.SetCode(ErrCode::ErrInvalid).SetReason("not implemented");
        return seastar::make_ready_future<Status<>>(s);
    }

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) override {
        if (pos >= storage_.size()) {
            Status<size_t> s;
            s.SetCode(ErrCode::ErrInvalid).SetReason("read out of bounds");
            return seastar::make_ready_future<Status<size_t>>(s);
        }
        size_t read_len = std::min(len, storage_.size() - pos);
        std::memcpy(b, storage_.data() + pos, read_len);
        read_count_++;
        total_read_bytes_ += read_len;
        Status<size_t> s;
        s.SetValue(read_len);
        return seastar::make_ready_future<Status<size_t>>(s);
    }

    seastar::future<Status<size_t>> Read(uint64_t pos, std::vector<iovec> iovs) override {
        Status<size_t> s;
        s.SetCode(ErrCode::ErrInvalid).SetReason("not implemented");
        return seastar::make_ready_future<Status<size_t>>(s);
    }

    seastar::future<> Close() override { return seastar::make_ready_future<>(); }

    uint64_t GetWriteCount() const { return write_count_; }
    uint64_t GetTotalWriteBytes() const { return total_write_bytes_; }
    uint64_t GetReadCount() const { return read_count_; }
    uint64_t GetTotalReadBytes() const { return total_read_bytes_; }

    void Reset() {
        write_count_ = 0;
        total_write_bytes_ = 0;
        read_count_ = 0;
        total_read_bytes_ = 0;
        disk_failed_ = false;
        fail_after_writes_ = 0;
        std::memset(storage_.data(), 0, storage_.size());
    }

    // 模拟磁盘故障功能
    void SetFailAfterWrites(uint64_t count) { fail_after_writes_ = count; }
    void SetDiskFailed(bool failed) { disk_failed_ = failed; }
    bool IsDiskFailed() const { return disk_failed_; }

   private:
    std::string name_;
    std::vector<char> storage_;  // Pre-allocated vector
    size_t capacity_;
    uint32_t sector_size_;
    std::atomic<uint64_t> write_count_{0};
    std::atomic<uint64_t> total_write_bytes_{0};
    std::atomic<uint64_t> read_count_{0};
    std::atomic<uint64_t> total_read_bytes_{0};
    std::atomic<bool> disk_failed_{false};
    std::atomic<uint64_t> fail_after_writes_{0};
};

// Test helper: Create test data
std::vector<uint8_t> GenerateTestData(size_t size) {
    static std::mt19937 gen(std::random_device{}());  // Initialize once
    static std::uniform_int_distribution<> dis(0, 255);

    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; i++) {
        data[i] = static_cast<uint8_t>(dis(gen));
    }
    return data;
}

// Test implementation of BaseJournalEntry
class TestJournalEntry : public BaseJournalEntry {
   public:
    TestJournalEntry(JournalRecordType type, const std::vector<uint8_t>& data, uint64_t id = 0)
        : type_(type), data_(data), id_(id == 0 ? GenerateID() : id) {}

    TestJournalEntry(JournalRecordType type, size_t size, uint64_t id = 0)
        : type_(type), data_(GenerateTestData(size)), id_(id == 0 ? GenerateID() : id) {}

    ~TestJournalEntry() override = default;

    Status<> MarshalTo(char* buffer) override {
        Status<> s;
        // Format: [id (8 bytes)][data]
        std::memcpy(buffer, &id_, sizeof(id_));
        if (data_.size() > 0) {
            std::memcpy(buffer + sizeof(id_), data_.data(), data_.size());
        }
        return s;
    }

    Status<> UnmarshalFrom(const char* buffer, size_t size) override {
        Status<> s;
        if (size < sizeof(id_)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("buffer too small for ID");
            return s;
        }
        std::memcpy(&id_, buffer, sizeof(id_));
        data_.resize(size - sizeof(id_));
        if (data_.size() > 0) {
            std::memcpy(data_.data(), buffer + sizeof(id_), data_.size());
        }
        return s;
    }

    uint32_t ID() override { return id_; }
    uint8_t Size() override { return sizeof(id_) + data_.size(); }  // Total size including ID
    JournalRecordType Type() override { return type_; }

    const std::vector<uint8_t>& GetData() const { return data_; }

   private:
    static uint64_t GenerateID() {
        static std::atomic<uint64_t> counter{1};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

    JournalRecordType type_;
    std::vector<uint8_t> data_;
    uint64_t id_;
};

// Test 1: Basic append and flush
SEASTAR_TEST_CASE(test_basic_append) {
    auto device = std::make_unique<MockDevice>();
    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 64ull << 20,
    };

    // Format journal
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_basic_append failed: Format failed: " << format_s.Reason()
                  << std::endl;
        co_return;
    }

    // Create journal with empty callbacks for supported types
    std::map<JournalRecordType, CheckpointCallBack> callbacks;
    // Register supported types with dummy callbacks
    callbacks[JournalRecordType::SliceMeta] =
        [](std::map<uint64_t, BaseJournalEntryPtr>) -> seastar::future<Status<>> {
        co_return Status<>();
    };

    auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result.OK()) {
        std::cerr << "✗ test_basic_append failed: Create failed: " << journal_result.Reason()
                  << std::endl;
        co_return;
    }
    auto journal = std::move(journal_result.Value());
    std::cout << "create ok!!!" << std::endl;

    // Append some records (use shared_ptr)
    auto entry1 = seastar::make_shared<TestJournalEntry>(JournalRecordType::SliceMeta, 100);
    auto entry2 = seastar::make_shared<TestJournalEntry>(JournalRecordType::SliceMeta, 3000);

    auto s1 = co_await journal->Append(entry1);
    if (!s1.OK()) {
        std::cerr << "test_basic_append failed: " << s1.Reason() << std::endl;
        co_return;
    }

    auto s2 = co_await journal->Append(entry2);
    if (!s2.OK()) {
        std::cerr << "test_basic_append failed: " << s2.Reason() << std::endl;
        co_return;
    }

    // Wait for flush
    co_await seastar::sleep(std::chrono::milliseconds(200));

    // Stop and verify
    co_await journal->Stop();
    // Verify IO operations occurred
    if (device->GetWriteCount() == 0 || device->GetTotalWriteBytes() == 0) {
        std::cerr << "✗ test_basic_append failed: No IO operations occurred" << std::endl;
        co_return;
    }
    std::cout << "✓ test_basic_append passed (Write count: " << device->GetWriteCount() << ")"
              << std::endl;
}

SEASTAR_TEST_CASE(test_throughput_benchmark) {
    auto device = std::make_unique<MockDevice>();
    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 64ull << 20,
    };

    // Format journal
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_throughput_benchmark failed: Format failed: " << format_s.Reason()
                  << std::endl;
        co_return;
    }

    // Create journal with empty callbacks for supported types
    std::map<JournalRecordType, CheckpointCallBack> callbacks;
    // Register supported types with dummy callbacks
    callbacks[JournalRecordType::SliceMeta] =
        [](std::map<uint64_t, BaseJournalEntryPtr>) -> seastar::future<Status<>> {
        co_return Status<>();
    };

    auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result.OK()) {
        std::cerr << "✗ test_throughput_benchmark failed: Create failed: "
                  << journal_result.Reason() << std::endl;
        co_return;
    }
    auto journal = std::move(journal_result.Value());

    constexpr int kRecordCount = 100000;
    constexpr size_t kRecordSize = 80;

    auto start_time = std::chrono::steady_clock::now();
    auto prepare_start = std::chrono::steady_clock::now();

    // 预先创建所有 TestJournalEntry 对象 (使用 shared_ptr)
    std::vector<seastar::shared_ptr<TestJournalEntry>> entries;
    entries.reserve(kRecordCount);

    for (int i = 0; i < kRecordCount; i++) {
        auto type = JournalRecordType::SliceMeta;
        entries.push_back(seastar::make_shared<TestJournalEntry>(type, kRecordSize));
    }

    auto prepare_end = std::chrono::steady_clock::now();
    auto append_start = std::chrono::steady_clock::now();

    // 有界并发 + 分批创建协程：避免一次性创建10万协程
    constexpr int kMaxConcurrency = 16;
    constexpr size_t kLaunchBatch = 100;
    seastar::semaphore sem(kMaxConcurrency);

    // 记录每个请求的耗时
    std::vector<int64_t> latencies;
    latencies.resize(kRecordCount);

    // 按批次创建协程，批内使用 parallel_for_each（仍受信号量限制）
    for (size_t batch_start = 0; batch_start < entries.size(); batch_start += kLaunchBatch) {
        size_t batch_end = std::min(batch_start + kLaunchBatch, entries.size());

        co_await seastar::parallel_for_each(
            boost::make_counting_iterator(batch_start), boost::make_counting_iterator(batch_end),
            [&journal, &sem, &entries, &latencies](size_t idx) -> seastar::future<> {
                auto units = co_await seastar::get_units(sem, 1);

                auto& entry = entries[idx];
                auto req_start = std::chrono::steady_clock::now();
                auto s = co_await journal->Append(entry);
                auto req_end = std::chrono::steady_clock::now();

                if (!s.OK()) {
                    throw std::runtime_error("Append failed: " + s.Reason());
                }

                auto latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(req_end - req_start)
                        .count();
                latencies[idx] = latency_us;
            })
            .handle_exception([](std::exception_ptr e) {
                try {
                    std::rethrow_exception(e);
                } catch (const std::exception& ex) {
                    std::cerr << "Concurrent append failed: " << ex.what() << std::endl;
                }
            });
    }

    auto append_end = std::chrono::steady_clock::now();

    // Wait for all flushes to complete
    co_await journal->Stop();

    auto stop_end = std::chrono::steady_clock::now();

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    auto prepare_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(prepare_end - prepare_start);
    auto append_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(append_end - append_start);
    auto stop_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(stop_end - append_end);

    double total_bytes = static_cast<double>(kRecordCount * kRecordSize);
    double throughput_mbps = (total_bytes / duration.count()) * 1000000.0 / (1024 * 1024);
    double ops_per_sec = (kRecordCount * 1000000.0) / duration.count();

    // 计算延迟统计
    std::sort(latencies.begin(), latencies.end());
    int64_t min_latency = latencies[0];
    int64_t max_latency = latencies[kRecordCount - 1];

    // 计算精确的中位数
    int64_t median_latency;
    if (kRecordCount % 2 == 0) {
        median_latency = (latencies[kRecordCount / 2 - 1] + latencies[kRecordCount / 2]) / 2;
    } else {
        median_latency = latencies[kRecordCount / 2];
    }

    int64_t p50 = latencies[kRecordCount / 2];
    int64_t p95 = latencies[static_cast<size_t>(kRecordCount * 0.95)];
    int64_t p99 = latencies[static_cast<size_t>(kRecordCount * 0.99)];
    int64_t p999 = latencies[static_cast<size_t>(kRecordCount * 0.999)];

    double avg_latency = 0;
    for (auto l : latencies) {
        avg_latency += l;
    }
    avg_latency /= kRecordCount;

    std::cout << "✓ test_throughput_benchmark passed:" << std::endl;
    std::cout << "  Test Config:" << std::endl;
    std::cout << "    - Records: " << kRecordCount << ", Size: " << kRecordSize << " bytes each"
              << std::endl;
    std::cout << "    - Max Concurrency: " << kMaxConcurrency << " (bounded)" << std::endl;
    std::cout << "    - Queue Depth: 128" << std::endl;
    std::cout << "  Performance:" << std::endl;
    std::cout << "    - Total Duration: " << duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "      * Prepare: " << prepare_duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "      * Append: " << append_duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "      * Stop: " << stop_duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "    - Throughput: " << throughput_mbps << " MB/s" << std::endl;
    std::cout << "    - Ops/sec: " << ops_per_sec << std::endl;
    std::cout << "  IO Statistics:" << std::endl;
    std::cout << "    - Write Count: " << device->GetWriteCount() << std::endl;
    std::cout << "    - Write Bytes: " << device->GetTotalWriteBytes() << std::endl;
    std::cout << "    - Avg Entry Size: "
              << (device->GetTotalWriteBytes() / device->GetWriteCount()) << " bytes" << std::endl;
    std::cout << "  Latency Distribution (per request, in microseconds):" << std::endl;
    std::cout << "    - Min:    " << min_latency << " us" << std::endl;
    std::cout << "    - Avg:    " << static_cast<int64_t>(avg_latency) << " us" << std::endl;
    std::cout << "    - Median: " << median_latency << " us" << std::endl;
    std::cout << "    - P50:    " << p50 << " us" << std::endl;
    std::cout << "    - P95:    " << p95 << " us" << std::endl;
    std::cout << "    - P99:    " << p99 << " us" << std::endl;
    std::cout << "    - P999:   " << p999 << " us" << std::endl;
    std::cout << "    - Max:    " << max_latency << " us" << std::endl;
}

// Test case: Replay functionality
SEASTAR_TEST_CASE(test_journal_replay) {
    std::cout << "\n=== Test: Journal Replay ===" << std::endl;

    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 64ull << 20,
    };
    auto device = std::make_unique<MockDevice>();  // Reuse same IO handler

    // Format journal
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_journal_replay failed: Format failed: " << format_s.Reason()
                  << std::endl;
        co_return;
    }

    // Step 1: Create journal and write some test data
    std::cout << "Step 1: Writing test data..." << std::endl;

    std::map<JournalRecordType, CheckpointCallBack> callbacks;
    callbacks[JournalRecordType::SliceMeta] =
        [](std::map<uint64_t, BaseJournalEntryPtr>) -> seastar::future<Status<>> {
        co_return Status<>();
    };

    auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result.OK()) {
        std::cerr << "✗ test_journal_replay failed: Create failed: " << journal_result.Reason()
                  << std::endl;
        co_return;
    }
    auto journal = std::move(journal_result.Value());

    // Write test entries with known IDs and data
    const size_t kTestEntryCount = 100000;  // Large scale stress testing
    const size_t kMaxConcurrency = 16;      // Concurrent writers

    std::vector<uint64_t> expected_ids;
    std::map<uint64_t, std::vector<uint8_t>> expected_data;

    // Pre-generate all test entries
    std::vector<seastar::shared_ptr<TestJournalEntry>> entries;
    entries.reserve(kTestEntryCount);

    for (size_t i = 0; i < kTestEntryCount; ++i) {
        auto type = JournalRecordType::SliceMeta;
        size_t size = 50 + (i * 7) % 70;  // Vary size: 50-120 bytes (Total with ID: 58-128 bytes)
        auto entry = seastar::make_shared<TestJournalEntry>(type, size, 1000 + i);

        expected_ids.push_back(entry->ID());
        expected_data[entry->ID()] = entry->GetData();
        entries.push_back(entry);
    }

    // Concurrent write with semaphore to control parallelism
    seastar::semaphore sem(kMaxConcurrency);
    std::atomic<size_t> success_count{0};
    std::atomic<size_t> failed_count{0};

    co_await seastar::parallel_for_each(
        entries.begin(), entries.end(), [&](auto& entry) -> seastar::future<> {
            co_await sem.wait();

            try {
                auto result = co_await journal->Append(entry);
                if (result.OK()) {
                    success_count++;
                } else {
                    failed_count++;
                    if (failed_count <= 10) {
                        std::cerr << "✗ Append failed: " << result.Reason() << std::endl;
                    }
                }
            } catch (...) {
                failed_count++;
            }

            sem.signal();
        });

    if (failed_count > 0) {
        std::cerr << "✗ test_journal_replay failed: " << failed_count << " appends failed"
                  << std::endl;
        co_return;
    }

    std::cout << "  Written " << kTestEntryCount << " entries (concurrent)" << std::endl;

    // Stop journal to ensure all data is flushed
    auto stop_s = co_await journal->Stop();
    if (!stop_s.OK()) {
        std::cerr << "✗ test_journal_replay failed: Stop failed: " << stop_s.Reason() << std::endl;
        co_return;
    }

    journal.reset();

    std::cout << "Step 2: Replaying journal..." << std::endl;

    // Step 2: Create a new journal instance and replay
    std::vector<uint64_t> replayed_ids;
    std::map<uint64_t, std::vector<uint8_t>> replayed_data;
    std::atomic<size_t> replay_count{0};

    // Replay callback to collect replayed entries
    auto replay_fn = [&](JournalRecordType type, const char* data,
                         size_t size) -> seastar::future<Status<>> {
        Status<> s;

        // Unmarshal the test entry
        // TestJournalEntry format: [id (8 bytes)][data (size-8 bytes)]
        if (size < sizeof(uint64_t)) {
            s.SetCode(ErrCode::ErrInvalid).SetReason("replayed data too small");
            co_return s;
        }

        uint64_t id;
        std::memcpy(&id, data, sizeof(id));

        std::vector<uint8_t> payload(size - sizeof(id));
        std::memcpy(payload.data(), data + sizeof(id), payload.size());

        replayed_ids.push_back(id);
        replayed_data[id] = std::move(payload);
        replay_count++;

        co_return s;
    };

    // Flush callback (called when switching arenas during replay)
    auto flush_fn = [&]() -> seastar::future<Status<>> {
        // Nothing to do in test
        co_return Status<>();
    };

    auto journal_result2 = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result2.OK()) {
        std::cerr << "✗ test_journal_replay failed: Second Create failed: "
                  << journal_result2.Reason() << std::endl;
        co_return;
    }
    auto journal2 = std::move(journal_result2.Value());

    auto replay_s = co_await journal2->Replay(replay_fn, flush_fn);
    if (!replay_s.OK()) {
        std::cerr << "✗ test_journal_replay failed: Replay failed: " << replay_s.Reason()
                  << std::endl;
        co_return;
    }

    std::cout << "  Replayed " << replay_count << " entries" << std::endl;

    // Step 3: Verify replayed data
    std::cout << "Step 3: Verifying replayed data..." << std::endl;

    bool verification_passed = true;

    // Check count
    if (replayed_ids.size() != expected_ids.size()) {
        std::cerr << "✗ Entry count mismatch: expected " << expected_ids.size() << ", got "
                  << replayed_ids.size() << std::endl;
        verification_passed = false;
    }

    // Check IDs (order may differ due to batching, so compare as sets)
    std::set<uint64_t> expected_id_set(expected_ids.begin(), expected_ids.end());
    std::set<uint64_t> replayed_id_set(replayed_ids.begin(), replayed_ids.end());

    if (expected_id_set != replayed_id_set) {
        std::cerr << "✗ ID set mismatch" << std::endl;
        verification_passed = false;

        // Show missing IDs
        std::vector<uint64_t> missing;
        std::set_difference(expected_id_set.begin(), expected_id_set.end(), replayed_id_set.begin(),
                            replayed_id_set.end(), std::back_inserter(missing));
        if (!missing.empty()) {
            std::cerr << "  Missing IDs: ";
            for (auto id : missing) std::cerr << id << " ";
            std::cerr << std::endl;
        }

        // Show extra IDs
        std::vector<uint64_t> extra;
        std::set_difference(replayed_id_set.begin(), replayed_id_set.end(), expected_id_set.begin(),
                            expected_id_set.end(), std::back_inserter(extra));
        if (!extra.empty()) {
            std::cerr << "  Extra IDs: ";
            for (auto id : extra) std::cerr << id << " ";
            std::cerr << std::endl;
        }
    }

    // Check data integrity for each ID
    size_t data_mismatch_count = 0;
    for (const auto& [id, expected] : expected_data) {
        auto it = replayed_data.find(id);
        if (it == replayed_data.end()) {
            std::cerr << "✗ ID " << id << " not found in replayed data" << std::endl;
            data_mismatch_count++;
            continue;
        }

        if (it->second != expected) {
            std::cerr << "✗ Data mismatch for ID " << id << ": expected size " << expected.size()
                      << ", got " << it->second.size() << std::endl;
            data_mismatch_count++;
        }
    }

    if (data_mismatch_count > 0) {
        std::cerr << "✗ Total data mismatches: " << data_mismatch_count << std::endl;
        verification_passed = false;
    }

    // Clean up
    auto stop_s2 = co_await journal2->Stop();
    if (!stop_s2.OK()) {
        std::cerr << "✗ test_journal_replay failed: Second Stop failed: " << stop_s2.Reason()
                  << std::endl;
        co_return;
    }

    if (verification_passed) {
        std::cout << "✓ test_journal_replay passed!" << std::endl;
        std::cout << "  All " << kTestEntryCount << " entries replayed correctly" << std::endl;
    } else {
        std::cerr << "✗ test_journal_replay failed: Verification failed" << std::endl;
    }
}

// Test case: Checkpoint functionality
SEASTAR_TEST_CASE(test_checkpoint) {
    std::cout << "\n=== Test: Checkpoint Functionality ===" << std::endl;

    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 1ull << 20,  // Small arena (1MB) to trigger checkpoint faster
    };
    auto device = std::make_unique<MockDevice>();

    // Format journal
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_checkpoint failed: Format failed: " << format_s.Reason() << std::endl;
        co_return;
    }

    // Track checkpoint calls
    std::atomic<size_t> checkpoint_count{0};
    std::atomic<size_t> checkpoint_entries{0};
    std::vector<uint64_t> checkpointed_ids;
    seastar::semaphore checkpoint_mutex(1);

    // Setup callbacks that track checkpoint invocations
    std::map<JournalRecordType, CheckpointCallBack> callbacks;
    callbacks[JournalRecordType::SliceMeta] =
        [&](std::map<uint64_t, BaseJournalEntryPtr> entries) -> seastar::future<Status<>> {
        Status<> s;

        std::cout << "  [Checkpoint] Checkpointing " << entries.size() << " SliceMeta entries"
                  << std::endl;
        if (!entries.empty()) {
            checkpoint_count++;
            co_await checkpoint_mutex.wait();
            for (auto& [id, entry] : entries) {
                checkpointed_ids.push_back(id);
                checkpoint_entries++;
            }
            checkpoint_mutex.signal();
        }

        co_return s;
    };

    auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result.OK()) {
        std::cerr << "✗ test_checkpoint failed: Create failed: " << journal_result.Reason()
                  << std::endl;
        co_return;
    }
    auto journal = std::move(journal_result.Value());

    std::cout << "Step 1: Writing entries to fill first arena and trigger checkpoint..."
              << std::endl;

    // Calculate how many entries needed to fill arena
    // Note: layout.journal_arena_size is the size of a single arena (not total for both)
    const size_t entry_size = 100;                                        // Average entry size
    const size_t arena_size = layout.journal_arena_size;                  // Single arena size
    const size_t arena_capacity_bytes = arena_size - kJournalHeaderSize;  // Exclude header
    const size_t max_records = arena_capacity_bytes / kJournalRecordSize;

    // Write enough entries to fill > 1 arena to trigger checkpoint
    // Assume ~30 entries can fit in one 4KB JournalRecord (with 100 byte entries + overhead)
    const size_t entries_per_record = 30;
    const size_t target_entries = max_records * entries_per_record * 2;  // 2x arena capacity

    std::cout << "  Arena size: " << arena_size << " bytes (" << (arena_size / 1024) << " KB)"
              << std::endl;
    std::cout << "  Arena capacity: " << arena_capacity_bytes << " bytes" << std::endl;
    std::cout << "  JournalRecord size: " << kJournalRecordSize << " bytes" << std::endl;
    std::cout << "  Max records per arena: " << max_records << std::endl;
    std::cout << "  Target entries: " << target_entries << std::endl;

    std::vector<uint64_t> written_ids;

    // Write entries to fill first arena
    size_t failed_count = 0;
    for (size_t i = 0; i < target_entries; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, 2000 + i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            failed_count++;
            if (failed_count <= 5) {
                std::cerr << "  [Warning] Append failed at entry " << i << ": " << append_s.Reason()
                          << std::endl;
            }
        } else {
            written_ids.push_back(entry->ID());
        }
    }
    if (failed_count > 0) {
        std::cout << "  " << failed_count << " appends failed (arena full is expected)"
                  << std::endl;
    }

    std::cout << "  Written " << written_ids.size() << " entries" << std::endl;
    std::cout << "  Device stats: " << device->GetWriteCount() << " writes, "
              << (device->GetTotalWriteBytes() / 1024) << " KB total" << std::endl;

    // Give checkpoint loop time to run
    co_await seastar::sleep(std::chrono::milliseconds(100));

    std::cout << "Step 2: Continue writing more entries to trigger arena switch and checkpoint..."
              << std::endl;

    // Write more entries to exceed arena capacity and trigger checkpoint
    const size_t additional_entries = target_entries;  // Write same amount again
    failed_count = 0;
    for (size_t i = 0; i < additional_entries; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry =
            seastar::make_shared<TestJournalEntry>(type, entry_size, 2000 + target_entries + i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            failed_count++;
            if (failed_count <= 5) {
                std::cerr << "  [Warning] Append failed at entry " << (target_entries + i) << ": "
                          << append_s.Reason() << std::endl;
            }
        } else {
            written_ids.push_back(entry->ID());
        }
    }
    if (failed_count > 0) {
        std::cout << "  " << failed_count << " appends failed in step 2" << std::endl;
    }

    std::cout << "  Total written: " << written_ids.size() << " entries" << std::endl;
    std::cout << "  Device stats: " << device->GetWriteCount() << " writes, "
              << (device->GetTotalWriteBytes() / 1024) << " KB total" << std::endl;

    // Wait for checkpoint to complete
    co_await seastar::sleep(std::chrono::milliseconds(200));

    std::cout << "  Current checkpoint_count: " << checkpoint_count << std::endl;
    std::cout << "  Current checkpoint_entries: " << checkpoint_entries << std::endl;

    std::cout << "Step 3: Stopping journal and checking checkpoint..." << std::endl;

    auto stop_s = co_await journal->Stop();
    if (!stop_s.OK()) {
        std::cerr << "✗ test_checkpoint failed: Stop failed: " << stop_s.Reason() << std::endl;
        co_return;
    }

    std::cout << "Step 4: Verification..." << std::endl;
    std::cout << "  Checkpoint calls: " << checkpoint_count << std::endl;
    std::cout << "  Checkpointed entries: " << checkpoint_entries << std::endl;

    bool passed = true;

    // Check if checkpoint was triggered
    if (checkpoint_count == 0) {
        std::cerr << "✗ No checkpoint was triggered!" << std::endl;
        passed = false;
    }

    // Check if entries were checkpointed
    if (checkpoint_entries == 0) {
        std::cerr << "✗ No entries were checkpointed!" << std::endl;
        passed = false;
    } else {
        std::cout << "  ✓ Checkpoint triggered successfully" << std::endl;
        std::cout << "  ✓ " << checkpoint_entries << " entries checkpointed" << std::endl;
    }

    // Verify checkpointed IDs are from written IDs
    std::set<uint64_t> written_set(written_ids.begin(), written_ids.end());
    std::cout << "  Written IDs count: " << written_ids.size() << ", unique: " << written_set.size()
              << std::endl;
    std::cout << "  Checkpointed IDs count: " << checkpointed_ids.size() << std::endl;
    std::cout << "  Written ID range: " << *written_set.begin() << " - " << *written_set.rbegin()
              << std::endl;

    size_t invalid_checkpoints = 0;
    std::vector<uint64_t> invalid_ids;
    for (auto id : checkpointed_ids) {
        if (written_set.find(id) == written_set.end()) {
            invalid_checkpoints++;
            if (invalid_ids.size() < 10) {  // Store first 10 for debugging
                invalid_ids.push_back(id);
            }
        }
    }

    if (invalid_checkpoints > 0) {
        std::cerr << "✗ " << invalid_checkpoints << " invalid IDs in checkpoint" << std::endl;
        std::cerr << "  First few invalid IDs: ";
        for (auto id : invalid_ids) {
            std::cerr << id << " ";
        }
        std::cerr << std::endl;
        passed = false;
    }

    if (passed) {
        std::cout << "✓ test_checkpoint passed!" << std::endl;
    } else {
        std::cerr << "✗ test_checkpoint failed!" << std::endl;
    }
}

// Test: Arena switch requires checkpoint completion
SEASTAR_TEST_CASE(test_arena_switch_checkpoint_required) {
    std::cout << "\n=== Test: Arena Switch Requires Checkpoint ===" << std::endl;

    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 512ull << 10,  // Very small arena (512KB) to quickly fill
    };
    auto device = std::make_unique<MockDevice>();

    // Format journal
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_arena_switch_checkpoint_required failed: Format failed: "
                  << format_s.Reason() << std::endl;
        co_return;
    }

    // Track checkpoint completion
    std::atomic<bool> arena0_checkpointed{false};
    std::atomic<bool> arena1_checkpointed{false};
    std::atomic<size_t> checkpoint_count{0};

    // Setup callbacks that track which arena is checkpointed
    std::map<JournalRecordType, CheckpointCallBack> callbacks;
    callbacks[JournalRecordType::SliceMeta] =
        [&](std::map<uint64_t, BaseJournalEntryPtr> entries) -> seastar::future<Status<>> {
        if (!entries.empty()) {
            checkpoint_count++;
            std::cout << "  [Checkpoint] SliceMeta checkpointed " << entries.size()
                      << " entries (checkpoint #" << checkpoint_count << ")" << std::endl;
        }
        co_return Status<>();
    };

    auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result.OK()) {
        std::cerr << "✗ test_arena_switch_checkpoint_required failed: Create failed: "
                  << journal_result.Reason() << std::endl;
        co_return;
    }
    auto journal = std::move(journal_result.Value());

    std::cout << "Step 1: Fill arena 0..." << std::endl;

    // Fill first arena (arena 0)
    const size_t entry_size = 100;
    const size_t arena_capacity =
        (layout.journal_arena_size - kJournalHeaderSize) / kJournalRecordSize;
    const size_t entries_per_record = 30;
    const size_t entries_to_fill_arena = arena_capacity * entries_per_record;

    size_t total_written = 0;
    size_t failed_count = 0;

    // Fill arena 0
    for (size_t i = 0; i < entries_to_fill_arena * 2; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, 10000 + i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            failed_count++;
            if (failed_count == 1) {
                std::cout << "  Arena 0 filled after " << total_written << " entries" << std::endl;
            }
            break;
        }
        total_written++;
    }

    // Give checkpoint some time
    co_await seastar::sleep(std::chrono::milliseconds(50));

    std::cout << "Step 2: Fill arena 1..." << std::endl;

    // Now write to arena 1
    size_t arena1_written = 0;
    failed_count = 0;
    for (size_t i = 0; i < entries_to_fill_arena * 2; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, 20000 + i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            failed_count++;
            if (failed_count == 1) {
                std::cout << "  Arena 1 filled after " << arena1_written << " entries" << std::endl;
            }
            break;
        }
        arena1_written++;
    }

    // Give checkpoint time to complete
    co_await seastar::sleep(std::chrono::milliseconds(100));

    std::cout << "Step 3: Try to write more (should require arena 0 checkpoint)..." << std::endl;

    // Try to write more - this should either:
    // 1. Succeed if arena 0 has been checkpointed
    // 2. Fail with "wait for checkpoint" if arena 0 is not ready

    bool can_switch_back = false;
    for (size_t i = 0; i < 10; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, 30000 + i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            std::cout << "  Append failed: " << append_s.Reason() << std::endl;
            if (append_s.Reason().find("wait for checkpoint") != std::string::npos) {
                std::cout << "  ✓ Correctly blocked: arena 0 not yet checkpointed" << std::endl;
            }
            break;
        } else {
            can_switch_back = true;
            std::cout << "  ✓ Can write: arena 0 has been checkpointed" << std::endl;
            break;
        }
    }

    auto stop_s = co_await journal->Stop();
    if (!stop_s.OK()) {
        std::cerr << "✗ test_arena_switch_checkpoint_required failed: Stop failed: "
                  << stop_s.Reason() << std::endl;
        co_return;
    }

    std::cout << "Summary:" << std::endl;
    std::cout << "  Total checkpoints triggered: " << checkpoint_count << std::endl;
    std::cout << "  Arena 0 written: " << total_written << " entries" << std::endl;
    std::cout << "  Arena 1 written: " << arena1_written << " entries" << std::endl;

    bool passed = true;

    // Verify checkpoint was triggered
    if (checkpoint_count == 0) {
        std::cerr << "✗ No checkpoint was triggered!" << std::endl;
        passed = false;
    }

    if (passed) {
        std::cout << "✓ test_arena_switch_checkpoint_required passed!" << std::endl;
    } else {
        std::cerr << "✗ test_arena_switch_checkpoint_required failed!" << std::endl;
    }
}

// Test case: Crash recovery with replay
SEASTAR_TEST_CASE(test_crash_recovery) {
    std::cout << "\n=== Test: Crash Recovery with Replay ===" << std::endl;

    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 2ull << 20,  // 2MB arena for faster testing
    };

    // Use persistent IO handler across restarts
    auto device = std::make_unique<MockDevice>();

    // Format journal once at the beginning
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_crash_recovery failed: Format failed: " << format_s.Reason()
                  << std::endl;
        co_return;
    }

    std::cout << "Initial format completed" << std::endl;

    // Track all written entries across crashes
    std::map<uint64_t, std::vector<uint8_t>> all_written_data;
    std::atomic<uint64_t> next_id{1000};

    const size_t kNumCycles = 5;  // Number of crash/recovery cycles
    const size_t kEntriesPerCycle = 5000;

    bool test_passed = true;

    for (size_t cycle = 0; cycle < kNumCycles; ++cycle) {
        std::cout << "\n--- Cycle " << (cycle + 1) << "/" << kNumCycles << " ---" << std::endl;

        // Setup callbacks for this cycle
        std::map<JournalRecordType, CheckpointCallBack> callbacks;
        callbacks[JournalRecordType::SliceMeta] =
            [](std::map<uint64_t, BaseJournalEntryPtr>) -> seastar::future<Status<>> {
            co_return Status<>();
        };

        // Create journal
        auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
        if (!journal_result.OK()) {
            std::cerr << "✗ Cycle " << cycle << ": Create failed: " << journal_result.Reason()
                      << std::endl;
            test_passed = false;
            break;
        }
        auto journal = std::move(journal_result.Value());

        std::cout << "Step 1: Writing " << kEntriesPerCycle << " entries..." << std::endl;

        // Write entries
        std::vector<uint64_t> current_cycle_ids;
        for (size_t i = 0; i < kEntriesPerCycle; ++i) {
            auto type = JournalRecordType::SliceMeta;
            uint64_t id = next_id++;

            // Use deterministic data generation for this test
            std::vector<uint8_t> data(80);
            for (size_t j = 0; j < data.size(); ++j) {
                data[j] = static_cast<uint8_t>((id + j) % 256);
            }

            // Create entry with specific data
            auto entry = seastar::make_shared<TestJournalEntry>(type, data, id);
            all_written_data[id] = data;
            current_cycle_ids.push_back(id);

            auto append_s = co_await journal->Append(entry);
            if (!append_s.OK()) {
                // Arena full is acceptable
                break;
            }
        }

        std::cout << "  Written " << current_cycle_ids.size()
                  << " entries (total: " << all_written_data.size() << ")" << std::endl;

        // Give checkpoint some time to run
        co_await seastar::sleep(std::chrono::milliseconds(50));

        // Simulate crash: Stop journal to flush pending operations,
        // but data may still be in uncommitted state (not checkpointed)
        std::cout << "Step 2: Stopping journal (simulating crash after partial checkpoint)..."
                  << std::endl;
        auto stop_s1 = co_await journal->Stop();
        if (!stop_s1.OK()) {
            std::cerr << "  Warning: Stop failed: " << stop_s1.Reason() << std::endl;
        }
        journal.reset();

        co_await seastar::sleep(std::chrono::milliseconds(10));

        // Restart and replay
        std::cout << "Step 3: Restarting and replaying..." << std::endl;

        std::map<uint64_t, std::vector<uint8_t>> replayed_data;
        std::atomic<size_t> replay_count{0};

        auto replay_fn = [&](JournalRecordType type, const char* data,
                             size_t size) -> seastar::future<Status<>> {
            Status<> s;

            if (size < sizeof(uint64_t)) {
                s.SetCode(ErrCode::ErrInvalid).SetReason("replayed data too small");
                co_return s;
            }

            uint64_t id;
            std::memcpy(&id, data, sizeof(id));

            std::vector<uint8_t> payload(size - sizeof(id));
            std::memcpy(payload.data(), data + sizeof(id), payload.size());

            replayed_data[id] = std::move(payload);
            replay_count++;

            co_return s;
        };

        auto flush_fn = [&]() -> seastar::future<Status<>> { co_return Status<>(); };

        auto journal_result2 = co_await Journal::Create(layout, device.get(), callbacks);
        if (!journal_result2.OK()) {
            std::cerr << "✗ Cycle " << cycle
                      << ": Restart Create failed: " << journal_result2.Reason() << std::endl;
            test_passed = false;
            break;
        }
        auto journal2 = std::move(journal_result2.Value());

        auto replay_s = co_await journal2->Replay(replay_fn, flush_fn);
        if (!replay_s.OK()) {
            std::cerr << "✗ Cycle " << cycle << ": Replay failed: " << replay_s.Reason()
                      << std::endl;
            test_passed = false;
            break;
        }

        std::cout << "  Replayed " << replay_count << " entries" << std::endl;

        // Verify replayed data
        std::cout << "Step 4: Verifying replayed data..." << std::endl;

        size_t data_errors = 0;
        for (const auto& [id, expected] : replayed_data) {
            auto it = all_written_data.find(id);
            if (it == all_written_data.end()) {
                std::cerr << "  ✗ Replayed unexpected ID: " << id << std::endl;
                data_errors++;
                if (data_errors > 5) break;
                continue;
            }

            if (it->second != expected) {
                std::cerr << "  ✗ Data mismatch for ID " << id << std::endl;
                data_errors++;
                if (data_errors > 5) break;
            }
        }

        if (data_errors > 0) {
            std::cerr << "✗ Cycle " << cycle << ": " << data_errors << " data errors" << std::endl;
            test_passed = false;
            break;
        }

        std::cout << "  ✓ All replayed data verified" << std::endl;

        // Continue using journal2 for the next cycle (or clean shutdown for last cycle)
        if (cycle == kNumCycles - 1) {
            // Last cycle: clean shutdown
            auto stop_s = co_await journal2->Stop();
            if (!stop_s.OK()) {
                std::cerr << "✗ Cycle " << cycle << ": Final Stop failed: " << stop_s.Reason()
                          << std::endl;
                test_passed = false;
                break;
            }
        } else {
            // Not last cycle: stop for next restart
            auto stop_s = co_await journal2->Stop();
            if (!stop_s.OK()) {
                std::cerr << "✗ Cycle " << cycle << ": Stop failed: " << stop_s.Reason()
                          << std::endl;
                test_passed = false;
                break;
            }
            journal2.reset();
        }

        std::cout << "✓ Cycle " << (cycle + 1) << " completed successfully" << std::endl;
    }

    if (test_passed) {
        std::cout << "\n✓ test_crash_recovery passed! Completed " << kNumCycles
                  << " crash/recovery cycles" << std::endl;
        std::cout << "  Total entries written across all cycles: " << all_written_data.size()
                  << std::endl;
    } else {
        std::cerr << "\n✗ test_crash_recovery failed!" << std::endl;
    }
}

// Test: Disk failure during write operations
SEASTAR_TEST_CASE(test_disk_failure) {
    std::cout << "\n=== Test: Disk Failure During Write ===" << std::endl;

    auto layout = JournalConfig{
        .start_offset = 0,
        .journal_arena_size = 2ull << 20,
    };
    auto device = std::make_unique<MockDevice>();

    // Format journal
    auto format_s = co_await Journal::Format(device.get(), layout);
    if (!format_s.OK()) {
        std::cerr << "✗ test_disk_failure failed: Format failed: " << format_s.Reason()
                  << std::endl;
        co_return;
    }

    // Setup callbacks
    std::map<JournalRecordType, CheckpointCallBack> callbacks;
    callbacks[JournalRecordType::SliceMeta] =
        [](std::map<uint64_t, BaseJournalEntryPtr> entries) -> seastar::future<Status<>> {
        co_return Status<>();
    };

    auto journal_result = co_await Journal::Create(layout, device.get(), callbacks);
    if (!journal_result.OK()) {
        std::cerr << "✗ test_disk_failure failed: Create failed: " << journal_result.Reason()
                  << std::endl;
        co_return;
    }
    auto journal = std::move(journal_result.Value());

    bool passed = true;
    const size_t entry_size = 100;

    std::cout << "Step 1: Write entries successfully..." << std::endl;

    // Write first 50 entries successfully
    size_t successful_writes = 0;
    for (size_t i = 0; i < 50; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            std::cerr << "✗ Initial write failed at " << i << ": " << append_s.Reason()
                      << std::endl;
            passed = false;
            break;
        }
        successful_writes++;
    }

    std::cout << "  ✓ Successfully wrote " << successful_writes << " entries" << std::endl;
    uint64_t writes_before_failure = device->GetWriteCount();
    std::cout << "  Device write count: " << writes_before_failure << std::endl;

    // Simulate disk failure after next 5 writes
    std::cout << "\nStep 2: Simulate disk failure (trigger after 5 more writes)..." << std::endl;
    device->SetFailAfterWrites(writes_before_failure + 5);

    // Try to write more entries - some should succeed, then all should fail
    size_t writes_after_trigger = 0;
    size_t failed_writes = 0;
    bool disk_failure_detected = false;

    for (size_t i = 100; i < 200; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, i);

        auto append_s = co_await journal->Append(entry);
        if (!append_s.OK()) {
            // Check that it's a disk I/O error
            if (append_s.Code() == ErrCode::ErrEIO ||
                append_s.Reason().find("I/O error") != std::string::npos) {
                if (!disk_failure_detected) {
                    std::cout << "  ✓ Disk failure detected (write #" << i
                              << "): " << append_s.Reason() << std::endl;
                    disk_failure_detected = true;
                }
                failed_writes++;
            } else {
                std::cerr << "  ✗ Error type mismatch (expected ErrEIO): " << append_s.Reason()
                          << std::endl;
                passed = false;
            }

            // After disk failure, verify more failed writes
            if (disk_failure_detected) {
                writes_after_trigger++;
                if (writes_after_trigger >= 10) {
                    // Verified enough failed writes
                    break;
                }
            }
        } else {
            // Success is expected before disk failure triggers
            if (disk_failure_detected) {
                std::cerr << "  ✗ Write succeeded after disk failure detected!" << std::endl;
                passed = false;
                break;
            }
        }
    }

    std::cout << "  Failed writes after disk failure: " << failed_writes << std::endl;
    std::cout << "  Device status: " << (device->IsDiskFailed() ? "FAILED" : "OK") << std::endl;

    if (!disk_failure_detected) {
        std::cerr << "✗ Disk failure was not detected!" << std::endl;
        passed = false;
    }

    if (failed_writes == 0) {
        std::cerr << "✗ No write failures after disk failure!" << std::endl;
        passed = false;
    }

    // Give some time for error to propagate through the system
    co_await seastar::sleep(std::chrono::milliseconds(50));

    std::cout << "\nStep 3: Verify all subsequent operations return errors..." << std::endl;

    // Verify a few more operations to ensure consistent failure
    size_t verified_failures = 0;
    for (size_t i = 0; i < 10; ++i) {
        auto type = JournalRecordType::SliceMeta;
        auto entry = seastar::make_shared<TestJournalEntry>(type, entry_size, 300 + i);

        auto append_s = co_await journal->Append(entry);
        if (append_s.OK()) {
            std::cerr << "  ✗ Write #" << i << " succeeded after disk failure!" << std::endl;
            passed = false;
        } else {
            // After queue abort, should get either ErrEIO or ErrUnknown (from queue abort
            // exception)
            if (append_s.Code() == ErrCode::ErrEIO || append_s.Code() == ErrCode::ErrUnknown) {
                verified_failures++;
            } else {
                std::cerr << "  ✗ Unexpected error code (expected ErrEIO or ErrUnknown, got "
                          << static_cast<int>(append_s.Code()) << "): " << append_s.Reason()
                          << std::endl;
            }
        }
    }
    std::cout << "  ✓ Verified " << verified_failures << " consistent failures" << std::endl;

    auto stop_s = co_await journal->Stop();
    // Stop might also fail due to disk failure, which is acceptable
    if (!stop_s.OK()) {
        std::cout << "  Note: Stop failed (expected with disk failure): " << stop_s.Reason()
                  << std::endl;
    }

    if (passed && verified_failures >= 5) {
        std::cout << "\n✓ test_disk_failure passed!" << std::endl;
        std::cout << "  Verified that disk failure propagates to all subsequent operations"
                  << std::endl;
    } else {
        std::cerr << "\n✗ test_disk_failure failed!" << std::endl;
    }
}

}  // namespace blobnode
}  // namespace blobstore
