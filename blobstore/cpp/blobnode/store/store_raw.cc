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

#include "store_raw.h"

#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
#include <utility>

#include "blobnode/device/device.h"
#include "blobnode/device/kernel_device.h"
#include "blobnode/device/spdk_device.h"
#include "common/status.h"

namespace blobstore {
namespace blobnode {

RawStore::RawStore(StoreConfig cfg_in) : cfg_(std::move(cfg_in)) {
    chunk_op_limiter_.reserve(kDefaultChunkSemaCount);
    for (auto i = 0; i < kDefaultChunkSemaCount; ++i) {
        chunk_op_limiter_.emplace_back(1);
    }
    // TODO: init format_
}

FutureStatus<StorePtr> RawStore::Open(StoreConfig cfg) noexcept {
    Status<StorePtr> s;
    std::unique_ptr<RawStore> store_ptr(new RawStore(cfg));
    Status<DevicePtr> ss;

    switch (cfg.dev_driver_type) {
        case DeviceDriverType::Kernel:
            ss = co_await KernelDevice::Create(cfg.device);
            break;
        case DeviceDriverType::Spdk:
            ss = co_await SpdkDevice::Create(cfg.device, cfg.ns_id, cfg.qpair_n);
            break;
    };
    if (!ss) {
        s.SetCode(ss.Code()).SetReason(ss.Reason());
        co_return s;
    }
    store_ptr->dev_ = std::move(ss.Value());

    // read super block and unmarshal
    auto buf = seastar::temporary_buffer<char>::aligned(::blobstore::kSectorSize,
                                                        rawStoreFormatLayoutV1.super_block_size);
    auto rs = co_await store_ptr->dev_->Read(rawStoreFormatLayoutV1.start_offset, buf.get_write(),
                                             rawStoreFormatLayoutV1.super_block_size);
    if (!rs) {
        s.SetCode(rs.Code()).SetReason(rs.Reason());
        co_return s;
    }

    SuperBlock superblock;
    superblock.block_size = rawStoreFormatLayoutV1.super_block_size;

    auto us = superblock.Decode(buf.get());
    if (!us) {
        s.SetCode(us.Code()).SetReason(us.Reason());
        co_return s;
    }
    store_ptr->superblock_ = superblock;
    store_ptr->slice_registry_.checkpoint_buffer =
        seastar::temporary_buffer<char>::aligned(::blobstore::kSectorSize, (1ull << 20));

    s.SetValue(std::move(store_ptr));
    co_return s;
}

// Store implements

FutureStatus<> RawStore::Load(Trace& t) noexcept {
    Status<> s;
    // TODO
    co_return s;
}

FutureStatus<> RawStore::Format(Trace& t, DiskMetaInfo disk_meta) noexcept {
    Status<> s;
    if (disk_meta.format() == ::blobstore::kFormatDiskTypeRawDeviceV1) {
        s = co_await FormatV1(t, disk_meta);
    } else {
        s.SetCode(ErrCode::ErrInvalid).SetReason("store: invalid format device");
        co_return s;
    }
    co_return s;
}

FutureStatus<DiskMetaInfo> RawStore::LoadFormat(Trace& t) noexcept {
    Status<DiskMetaInfo> s;
    s.SetValue(superblock_.DiskMeta());
    co_return s;
};

FutureStatus<> RawStore::UpdateFormatInfo(Trace& t, DiskID disk_id,
                                          DiskMetaInfo disk_meta) noexcept {
    Status<> s;
    if (disk_meta.format() == ::blobstore::kFormatDiskTypeRawDeviceV1) {
        SuperBlockInfo superblock;
        superblock.mutable_meta()->CopyFrom(disk_meta);
        superblock.mutable_layout()->CopyFrom(superblock_.super_block_info.layout());
        s = co_await UpsertSuperBlock(superblock);
    } else {
        s.SetCode(ErrCode::ErrInvalid).SetReason("store: invalid format type of disk");
        co_return s;
    }
    co_return s;
}

FutureStatus<ChunkHandlerPtr> RawStore::OpenChunk(Trace& t, ChunkID chunk_id,
                                                  uint64_t size) noexcept {
    Status<ChunkHandlerPtr> s;

    co_await AcquireChunkLimit(chunk_id);
    auto release = seastar::defer([&] { ReleaseChunkLimit(chunk_id); });

    s = GetChunk(chunk_id);
    if (s) {
        co_return s;
    }

    auto alloc_s = AllocChunk();
    if (!alloc_s) {
        s.SetCode(alloc_s.Code()).SetReason(alloc_s.Reason());
        co_return s;
    }

    auto chunk = alloc_s.Value();
    auto chunk_meta = chunk->GetMeta();
    chunk_meta.chunk_meta_info.set_chunk_id(reinterpret_cast<const char*>(chunk_id.data.data()));
    chunk_meta.chunk_meta_info.set_status(static_cast<uint32_t>(ChunkStatus::Normal));
    chunk_meta.chunk_meta_info.set_epoch(chunk_meta.chunk_meta_info.epoch() + 1);
    auto update_s = co_await UpsertChunkMeta(chunk_meta);
    if (!update_s) {
        FreeChunk(chunk);
        s.SetCode(update_s.Code()).SetReason(update_s.Reason());
        co_return s;
    }
    chunk->UpdateMeta(std::move(chunk_meta));
    AddChunk(chunk_id, chunk);
    s.SetValue(std::move(chunk));
    co_return s;
}

Status<ChunkMetaInfoView> RawStore::GetChunkMeta(Trace& t, ChunkID chunk_id) noexcept {
    Status<blobnode::ChunkMetaInfoView> s;
    auto gs = GetChunk(chunk_id);
    if (!gs.OK()) {
        s.SetCode(gs.Code()).SetReason(gs.Reason());
        return s;
    }
    auto chunk = gs.Value();
    auto cmi = gs.Value()->GetChunkMetaInfo();

    ChunkMetaInfoView view{.owner = std::move(chunk), .meta = &cmi};
    s.SetValue(std::move(view));
    return s;
}

FutureStatus<> RawStore::UpdateChunkMeta(Trace& t, ChunkID chunk_id,
                                         ChunkMetaInfo chunk_meta) noexcept {
    Status<> s;

    co_await AcquireChunkLimit(chunk_id);
    auto release = seastar::defer([&] { ReleaseChunkLimit(chunk_id); });

    auto chunk_s = GetChunk(chunk_id);
    if (!chunk_s) {
        s.SetCode(chunk_s.Code()).SetReason(chunk_s.Reason());
        co_return s;
    }
    auto chunk = chunk_s.Value();

    auto meta = chunk->GetMeta();
    meta.chunk_meta_info = chunk_meta;
    // do persistence first
    s = co_await UpsertChunkMeta(meta);
    if (!s) {
        co_return s;
    }
    // update vuids and chunk meta in memory
    chunk->UpdateMeta(std::move(meta));
    AddVuid(chunk_meta.vuid(), chunk_id);
    co_return s;
}

Status<ChunkID> RawStore::GetVuidBind(Trace& t, Vuid vuid) noexcept { return GetVuid(vuid); }

// DeleteChunk marks a chunk as released and schedules it for cleanup.
// Note: This function uses rwlock because it spans co_await points,
// meaning other coroutines could access chunk_registry_ during the IO wait.
FutureStatus<> RawStore::DeleteChunk(Trace& t, ChunkID chunk_id) noexcept {
    Status<> s;

    co_await AcquireChunkLimit(chunk_id);
    auto release = seastar::defer([&] { ReleaseChunkLimit(chunk_id); });

    co_await chunk_registry_.lock.write_lock();
    auto unlock = seastar::defer([this] { chunk_registry_.lock.write_unlock(); });

    auto it = chunk_registry_.chunks.find(chunk_id);
    if (it == chunk_registry_.chunks.end()) {
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: chunk not found");
        co_return s;
    }
    auto chunk = it->second;

    auto meta = chunk->GetMeta();
    meta.chunk_meta_info.set_status(static_cast<uint32_t>(ChunkStatus::Release));

    // Persist chunk meta first (this is where we yield)
    s = co_await UpsertChunkMeta(meta);
    if (!s) {
        co_return s;
    }

    chunk_registry_.chunks.erase(chunk_id);
    chunk_registry_.vuids.erase(chunk_id.GetVuid());

    chunk->UpdateMeta(std::move(meta));
    chunk_registry_.pending_clean_chunks.push_back(chunk);

    co_return s;
}

Status<std::unordered_map<ChunkID, ChunkMetaInfoView>> RawStore::ListChunkMetas(Trace& t) noexcept {
    Status<std::unordered_map<ChunkID, ChunkMetaInfoView>> s;
    std::unordered_map<ChunkID, ChunkMetaInfoView> chunks;
    for (const auto& kv : chunk_registry_.chunks) {
        auto cmi = kv.second->GetChunkMetaInfo();
        chunks[kv.first] = ChunkMetaInfoView{.owner = kv.second, .meta = &cmi};
    }

    s.SetValue(chunks);
    return s;
}

Status<std::unordered_map<Vuid, ChunkID>> RawStore::ListVuidMetas(Trace& t) noexcept {
    Status<std::unordered_map<Vuid, ChunkID>> s;
    std::unordered_map<Vuid, ChunkID> vuids;

    for (const auto& kv : chunk_registry_.vuids) {
        vuids[kv.first] = kv.second;
    }
    s.SetValue(vuids);

    return s;
}

FutureStatus<> RawStore::Close(Trace& t) noexcept {
    Status<> s;
    co_await dev_->Close();
    co_return s;
}

// SliceHandler implements

Status<SliceMetaPtr> RawStore::AllocSlice(SliceID sid, Vuid vuid, uint32_t chunk_epoch) {
    Status<SliceMetaPtr> s;
    // TODO
    return s;
}

FutureStatus<> RawStore::UpdateSlice(SliceMetaPtr sm) {
    auto s = co_await UpsertSliceMetaInPersistence(sm);
    // TODO
    co_return s;
}

FutureStatus<> RawStore::DeleteSlice(SliceMetaPtr sm) {
    Status<> s;
    // TODO
    co_return s;
}

FutureStatus<> RawStore::UpsertSliceMetaInPersistence(SliceMetaPtr sm) noexcept {
    Status<> s;
    // TODO
    co_return s;
}

void RawStore::UpsertSliceMetaInMemory(SliceMetaPtr sm) noexcept {
    // TODO
}

// private method

FutureStatus<> RawStore::FormatV1(Trace& t, DiskMetaInfo disk_meta) noexcept {
    Status<> s;

    // update store format layout
    if (disk_meta.format() == ::blobstore::kFormatDiskTypeRawDeviceV1) {
        format_ = rawStoreFormatLayoutV1;
    }

    auto capacity = dev_->Capacity();
    // calculate chunk count
    auto available_size = capacity - format_.super_block_size - format_.log_arena_size * 2;
    auto max_chunk_count = available_size / format_.chunk_arena_size;
    auto chunk_meta_size = max_chunk_count * format_.chunk_meta_size;
    auto slice_meta_size =
        max_chunk_count * format_.chunk_arena_size / format_.slice_size * format_.slice_meta_size;
    // padding to valid disk sliceSize size range
    while (chunk_meta_size + slice_meta_size + max_chunk_count * format_.chunk_arena_size >
           available_size) {
        max_chunk_count -= 1;
        chunk_meta_size = max_chunk_count * format_.chunk_meta_size;
        slice_meta_size = max_chunk_count * format_.chunk_arena_size / format_.slice_size *
                          format_.slice_meta_size;
    }
    // calculate slice count
    auto max_slice_count = max_chunk_count * format_.chunk_arena_size / format_.slice_size;

    // write header finally which means format has been done
    SuperBlockInfo superblock;
    auto meta = superblock.mutable_meta();
    *meta = disk_meta;
    auto layout = superblock.mutable_layout();

    uint64_t offset = format_.start_offset;
    offset += format_.super_block_size;
    layout->set_log_arena_start(offset);
    offset += format_.log_arena_size * 2;
    layout->set_chunk_meta_start(offset);
    offset += chunk_meta_size;
    layout->set_slice_data_start(offset);
    offset += slice_meta_size;
    layout->set_slice_meta_start(offset);
    layout->set_max_chunk_count(max_chunk_count);
    layout->set_max_slice_count(max_slice_count);

    // TODO: Init Journal, free chunks, free slices

    s = co_await UpsertSuperBlock(std::move(superblock));
    co_return s;
}

FutureStatus<> RawStore::UpsertSuperBlock(SuperBlockInfo superblock) noexcept {
    Status<> s;

    auto new_superblock = superblock_;
    auto buf = dev_->Alloc(format_.super_block_size);
    new_superblock.super_block_info = std::move(superblock);
    s = new_superblock.Encode(buf.get_write());
    if (!s) {
        co_return s;
    }
    s = co_await dev_->Write(format_.start_offset, buf.get(), buf.size());
    if (!s) {
        co_return s;
    }

    superblock_ = std::move(new_superblock);
    co_return s;
}

FutureStatus<> RawStore::AcquireChunkLimit(ChunkID chunk_id) noexcept {
    Status<> s;
    Vuid vuid = chunk_id.GetVuid();
    auto idx = vuid % chunk_op_limiter_.size();
    co_await chunk_op_limiter_[idx].wait(1);
    co_return s;
}

void RawStore::ReleaseChunkLimit(ChunkID chunk_id) noexcept {
    Vuid vuid = chunk_id.GetVuid();
    auto idx = vuid % chunk_op_limiter_.size();
    chunk_op_limiter_[idx].signal(1);
}

Status<ChunkHandlerPtr> RawStore::AllocChunk() noexcept {
    Status<ChunkHandlerPtr> s;
    // TODO
    return s;
}

void RawStore::FreeChunk(ChunkHandlerPtr ch) noexcept {
    auto* node = new FreeChunkItem{};
    node->chunk = std::move(ch);
    chunk_pool_.free_list.push_back(*node);
}

FutureStatus<> RawStore::UpsertChunkMeta(ChunkMeta chunk_meta) noexcept {
    Status<> s;
    // TODO
    co_return s;
}

Status<ChunkHandlerPtr> RawStore::GetChunk(ChunkID chunk_id) noexcept {
    Status<ChunkHandlerPtr> s;
    // co_await chunk_registry_.lock.read_lock();
    // auto unlock = seastar::defer([this]{ chunk_registry_.lock.read_unlock(); });
    auto it = chunk_registry_.chunks.find(chunk_id);
    if (it == chunk_registry_.chunks.end()) {
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: chunk not found");
        return s;
    }
    s.SetValue(it->second);
    return s;
}

Status<ChunkHandlerPtr> RawStore::GetChunkByVuid(Vuid vuid) noexcept {
    Status<ChunkHandlerPtr> s;
    auto it_id = chunk_registry_.vuids.find(vuid);
    if (it_id == chunk_registry_.vuids.end()) {
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: vuid not found");
        return s;
    }
    auto it = chunk_registry_.chunks.find(it_id->second);
    if (it == chunk_registry_.chunks.end()) {
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: chunk not found");
        return s;
    }
    return s.SetValue(it->second);
}

void RawStore::AddChunk(ChunkID chunk_id, ChunkHandlerPtr ch) noexcept {
    chunk_registry_.chunks[chunk_id] = std::move(ch);
}

Status<ChunkID> RawStore::GetVuid(Vuid v) noexcept {
    Status<ChunkID> s;
    auto it = chunk_registry_.vuids.find(v);
    if (it == chunk_registry_.vuids.end()) {
        s.SetCode(ErrCode::ErrNotFound).SetReason("store: vuid not found");
        return s;
    }
    return s.SetValue(it->second);
}

void RawStore::AddVuid(Vuid vuid, ChunkID chunk_id) noexcept {
    chunk_registry_.vuids[vuid] = chunk_id;
    return;
}

}  // namespace blobnode
}  // namespace blobstore
