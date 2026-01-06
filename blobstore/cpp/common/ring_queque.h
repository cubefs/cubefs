#pragma once

#include <cstddef>
#include <vector>

#include "common/util.h"

namespace blobstore {

// Fixed-size circular queue (ring queue) for efficient FIFO operations
// Thread-safe for single producer/single consumer scenarios
// Not thread-safe for concurrent access
template <typename T>
class RingQueue {
   private:
    std::vector<T> ring_;
    size_t capacity_;       // actual capacity
    size_t capacity_mask_;  // (power of capacity) - 1 (for fast modulo)
    size_t read_idx_ = 0;   // Read position (head of queue)
    size_t write_idx_ = 0;  // Write position (tail of queue)
    size_t count_ = 0;      // Number of elements in queue

   public:
    explicit RingQueue(size_t capacity)
        : capacity_(capacity), capacity_mask_(NextPowerOf2(capacity) - 1) {
        ring_.resize(capacity_mask_ + 1);
    }

    // Returns true if successful, false if queue is full
    bool Push(const T& value) noexcept {
        if (IsFull()) {
            return false;
        }
        ring_[write_idx_] = value;
        write_idx_ = (write_idx_ + 1) & capacity_mask_;
        count_++;
        return true;
    }
    bool Push(T&& value) noexcept {
        if (IsFull()) {
            return false;
        }
        ring_[write_idx_] = std::move(value);
        write_idx_ = (write_idx_ + 1) & capacity_mask_;
        count_++;
        return true;
    }

    // Returns true if successful, false if queue is empty
    bool Pop(T& out) noexcept {
        if (IsEmpty()) {
            return false;
        }
        out = std::move(ring_[read_idx_]);
        read_idx_ = (read_idx_ + 1) & capacity_mask_;
        count_--;
        return true;
    }

    // Clear all elements
    void Clear() noexcept {
        read_idx_ = 0;
        write_idx_ = 0;
        count_ = 0;
    }

    // Returns pointer to element if available, nullptr if empty
    const T* Front() const noexcept {
        if (IsEmpty()) {
            return nullptr;
        }
        return &ring_[read_idx_];
    }

    T* Front() noexcept {
        if (IsEmpty()) {
            return nullptr;
        }
        return &ring_[read_idx_];
    }

    inline size_t Size() const noexcept { return count_; }
    inline bool IsEmpty() const noexcept { return count_ == 0; }
    inline bool IsFull() const noexcept { return count_ == capacity_; }
    inline size_t Capacity() noexcept { return capacity_; }
};

}  // namespace blobstore
