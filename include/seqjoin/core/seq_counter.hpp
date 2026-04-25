// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>

namespace seqjoin {

/// Monotonic sequence counter shared by all sources within one NJoin.
/// Each insert gets a unique, strictly increasing seq.
/// 0 is a valid seq. Overflow wraps — correctness is maintained by
/// signed comparison: (int64_t)(a - b) > 0.
class SeqCounter {
public:
    SeqCounter() noexcept = default;

    /// Returns the next sequence number (0, 1, 2, ...).
    [[nodiscard]] uint64_t next() noexcept {
        return counter_.fetch_add(1, std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t> counter_{0};
};

/// Signed wraparound comparison: true if a is "after" b.
/// Handles overflow correctly for up to 2^63 distance.
[[nodiscard]] inline bool seq_after(uint64_t a, uint64_t b) noexcept {
    return static_cast<int64_t>(a - b) > 0;
}

} // namespace seqjoin
