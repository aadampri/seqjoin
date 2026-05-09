// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <vector>

namespace seqjoin {

/// Entry in a snapshot: value + seq.
template <class T>
struct SnapEntry {
    T value{};
    uint64_t seq{0};

    SnapEntry() noexcept = default;
    SnapEntry(const T& v, uint64_t s) : value(v), seq(s) {}
    SnapEntry(T&& v, uint64_t s) : value(std::move(v)), seq(s) {}
};

/// A snapshot from one source: vector of (value, seq) pairs.
template <class T>
using Snapshot = std::vector<SnapEntry<T>>;

} // namespace seqjoin
