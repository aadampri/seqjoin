// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <vector>

namespace seqjoin {

/// Entry in a snapshot: value + seq (owning copy).
template <class T>
struct SnapEntry {
    T value{};
    uint64_t seq{0};

    SnapEntry() noexcept = default;
    SnapEntry(const T& v, uint64_t s) : value(v), seq(s) {}
    SnapEntry(T&& v, uint64_t s) : value(std::move(v)), seq(s) {}
};

/// Reference into a snapshot: pointer to value + seq (non-owning).
/// Used by COW snapshots where the immutable map stays alive via shared_ptr.
/// The cross-product engine works uniformly via get_value(entry) / get_seq(entry).
template <class T>
struct SnapRef {
    const T* value_ptr{nullptr};
    uint64_t seq{0};

    SnapRef() noexcept = default;
    SnapRef(const T& v, uint64_t s) noexcept : value_ptr(&v), seq(s) {}

    const T& value() const noexcept { return *value_ptr; }
};

// Uniform accessors for cross-product: works with SnapEntry<T> and SnapRef<T>.
template <class T>
const T& get_value(const SnapEntry<T>& e) { return e.value; }

template <class T>
const T& get_value(const SnapRef<T>& e) { return *e.value_ptr; }

template <class E>
uint64_t get_seq(const E& e) { return e.seq; }

/// A snapshot from one source: vector of (value, seq) pairs.
template <class T>
using Snapshot = std::vector<SnapEntry<T>>;

} // namespace seqjoin
