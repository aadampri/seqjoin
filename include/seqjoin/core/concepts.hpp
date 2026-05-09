// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstdint>
#include <optional>
#include <vector>

namespace seqjoin {

/// A RetentionPolicy manages what values are stored and how they are scanned.
/// Each policy must provide:
///   - value_type        — the stored element type
///   - insert(value, seq) -> optional<uint64_t>  — store value with seq, return seq on success
///   - scan()            — return a scannable range of (value, seq) pairs
///   - clear()           — remove all stored values
template <class P>
concept RetentionPolicy = requires(P p, typename P::value_type v, uint64_t seq) {
    typename P::value_type;
    { p.insert(std::move(v), seq) } -> std::same_as<std::optional<uint64_t>>;
    { p.scan() };
    { p.clear() } -> std::same_as<void>;
};

/// A RemovablePolicy additionally supports removal of individual values.
template <class P>
concept RemovablePolicy = RetentionPolicy<P> && requires(P p, typename P::value_type v) {
    { p.remove(v) } -> std::same_as<bool>;
};

/// A LivenessStrategy decides whether a stored value is still alive.
/// - is_alive(value) -> bool
/// Evaluated at scan time under the source spinlock.
template <class L, class T>
concept LivenessStrategy = requires(L l, const T& v) {
    { l.is_alive(v) } -> std::same_as<bool>;
};

} // namespace seqjoin
