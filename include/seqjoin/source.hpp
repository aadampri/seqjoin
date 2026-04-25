// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>

#include "core/spinlock.hpp"
#include "core/seq_counter.hpp"
#include "liveness/always_alive.hpp"

namespace seqjoin {

/// Source<T, Policy, Liveness> — a single input slot in an NJoin.
///
/// Combines: retention policy (what to store) + liveness strategy (when values die)
///           + per-source spinlock (never held during emit).
///
/// Thread safety: all public methods are thread-safe.
/// The spinlock is exclusive (Rigtorp) and held only for the duration of
/// insert or scan — never during the emit callback.
template <class T,
          class Policy,
          class Liveness = AlwaysAlive>
class Source {
public:
    using value_type = T;

    Source() noexcept = default;
    explicit Source(Policy policy) : policy_(std::move(policy)) {}
    Source(Policy policy, Liveness liveness)
        : policy_(std::move(policy)), liveness_(std::move(liveness)) {}

    // Movable (lock_ is default-constructed in the new object)
    Source(Source&& other) noexcept
        : policy_(std::move(other.policy_))
        , liveness_(std::move(other.liveness_)) {}
    Source& operator=(Source&&) = delete;
    Source(const Source&) = delete;
    Source& operator=(const Source&) = delete;

    /// Insert a value with a pre-assigned seq.
    /// Returns the seq on success, nullopt if rejected (e.g., duplicate in RetainAll).
    std::optional<uint64_t> insert(const T& value, uint64_t seq) {
        std::lock_guard<SpinLock> guard(lock_);
        return policy_.insert(value, seq);
    }

    /// Scan under lock: calls visitor(value, seq) for each alive stored element.
    /// The spinlock is held for the duration of the scan.
    /// Returns the number of elements visited.
    template <class Visitor>
    std::size_t scan(Visitor&& visitor) {
        std::lock_guard<SpinLock> guard(lock_);
        std::size_t count = 0;
        if constexpr (requires { policy_.scan().has_value(); }) {
            // RetainLatest — optional<pair<T, seq>>
            const auto& data = policy_.scan();
            if (data.has_value()) {
                const auto& [val, seq] = *data;
                if (liveness_.is_alive(val)) {
                    visitor(val, seq);
                    ++count;
                }
            }
        } else {
            // RetainAll — map<T, seq>
            for (const auto& [val, seq] : policy_.scan()) {
                if (liveness_.is_alive(val)) {
                    visitor(val, seq);
                    ++count;
                }
            }
        }
        return count;
    }

    /// Snapshot scan: collects results into a vector while holding the lock,
    /// then releases the lock. Caller iterates the vector outside the lock.
    /// This is used by NJoin to avoid holding the lock during cross-product/emit.
    template <class Container>
    void scan_into(Container& out) {
        std::lock_guard<SpinLock> guard(lock_);
        if constexpr (requires { policy_.scan().has_value(); }) {
            const auto& data = policy_.scan();
            if (data.has_value()) {
                const auto& [val, seq] = *data;
                if (liveness_.is_alive(val)) {
                    out.emplace_back(val, seq);
                }
            }
        } else {
            for (const auto& [val, seq] : policy_.scan()) {
                if (liveness_.is_alive(val)) {
                    out.emplace_back(val, seq);
                }
            }
        }
    }

    void clear() {
        std::lock_guard<SpinLock> guard(lock_);
        policy_.clear();
    }

private:
    Policy policy_{};
    Liveness liveness_{};
    SpinLock lock_;
};

} // namespace seqjoin
