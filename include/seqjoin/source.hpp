// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>

#include "core/spinlock.hpp"
#include "core/seq_counter.hpp"
#include "core/snap_entry.hpp"
#include "core/concepts.hpp"
#include "liveness/always_alive.hpp"
#include "policy/retain_latest.hpp"
#include "source_view.hpp"

namespace seqjoin {

// ─── Default-policy trait ────────────────────────────────────────────
// Maps a bare type T to its default policy. Specializable by users.
// Primary: RetainLatest<T>.
template <class T>
struct DefaultPolicy { using type = RetainLatest<T>; };

template <class T>
using DefaultPolicy_t = typename DefaultPolicy<T>::type;

/// Source<T, Policy, Liveness> — a single input slot in an NJoin.
///
/// Combines: retention policy (what to store) + liveness strategy (when values die)
///           + per-source spinlock (never held during emit).
///
/// Thread safety: all public methods are thread-safe.
/// The spinlock is exclusive (Rigtorp) and held only for the duration of
/// insert or scan — never during the emit callback.
///
/// Convenience: Source<T> defaults Policy to RetainLatest<T>.
template <class T,
          class Policy   = DefaultPolicy_t<T>,
          class Liveness = AlwaysAlive>
    requires RetentionPolicy<Policy>
class Source {
public:
    using value_type = T;
    using policy_type = Policy;
    using liveness_type = Liveness;

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
    std::optional<uint64_t> insert(const T& value, uint64_t seq) requires std::copy_constructible<T> {
        std::lock_guard<SpinLock> guard(lock_);
        return policy_.insert(value, seq);
    }
    std::optional<uint64_t> insert(T&& value, uint64_t seq) {
        std::lock_guard<SpinLock> guard(lock_);
        return policy_.insert(std::move(value), seq);
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

    /// Remove a value. Only available when the policy supports removal.
    bool remove(const T& value)
        requires RemovablePolicy<Policy>
    {
        std::lock_guard<SpinLock> guard(lock_);
        return policy_.remove(value);
    }

    /// Snapshot: returns a policy-appropriate snapshot under the lock.
    /// For COW policies (those providing snapshot()): O(1) shared_ptr grab.
    /// For others: collects values into a Snapshot<T> vector.
    auto snapshot() {
        std::lock_guard<SpinLock> guard(lock_);
        if constexpr (requires { policy_.snapshot(); }) {
            // COW policy — O(1) snapshot via shared_ptr
            static_assert(std::is_same_v<Liveness, AlwaysAlive>,
                          "COW snapshot requires AlwaysAlive liveness (liveness filtering not yet supported)");
            return policy_.snapshot();
        } else if constexpr (requires { policy_.scan().has_value(); }) {
            // RetainLatest — optional<pair>
            Snapshot<T> snap;
            const auto& data = policy_.scan();
            if (data.has_value()) {
                const auto& [val, seq] = *data;
                if (liveness_.is_alive(val)) {
                    snap.emplace_back(val, seq);
                }
            }
            return snap;
        } else {
            // RetainAll / RetainByKey — iterable range
            Snapshot<T> snap;
            for (const auto& [val, seq] : policy_.scan()) {
                if (liveness_.is_alive(val)) {
                    snap.emplace_back(val, seq);
                }
            }
            return snap;
        }
    }

    /// Create a projected view into this source (only for tuple-valued sources).
    /// Returns a SourceView that projects the specified tuple indices.
    ///
    /// Example:
    ///   Source<tuple<A,B,C>> src;
    ///   auto v_ab = src.view<0,1>();  // projects (A,B)
    ///   auto v_c  = src.view<2>();    // projects C
    ///
    template <std::size_t... ProjIs>
        requires (std::tuple_size<T>::value > 0)
    auto view() {
        return SourceView<Source, ProjIs...>(*this);
    }

private:
    Policy policy_{};
    [[no_unique_address]] Liveness liveness_{};
    SpinLock lock_;
};

} // namespace seqjoin
