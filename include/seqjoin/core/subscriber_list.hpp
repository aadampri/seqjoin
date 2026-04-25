// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "spinlock.hpp"

namespace seqjoin {

/// Thread-safe subscriber list using COW (copy-on-write).
///
/// - fire() is wait-free: atomic load of a shared_ptr snapshot, then iterate.
/// - subscribe() uses a single-bit exclusive spinlock to serialize writers,
///   copies the vector, appends, and atomically swaps the pointer.
/// - Readers never block writers. Writers never block readers.
template <class... Args>
class SubscriberList {
public:
    using Fn = std::function<void(const Args&...)>;

    SubscriberList() noexcept = default;

    SubscriberList(const SubscriberList&) = delete;
    SubscriberList& operator=(const SubscriberList&) = delete;

    /// Fire all subscribers with the given arguments.
    /// Wait-free: takes no lock, iterates an immutable snapshot.
    void fire(const Args&... args) {
        auto snap = subs_.load(std::memory_order_acquire);
        if (!snap) return;
        for (const auto& fn : *snap) {
            fn(args...);
        }
    }

    /// Add a subscriber. Serialized against other subscribe() calls via spinlock.
    /// Does not block concurrent fire() calls.
    void subscribe(Fn fn) {
        std::lock_guard<SpinLock> guard(write_lock_);

        auto old = subs_.load(std::memory_order_relaxed);
        auto copy = old ? std::make_shared<Snapshot>(*old)
                        : std::make_shared<Snapshot>();
        copy->emplace_back(std::move(fn));
        subs_.store(std::shared_ptr<const Snapshot>(std::move(copy)),
                    std::memory_order_release);
    }

    /// Number of subscribers (approximate — may race with subscribe()).
    [[nodiscard]] std::size_t size() const noexcept {
        auto snap = subs_.load(std::memory_order_relaxed);
        return snap ? snap->size() : 0;
    }

    /// Force-emit pending (for shutdown of debounced adapters wired to this list).
    [[nodiscard]] bool empty() const noexcept { return size() == 0; }

private:
    using Snapshot = std::vector<Fn>;

    std::atomic<std::shared_ptr<const Snapshot>> subs_;
    SpinLock write_lock_;
};

} // namespace seqjoin
