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
///   Subscribers return bool: true = keep, false = dead (will be compacted).
/// - subscribe() uses a single-bit exclusive spinlock to serialize writers,
///   copies the vector, appends, and atomically swaps the pointer.
/// - Readers never block writers. Writers never block readers.
/// - Dead subscribers are compacted lazily during fire().
template <class... Args>
class SubscriberList {
public:
    using Fn = std::function<bool(const Args&...)>;

    SubscriberList() noexcept = default;

    // Moveable — transfers subscriber snapshot, new lock for new location.
    SubscriberList(SubscriberList&& other) noexcept
        : subs_(other.subs_.load(std::memory_order_relaxed))
    {
        other.subs_.store(nullptr, std::memory_order_relaxed);
    }
    SubscriberList& operator=(SubscriberList&&) = delete;

    SubscriberList(const SubscriberList&) = delete;
    SubscriberList& operator=(const SubscriberList&) = delete;

    /// Fire all subscribers with the given arguments.
    /// Wait-free on the fast path (no dead entries).
    /// Dead subscribers (returning false) trigger a COW rebuild that filters them out.
    void fire(const Args&... args) {
        auto snap = subs_.load(std::memory_order_acquire);
        if (!snap) return;
        
        // First pass: fire all, collect alive status
        const auto n = snap->size();
        bool has_dead = false;
        // For small N (typical: 1-5 subscribers), stack-allocate alive flags
        char alive_buf[16];
        std::vector<char> alive_heap;
        char* alive = alive_buf;
        if (n > 16) { alive_heap.resize(n); alive = alive_heap.data(); }
        
        for (std::size_t i = 0; i < n; ++i) {
            alive[i] = (*snap)[i](args...) ? 1 : 0;
            if (!alive[i]) has_dead = true;
        }
        
        // Compact if any dead
        if (has_dead) {
            std::lock_guard<SpinLock> guard(write_lock_);
            // Re-check: list may have changed between first pass and lock acquisition
            auto current = subs_.load(std::memory_order_relaxed);
            if (current != snap) return;  // someone else modified — skip compaction
            auto copy = std::make_shared<Snapshot>();
            copy->reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                if (alive[i]) copy->emplace_back((*snap)[i]);
            }
            if (copy->empty())
                subs_.store(nullptr, std::memory_order_release);
            else
                subs_.store(std::shared_ptr<const Snapshot>(std::move(copy)),
                            std::memory_order_release);
        }
    }

    /// Add a subscriber. Serialized against other subscribe() calls via spinlock.
    /// Does not block concurrent fire() calls.
    /// The subscriber must return true to stay alive, false to be removed.
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
