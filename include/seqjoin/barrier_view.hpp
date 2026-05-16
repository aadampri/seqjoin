// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/spinlock.hpp"
#include "shared_source.hpp"

namespace seqjoin {

/// BarrierView<T, Key, Handle, HandleExtract> — filtered + gated subscription.
///
/// Connects a SharedSource to a specific NJoin slot. The BarrierView:
///   1. Filters: only reacts to keys in its required set.
///   2. Gates: suppresses output until ALL required keys are present.
///   3. Assembles: builds a contiguous array of extracted handles.
///
/// When the barrier transitions from incomplete → complete (or a relevant key
/// changes while complete), it calls the injector function to push the assembled
/// span into the downstream NJoin slot.
///
/// Template parameters:
///   T             — source value type (e.g., ImageView{id, handle})
///   Key           — key type (e.g., int)
///   Handle        — the type exposed to the NJoin (e.g., VkImageView)
///   HandleExtract — callable: const T& → Handle (extracts the handle from T)
///
/// Thread safety:
///   - re-evaluate() is called from SharedSource's subscriber fire (potentially
///     from any thread that does insert/remove). The internal spinlock serializes.
///   - The injector (NJoin's add<I>) is thread-safe by NJoin's own guarantees.
template <class T,
          class Key           = std::decay_t<decltype(std::get<0>(std::declval<const T&>()))>,
          class Handle        = T,
          class HandleExtract = std::identity>
class BarrierView {
public:
    using key_type    = Key;
    using handle_type = Handle;
    using injector_fn = std::function<void(std::span<const Handle>)>;

    /// Construct a BarrierView.
    ///   source    — the SharedSource to subscribe to
    ///   keys      — the required key-set (runtime)
    ///   injector  — function to call when barrier is complete/updated
    ///               (typically wraps NJoin::add<I>())
    ///   extract   — callable to extract Handle from T (default: identity)
    template <class Source>
    BarrierView(Source& source,
                std::unordered_set<Key> keys,
                injector_fn injector,
                HandleExtract extract = {})
        : required_keys_(std::move(keys))
        , injector_(std::move(injector))
        , extract_(std::move(extract))
    {
        assembled_.reserve(required_keys_.size());

        // Subscribe to the SharedSource for change notifications
        source.subscribe([this](const Key& key, const auto& src) {
            on_change(key, src);
        });

        // Initial evaluation — check if already complete
        evaluate_full(source);
    }

    /// Change the required key-set at runtime (e.g., framebuffer reconfiguration).
    template <class Source>
    void set_required_keys(std::unordered_set<Key> new_keys, const Source& source) {
        std::lock_guard<SpinLock> guard(lock_);
        required_keys_ = std::move(new_keys);
        assembled_.clear();
        assembled_.reserve(required_keys_.size());
        complete_ = false;
        evaluate_locked(source);
    }

    /// Query: is the barrier currently complete?
    [[nodiscard]] bool is_complete() const noexcept {
        std::lock_guard<SpinLock> guard(lock_);
        return complete_;
    }

    /// Query: current required key count.
    [[nodiscard]] std::size_t required_count() const noexcept {
        std::lock_guard<SpinLock> guard(lock_);
        return required_keys_.size();
    }

private:
    std::unordered_set<Key> required_keys_;
    std::vector<Handle> assembled_;
    bool complete_ = false;
    injector_fn injector_;
    [[no_unique_address]] HandleExtract extract_{};
    mutable SpinLock lock_;

    // Ordered keys for deterministic assembly order
    // (stored as a sorted vector for stable handle array output)
    std::vector<Key> ordered_keys() const {
        std::vector<Key> keys(required_keys_.begin(), required_keys_.end());
        std::sort(keys.begin(), keys.end());
        return keys;
    }

    /// Called by SharedSource subscriber when a key changes.
    template <class Source>
    void on_change(const Key& key, const Source& source) {
        std::lock_guard<SpinLock> guard(lock_);
        if (!required_keys_.contains(key)) return;  // irrelevant key
        evaluate_locked(source);
    }

    /// Full evaluation (called on construction and key-set change).
    template <class Source>
    void evaluate_full(const Source& source) {
        std::lock_guard<SpinLock> guard(lock_);
        evaluate_locked(source);
    }

    /// Core evaluation logic (must be called under lock_).
    /// Checks if all required keys are present, assembles handles, fires injector.
    template <class Source>
    void evaluate_locked(const Source& source) {
        auto snap = source.snapshot();
        assembled_.clear();

        auto keys = ordered_keys();
        for (const auto& key : keys) {
            auto it = snap->find(key);
            if (it == snap->end()) {
                complete_ = false;
                return;  // incomplete — stop
            }
            assembled_.push_back(extract_(*it->second));
        }
        complete_ = true;

        // Fire the injector (outside conceptual lock — but we're under lock_ here
        // to prevent re-entrant evaluation. The NJoin's add() is safe to call
        // while we hold our lock since NJoin has its own per-source locks.)
        injector_(std::span<const Handle>(assembled_));
    }
};

/// Convenience: create a BarrierView that injects into an NJoin's add<SourceIdx>.
///
/// Usage:
///   auto barrier = make_barrier(shared_views, fb_join, 
///                               std::unordered_set<int>{0, 1, 3},
///                               [](const ImageView& v) { return v.handle; });
///
template <std::size_t SourceIdx, class Source, class Join, class Keys, class Extract = std::identity>
auto make_barrier(Source& source, Join& join, Keys&& keys, Extract extract = {}) {
    using T = typename Source::value_type;
    using Key = typename Source::key_type;
    using Handle = std::decay_t<std::invoke_result_t<Extract, const T&>>;

    auto injector = [&join](std::span<const Handle> handles) {
        // Pack the span into a vector for the NJoin source
        // (NJoin source stores the value — here a vector of handles)
        std::vector<Handle> vec(handles.begin(), handles.end());
        join.template add<SourceIdx>(std::move(vec));
    };

    return BarrierView<T, Key, Handle, Extract>(
        source,
        std::unordered_set<Key>(std::forward<Keys>(keys)),
        std::move(injector),
        std::move(extract));
}

} // namespace seqjoin
