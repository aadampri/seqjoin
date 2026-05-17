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

/// BarrierView<T, HandleExtract, Key, Handle> — filtered + gated subscription.
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
/// Moveable: internal state lives on the heap behind shared_ptr. Subscribers
/// hold a weak_ptr — if the BarrierView is destroyed, they auto-expire.
///
/// Template parameters:
///   T             — source value type (e.g., ImageView{id, handle})
///   HandleExtract — callable: const T& → Handle (default: identity)
///   Key           — key type, deduced from std::get<0>(T) by default
///   Handle        — the type exposed to the NJoin, deduced from HandleExtract
///
/// Thread safety:
///   - on_change() is called from SharedSource's subscriber fire (potentially
///     from any thread that does insert/remove). The internal spinlock serializes.
///   - The injector (NJoin's add<I>) is thread-safe by NJoin's own guarantees.
template <class T,
          class Key,
          class HandleExtract = std::identity,
          class Handle        = std::decay_t<std::invoke_result_t<HandleExtract, const T&>>>
class BarrierView {
public:
    using key_type    = Key;
    using handle_type = Handle;
    using injector_fn = std::function<void(std::span<const Handle>)>;

    /// Construct a BarrierView.
    ///   source    — the SharedSource to subscribe to
    ///   keys      — ordered key list determining output layout (duplicates allowed)
    ///   injector  — function to call when barrier is complete/updated
    ///               (typically wraps NJoin::add<I>())
    ///   extract   — callable to extract Handle from T (default: identity)
    template <class Source>
    BarrierView(Source& source,
                std::vector<Key> keys,
                injector_fn injector,
                HandleExtract extract = {})
        : state_(std::make_shared<State>(std::move(keys), std::move(injector), std::move(extract)))
    {
        // Subscribe with weak_ptr — auto-expires when BarrierView is destroyed
        source.subscribe([weak = std::weak_ptr<State>(state_)](const Key& key, const auto& src) -> bool {
            auto state = weak.lock();
            if (!state) return false;  // dead — remove from subscriber list
            state->on_change(key, src);
            return true;
        });

        // Initial evaluation — check if already complete
        state_->evaluate_full(source);
    }

    /// Change the required key-set at runtime (e.g., framebuffer reconfiguration).
    template <class Source>
    void set_required_keys(std::vector<Key> new_keys, const Source& source) {
        state_->set_required_keys(std::move(new_keys), source);
    }

    /// Replace the injector (e.g., after NJoin is moved/rebuilt).
    void set_injector(injector_fn fn) {
        state_->set_injector(std::move(fn));
    }

    /// Query: is the barrier currently complete?
    [[nodiscard]] bool is_complete() const noexcept {
        return state_->is_complete();
    }

    /// Query: current required key count.
    [[nodiscard]] std::size_t required_count() const noexcept {
        return state_->required_count();
    }

private:
    struct State {
        std::vector<Key> keys_ordered;          // output layout order (may have duplicates)
        std::unordered_set<Key> keys_set;       // for fast on_change filtering (unique)
        std::vector<Handle> assembled;
        bool complete = false;
        injector_fn injector;
        [[no_unique_address]] HandleExtract extract{};
        mutable SpinLock lock;

        State(std::vector<Key> ordered, injector_fn inj, HandleExtract ext)
            : keys_ordered(std::move(ordered))
            , keys_set(keys_ordered.begin(), keys_ordered.end())
            , injector(std::move(inj))
            , extract(std::move(ext))
        {
            assembled.reserve(keys_ordered.size());
        }

        void set_required_keys(std::vector<Key> new_keys, const auto& source) {
            std::lock_guard<SpinLock> guard(lock);
            keys_ordered = std::move(new_keys);
            keys_set.clear();
            keys_set.insert(keys_ordered.begin(), keys_ordered.end());
            assembled.clear();
            assembled.reserve(keys_ordered.size());
            complete = false;
            evaluate_locked(source);
        }

        void set_injector(injector_fn fn) {
            std::lock_guard<SpinLock> guard(lock);
            injector = std::move(fn);
        }

        [[nodiscard]] bool is_complete() const noexcept {
            std::lock_guard<SpinLock> guard(lock);
            return complete;
        }

        [[nodiscard]] std::size_t required_count() const noexcept {
            std::lock_guard<SpinLock> guard(lock);
            return keys_ordered.size();
        }

        void on_change(const Key& key, const auto& source) {
            std::lock_guard<SpinLock> guard(lock);
            if (!keys_set.contains(key)) return;
            evaluate_locked(source);
        }

        void evaluate_full(const auto& source) {
            std::lock_guard<SpinLock> guard(lock);
            evaluate_locked(source);
        }

    private:
        void evaluate_locked(const auto& source) {
            auto snap = source.snapshot();
            assembled.clear();

            for (const auto& key : keys_ordered) {
                auto it = snap->find(key);
                if (it == snap->end()) {
                    complete = false;
                    return;
                }
                assembled.push_back(extract(*it->second));
            }
            complete = true;
            injector(std::span<const Handle>(assembled));
        }
    };

    std::shared_ptr<State> state_;
};

/// Convenience: create a BarrierView that injects into an NJoin's add<SourceIdx>.
///
/// Usage:
///   auto barrier = make_barrier<0>(shared_views, fb_join, 
///                               std::vector<int>{0, 1, 3},
///                               [](const ImageView& v) { return v.handle; });
///
template <std::size_t SourceIdx, class Source, class Join, class Extract = std::identity>
auto make_barrier(Source& source, Join& join, std::vector<typename Source::key_type> keys, Extract extract = {}) {
    using T = typename Source::value_type;
    using Key = typename Source::key_type;
    using Handle = std::decay_t<std::invoke_result_t<Extract, const T&>>;

    auto injector = [&join](std::span<const Handle> handles) {
        std::vector<Handle> vec(handles.begin(), handles.end());
        join.template add<SourceIdx>(std::move(vec));
    };

    return BarrierView<T, Key, Extract, Handle>(
        source,
        std::move(keys),
        std::move(injector),
        std::move(extract));
}

} // namespace seqjoin
