// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/spinlock.hpp"
#include "core/subscriber_list.hpp"

namespace seqjoin {

/// SharedSource<T, KeyProj, Key, Hash, Eq> — a reactive keyed store.
///
/// Holds the single canonical state for a pool of keyed values.
/// Multiple NJoins can subscribe via BarrierView handles.
///
/// NOT an NJoin — no seq counter, no cross-product, no ownership check.
/// It is purely a mutable store + notification mechanism.
///
/// Thread safety: insert/remove are serialized via spinlock.
/// Notification (fire) is wait-free (COW subscriber list).
///
/// Template parameters:
///   T       — stored value type (e.g., ImageView, RenderPass)
///   KeyProj — callable: const T& → Key (default: std::get<0>)
///   Key     — key type, deduced from KeyProj return type
///   Hash    — hash for Key
///   Eq      — equality for Key
template <class T,
          class KeyProj = decltype([](const T& v) -> decltype(auto) { return std::get<0>(v); }),
          class Key     = std::decay_t<std::invoke_result_t<KeyProj, const T&>>,
          class Hash    = std::hash<Key>,
          class Eq      = std::equal_to<Key>>
class SharedSource {
public:
    using value_type = T;
    using key_type   = Key;
    using map_type   = std::unordered_map<Key, std::shared_ptr<const T>, Hash, Eq>;

    SharedSource() : data_(std::make_shared<const map_type>()) {}

    /// Insert or update a value. Returns true if the store changed (new key or different value).
    /// Notifies subscribers with the affected key.
    bool insert(const T& value) requires std::copy_constructible<T> {
        Key key = key_proj_(value);
        bool changed = false;
        {
            std::lock_guard<SpinLock> guard(lock_);
            auto it = data_->find(key);
            if (it != data_->end() && *it->second == value)
                return false;  // same key+value → no change
            auto copy = std::make_shared<map_type>(*data_);
            (*copy)[key] = std::make_shared<const T>(value);
            data_ = std::move(copy);
            changed = true;
        }
        if (changed) subscribers_.fire(key, *this);
        return true;
    }

    bool insert(T&& value) {
        Key key = key_proj_(value);
        bool changed = false;
        {
            std::lock_guard<SpinLock> guard(lock_);
            auto it = data_->find(key);
            if (it != data_->end() && *it->second == value)
                return false;
            auto copy = std::make_shared<map_type>(*data_);
            auto val_ptr = std::make_shared<const T>(std::move(value));
            (*copy)[key] = val_ptr;
            data_ = std::move(copy);
            changed = true;
        }
        if (changed) subscribers_.fire(key, *this);
        return true;
    }

    /// Remove a value by key. Returns true if found and removed.
    /// Notifies subscribers with the removed key.
    bool remove(const Key& key) {
        bool changed = false;
        {
            std::lock_guard<SpinLock> guard(lock_);
            if (!data_->contains(key)) return false;
            auto copy = std::make_shared<map_type>(*data_);
            copy->erase(key);
            data_ = std::move(copy);
            changed = true;
        }
        if (changed) subscribers_.fire(key, *this);
        return true;
    }

    /// O(1) snapshot — returns shared_ptr to current immutable map.
    [[nodiscard]] std::shared_ptr<const map_type> snapshot() const {
        std::lock_guard<SpinLock> guard(lock_);
        return data_;
    }

    /// Check if a key exists.
    [[nodiscard]] bool contains(const Key& key) const {
        auto snap = snapshot();
        return snap->contains(key);
    }

    /// Get a value by key (nullptr if not present).
    [[nodiscard]] std::shared_ptr<const T> get(const Key& key) const {
        auto snap = snapshot();
        auto it = snap->find(key);
        return it != snap->end() ? it->second : nullptr;
    }

    /// Number of entries.
    [[nodiscard]] std::size_t size() const {
        auto snap = snapshot();
        return snap->size();
    }

    /// Subscribe to change notifications.
    /// The callback receives (key, SharedSource&) on every insert/remove.
    void subscribe(std::function<void(const Key&, const SharedSource&)> fn) {
        subscribers_.subscribe(std::move(fn));
    }

private:
    std::shared_ptr<const map_type> data_;
    mutable SpinLock lock_;
    SubscriberList<Key, SharedSource> subscribers_;
    [[no_unique_address]] KeyProj key_proj_{};
};

} // namespace seqjoin
