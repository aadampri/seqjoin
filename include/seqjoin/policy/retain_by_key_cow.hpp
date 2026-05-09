// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <concepts>
#include <functional>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "../core/snap_entry.hpp"

namespace seqjoin {

/// Default key projector: extracts std::get<0> from a tuple-like type.
/// (Reuses the same ProjectFirst from retain_by_key.hpp; included here
///  for self-contained compilation when only this header is used.)
struct ProjectFirstCOW {
    template <class T>
    decltype(auto) operator()(const T& t) const { return std::get<0>(t); }
};

/// CowKeyedSnapshot — immutable snapshot from a RetainByKeyCOW policy.
///
/// Wraps a shared_ptr to an immutable keyed map. Iteration yields SnapRef<T>
/// (pointer to value + seq), compatible with the cross-product engine.
/// Values are stored as shared_ptr<const T> so the map clone never copies T.
template <class T, class Key, class Hash, class Eq>
class CowKeyedSnapshot {
public:
    using map_type = std::unordered_map<Key, std::pair<std::shared_ptr<const T>, uint64_t>, Hash, Eq>;
    using value_type = SnapRef<T>;

    CowKeyedSnapshot() = default;
    explicit CowKeyedSnapshot(std::shared_ptr<const map_type> d) : data_(std::move(d)) {}

    [[nodiscard]] bool empty() const { return !data_ || data_->empty(); }
    [[nodiscard]] std::size_t size() const { return data_ ? data_->size() : 0; }

    struct Iterator {
        using inner_it = typename map_type::const_iterator;
        inner_it it_;

        SnapRef<T> operator*() const {
            return SnapRef<T>{*it_->second.first, it_->second.second};
        }
        Iterator& operator++() { ++it_; return *this; }
        bool operator!=(const Iterator& o) const { return it_ != o.it_; }
        bool operator==(const Iterator& o) const { return it_ == o.it_; }
    };

    Iterator begin() const { return Iterator{data_->begin()}; }
    Iterator end() const { return Iterator{data_->end()}; }

private:
    std::shared_ptr<const map_type> data_;
};

/// RetainByKeyCOW<T, KeyProj> — copy-on-write keyed upsert policy.
///
/// Combines RetainByKey's keyed upsert semantics with COW snapshot mechanics.
/// Values are stored as shared_ptr<const T> so the map clone only copies
/// pointers (zero T copies), enabling move-only types.
///
///   - insert: COW-clones the map (O(n) pointer copies), applies upsert, swaps in.
///   - snapshot(): O(1) — returns shared_ptr to current immutable map.
///   - remove: COW-clones, erases, swaps. O(n) pointer copies.
///
/// Keyed upsert semantics (same as RetainByKey):
///   - New key: insert (emit triggers)
///   - Same key, different value: overwrite (emit triggers)
///   - Same key, same value: reject (return nullopt, no emit)
///
/// Trade-off vs RetainByKey:
///   - Insert: O(n) pointer copies (COW clone) vs O(1) amortized
///   - Snapshot: O(1) (shared_ptr copy) vs O(n) (copy all values)
///   - Best for: large keyed registries with frequent snapshots, move-only types
template <class T,
          class KeyProj = ProjectFirstCOW,
          class Hash    = std::hash<std::decay_t<std::invoke_result_t<KeyProj, const T&>>>,
          class Eq      = std::equal_to<std::decay_t<std::invoke_result_t<KeyProj, const T&>>>>
class RetainByKeyCOW {
public:
    using value_type = T;
    using key_type = std::decay_t<std::invoke_result_t<KeyProj, const T&>>;
    using map_type = std::unordered_map<key_type, std::pair<std::shared_ptr<const T>, uint64_t>, Hash, Eq>;

    RetainByKeyCOW()
        : data_(std::make_shared<const map_type>()) {}

    /// Insert with keyed upsert semantics (COW).
    std::optional<uint64_t> insert(const T& value, uint64_t seq) requires std::copy_constructible<T> {
        auto key = key_proj_(value);
        auto it = data_->find(key);
        if (it != data_->end() && *it->second.first == value)
            return std::nullopt;  // same key+value → reject
        auto copy = std::make_shared<map_type>(*data_);
        (*copy)[std::move(key)] = {std::make_shared<const T>(value), seq};
        data_ = std::move(copy);
        return seq;
    }
    std::optional<uint64_t> insert(T&& value, uint64_t seq) {
        auto key = key_proj_(value);
        auto it = data_->find(key);
        if (it != data_->end() && *it->second.first == value)
            return std::nullopt;
        auto copy = std::make_shared<map_type>(*data_);
        (*copy)[std::move(key)] = {std::make_shared<const T>(std::move(value)), seq};
        data_ = std::move(copy);
        return seq;
    }

    /// O(1) snapshot — shared_ptr to current immutable map.
    [[nodiscard]] CowKeyedSnapshot<T, key_type, Hash, Eq> snapshot() const {
        return CowKeyedSnapshot<T, key_type, Hash, Eq>{data_};
    }

    /// ScanView for Source::scan() compatibility (visitor-based path).
    class ScanView {
        const map_type& map_;
    public:
        explicit ScanView(const map_type& m) : map_(m) {}

        struct Iterator {
            typename map_type::const_iterator it_;

            std::pair<const T&, const uint64_t&> operator*() const {
                return {*it_->second.first, it_->second.second};
            }
            Iterator& operator++() { ++it_; return *this; }
            bool operator!=(const Iterator& o) const { return it_ != o.it_; }
            bool operator==(const Iterator& o) const { return it_ == o.it_; }
        };

        Iterator begin() const { return {map_.begin()}; }
        Iterator end() const { return {map_.end()}; }
    };

    [[nodiscard]] ScanView scan() const noexcept { return ScanView{*data_}; }

    [[nodiscard]] std::size_t size() const noexcept { return data_->size(); }
    [[nodiscard]] bool empty() const noexcept { return data_->empty(); }

    /// Remove by full value (extracts key, COW clone). Returns true if found.
    bool remove(const T& value) {
        auto key = key_proj_(value);
        if (data_->find(key) == data_->end()) return false;
        auto copy = std::make_shared<map_type>(*data_);
        copy->erase(key);
        data_ = std::move(copy);
        return true;
    }

    /// Remove by key directly (COW clone). Returns true if found.
    bool remove_by_key(const key_type& key) {
        if (data_->find(key) == data_->end()) return false;
        auto copy = std::make_shared<map_type>(*data_);
        copy->erase(key);
        data_ = std::move(copy);
        return true;
    }

    void clear() {
        data_ = std::make_shared<const map_type>();
    }

private:
    std::shared_ptr<const map_type> data_;
    [[no_unique_address]] KeyProj key_proj_{};
};

} // namespace seqjoin
