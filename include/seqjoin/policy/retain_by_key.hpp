// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <concepts>
#include <functional>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "key_projection.hpp"

namespace seqjoin {

/// RetainByKey<T, KeyProj> — keyed upsert policy.
///
/// Stores one entry per unique key. On insert:
///   - New key: insert (emit triggers)
///   - Same key, different value: overwrite (emit triggers)
///   - Same key, same value: reject (return nullopt, no emit)
///
/// The key is extracted from T via KeyProj (default: std::get<0>(t)).
/// Internally: unordered_map<Key, pair<T, seq>>
///   → key is stored once in the map key
///   → full T is stored in the value (unavoidable for scan() to yield T)
///     Note: for SSO-eligible strings (≤22 chars), this is free.
///     A future split-storage variant can eliminate this for large keys.
///
/// scan() interface: returns a view where iteration yields (T&, uint64_t&)
/// pairs, compatible with Source's existing scan loop.
template <class T,
          class KeyProj = ProjectFirst,
          class Hash    = std::hash<std::decay_t<std::invoke_result_t<KeyProj, const T&>>>,
          class Eq      = std::equal_to<std::decay_t<std::invoke_result_t<KeyProj, const T&>>>>
class RetainByKey {
public:
    using value_type = T;
    using key_type = std::decay_t<std::invoke_result_t<KeyProj, const T&>>;

    /// Insert with keyed upsert semantics.
    /// Returns seq on success (new key or changed value), nullopt if unchanged.
    std::optional<uint64_t> insert(const T& value, uint64_t seq) requires std::copy_constructible<T> {
        auto key = key_proj_(value);
        auto it = data_.find(key);
        if (it != data_.end()) {
            if (it->second.first == value)
                return std::nullopt;
            it->second = {value, seq};
            return seq;
        }
        data_.emplace(std::move(key), std::pair{value, seq});
        return seq;
    }
    std::optional<uint64_t> insert(T&& value, uint64_t seq) {
        auto key = key_proj_(value);
        auto it = data_.find(key);
        if (it != data_.end()) {
            if (it->second.first == value)
                return std::nullopt;
            it->second = {std::move(value), seq};
            return seq;
        }
        data_.emplace(std::move(key), std::pair{std::move(value), seq});
        return seq;
    }

    /// Scan view: adapts map iteration to yield (const T&, uint64_t) pairs.
    /// Compatible with Source's `for (auto& [val, seq] : policy_.scan())` loop.
    class ScanView {
        using Map = std::unordered_map<key_type, std::pair<T, uint64_t>, Hash, Eq>;
        const Map& map_;
    public:
        explicit ScanView(const Map& m) : map_(m) {}

        struct Iterator {
            using inner_it = typename Map::const_iterator;
            inner_it it_;

            // Yields pair<const T&, const uint64_t&> to match the [val, seq] pattern
            std::pair<const T&, const uint64_t&> operator*() const {
                return {it_->second.first, it_->second.second};
            }
            Iterator& operator++() { ++it_; return *this; }
            bool operator!=(const Iterator& o) const { return it_ != o.it_; }
            bool operator==(const Iterator& o) const { return it_ == o.it_; }
        };

        Iterator begin() const { return {map_.begin()}; }
        Iterator end() const { return {map_.end()}; }
    };

    [[nodiscard]] ScanView scan() const noexcept { return ScanView{data_}; }

    /// Number of stored entries (one per unique key).
    [[nodiscard]] std::size_t size() const noexcept { return data_.size(); }

    [[nodiscard]] bool empty() const noexcept { return data_.empty(); }

    /// Remove by key. Returns true if an entry was removed.
    bool remove_by_key(const key_type& key) { return data_.erase(key) > 0; }

    /// Remove by full value (extracts key, then erases). Returns true if found.
    bool remove(const T& value) { return data_.erase(key_proj_(value)) > 0; }

    void clear() { data_.clear(); }

private:
    std::unordered_map<key_type, std::pair<T, uint64_t>, Hash, Eq> data_;
    [[no_unique_address]] KeyProj key_proj_{};
};

} // namespace seqjoin
