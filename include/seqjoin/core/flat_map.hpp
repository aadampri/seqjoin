// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <utility>
#include <vector>

namespace seqjoin {

/// FlatMap<Key, Value> — sorted vector of pairs.
///
/// Minimal associative container with the same interface subset used by
/// SharedSource (find, operator[], erase, contains, begin/end, empty, size).
///
/// Optimized for small N (< ~30 entries): cache-friendly, zero per-element
/// overhead, trivial to clone for COW. Linear scan for find when N < 8,
/// binary search otherwise.
template <class Key, class Value, class Compare = std::less<Key>>
class FlatMap {
public:
    using key_type    = Key;
    using mapped_type = Value;
    using value_type  = std::pair<Key, Value>;
    using iterator    = typename std::vector<value_type>::iterator;
    using const_iterator = typename std::vector<value_type>::const_iterator;

    FlatMap() = default;

    iterator begin() noexcept { return data_.begin(); }
    iterator end() noexcept { return data_.end(); }
    const_iterator begin() const noexcept { return data_.begin(); }
    const_iterator end() const noexcept { return data_.end(); }

    [[nodiscard]] bool empty() const noexcept { return data_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return data_.size(); }

    iterator find(const Key& key) noexcept {
        auto it = lower_bound(key);
        if (it != data_.end() && !comp_(key, it->first) && !comp_(it->first, key))
            return it;
        return data_.end();
    }

    const_iterator find(const Key& key) const noexcept {
        auto it = lower_bound(key);
        if (it != data_.end() && !comp_(key, it->first) && !comp_(it->first, key))
            return it;
        return data_.end();
    }

    bool contains(const Key& key) const noexcept {
        return find(key) != data_.end();
    }

    Value& operator[](const Key& key) {
        auto it = lower_bound(key);
        if (it != data_.end() && !comp_(key, it->first) && !comp_(it->first, key))
            return it->second;
        it = data_.insert(it, {key, Value{}});
        return it->second;
    }

    Value& operator[](Key&& key) {
        auto it = lower_bound(key);
        if (it != data_.end() && !comp_(key, it->first) && !comp_(it->first, key))
            return it->second;
        it = data_.insert(it, {std::move(key), Value{}});
        return it->second;
    }

    std::size_t erase(const Key& key) {
        auto it = find(key);
        if (it == data_.end()) return 0;
        data_.erase(it);
        return 1;
    }

    void clear() { data_.clear(); }

private:
    std::vector<value_type> data_;
    [[no_unique_address]] Compare comp_{};

    iterator lower_bound(const Key& key) noexcept {
        return std::lower_bound(data_.begin(), data_.end(), key,
            [this](const value_type& p, const Key& k) { return comp_(p.first, k); });
    }

    const_iterator lower_bound(const Key& key) const noexcept {
        return std::lower_bound(data_.begin(), data_.end(), key,
            [this](const value_type& p, const Key& k) { return comp_(p.first, k); });
    }
};

} // namespace seqjoin
