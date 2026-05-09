// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

#include "../core/snap_entry.hpp"

namespace seqjoin {

// Forward declaration
template <class T, class Hash, class Eq>
class RetainAllCOW;

/// CowSnapshot — a self-contained, immutable snapshot from a RetainAllCOW policy.
///
/// Wraps a shared_ptr to an immutable map. The map stays alive as long as any
/// snapshot references it, decoupled from future inserts/removes on the source.
/// Iteration yields SnapEntry<T> values compatible with the cross-product engine.
///
/// Lock-free after construction: the shared_ptr is grabbed under the source
/// spinlock (O(1)), then all iteration happens outside the lock.
template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class CowSnapshot {
public:
    using map_type = std::unordered_map<T, uint64_t, Hash, Eq>;
    using value_type = SnapRef<T>;

    CowSnapshot() = default;
    explicit CowSnapshot(std::shared_ptr<const map_type> d) : data_(std::move(d)) {}

    [[nodiscard]] bool empty() const { return !data_ || data_->empty(); }
    [[nodiscard]] std::size_t size() const { return data_ ? data_->size() : 0; }

    struct Iterator {
        using inner_it = typename map_type::const_iterator;
        inner_it it_;

        SnapRef<T> operator*() const {
            return SnapRef<T>{it_->first, it_->second};
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

/// RetainAllCOW<T> — copy-on-write retention policy.
///
/// Stores all unique values in a shared immutable map. Semantics:
///   - insert: clones the map (O(n)), inserts into the clone, swaps in.
///     Rejects duplicates (returns nullopt).
///   - snapshot(): returns a CowSnapshot wrapping the current shared_ptr.
///     O(1) — just a refcount bump. The snapshot is decoupled from future mutations.
///   - remove: clones, erases, swaps. Returns true if found.
///   - scan(): returns a ScanView for Source::scan() compatibility.
///
/// Trade-off vs RetainAll:
///   - Insert: O(n) (clone) vs O(1) amortized
///   - Snapshot: O(1) (shared_ptr copy) vs O(n) (copy all values into vector)
///   - Best for: large sets, frequent snapshots, read-heavy workloads
///
/// Thread safety: all methods are called under the Source spinlock.
/// The immutable map is safe to read concurrently from any thread.
template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class RetainAllCOW {
public:
    using value_type = T;
    using map_type = std::unordered_map<T, uint64_t, Hash, Eq>;

    RetainAllCOW()
        : data_(std::make_shared<const map_type>()) {}

    /// Insert value with seq. Returns nullopt if value already exists (dedup).
    /// Clones the internal map, inserts, and swaps (COW).
    std::optional<uint64_t> insert(const T& value, uint64_t seq) {
        auto copy = std::make_shared<map_type>(*data_);
        auto [it, inserted] = copy->emplace(value, seq);
        if (!inserted) return std::nullopt;
        data_ = std::move(copy);
        return seq;
    }
    std::optional<uint64_t> insert(T&& value, uint64_t seq) {
        auto copy = std::make_shared<map_type>(*data_);
        auto [it, inserted] = copy->emplace(std::move(value), seq);
        if (!inserted) return std::nullopt;
        data_ = std::move(copy);
        return seq;
    }

    /// O(1) snapshot — returns a CowSnapshot holding a shared_ptr to the
    /// current immutable map. Future inserts/removes don't affect this snapshot.
    [[nodiscard]] CowSnapshot<T, Hash, Eq> snapshot() const {
        return CowSnapshot<T, Hash, Eq>{data_};
    }

    /// ScanView for Source::scan() compatibility (visitor-based path).
    /// Iterates the current map, yielding (const T&, uint64_t) pairs.
    class ScanView {
        const map_type& map_;
    public:
        explicit ScanView(const map_type& m) : map_(m) {}

        struct Iterator {
            typename map_type::const_iterator it_;

            std::pair<const T&, const uint64_t&> operator*() const {
                return {it_->first, it_->second};
            }
            Iterator& operator++() { ++it_; return *this; }
            bool operator!=(const Iterator& o) const { return it_ != o.it_; }
            bool operator==(const Iterator& o) const { return it_ == o.it_; }
        };

        Iterator begin() const { return {map_.begin()}; }
        Iterator end() const { return {map_.end()}; }
    };

    /// scan() for Source::scan() and RetentionPolicy concept compatibility.
    [[nodiscard]] ScanView scan() const noexcept { return ScanView{*data_}; }

    [[nodiscard]] std::size_t size() const noexcept { return data_->size(); }
    [[nodiscard]] bool empty() const noexcept { return data_->empty(); }

    /// Remove a specific value (COW). Returns true if it was present.
    bool remove(const T& value) {
        auto copy = std::make_shared<map_type>(*data_);
        if (copy->erase(value) == 0) return false;
        data_ = std::move(copy);
        return true;
    }

    void clear() {
        data_ = std::make_shared<const map_type>();
    }

private:
    std::shared_ptr<const map_type> data_;
};

} // namespace seqjoin
