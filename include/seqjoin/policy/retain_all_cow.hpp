// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <concepts>
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

/// Hash/Eq wrappers that dereference shared_ptr for map keying.
namespace detail_cow {

template <class Hash>
struct PtrHash {
    [[no_unique_address]] Hash h{};
    template <class T>
    std::size_t operator()(const std::shared_ptr<const T>& p) const {
        return h(*p);
    }
};

template <class Eq>
struct PtrEq {
    [[no_unique_address]] Eq eq{};
    template <class T>
    bool operator()(const std::shared_ptr<const T>& a,
                    const std::shared_ptr<const T>& b) const {
        return eq(*a, *b);
    }
};

} // namespace detail_cow

/// CowSnapshot — a self-contained, immutable snapshot from a RetainAllCOW policy.
///
/// Wraps a shared_ptr to an immutable map. The map stays alive as long as any
/// snapshot references it, decoupled from future inserts/removes on the source.
/// Iteration yields SnapRef<T> values compatible with the cross-product engine.
/// Values are stored as shared_ptr<const T> so the map clone never copies T.
///
/// Lock-free after construction: the shared_ptr is grabbed under the source
/// spinlock (O(1)), then all iteration happens outside the lock.
template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class CowSnapshot {
public:
    using map_type = std::unordered_map<std::shared_ptr<const T>, uint64_t,
                                        detail_cow::PtrHash<Hash>,
                                        detail_cow::PtrEq<Eq>>;
    using value_type = SnapRef<T>;

    CowSnapshot() = default;
    explicit CowSnapshot(std::shared_ptr<const map_type> d) : data_(std::move(d)) {}

    [[nodiscard]] bool empty() const { return !data_ || data_->empty(); }
    [[nodiscard]] std::size_t size() const { return data_ ? data_->size() : 0; }

    struct Iterator {
        using inner_it = typename map_type::const_iterator;
        inner_it it_;

        SnapRef<T> operator*() const {
            return SnapRef<T>{*it_->first, it_->second};
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
/// Stores all unique values in a shared immutable map as shared_ptr<const T>.
/// Map clone only copies shared_ptrs (O(n) pointer copies, zero T copies),
/// enabling move-only types.
///
/// Semantics:
///   - insert: clones the map (O(n) pointer copies), inserts into the clone,
///     swaps in. Rejects duplicates (returns nullopt).
///   - snapshot(): returns a CowSnapshot wrapping the current shared_ptr.
///     O(1) — just a refcount bump. The snapshot is decoupled from future mutations.
///   - remove: clones, erases, swaps. Returns true if found.
///   - scan(): returns a ScanView for Source::scan() compatibility.
///
/// Trade-off vs RetainAll:
///   - Insert: O(n) pointer copies (COW clone) vs O(1) amortized
///   - Snapshot: O(1) (shared_ptr copy) vs O(n) (copy all values into vector)
///   - Best for: large sets, frequent snapshots, read-heavy workloads,
///     move-only types
///
/// Thread safety: all methods are called under the Source spinlock.
/// The immutable map is safe to read concurrently from any thread.
template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class RetainAllCOW {
public:
    using value_type = T;
    using map_type = std::unordered_map<std::shared_ptr<const T>, uint64_t,
                                        detail_cow::PtrHash<Hash>,
                                        detail_cow::PtrEq<Eq>>;

    RetainAllCOW()
        : data_(std::make_shared<const map_type>()) {}

    /// Insert value with seq. Returns nullopt if value already exists (dedup).
    /// Clones the internal map (pointer copies only), inserts, and swaps (COW).
    std::optional<uint64_t> insert(const T& value, uint64_t seq) requires std::copy_constructible<T> {
        // Non-owning probe for duplicate check (avoids copy just to check)
        std::shared_ptr<const T> probe(std::shared_ptr<const T>{}, &value);
        if (data_->contains(probe)) return std::nullopt;
        auto copy = std::make_shared<map_type>(*data_);
        copy->emplace(std::make_shared<const T>(value), seq);
        data_ = std::move(copy);
        return seq;
    }
    std::optional<uint64_t> insert(T&& value, uint64_t seq) {
        std::shared_ptr<const T> probe(std::shared_ptr<const T>{}, &value);
        if (data_->contains(probe)) return std::nullopt;
        auto copy = std::make_shared<map_type>(*data_);
        copy->emplace(std::make_shared<const T>(std::move(value)), seq);
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
                return {*it_->first, it_->second};
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
    /// Uses a non-owning aliasing shared_ptr as probe, so T need not be copyable.
    bool remove(const T& value) {
        // Aliasing ctor: shares ownership with empty shared_ptr (owns nothing),
        // but points to value for hash/eq lookup.
        std::shared_ptr<const T> probe(std::shared_ptr<const T>{}, &value);
        auto copy = std::make_shared<map_type>(*data_);
        if (copy->erase(probe) == 0) return false;
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
