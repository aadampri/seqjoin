// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>

#include "core/seq_counter.hpp"
#include "cross_product.hpp"
#include "source.hpp"

namespace seqjoin {

/// NJoin<EmitFn, Sources...> — the core N-way reactive join.
///
/// Template parameters:
///   EmitFn    — callable invoked with one value from each source
///   Sources   — Source<T, Policy, Liveness> for each input slot
///
/// Thread safety: add<I>() is safe to call concurrently from any thread.
/// Each source has its own spinlock. The emit callback is invoked outside
/// all locks — only after snapshots are taken and the cross-product is computed.
///
/// Invariant: each valid N-tuple is emitted exactly once, by the thread that
/// inserted the element with the highest seq in that tuple (seq-ownership).
template <class EmitFn, class... Sources>
class NJoin {
    static constexpr std::size_t N = sizeof...(Sources);

public:
    NJoin(EmitFn emit, Sources... sources)
        : emit_(std::move(emit))
        , sources_(std::make_tuple(std::move(sources)...)) {}

    /// Insert a value into source I.
    /// Assigns a seq, inserts into the source, snapshots all other sources,
    /// and runs the cross-product with seq-ownership filter.
    template <std::size_t I>
    void add(const typename std::tuple_element_t<I, std::tuple<Sources...>>::value_type& value) {
        static_assert(I < N, "Source index out of range");

        // 1. Assign a monotonic seq
        const uint64_t seq = seq_counter_.next();

        // 2. Insert into source I (under source I's spinlock)
        auto& source_i = std::get<I>(sources_);
        auto result = source_i.insert(value, seq);
        if (!result) return;  // rejected (e.g., duplicate in RetainAll)

        // 3. Snapshot all sources (each under its own spinlock, one at a time)
        auto snapshots = snapshot_all();

        // 4. Run cross-product with seq-ownership: only emit tuples where
        //    the max-seq element is from source I.
        detail::cross_product<I>(emit_, snapshots, seq);
    }

    /// Access a source by index (for testing/inspection).
    template <std::size_t I>
    auto& source() { return std::get<I>(sources_); }

    template <std::size_t I>
    const auto& source() const { return std::get<I>(sources_); }

private:
    EmitFn emit_;
    std::tuple<Sources...> sources_;
    SeqCounter seq_counter_;

    /// Snapshot all sources into a tuple of vectors.
    /// Each source's spinlock is acquired and released independently.
    auto snapshot_all() {
        return snapshot_all_impl(std::index_sequence_for<Sources...>{});
    }

    template <std::size_t... Is>
    auto snapshot_all_impl(std::index_sequence<Is...>) {
        return std::make_tuple(snapshot_one<Is>()...);
    }

    template <std::size_t I>
    auto snapshot_one() {
        using T = typename std::tuple_element_t<I, std::tuple<Sources...>>::value_type;
        Snapshot<T> snap;
        std::get<I>(sources_).scan_into(snap);
        return snap;
    }
};

/// Deduction guide: NJoin(emit, source0, source1, ...)
template <class EmitFn, class... Sources>
NJoin(EmitFn, Sources...) -> NJoin<EmitFn, Sources...>;

} // namespace seqjoin
