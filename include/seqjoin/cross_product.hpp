// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "core/seq_counter.hpp"
#include "core/snap_entry.hpp"

namespace seqjoin {

namespace detail {

/// Recursive cross-product engine.
///
/// Given N snapshots (one per source), enumerate all N-tuples where:
///   1. Every source contributes exactly one value
///   2. The element with the maximum seq in the tuple is at source index `trigger_idx`
///
/// Condition 2 is the seq-ownership rule: only the source that received the
/// newest element may emit, preventing duplicate emissions.
///
/// The recursion builds up a tuple of (value, seq) one source at a time.
/// At the base case (all sources consumed), it checks the ownership condition
/// and calls the emit function.

/// Base case: all sources consumed. Check ownership and emit.
template <std::size_t TriggerIdx, class EmitFn, class Tuple>
void cross_product_impl(
    EmitFn& emit,
    const Tuple& selected,
    uint64_t trigger_seq)
{
    // Find the max seq across all selected entries
    uint64_t max_seq = 0;
    std::size_t max_idx = 0;
    std::apply([&](const auto*... entries) {
        std::size_t idx = 0;
        ((void)([&] {
            if (idx == 0 || seq_after(get_seq(*entries), max_seq)) {
                max_seq = get_seq(*entries);
                max_idx = idx;
            }
            ++idx;
        }()), ...);
    }, selected);

    // Seq-ownership: only emit if the trigger source has the max seq
    // AND the selected trigger entry is the one that was just inserted.
    // The second check prevents re-emitting old tuples when RetainAll
    // keeps historical values in the trigger source.
    if (max_idx != TriggerIdx) return;
    if (get_seq(*std::get<TriggerIdx>(selected)) != trigger_seq) return;

    // Emit! Extract values from the tuple.
    std::apply([&](const auto*... entries) {
        emit(get_value(*entries)...);
    }, selected);
}

/// Recursive step: iterate source I's snapshot, recurse to I+1.
template <std::size_t I, std::size_t N, std::size_t TriggerIdx,
          class EmitFn, class Snapshots, class Tuple>
void cross_product_recurse(
    EmitFn& emit,
    const Snapshots& snapshots,
    Tuple& selected,
    uint64_t trigger_seq)
{
    if constexpr (I == N) {
        // Base case
        cross_product_impl<TriggerIdx>(emit, selected, trigger_seq);
    } else {
        const auto& snap = std::get<I>(snapshots);
        for (const auto& entry : snap) {
            std::get<I>(selected) = &entry;
            cross_product_recurse<I + 1, N, TriggerIdx>(
                emit, snapshots, selected, trigger_seq);
        }
    }
}

/// Entry point: enumerate cross-product of N snapshots with seq-ownership filter.
template <std::size_t TriggerIdx, class EmitFn, class... Snaps>
void cross_product(EmitFn& emit,
                   const std::tuple<Snaps...>& snapshots,
                   uint64_t trigger_seq)
{
    constexpr std::size_t N = sizeof...(Snaps);

    // Check: if any snapshot is empty, no tuples can be formed.
    bool any_empty = false;
    std::apply([&](const auto&... snaps) {
        ((any_empty = any_empty || snaps.empty()), ...);
    }, snapshots);
    if (any_empty) return;

    // Build the selected tuple type: one pointer per source entry (zero copies).
    using SelectedTuple = std::tuple<const typename Snaps::value_type*...>;
    SelectedTuple selected{};

    cross_product_recurse<0, N, TriggerIdx>(
        emit, snapshots, selected, trigger_seq);
}

} // namespace detail
} // namespace seqjoin
