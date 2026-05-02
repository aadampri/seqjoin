// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

namespace seqjoin {

// ─── SourceView<ParentSource, ProjIndices...> ────────────────────────
// A projected view into a grouped source.
//
// ParentSource: the original Source<tuple<A,B,C,...>>
// ProjIndices:  which tuple elements to expose (e.g., 0,1 → tuple<A,B>)
//
// Single-index projection unwraps: SourceView<S, 0> → value_type = A (not tuple<A>)
// Multi-index projection: SourceView<S, 0,1> → value_type = tuple<A,B>
//
// Views always flatten to the root source — no nesting.
// Views are non-owning (borrow parent via pointer).
//
// Thread safety: same as parent — all operations go through parent's spinlock.

namespace detail {

// Determine value_type for a view projection.
// Single index → unwrap (no tuple). Multiple indices → tuple.
template <class ParentValueType, std::size_t... ProjIs>
struct view_value_type;

// Single projection → unwrap
template <class ParentValueType, std::size_t I>
struct view_value_type<ParentValueType, I> {
    using type = std::tuple_element_t<I, ParentValueType>;
};

// Multi projection → tuple of elements
template <class ParentValueType, std::size_t I, std::size_t J, std::size_t... Rest>
struct view_value_type<ParentValueType, I, J, Rest...> {
    using type = std::tuple<std::tuple_element_t<I, ParentValueType>,
                            std::tuple_element_t<J, ParentValueType>,
                            std::tuple_element_t<Rest, ParentValueType>...>;
};

// Project a single value from a parent tuple.
// Single index → return element directly.
// Multi indices → return a new tuple.
template <std::size_t... ProjIs, class ParentValueType>
auto project_value(const ParentValueType& parent_tuple) {
    if constexpr (sizeof...(ProjIs) == 1) {
        return std::get<(ProjIs, ...)>(parent_tuple);
    } else {
        return std::make_tuple(std::get<ProjIs>(parent_tuple)...);
    }
}

// Compose view indices: given an outer view with ProjIs and an inner local index,
// map local → root. E.g., outer = <2,5,7>, local 1 → root 5.
template <std::size_t LocalIdx, std::size_t... ParentProjIs>
consteval std::size_t compose_index() {
    constexpr std::size_t indices[] = { ParentProjIs... };
    static_assert(LocalIdx < sizeof...(ParentProjIs), "Local index out of range");
    return indices[LocalIdx];
}

// Merge helper: concatenate two index sequences.
template <class Seq1, class Seq2>
struct merge_index_seqs;

template <std::size_t... Is, std::size_t... Js>
struct merge_index_seqs<std::index_sequence<Is...>, std::index_sequence<Js...>> {
    using type = std::index_sequence<Is..., Js...>;
};

// Check if a layout is a refinement of another.
// Fine is a refinement of Coarse if every block of Fine is contained
// within some block of Coarse.
template <class Fine, class Coarse>
struct is_refinement_of;

template <class... FineEntries, class... CoarseEntries>
struct is_refinement_of<Layout<FineEntries...>, Layout<CoarseEntries...>> {
private:
    // Check if all param indices of FineEntry are contained in some CoarseEntry
    template <class FineEntry>
    static consteval bool fine_entry_contained() {
        // For each param in FineEntry, check they all map to the same coarse block
        if constexpr (FineEntry::param_count == 0) {
            return true;
        } else {
            // All params in FineEntry must appear in one single CoarseEntry
            constexpr std::size_t first_param = FineEntry::param_indices[0];
            // Find which coarse entry contains first_param
            constexpr std::size_t coarse_idx = find_coarse_block(first_param);
            // Check all other params are in the same coarse block
            bool result = true;
            for (std::size_t i = 1; i < FineEntry::param_count; ++i) {
                if (find_coarse_block(FineEntry::param_indices[i]) != coarse_idx) {
                    result = false;
                }
            }
            return result;
        }
    }

    static consteval std::size_t find_coarse_block(std::size_t param_idx) {
        constexpr auto check = []<class Entry>(std::size_t pidx) consteval {
            for (std::size_t i = 0; i < Entry::param_count; ++i) {
                if (Entry::param_indices[i] == pidx) return true;
            }
            return false;
        };
        // Search each coarse entry
        std::size_t block = 0;
        bool found = false;
        auto search = [&]<class Entry>() consteval {
            if (!found && check.template operator()<Entry>(param_idx)) {
                found = true;
            } else if (!found) {
                ++block;
            }
        };
        (search.template operator()<CoarseEntries>(), ...);
        return block;
    }

public:
    static constexpr bool value = (fine_entry_contained<FineEntries>() && ...);
};

} // namespace detail

/// is_refinement_of_v<Fine, Coarse> — true if Fine refines Coarse in the partition lattice.
template <class Fine, class Coarse>
inline constexpr bool is_refinement_of_v = detail::is_refinement_of<Fine, Coarse>::value;

/// SourceView — a projected, non-owning view into a parent source.
///
/// Provides scan-only access to projected values.
/// Writable views (RMW insert) require the parent source to support read_latest().
///
/// ProjIndices are absolute indices into the parent's tuple, not local.
/// All views flatten to the root source (no SourceView-of-SourceView nesting).
template <class ParentSource, std::size_t... ProjIndices>
class SourceView {
    static_assert(sizeof...(ProjIndices) > 0, "SourceView must project at least one index");

    using parent_value_type = typename ParentSource::value_type;
    static_assert(std::tuple_size_v<parent_value_type> > 0,
                  "SourceView requires parent to store a tuple");

public:
    using value_type = typename detail::view_value_type<parent_value_type, ProjIndices...>::type;

    explicit SourceView(ParentSource& parent) noexcept : parent_(&parent) {}

    /// Scan projected values: calls visitor(projected_value, seq) for each entry.
    /// The parent's spinlock is acquired during the scan.
    template <class Visitor>
    std::size_t scan(Visitor&& visitor) {
        return parent_->scan([&](const parent_value_type& full_val, uint64_t seq) {
            visitor(detail::project_value<ProjIndices...>(full_val), seq);
        });
    }

    /// Snapshot scan: collect projected values into a container.
    template <class Container>
    void scan_into(Container& out) {
        parent_->scan([&](const parent_value_type& full_val, uint64_t seq) {
            out.emplace_back(detail::project_value<ProjIndices...>(full_val), seq);
        });
    }

    /// Create a sub-view: further projection from this view's index space.
    /// The result flattens to the root parent — no nesting.
    ///
    /// Example:
    ///   auto v_ab = source.view<0,1>();   // ProjIndices = {0,1}
    ///   auto v_a  = v_ab.view<0>();       // local 0 → root 0
    ///
    template <std::size_t... LocalIs>
    auto view() {
        // Map local indices through our ProjIndices to get root indices
        return SourceView<ParentSource,
                          detail::compose_index<LocalIs, ProjIndices...>()...>(
            *parent_);
    }

    /// Merge with another view from the same parent.
    /// Returns a SourceView with the union of both index sets.
    /// Compile-time only — both views must share the same ParentSource type.
    template <std::size_t... OtherIs>
    auto merge(const SourceView<ParentSource, OtherIs...>&) {
        return SourceView<ParentSource, ProjIndices..., OtherIs...>(*parent_);
    }

    /// Access the parent source.
    ParentSource& parent() noexcept { return *parent_; }
    const ParentSource& parent() const noexcept { return *parent_; }

private:
    ParentSource* parent_;
};

} // namespace seqjoin
