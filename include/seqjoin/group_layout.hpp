// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <tuple>
#include <type_traits>

namespace seqjoin {

// ─── GroupLayout ─────────────────────────────────────────────────────
// Compile-time mapping from lambda parameter indices to source indices.
//
// A Layout is a sequence of Slots and Groups:
//   Slot<I>      — lambda param I maps to its own dedicated source.
//   Group<I,J,...> — lambda params I,J,... share one source that stores
//                   std::tuple<ParamI, ParamJ, ...>.
//
// The Layout resolves:
//   - source_count: how many Source objects NJoin holds (M ≤ N)
//   - source_index_of<ParamIdx>: which source a given param maps to
//   - source_type<Entry, ParamTypes...>: the value_type of each source
//
// Example:
//   Lambda: [](const A& a, const B& b, const C& c)
//
//   Layout<Slot<0>, Slot<1>, Slot<2>>     → 3 sources (1:1, the default)
//   Layout<Group<0,1>, Slot<2>>           → 2 sources: Source<tuple<A,B>>, Source<C>

// ─── Slot: one lambda param → one source ─────────────────────────────
template <std::size_t ParamIdx>
struct Slot {
    static constexpr std::size_t param_count = 1;
    static constexpr std::size_t param_indices[] = { ParamIdx };
};

// ─── Group: multiple lambda params → one source (stored as tuple) ────
template <std::size_t... ParamIndices>
struct Group {
    static constexpr std::size_t param_count = sizeof...(ParamIndices);
    static constexpr std::size_t param_indices[] = { ParamIndices... };
};

// ─── Layout: sequence of Slots/Groups ────────────────────────────────
template <class... Entries>
struct Layout {
    /// Number of sources (= number of entries).
    static constexpr std::size_t source_count = sizeof...(Entries);

    /// Total number of lambda parameters covered.
    static constexpr std::size_t param_count = (Entries::param_count + ...);
};

// ─── Default layout: 1:1 mapping ────────────────────────────────────

namespace detail {

template <class Seq>
struct make_default_layout_impl;

template <std::size_t... Is>
struct make_default_layout_impl<std::index_sequence<Is...>> {
    using type = Layout<Slot<Is>...>;
};

} // namespace detail

/// DefaultLayout<N> = Layout<Slot<0>, Slot<1>, ..., Slot<N-1>>
template <std::size_t N>
using DefaultLayout = typename detail::make_default_layout_impl<
    std::make_index_sequence<N>>::type;

// ─── source_value_type: what a source stores ─────────────────────────
// For Slot<I>, the source stores the raw param type.
// For Group<I,J,...>, the source stores std::tuple<ParamI, ParamJ, ...>.

namespace detail {

// Given an Entry and a tuple of all param types, produce the source value_type.
template <class Entry, class ParamsTuple>
struct source_value_type_impl;

// Slot<I> → just the param type at index I
template <std::size_t I, class ParamsTuple>
struct source_value_type_impl<Slot<I>, ParamsTuple> {
    using type = std::tuple_element_t<I, ParamsTuple>;
};

// Group<Is...> → tuple of param types at indices Is...
template <std::size_t... Is, class ParamsTuple>
struct source_value_type_impl<Group<Is...>, ParamsTuple> {
    using type = std::tuple<std::tuple_element_t<Is, ParamsTuple>...>;
};

} // namespace detail

/// source_value_type<Entry, ParamsTuple> — the T for a source.
template <class Entry, class ParamsTuple>
using source_value_type = typename detail::source_value_type_impl<Entry, ParamsTuple>::type;

// ─── source_index_of: param index → source index ────────────────────

namespace detail {

template <std::size_t ParamIdx, class Entry>
consteval bool entry_contains() {
    for (std::size_t i = 0; i < Entry::param_count; ++i) {
        if (Entry::param_indices[i] == ParamIdx) return true;
    }
    return false;
}

template <std::size_t ParamIdx, std::size_t SourceIdx>
consteval std::size_t find_source_index_impl() {
    return static_cast<std::size_t>(-1); // not found
}

template <std::size_t ParamIdx, std::size_t SourceIdx, class Entry, class... Rest>
consteval std::size_t find_source_index_impl() {
    if constexpr (entry_contains<ParamIdx, Entry>()) {
        return SourceIdx;
    } else {
        return find_source_index_impl<ParamIdx, SourceIdx + 1, Rest...>();
    }
}

template <std::size_t ParamIdx, class LayoutT>
struct source_index_of_from_layout;

template <std::size_t ParamIdx, class... Entries>
struct source_index_of_from_layout<ParamIdx, Layout<Entries...>> {
    static constexpr std::size_t value =
        find_source_index_impl<ParamIdx, 0, Entries...>();
    static_assert(value != static_cast<std::size_t>(-1),
                  "ParamIdx not found in any layout entry");
};

} // namespace detail

/// source_index_of<ParamIdx, LayoutT> — which source index a param maps to.
template <std::size_t ParamIdx, class LayoutT>
inline constexpr std::size_t source_index_of =
    detail::source_index_of_from_layout<ParamIdx, LayoutT>::value;

// ─── Unpack: extract lambda args from source snapshots ───────────────
// For the cross-product emit step, we need to go from a tuple of
// SnapEntry values (one per source) back to individual lambda args.
//
// For Slot<I>: the source value IS the lambda arg — pass through.
// For Group<I,J,...>: the source value is tuple<ArgI, ArgJ,...> —
//   unpack into individual args at the correct positions.

namespace detail {

// Unpack one entry's contribution to the lambda args.
// For Slot: just forward the value.
template <class Entry>
struct entry_unpack;

template <std::size_t I>
struct entry_unpack<Slot<I>> {
    template <class T>
    static constexpr auto as_tuple(const T& value) {
        return std::tie(value);
    }
};

template <std::size_t... Is>
struct entry_unpack<Group<Is...>> {
    template <class Tuple>
    static constexpr auto as_tuple(const Tuple& value) {
        // value is already a tuple — just forward it
        return value;
    }
};

/// Unpack all entries: given source values as a tuple (one per source),
/// produce a flat tuple of lambda args.
/// SourceIdx is used to index into the values tuple positionally.
template <class LayoutT, class SourceValuesTuple, std::size_t... SourceIs>
constexpr auto layout_unpack_impl(const SourceValuesTuple& values,
                                   std::index_sequence<SourceIs...>);

template <class... Entries, class SourceValuesTuple, std::size_t... SourceIs>
constexpr auto layout_unpack_impl_helper(
    const Layout<Entries...>&,
    const SourceValuesTuple& values,
    std::index_sequence<SourceIs...>)
{
    return std::tuple_cat(
        entry_unpack<Entries>::as_tuple(std::get<SourceIs>(values))...
    );
}

template <class LayoutT, class SourceValuesTuple>
constexpr auto layout_unpack(const SourceValuesTuple& values) {
    return layout_unpack_impl_helper(
        LayoutT{}, values,
        std::make_index_sequence<LayoutT::source_count>{});
}

} // namespace detail

} // namespace seqjoin
