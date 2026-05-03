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
// A Layout is a sequence of Groups:
//   Group<I>       — single param → own source (stores T directly, no tuple wrapper)
//   Group<I,J,...> — multiple params → one source (stores std::tuple<...>)
//
// Slot<I> is a type alias for Group<I> (documentation convenience).
//
// The Layout resolves:
//   - source_count: how many Source objects NJoin holds (M ≤ N)
//   - source_index_of<ParamIdx>: which source a given param maps to
//   - source_value_type<Entry, ParamTypes...>: the value_type of each source
//
// Example:
//   Lambda: [](const A& a, const B& b, const C& c)
//
//   Layout<Group<0>, Group<1>, Group<2>>  → 3 sources: A, B, C (1:1, the default)
//   Layout<Group<0,1>, Group<2>>          → 2 sources: tuple<A,B>, C

// ─── Policies: per-source retention policy selection ─────────────────
//
// Policies<P0, P1, ...> specifies a retention policy template for each source.
// Each Pi is a template<class T> class — it will be instantiated with the
// source's value_type to produce the concrete policy.
//
// Use AutoPolicy as a placeholder for "use the default policy for this source."
//
// Example:
//   Policies<RetainByKey, AutoPolicy, RetainAll>
//   → Source 0: RetainByKey<T0>
//   → Source 1: DefaultPolicy_t<T1>  (= RetainLatest)
//   → Source 2: RetainAll<T2>

/// Sentinel: "use the default policy for this source."
template <class T>
struct AutoPolicy_t;

/// Policy template list — one entry per source (positional).
template <template<class> class... Ps>
struct Policies {
    static constexpr std::size_t count = sizeof...(Ps);
};

// ─── apply_policy: resolve a single policy entry ─────────────────────
namespace detail {

template <template<class> class P, class T>
struct apply_policy_impl {
    using type = P<T>;
};

// AutoPolicy_t means "default"
template <class T>
struct apply_policy_impl<AutoPolicy_t, T> {
    using type = void;  // sentinel — handled by make_sources
};

} // namespace detail

// ─── Group: one or more lambda params → one source ───────────────────
template <std::size_t... ParamIndices>
struct Group {
    static_assert(sizeof...(ParamIndices) > 0, "Group must have at least one param index");
    static constexpr std::size_t param_count = sizeof...(ParamIndices);
    static constexpr std::size_t param_indices[] = { ParamIndices... };
};

/// Slot<I> is simply Group<I> — convenience alias for documentation.
template <std::size_t I>
using Slot = Group<I>;

// ─── Layout: sequence of Groups ──────────────────────────────────────
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
    using type = Layout<Group<Is>...>;
};

} // namespace detail

/// DefaultLayout<N> = Layout<Group<0>, Group<1>, ..., Group<N-1>>
template <std::size_t N>
using DefaultLayout = typename detail::make_default_layout_impl<
    std::make_index_sequence<N>>::type;

// ─── source_value_type: what a source stores ─────────────────────────
// For Group<I> (single): stores the raw param type T (no tuple wrapping).
// For Group<I,J,...> (multi): stores std::tuple<ParamI, ParamJ, ...>.

namespace detail {

template <class Entry, class ParamsTuple>
struct source_value_type_impl;

// Group<I> (single param) → raw type, no tuple
template <std::size_t I, class ParamsTuple>
struct source_value_type_impl<Group<I>, ParamsTuple> {
    using type = std::tuple_element_t<I, ParamsTuple>;
};

// Group<I, J, Rest...> (2+ params) → tuple of param types
template <std::size_t I, std::size_t J, std::size_t... Rest, class ParamsTuple>
struct source_value_type_impl<Group<I, J, Rest...>, ParamsTuple> {
    using type = std::tuple<std::tuple_element_t<I, ParamsTuple>,
                            std::tuple_element_t<J, ParamsTuple>,
                            std::tuple_element_t<Rest, ParamsTuple>...>;
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
// source values (one per source) back to individual lambda args.
//
// Group<I>:       source stores T directly — pass through as a 1-element tie
// Group<I,J,...>: source stores tuple<A,B,...> — forward as-is (already a tuple)

namespace detail {

// Unpack one entry's contribution to the lambda args.
template <class Entry>
struct entry_unpack;

// Group<I> (single param): pass value through as a 1-tuple (tie)
template <std::size_t I>
struct entry_unpack<Group<I>> {
    template <class T>
    static constexpr auto as_tuple(const T& value) {
        return std::tie(value);
    }
};

// Group<I, J, Rest...> (multi): value is already a tuple — forward it
template <std::size_t I, std::size_t J, std::size_t... Rest>
struct entry_unpack<Group<I, J, Rest...>> {
    template <class Tuple>
    static constexpr auto as_tuple(const Tuple& value) {
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
