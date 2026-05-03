// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <tuple>
#include <type_traits>
#include <utility>

#include "core/callable_traits.hpp"
#include "group_layout.hpp"
#include "njoin.hpp"

namespace seqjoin {

// ─── IsLayout concept ───────────────────────────────────────────────
// Distinguishes Layout<...> from storage types in template argument lists.

namespace detail {

template <class T>
struct is_layout : std::false_type {};

template <class... Entries>
struct is_layout<Layout<Entries...>> : std::true_type {};

template <class T>
struct is_policies : std::false_type {};

template <template<class> class... Ps>
struct is_policies<Policies<Ps...>> : std::true_type {};

} // namespace detail

template <class T>
concept IsLayout = detail::is_layout<T>::value;

template <class T>
concept IsPolicies = detail::is_policies<T>::value;

// ─── make_reactive: 6 overloads ─────────────────────────────────────
//
// (1) make_reactive(fn)                           — layout + storage + policies deduced
// (2) make_reactive<LayoutT>(fn)                  — layout explicit, rest deduced
// (3) make_reactive<LayoutT, Ts...>(fn)           — layout + storage explicit, policies default
// (4) make_reactive<Ts...>(fn)                    — storage explicit, default layout
// (5) make_reactive<LayoutT, PoliciesT>(fn)       — layout + policies explicit, storage deduced
// (6) make_reactive<LayoutT, PoliciesT, Ts...>(fn)— layout + policies + storage all explicit
//
// All return NJoin directly (no wrapper).

// ─── Overload (1): Everything deduced ────────────────────────────────
/// make_reactive(lambda) — layout and storage fully deduced from callable.
///
/// Example:
///   auto j = make_reactive([](const string& name, int age) { ... });
///   j.add<0>(string("alice"));
///   j.add<1>(30);
///
template <class Fn>
    requires (!IsLayout<std::decay_t<Fn>>)
auto make_reactive(Fn&& fn) {
    using Traits = callable_traits<std::decay_t<Fn>>;
    using LayoutT = DefaultLayout<Traits::arity>;
    using Params = typename detail::decayed_params_tuple<
        Traits, std::make_index_sequence<Traits::arity>>::type;
    using Sources = typename detail::make_sources<LayoutT, Params>::type;
    using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

    return JoinT(std::forward<Fn>(fn));
}

// ─── Overloads (2), (3), (5), (6): Layout explicit ──────────────────
/// Unified dispatch for layout-first overloads.
///
/// Template args after LayoutT are classified:
///   - If first remaining arg is Policies<...>:
///       (5) no more args → storage deduced
///       (6) remaining args → storage explicit
///   - Otherwise:
///       (2) no args → storage deduced
///       (3) args → storage types explicit
///
template <class LayoutT, class... Rest, class Fn>
    requires IsLayout<LayoutT>
auto make_reactive(Fn&& fn) {
    using Traits = callable_traits<std::decay_t<Fn>>;

    static_assert(LayoutT::param_count == Traits::arity,
                  "Layout param_count must match lambda arity");

    if constexpr (sizeof...(Rest) == 0) {
        // Overload (2): layout only, storage deduced, default policies
        using Params = typename detail::decayed_params_tuple<
            Traits, std::make_index_sequence<Traits::arity>>::type;
        using Sources = typename detail::make_sources<LayoutT, Params>::type;
        using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

        return JoinT(std::forward<Fn>(fn));
    } else {
        // Extract first of Rest...
        using First = std::tuple_element_t<0, std::tuple<Rest...>>;

        if constexpr (IsPolicies<First>) {
            // Policies detected — overloads (5) or (6)
            using PoliciesT = First;

            // Remaining types after Policies (if any) are storage types
            constexpr std::size_t remaining = sizeof...(Rest) - 1;

            if constexpr (remaining == 0) {
                // Overload (5): layout + policies, storage deduced
                using Params = typename detail::decayed_params_tuple<
                    Traits, std::make_index_sequence<Traits::arity>>::type;
                using Sources = typename detail::make_sources_with_policies<
                    LayoutT, Params, PoliciesT>::type;
                using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

                return JoinT(std::forward<Fn>(fn));
            } else {
                // Overload (6): layout + policies + explicit storage
                // Extract storage types: all of Rest... except the first (Policies)
                using Params = typename detail::tuple_tail<std::tuple<Rest...>>::type;

                static_assert(std::tuple_size_v<Params> == Traits::arity,
                              "Number of explicit storage types must match lambda arity");

                using Sources = typename detail::make_sources_with_policies<
                    LayoutT, Params, PoliciesT>::type;
                using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

                return JoinT(std::forward<Fn>(fn));
            }
        } else {
            // No Policies — overload (3): layout + explicit storage, default policies
            static_assert(sizeof...(Rest) == Traits::arity,
                          "Number of explicit storage types must match lambda arity");
            using Params = std::tuple<Rest...>;
            using Sources = typename detail::make_sources<LayoutT, Params>::type;
            using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

            return JoinT(std::forward<Fn>(fn));
        }
    }
}

// ─── Overload (4): Storage explicit, default layout ──────────────────
/// make_reactive<Ts...>(fn) — storage types explicit, default 1:1 layout.
///
/// Use when decay_arg_t doesn't match the desired storage type:
///   auto j = make_reactive<string, int>([](string_view sv, int n) { ... });
///   // Source<string> (owning), not Source<string_view>
///
template <class T0, class... Ts, class Fn>
    requires (!IsLayout<T0> && !IsPolicies<T0>)
auto make_reactive(Fn&& fn) {
    using Traits = callable_traits<std::decay_t<Fn>>;
    constexpr std::size_t arity = 1 + sizeof...(Ts);

    static_assert(arity == Traits::arity,
                  "Number of explicit storage types must match lambda arity");

    using LayoutT = DefaultLayout<arity>;
    using Params = std::tuple<T0, Ts...>;
    using Sources = typename detail::make_sources<LayoutT, Params>::type;
    using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

    return JoinT(std::forward<Fn>(fn));
}

} // namespace seqjoin
