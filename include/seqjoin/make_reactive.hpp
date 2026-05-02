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

} // namespace detail

template <class T>
concept IsLayout = detail::is_layout<T>::value;

// ─── make_reactive: 4 overloads ─────────────────────────────────────
//
// (1) make_reactive(fn)                    — layout + storage deduced
// (2) make_reactive<LayoutT>(fn)           — layout explicit, storage deduced
// (3) make_reactive<LayoutT, Ts...>(fn)    — layout + storage explicit
// (4) make_reactive<Ts...>(fn)             — storage explicit, default layout
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

// ─── Overloads (2) and (3): Layout explicit ──────────────────────────
/// make_reactive<LayoutT>(fn) — layout explicit, storage deduced.
/// make_reactive<LayoutT, Ts...>(fn) — layout + storage explicit.
///
/// When sizeof...(Ts) == 0: storage deduced from callable (overload 2).
/// When sizeof...(Ts) > 0:  Ts are the storage types (overload 3).
///
/// Examples:
///   // (2) Layout only
///   auto j = make_reactive<Layout<Group<0,1>, Group<2>>>(fn);
///
///   // (3) Layout + explicit storage
///   auto j = make_reactive<Layout<Group<0,1>, Group<2>>, string, int, double>(fn);
///
template <class LayoutT, class... Ts, class Fn>
    requires IsLayout<LayoutT>
auto make_reactive(Fn&& fn) {
    using Traits = callable_traits<std::decay_t<Fn>>;

    static_assert(LayoutT::param_count == Traits::arity,
                  "Layout param_count must match lambda arity");

    if constexpr (sizeof...(Ts) == 0) {
        // Overload (2): storage deduced
        using Params = typename detail::decayed_params_tuple<
            Traits, std::make_index_sequence<Traits::arity>>::type;
        using Sources = typename detail::make_sources<LayoutT, Params>::type;
        using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

        return JoinT(std::forward<Fn>(fn));
    } else {
        // Overload (3): storage explicit
        static_assert(sizeof...(Ts) == Traits::arity,
                      "Number of explicit storage types must match lambda arity");
        using Params = std::tuple<Ts...>;
        using Sources = typename detail::make_sources<LayoutT, Params>::type;
        using JoinT = detail::njoin_type_t<std::decay_t<Fn>, LayoutT, Sources>;

        return JoinT(std::forward<Fn>(fn));
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
    requires (!IsLayout<T0>)
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
