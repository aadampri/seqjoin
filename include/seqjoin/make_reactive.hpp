// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <tuple>
#include <type_traits>
#include <utility>

#include "core/callable_traits.hpp"
#include "group_layout.hpp"
#include "njoin.hpp"
#include "source.hpp"

namespace seqjoin {

// ─── Reactive<Layout, NJoinT> ────────────────────────────────────────
// A thin wrapper around NJoin that provides:
//   - Group-aware add<SourceIdx>(args...) — packs multi-param groups into tuples
//   - Emit wrapper that unpacks source values back into flat lambda args
//
// Users don't construct this directly — use make_reactive().

template <class LayoutT, class NJoinT>
class Reactive {
public:
    explicit Reactive(NJoinT join) : join_(std::move(join)) {}

    /// add<SourceIdx>(args...) — insert into a specific source.
    /// For single-param groups (Group<I>): add<idx>(value)
    /// For multi-param groups (Group<I,J,...>): add<idx>(a, b, ...)
    ///   which packs args into a tuple and forwards to the underlying NJoin.
    template <std::size_t SourceIdx, class... Args>
    void add(Args&&... args) {
        if constexpr (sizeof...(Args) == 1) {
            // Single arg — forward directly (handles Group<I> single-param case)
            join_.template add<SourceIdx>(std::forward<Args>(args)...);
        } else {
            // Multiple args — pack into tuple (Group<I,J,...> multi-param case)
            join_.template add<SourceIdx>(
                std::make_tuple(std::forward<Args>(args)...));
        }
    }

    /// Access the underlying NJoin (for testing/advanced use).
    NJoinT& njoin() { return join_; }
    const NJoinT& njoin() const { return join_; }

private:
    NJoinT join_;
};

// ─── make_reactive implementation details ────────────────────────────

namespace detail {

// Build the Source types tuple from Layout + param types.
// Each entry in the layout maps to one Source<ValueType>.
template <class LayoutT, class ParamsTuple>
struct make_sources;

template <class... Entries, class ParamsTuple>
struct make_sources<Layout<Entries...>, ParamsTuple> {
    using type = std::tuple<
        Source<source_value_type<Entries, ParamsTuple>>...
    >;
};

// Build a tuple of decayed param types from callable_traits.
template <class Traits, class Seq>
struct decayed_params_tuple;

template <class Traits, std::size_t... Is>
struct decayed_params_tuple<Traits, std::index_sequence<Is...>> {
    using type = std::tuple<typename Traits::template decay_arg_t<Is>...>;
};

// Create the emit wrapper: takes source-level values, unpacks via layout, calls user fn.
template <class LayoutT, class UserFn>
struct emit_wrapper {
    UserFn user_fn;

    template <class... SourceValues>
    void operator()(const SourceValues&... source_values) {
        auto flat = layout_unpack<LayoutT>(std::tie(source_values...));
        std::apply(user_fn, flat);
    }
};

// Construct NJoin from wrapper + default-constructed sources.
template <class LayoutT, class ParamsTuple, class WrapperFn, std::size_t... Is>
auto make_njoin_from_layout(WrapperFn wrapper, std::index_sequence<Is...>) {
    using SourcesTuple = typename make_sources<LayoutT, ParamsTuple>::type;
    return NJoin(
        std::move(wrapper),
        std::tuple_element_t<Is, SourcesTuple>{}...
    );
}

} // namespace detail

// ─── make_reactive (with explicit Layout) ────────────────────────────
/// Tier 1: make_reactive<Layout<...>>(lambda)
///
/// Deduces source types from the lambda signature + layout.
/// Returns a Reactive wrapper with group-aware add<SourceIdx>().
///
/// Example:
///   auto r = make_reactive<Layout<Group<0,1>, Group<2>>>(
///       [](const string& e, const Config& c, const Health& h) { ... }
///   );
///   r.add<0>(endpoint, config);  // packs into tuple, inserts into source 0
///   r.add<1>(health);            // inserts into source 1
///
template <class LayoutT, class Fn>
auto make_reactive(Fn&& fn) {
    using Traits = callable_traits<std::decay_t<Fn>>;

    static_assert(LayoutT::param_count == Traits::arity,
                  "Layout param_count must match lambda arity");

    using DecayedParams = typename detail::decayed_params_tuple<
        Traits, std::make_index_sequence<Traits::arity>>::type;

    using Wrapper = detail::emit_wrapper<LayoutT, std::decay_t<Fn>>;
    auto wrapper = Wrapper{std::forward<Fn>(fn)};

    auto join = detail::make_njoin_from_layout<LayoutT, DecayedParams>(
        std::move(wrapper),
        std::make_index_sequence<LayoutT::source_count>{});

    return Reactive<LayoutT, decltype(join)>(std::move(join));
}

// ─── make_reactive (default 1:1 layout) ──────────────────────────────
/// Tier 0: make_reactive(lambda)
///
/// No groups — each param gets its own source with RetainLatest.
/// Equivalent to constructing NJoin directly but with type deduction.
///
/// Example:
///   auto r = make_reactive([](const string& name, int age) { ... });
///   r.add<0>(string("alice"));
///   r.add<1>(30);
///
template <class Fn>
auto make_reactive(Fn&& fn) {
    using Traits = callable_traits<std::decay_t<Fn>>;
    using LayoutT = DefaultLayout<Traits::arity>;
    return make_reactive<LayoutT>(std::forward<Fn>(fn));
}

} // namespace seqjoin
