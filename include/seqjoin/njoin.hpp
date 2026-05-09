// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "core/seq_counter.hpp"
#include "core/callable_traits.hpp"
#include "cross_product.hpp"
#include "group_layout.hpp"
#include "source.hpp"

namespace seqjoin {

// Forward declaration
template <class EmitFn, class LayoutT, class... Sources>
class NJoin;

namespace detail {

// tuple_tail: given tuple<A, B, C, ...>, produce tuple<B, C, ...>
template <class Tuple>
struct tuple_tail;

template <class Head, class... Tail>
struct tuple_tail<std::tuple<Head, Tail...>> {
    using type = std::tuple<Tail...>;
};

// Build Source types from Layout + Params tuple (default policies).
template <class LayoutT, class ParamsTuple>
struct make_sources;

template <class... Entries, class ParamsTuple>
struct make_sources<Layout<Entries...>, ParamsTuple> {
    using type = std::tuple<
        Source<source_value_type<Entries, ParamsTuple>>...
    >;
};

// Build Source types from Layout + Params + Policies.
// Each policy template is applied to its corresponding source value type.
// AutoPolicy_t means "use default".
template <class LayoutT, class ParamsTuple, class PoliciesT>
struct make_sources_with_policies;

template <class... Entries, class ParamsTuple, template<class> class... Ps>
struct make_sources_with_policies<Layout<Entries...>, ParamsTuple, Policies<Ps...>> {
    static_assert(sizeof...(Entries) == sizeof...(Ps),
                  "Number of policies must match number of sources (layout entries)");

    // Helper: pick Source<T, P<T>> or Source<T> (for AutoPolicy_t)
    template <class Entry, template<class> class P>
    struct resolve_source {
        using T = source_value_type<Entry, ParamsTuple>;
        using type = Source<T, P<T>>;  // explicit policy
    };

    template <class Entry>
    struct resolve_source<Entry, AutoPolicy_t> {
        using T = source_value_type<Entry, ParamsTuple>;
        using type = Source<T>;        // default policy
    };

    using type = std::tuple<typename resolve_source<Entries, Ps>::type...>;
};

// Build decayed param types from callable_traits.
template <class Traits, class Seq>
struct decayed_params_tuple;

template <class Traits, std::size_t... Is>
struct decayed_params_tuple<Traits, std::index_sequence<Is...>> {
    using type = std::tuple<typename Traits::template decay_arg_t<Is>...>;
};

// Unpack a tuple type into NJoin Sources... parameter pack.
template <class EmitFn, class LayoutT, class SourcesTuple>
struct njoin_type;

template <class EmitFn, class LayoutT, class... Sources>
struct njoin_type<EmitFn, LayoutT, std::tuple<Sources...>> {
    using type = NJoin<EmitFn, LayoutT, Sources...>;
};

template <class EmitFn, class LayoutT, class SourcesTuple>
using njoin_type_t = typename njoin_type<EmitFn, LayoutT, SourcesTuple>::type;

} // namespace detail

/// NJoin<EmitFn, LayoutT, Sources...> — the core N-way reactive join (Option C).
///
/// Layout determines how lambda parameters map to sources:
///   - DefaultLayout<N>: 1:1 (each param gets its own source)
///   - Layout<Group<0,1>, Group<2>>: params 0,1 share source 0
///
/// Construction:
///   - Via make_reactive(fn): sources deduced and default-constructed
///   - Direct: NJoin(emit, Source<A>{}, Source<B>{}) via CTAD
///
/// Thread safety: add<I>() is safe to call concurrently from any thread.
template <class EmitFn, class LayoutT, class... Sources>
class NJoin {
    static constexpr std::size_t M = sizeof...(Sources);  // number of sources
    static constexpr std::size_t N = LayoutT::param_count; // number of lambda params

    static_assert(M == LayoutT::source_count,
                  "Number of Sources must match Layout::source_count");

public:
    using layout_type = LayoutT;

    /// Default-construct all sources (used by make_reactive).
    explicit NJoin(EmitFn emit)
        : emit_(std::move(emit)) {}

    /// Construct with pre-built sources (backward compat / custom policies).
    NJoin(EmitFn emit, Sources... sources)
        : emit_(std::move(emit))
        , sources_(std::move(sources)...) {}

    // Movable
    NJoin(NJoin&&) noexcept = default;
    NJoin& operator=(NJoin&&) = delete;
    NJoin(const NJoin&) = delete;
    NJoin& operator=(const NJoin&) = delete;

    /// add<SourceIdx>(args...) — insert into a specific source.
    /// For single-param groups (Group<I>): add<idx>(value)
    /// For multi-param groups (Group<I,J,...>): add<idx>(a, b, ...)
    ///   which packs args into a tuple and forwards to the underlying source.
    template <std::size_t SourceIdx, class... Args>
    void add(Args&&... args) {
        static_assert(SourceIdx < M, "Source index out of range");

        // 1. Assign a monotonic seq
        const uint64_t seq = seq_counter_.next();

        // 2. Insert into source (pack multi-args into tuple for group sources)
        auto& src = std::get<SourceIdx>(sources_);
        std::optional<uint64_t> result;
        if constexpr (sizeof...(Args) == 1) {
            result = src.insert(std::forward<Args>(args)..., seq);
        } else {
            result = src.insert(
                std::make_tuple(std::forward<Args>(args)...), seq);
        }
        if (!result) return;  // rejected (e.g., duplicate in RetainAll)

        // 3. Snapshot all sources
        auto snapshots = snapshot_all();

        // 4. Cross-product with seq-ownership filter + layout unpack for emit
        auto wrapper = make_emit_wrapper();
        detail::cross_product<SourceIdx>(
            wrapper, snapshots, seq);
    }

    /// Access a source by index.
    template <std::size_t I>
    auto& source() { return std::get<I>(sources_); }

    template <std::size_t I>
    const auto& source() const { return std::get<I>(sources_); }

private:
    EmitFn emit_;
    std::tuple<Sources...> sources_;
    SeqCounter seq_counter_;

    // Emit wrapper: takes source-level values, unpacks via layout, calls user fn.
    // Constructed on-the-fly to avoid storing a pointer that invalidates on move.
    struct EmitWrapper {
        EmitFn* fn;

        template <class... SourceValues>
        void operator()(const SourceValues&... source_values) {
            auto flat = detail::layout_unpack<LayoutT>(std::tie(source_values...));
            std::apply(*fn, flat);
        }
    };

    EmitWrapper make_emit_wrapper() noexcept { return EmitWrapper{&emit_}; }

    /// Snapshot all sources into a tuple of vectors.
    auto snapshot_all() {
        return snapshot_all_impl(std::make_index_sequence<M>{});
    }

    template <std::size_t... Is>
    auto snapshot_all_impl(std::index_sequence<Is...>) {
        return std::make_tuple(snapshot_one<Is>()...);
    }

    template <std::size_t I>
    auto snapshot_one() {
        return std::get<I>(sources_).snapshot();
    }
};

/// CTAD: NJoin(emit, source0, source1, ...) → DefaultLayout, no grouping.
template <class EmitFn, class... Sources>
NJoin(EmitFn, Sources...) -> NJoin<EmitFn, DefaultLayout<sizeof...(Sources)>, Sources...>;

} // namespace seqjoin
