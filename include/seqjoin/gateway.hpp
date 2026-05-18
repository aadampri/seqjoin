// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "barrier_view.hpp"
#include "make_reactive.hpp"

namespace seqjoin {

// ─── Gate descriptor ────────────────────────────────────────────────
// Lightweight value capturing wiring intent. No allocation until gateway().

template <class Source, class Extract>
struct GateDescriptor {
    using source_type  = Source;
    using key_type     = typename Source::key_type;
    using value_type   = typename Source::value_type;
    using extract_type = Extract;
    using handle_type  = std::decay_t<std::invoke_result_t<Extract, const value_type&>>;

    Source& source;
    std::vector<key_type> keys;
    Extract extract;
};

/// gate(source, keys, [extract]) — create a gate descriptor.
template <class Source, class Extract = std::identity>
auto gate(Source& source, std::vector<typename Source::key_type> keys, Extract extract = {}) {
    return GateDescriptor<Source, Extract>{source, std::move(keys), std::move(extract)};
}

// ─── Gateway: owning handle ─────────────────────────────────────────

namespace detail {

template <class Desc>
using barrier_for_t = BarrierView<
    typename Desc::value_type,
    typename Desc::key_type,
    typename Desc::extract_type,
    typename Desc::handle_type>;

template <class... Descs>
using barriers_tuple_t = std::tuple<barrier_for_t<Descs>...>;

} // namespace detail

template <class NJoinT, class... Descs>
class Gateway {
public:
    Gateway(std::shared_ptr<NJoinT> join,
            detail::barriers_tuple_t<Descs...> barriers,
            std::tuple<typename Descs::source_type*...> srcs)
        : join_(std::move(join))
        , barriers_(std::move(barriers))
        , source_ptrs_(srcs)
    {}

    Gateway(Gateway&&) noexcept = default;
    Gateway& operator=(Gateway&&) noexcept = default;
    Gateway(const Gateway&) = delete;
    Gateway& operator=(const Gateway&) = delete;

    /// Identity equality — same underlying NJoin instance.
    bool operator==(const Gateway& other) const noexcept {
        return join_ == other.join_;
    }

    /// Change the key-set for gate I at runtime.
    template <std::size_t I>
    void set_keys(std::vector<typename std::tuple_element_t<I, std::tuple<Descs...>>::key_type> new_keys) {
        std::get<I>(barriers_).set_required_keys(std::move(new_keys), *std::get<I>(source_ptrs_));
    }

    /// Query: is gate I complete?
    template <std::size_t I>
    [[nodiscard]] bool is_gate_complete() const {
        return std::get<I>(barriers_).is_complete();
    }

    /// Query: are ALL gates complete?
    [[nodiscard]] bool is_complete() const {
        return is_complete_impl(std::index_sequence_for<Descs...>{});
    }

private:
    std::shared_ptr<NJoinT> join_;
    detail::barriers_tuple_t<Descs...> barriers_;
    std::tuple<typename Descs::source_type*...> source_ptrs_;

    template <std::size_t... Is>
    bool is_complete_impl(std::index_sequence<Is...>) const {
        return (std::get<Is>(barriers_).is_complete() && ...);
    }
};

// ─── gateway() factory ──────────────────────────────────────────────

namespace detail {

template <std::size_t I, class JoinPtr, class Desc>
auto make_barrier_from_gate(JoinPtr& join_ptr, Desc& desc) {
    using Handle = typename Desc::handle_type;
    using T = typename Desc::value_type;
    using Key = typename Desc::key_type;
    using Extract = typename Desc::extract_type;

    auto injector = [join_ptr](std::span<const Handle> handles) {
        std::vector<Handle> vec(handles.begin(), handles.end());
        join_ptr->template add<I>(std::move(vec));
    };

    return BarrierView<T, Key, Extract, Handle>(
        desc.source,
        std::move(desc.keys),
        std::move(injector),
        std::move(desc.extract));
}

template <class Fn, class... Descs, std::size_t... Is>
auto gateway_impl(Fn&& fn, std::tuple<Descs...> descs_tuple, std::index_sequence<Is...>) {
    using LayoutT = Layout<Slot<Is>...>;

    // NJoin receives const vector<Handle>& per slot; wrap to give user span<const Handle>
    auto wrapped_fn = [user_fn = std::forward<Fn>(fn)](
        const std::vector<typename Descs::handle_type>&... vecs) {
        user_fn(std::span<const typename Descs::handle_type>(vecs)...);
    };

    auto join_ptr = std::make_shared<decltype(make_reactive<LayoutT>(std::move(wrapped_fn)))>(
        make_reactive<LayoutT>(std::move(wrapped_fn)));

    auto barriers = std::make_tuple(
        make_barrier_from_gate<Is>(join_ptr, std::get<Is>(descs_tuple))...
    );

    auto source_ptrs = std::make_tuple(&std::get<Is>(descs_tuple).source...);

    using JoinT = std::remove_reference_t<decltype(*join_ptr)>;
    return Gateway<JoinT, Descs...>(std::move(join_ptr), std::move(barriers), source_ptrs);
}

// ─── Dispatch overloads (1–4 gates) ────────────────────────────────
// Last argument is always the callback; preceding args are gate descriptors.

template <class T>
concept IsGateDescriptor = requires { typename T::handle_type; typename T::key_type; };

template <IsGateDescriptor Desc, class Fn>
auto gateway_dispatch(Desc desc, Fn&& fn) {
    auto t = std::make_tuple(std::move(desc));
    return gateway_impl(std::forward<Fn>(fn), std::move(t), std::index_sequence<0>{});
}

template <IsGateDescriptor D1, IsGateDescriptor D2, class Fn>
auto gateway_dispatch(D1 d1, D2 d2, Fn&& fn) {
    auto t = std::make_tuple(std::move(d1), std::move(d2));
    return gateway_impl(std::forward<Fn>(fn), std::move(t), std::index_sequence<0, 1>{});
}

template <IsGateDescriptor D1, IsGateDescriptor D2, IsGateDescriptor D3, class Fn>
auto gateway_dispatch(D1 d1, D2 d2, D3 d3, Fn&& fn) {
    auto t = std::make_tuple(std::move(d1), std::move(d2), std::move(d3));
    return gateway_impl(std::forward<Fn>(fn), std::move(t), std::index_sequence<0, 1, 2>{});
}

template <IsGateDescriptor D1, IsGateDescriptor D2, IsGateDescriptor D3, IsGateDescriptor D4, class Fn>
auto gateway_dispatch(D1 d1, D2 d2, D3 d3, D4 d4, Fn&& fn) {
    auto t = std::make_tuple(std::move(d1), std::move(d2), std::move(d3), std::move(d4));
    return gateway_impl(std::forward<Fn>(fn), std::move(t), std::index_sequence<0, 1, 2, 3>{});
}

} // namespace detail

/// gateway(gate(...), ..., callback) — declarative reactive wiring.
///
/// Usage:
///   auto fb = gateway(
///       gate(shared_views, {0, 1, 3}, view_ext),
///       gate(shared_passes, {"main"}, pass_ext),
///       [](std::span<const uint64_t> views, std::span<const uint64_t> passes) { ... }
///   );
///
template <class... Args>
auto gateway(Args&&... args) {
    return detail::gateway_dispatch(std::forward<Args>(args)...);
}

} // namespace seqjoin
