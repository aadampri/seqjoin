// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <tuple>
#include <type_traits>

namespace seqjoin {

// ─── callable_traits ─────────────────────────────────────────────────
// Extracts parameter types from a callable (lambda, function pointer,
// std::function, or any object with a single non-overloaded operator()).
//
// Usage:
//   auto fn = [](const std::string& s, int n) { ... };
//   using Traits = callable_traits<decltype(fn)>;
//   // Traits::arity          == 2
//   // Traits::arg_t<0>       == const std::string&
//   // Traits::decay_arg_t<0> == std::string
//   // Traits::args_tuple     == std::tuple<const std::string&, int>

namespace detail {

// Base: shared body for all callable forms.
template <class R, class... Args>
struct callable_traits_base {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// Primary: strip callable down to its operator()
template <class T>
struct callable_traits_impl
    : callable_traits_impl<decltype(&std::remove_reference_t<T>::operator())> {};

template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...) const> : callable_traits_base<R, Args...> {};

template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...)> : callable_traits_base<R, Args...> {};

template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...) const noexcept> : callable_traits_base<R, Args...> {};

template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...) noexcept> : callable_traits_base<R, Args...> {};

template <class R, class... Args>
struct callable_traits_impl<R(*)(Args...)> : callable_traits_base<R, Args...> {};

template <class R, class... Args>
struct callable_traits_impl<R(*)(Args...) noexcept> : callable_traits_base<R, Args...> {};

template <class R, class... Args>
struct callable_traits_impl<R(&)(Args...)> : callable_traits_base<R, Args...> {};

} // namespace detail

template <class F>
struct callable_traits : detail::callable_traits_impl<F> {
    using base = detail::callable_traits_impl<F>;
    using base::arity;

    /// Decayed (no ref, no const) type of argument I.
    template <std::size_t I>
    using decay_arg_t = std::decay_t<typename base::template arg_t<I>>;
};

} // namespace seqjoin
