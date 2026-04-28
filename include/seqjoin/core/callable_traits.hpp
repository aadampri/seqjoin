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

// Primary: strip callable down to its operator()
template <class T>
struct callable_traits_impl
    : callable_traits_impl<decltype(&std::remove_reference_t<T>::operator())> {};

// const member function pointer
template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...) const> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// non-const member function pointer
template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...)> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// noexcept const member function pointer
template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...) const noexcept> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// noexcept non-const member function pointer
template <class C, class R, class... Args>
struct callable_traits_impl<R(C::*)(Args...) noexcept> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// plain function pointer
template <class R, class... Args>
struct callable_traits_impl<R(*)(Args...)> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// plain function pointer (noexcept)
template <class R, class... Args>
struct callable_traits_impl<R(*)(Args...) noexcept> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

// plain function reference
template <class R, class... Args>
struct callable_traits_impl<R(&)(Args...)> {
    static constexpr std::size_t arity = sizeof...(Args);
    using return_type = R;
    using args_tuple = std::tuple<Args...>;
    template <std::size_t I>
    using arg_t = std::tuple_element_t<I, args_tuple>;
};

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
