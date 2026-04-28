// seqjoin — callable_traits + group_layout tests
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <string>
#include <tuple>
#include <type_traits>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── callable_traits tests ───────────────────────────────────────────

void test_callable_traits_lambda() {
    auto fn = [](const std::string& s, int n, double d) { (void)s; (void)n; (void)d; };
    using Traits = callable_traits<decltype(fn)>;

    static_assert(Traits::arity == 3);
    static_assert(std::is_same_v<Traits::decay_arg_t<0>, std::string>);
    static_assert(std::is_same_v<Traits::decay_arg_t<1>, int>);
    static_assert(std::is_same_v<Traits::decay_arg_t<2>, double>);
    static_assert(std::is_same_v<Traits::return_type, void>);

    // arg_t preserves const ref
    static_assert(std::is_same_v<Traits::arg_t<0>, const std::string&>);
    static_assert(std::is_same_v<Traits::arg_t<1>, int>);

    std::cout << "  PASS: callable_traits_lambda\n";
}

void test_callable_traits_function_ptr() {
    using Traits = callable_traits<int(*)(float, char)>;
    static_assert(Traits::arity == 2);
    static_assert(std::is_same_v<Traits::return_type, int>);
    static_assert(std::is_same_v<Traits::decay_arg_t<0>, float>);
    static_assert(std::is_same_v<Traits::decay_arg_t<1>, char>);

    std::cout << "  PASS: callable_traits_function_ptr\n";
}

void test_callable_traits_mutable_lambda() {
    auto fn = [x = 0](int n) mutable { x += n; return x; };
    using Traits = callable_traits<decltype(fn)>;
    static_assert(Traits::arity == 1);
    static_assert(std::is_same_v<Traits::decay_arg_t<0>, int>);
    static_assert(std::is_same_v<Traits::return_type, int>);

    std::cout << "  PASS: callable_traits_mutable_lambda\n";
}

// ─── group_layout tests ─────────────────────────────────────────────

void test_default_layout() {
    using L = DefaultLayout<3>;  // Layout<Slot<0>, Slot<1>, Slot<2>>
    static_assert(L::source_count == 3);
    static_assert(L::param_count == 3);

    static_assert(source_index_of<0, L> == 0);
    static_assert(source_index_of<1, L> == 1);
    static_assert(source_index_of<2, L> == 2);

    std::cout << "  PASS: default_layout\n";
}

void test_grouped_layout() {
    // params: A(0), B(1), C(2)
    // Group<0,1> → source 0 stores tuple<A,B>, Slot<2> → source 1 stores C
    using L = Layout<Group<0, 1>, Slot<2>>;
    static_assert(L::source_count == 2);
    static_assert(L::param_count == 3);

    static_assert(source_index_of<0, L> == 0);  // param 0 → source 0 (group)
    static_assert(source_index_of<1, L> == 0);  // param 1 → source 0 (group)
    static_assert(source_index_of<2, L> == 1);  // param 2 → source 1

    std::cout << "  PASS: grouped_layout\n";
}

void test_source_value_type() {
    using Params = std::tuple<std::string, int, double>;

    // Slot<0> → string
    using T0 = source_value_type<Slot<0>, Params>;
    static_assert(std::is_same_v<T0, std::string>);

    // Slot<2> → double
    using T2 = source_value_type<Slot<2>, Params>;
    static_assert(std::is_same_v<T2, double>);

    // Group<0,1> → tuple<string, int>
    using TG = source_value_type<Group<0, 1>, Params>;
    static_assert(std::is_same_v<TG, std::tuple<std::string, int>>);

    std::cout << "  PASS: source_value_type\n";
}

void test_layout_unpack_default() {
    // 1:1 layout: 3 slots, values are raw types
    using L = DefaultLayout<3>;
    auto values = std::make_tuple(std::string("hello"), 42, 3.14);
    auto unpacked = detail::layout_unpack<L>(values);

    assert(std::get<0>(unpacked) == "hello");
    assert(std::get<1>(unpacked) == 42);
    assert(std::get<2>(unpacked) == 3.14);

    std::cout << "  PASS: layout_unpack_default\n";
}

void test_layout_unpack_grouped() {
    // Layout<Group<0,1>, Slot<2>>
    // Source 0 stores tuple<string, int>, Source 1 stores double
    using L = Layout<Group<0, 1>, Slot<2>>;
    auto values = std::make_tuple(
        std::make_tuple(std::string("hi"), 7),  // source 0: tuple<string,int>
        3.14                                     // source 1: double
    );
    auto unpacked = detail::layout_unpack<L>(values);

    // Should produce: (string, int, double) — 3 elements
    static_assert(std::tuple_size_v<decltype(unpacked)> == 3);
    assert(std::get<0>(unpacked) == "hi");
    assert(std::get<1>(unpacked) == 7);
    assert(std::get<2>(unpacked) == 3.14);

    std::cout << "  PASS: layout_unpack_grouped\n";
}

void test_complex_layout() {
    // 5 params: A(0), B(1), C(2), D(3), E(4)
    // Layout: Group<0,1>, Slot<2>, Group<3,4>
    // → 3 sources: tuple<A,B>, C, tuple<D,E>
    using L = Layout<Group<0, 1>, Slot<2>, Group<3, 4>>;
    static_assert(L::source_count == 3);
    static_assert(L::param_count == 5);

    static_assert(source_index_of<0, L> == 0);
    static_assert(source_index_of<1, L> == 0);
    static_assert(source_index_of<2, L> == 1);
    static_assert(source_index_of<3, L> == 2);
    static_assert(source_index_of<4, L> == 2);

    std::cout << "  PASS: complex_layout\n";
}

int main() {
    std::cout << "seqjoin Phase 4 tests (callable_traits + group_layout):\n";
    test_callable_traits_lambda();
    test_callable_traits_function_ptr();
    test_callable_traits_mutable_lambda();
    test_default_layout();
    test_grouped_layout();
    test_source_value_type();
    test_layout_unpack_default();
    test_layout_unpack_grouped();
    test_complex_layout();
    std::cout << "All Phase 4 tests passed.\n";
    return 0;
}
