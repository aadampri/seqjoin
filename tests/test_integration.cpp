// seqjoin — integration tests: NJoin + Layout + make_reactive + SourceView
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── Test 1: make_reactive with groups + source access ───────────────
// Demonstrates: make_reactive<Layout<...>>(fn), add<I>(args...), source<I>()
void test_grouped_join_with_source_access() {
    // Lambda: (name, age, score)
    // Layout: Group<0,1> → source 0 stores tuple<string,int>
    //         Group<2>   → source 1 stores double
    std::vector<std::string> emissions;

    auto join = make_reactive<Layout<Group<0, 1>, Group<2>>>(
        [&](const std::string& name, int age, double score) {
            emissions.push_back(name + ":" + std::to_string(age)
                                + ":" + std::to_string(score));
        });

    // Verify layout type
    using JoinT = decltype(join);
    using L = JoinT::layout_type;
    static_assert(L::source_count == 2);
    static_assert(L::param_count == 3);

    // source<0> stores tuple<string, int>
    auto& src0 = join.source<0>();
    static_assert(std::is_same_v<
        typename std::remove_reference_t<decltype(src0)>::value_type,
        std::tuple<std::string, int>>);

    // source<1> stores double
    auto& src1 = join.source<1>();
    static_assert(std::is_same_v<
        typename std::remove_reference_t<decltype(src1)>::value_type,
        double>);

    // Insert grouped data → source 0
    join.add<0>(std::string("alice"), 30);
    assert(emissions.empty());  // source 1 empty

    // Insert score → source 1 → triggers emit
    join.add<1>(99.5);
    assert(emissions.size() == 1);
    assert(emissions[0].find("alice") != std::string::npos);
    assert(emissions[0].find("30") != std::string::npos);

    // Update group → re-emit
    join.add<0>(std::string("bob"), 25);
    assert(emissions.size() == 2);
    assert(emissions[1].find("bob") != std::string::npos);

    std::cout << "  PASS: grouped_join_with_source_access\n";
}

// ─── Test 2: SourceView on a grouped source ──────────────────────────
// Demonstrates: source<I>().view<Is...>(), projected scan, sub-view
void test_source_view_from_grouped_join() {
    auto join = make_reactive<Layout<Group<0, 1, 2>>>(
        [](const std::string&, int, double) {});

    // Insert some grouped data
    join.add<0>(std::string("alice"), 30, 99.5);
    join.add<0>(std::string("bob"), 25, 88.0);

    // Get the source — stores tuple<string, int, double>
    auto& src = join.source<0>();

    // Create a view projecting just the name (index 0)
    auto v_name = src.view<0>();
    static_assert(std::is_same_v<decltype(v_name)::value_type, std::string>);

    // With RetainLatest, we should see only the latest value
    std::vector<std::string> names;
    v_name.scan([&](const std::string& name, uint64_t) {
        names.push_back(name);
    });
    assert(names.size() == 1);      // RetainLatest keeps only last
    assert(names[0] == "bob");

    // Create a view projecting (name, score) — indices 0 and 2
    auto v_name_score = src.view<0, 2>();
    using VS = decltype(v_name_score)::value_type;
    static_assert(std::is_same_v<VS, std::tuple<std::string, double>>);

    std::vector<std::tuple<std::string, double>> name_scores;
    v_name_score.scan([&](const std::tuple<std::string, double>& val, uint64_t) {
        name_scores.push_back(val);
    });
    assert(name_scores.size() == 1);
    assert(std::get<0>(name_scores[0]) == "bob");
    assert(std::get<1>(name_scores[0]) == 88.0);

    std::cout << "  PASS: source_view_from_grouped_join\n";
}

// ─── Test 3: Sub-view (view of a view) flattens to root ─────────────
void test_sub_view_flattening() {
    auto join = make_reactive<Layout<Group<0, 1, 2>>>(
        [](const std::string&, int, double) {});

    join.add<0>(std::string("x"), 42, 3.14);

    auto& src = join.source<0>();

    // view<0,1> projects (string, int)
    auto v_ab = src.view<0, 1>();

    // v_ab.view<1>() should project local index 1 → root index 1 (int)
    auto v_b = v_ab.view<1>();
    static_assert(std::is_same_v<decltype(v_b)::value_type, int>);

    int scanned_val = 0;
    v_b.scan([&](int val, uint64_t) { scanned_val = val; });
    assert(scanned_val == 42);

    // v_ab.view<0>() → root index 0 (string)
    auto v_a = v_ab.view<0>();
    static_assert(std::is_same_v<decltype(v_a)::value_type, std::string>);

    std::string scanned_name;
    v_a.scan([&](const std::string& name, uint64_t) { scanned_name = name; });
    assert(scanned_name == "x");

    std::cout << "  PASS: sub_view_flattening\n";
}

// ─── Test 4: View merge ──────────────────────────────────────────────
void test_view_merge() {
    auto join = make_reactive<Layout<Group<0, 1, 2>>>(
        [](const std::string&, int, double) {});

    join.add<0>(std::string("y"), 7, 2.71);

    auto& src = join.source<0>();

    auto v_a = src.view<0>();   // projects string
    auto v_c = src.view<2>();   // projects double

    // Merge: union of index sets → view<0, 2>
    auto v_ac = v_a.merge(v_c);
    using VAC = decltype(v_ac)::value_type;
    static_assert(std::is_same_v<VAC, std::tuple<std::string, double>>);

    std::tuple<std::string, double> result;
    v_ac.scan([&](const std::tuple<std::string, double>& val, uint64_t) {
        result = val;
    });
    assert(std::get<0>(result) == "y");
    assert(std::get<1>(result) == 2.71);

    std::cout << "  PASS: view_merge\n";
}

// ─── Test 5: is_refinement_of compile-time checks ────────────────────
void test_is_refinement_of() {
    // Floor refines everything
    using Floor = Layout<Group<0>, Group<1>, Group<2>>;
    using Mid   = Layout<Group<0, 1>, Group<2>>;
    using Ceil  = Layout<Group<0, 1, 2>>;

    static_assert(is_refinement_of_v<Floor, Mid>);
    static_assert(is_refinement_of_v<Floor, Ceil>);
    static_assert(is_refinement_of_v<Mid, Ceil>);

    // Ceiling refines only itself
    static_assert(is_refinement_of_v<Ceil, Ceil>);
    static_assert(!is_refinement_of_v<Ceil, Mid>);
    static_assert(!is_refinement_of_v<Ceil, Floor>);

    // Mid does not refine Floor (Group<0,1> is not in any single block of Floor)
    static_assert(!is_refinement_of_v<Mid, Floor>);

    // Incomparable: {0,1}{2} vs {0,2}{1}
    using A = Layout<Group<0, 1>, Group<2>>;
    using B = Layout<Group<0, 2>, Group<1>>;
    static_assert(!is_refinement_of_v<A, B>);
    static_assert(!is_refinement_of_v<B, A>);

    std::cout << "  PASS: is_refinement_of\n";
}

// ─── Test 6: Full end-to-end: make_reactive + groups + views + scan ──
// The complete workflow a user would follow.
void test_end_to_end() {
    // Scenario: service discovery join
    // Source 0: (endpoint, config) — grouped, arrive together
    // Source 1: health status — arrives independently
    std::vector<std::string> deployed;

    auto join = make_reactive<Layout<Group<0, 1>, Group<2>>>(
        [&](const std::string& endpoint, int port, bool healthy) {
            if (healthy) {
                deployed.push_back(endpoint + ":" + std::to_string(port));
            }
        });

    // Insert endpoint+config as a group
    join.add<0>(std::string("svc-a.prod"), 8080);
    assert(deployed.empty());

    // Insert health → triggers emit
    join.add<1>(true);
    assert(deployed.size() == 1);
    assert(deployed[0] == "svc-a.prod:8080");

    // Inspect the grouped source via a view
    auto& ep_config_src = join.source<0>();
    auto v_endpoint = ep_config_src.view<0>();
    auto v_port = ep_config_src.view<1>();

    std::string last_ep;
    v_endpoint.scan([&](const std::string& ep, uint64_t) { last_ep = ep; });
    assert(last_ep == "svc-a.prod");

    int last_port = 0;
    v_port.scan([&](int port, uint64_t) { last_port = port; });
    assert(last_port == 8080);

    // Update just endpoint+config, keep same health → re-emit
    join.add<0>(std::string("svc-b.prod"), 9090);
    assert(deployed.size() == 2);
    assert(deployed[1] == "svc-b.prod:9090");

    // Verify view reflects update
    v_endpoint.scan([&](const std::string& ep, uint64_t) { last_ep = ep; });
    assert(last_ep == "svc-b.prod");

    std::cout << "  PASS: end_to_end\n";
}

// ─── Test 7: make_reactive overload (4): explicit storage types ──────
void test_make_reactive_explicit_storage() {
    std::vector<std::string> emissions;

    // Overload (4): explicit storage, default layout
    // Lambda takes string_view but we store string
    auto join = make_reactive<std::string, int>(
        [&](const std::string& name, int age) {
            emissions.push_back(name + ":" + std::to_string(age));
        });

    join.add<0>(std::string("carol"));
    join.add<1>(40);
    assert(emissions.size() == 1);
    assert(emissions[0] == "carol:40");

    std::cout << "  PASS: make_reactive_explicit_storage\n";
}

int main() {
    std::cout << "seqjoin integration tests (NJoin + Layout + SourceView):\n";
    test_grouped_join_with_source_access();
    test_source_view_from_grouped_join();
    test_sub_view_flattening();
    test_view_merge();
    test_is_refinement_of();
    test_end_to_end();
    test_make_reactive_explicit_storage();
    std::cout << "All integration tests passed.\n";
    return 0;
}
