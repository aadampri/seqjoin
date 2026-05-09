// seqjoin — Policies<...> integration tests
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── Test 1: Overload (5) — Layout + Policies, storage deduced ───
// Grouped source with RetainByKey, second source with RetainLatest (auto)
void test_policies_layout_deduced_storage() {
    // Lambda: (string name, int age, double score)
    // Layout: Group<0,1> (name+age → one source), Slot<2> (score alone)
    // Policies: RetainByKey for source 0 (keys on name), AutoPolicy for source 1

    using L = Layout<Group<0, 1>, Slot<2>>;
    using P = Policies<RetainByKey, AutoPolicy_t>;

    std::vector<std::tuple<std::string, int, double>> emissions;

    auto join = make_reactive<L, P>(
        [&](const std::string& name, int age, double score) {
            emissions.emplace_back(name, age, score);
        });

    // Source 0 stores tuple<string, int> with RetainByKey (keys on get<0> = name)
    // Source 1 stores double with RetainLatest

    join.add<0>(std::string("alice"), 30);  // no emit (source 1 empty)
    assert(emissions.empty());

    join.add<1>(9.5);  // emit: ("alice", 30, 9.5)
    assert(emissions.size() == 1);
    assert(std::get<0>(emissions[0]) == "alice");
    assert(std::get<1>(emissions[0]) == 30);
    assert(std::get<2>(emissions[0]) == 9.5);
    emissions.clear();

    // Same key "alice", same value → rejected (no emit)
    join.add<0>(std::string("alice"), 30);
    assert(emissions.empty());

    // Same key "alice", different value → overwrite, emit
    join.add<0>(std::string("alice"), 31);
    assert(emissions.size() == 1);
    assert(std::get<1>(emissions[0]) == 31);
    emissions.clear();

    // New key "bob" → emit
    join.add<0>(std::string("bob"), 25);
    assert(emissions.size() == 1);
    assert(std::get<0>(emissions[0]) == "bob");
    emissions.clear();

    // Update score → emits for BOTH entries (alice + bob)
    join.add<1>(10.0);
    assert(emissions.size() == 2);

    std::cout << "  PASS: policies_layout_deduced_storage\n";
}

// ─── Test 2: Overload (5) — All sources with RetainAll ───
void test_policies_all_retain_all() {
    using L = Layout<Slot<0>, Slot<1>>;
    using P = Policies<RetainAll, RetainAll>;

    std::vector<std::pair<std::string, int>> emissions;

    auto join = make_reactive<L, P>(
        [&](const std::string& name, const int& n) {
            emissions.emplace_back(name, n);
        });

    join.add<0>(std::string("x"));
    join.add<0>(std::string("y"));
    assert(emissions.empty());  // source 1 empty

    join.add<1>(1);
    // Cross-product: {x, y} × {1} = 2
    // Seq-ownership: 1 has max seq → both emit
    assert(emissions.size() == 2);
    emissions.clear();

    join.add<1>(2);
    // Cross-product: {x, y} × {1, 2} = 4
    // Seq-ownership: only tuples where source 1's entry has max seq (= seq of "2")
    // So: (x, 2) and (y, 2) → 2 emissions
    assert(emissions.size() == 2);

    std::cout << "  PASS: policies_all_retain_all\n";
}

// ─── Test 3: Overload (5) — Mixed: RetainByKey + RetainAll ───
void test_policies_mixed() {
    // Devices keyed by ID × a set of configs
    using Device = std::tuple<int, std::string>;  // (device_id, status)

    using L = Layout<Slot<0>, Slot<1>>;
    using P = Policies<RetainByKey, RetainAll>;

    std::vector<std::pair<Device, std::string>> emissions;

    auto join = make_reactive<L, P>(
        [&](const Device& dev, const std::string& cfg) {
            emissions.emplace_back(dev, cfg);
        });

    join.add<0>(Device{1, "online"});
    join.add<1>(std::string("cfgA"));
    assert(emissions.size() == 1);
    emissions.clear();

    join.add<1>(std::string("cfgB"));
    // Device 1 × {cfgA, cfgB}: only (device1, cfgB) emits (seq-ownership)
    assert(emissions.size() == 1);
    assert(emissions[0].second == "cfgB");
    emissions.clear();

    // Update device 1 status → emits for all configs in source 1
    join.add<0>(Device{1, "offline"});
    assert(emissions.size() == 2);  // (device1_new, cfgA) + (device1_new, cfgB)
    assert(std::get<1>(emissions[0].first) == "offline");

    std::cout << "  PASS: policies_mixed\n";
}

// ─── Test 4: Overload (2) still works (no policies) ───
void test_no_policies_still_works() {
    using L = Layout<Group<0, 1>, Slot<2>>;

    std::vector<std::tuple<std::string, int, double>> emissions;

    auto join = make_reactive<L>(
        [&](const std::string& name, int age, double score) {
            emissions.emplace_back(name, age, score);
        });

    // Default policy = RetainLatest for both sources
    join.add<0>(std::string("alice"), 30);
    join.add<1>(9.5);
    assert(emissions.size() == 1);
    emissions.clear();

    // Overwrite source 0 (RetainLatest): emit with new value
    join.add<0>(std::string("alice"), 31);
    assert(emissions.size() == 1);
    assert(std::get<1>(emissions[0]) == 31);

    std::cout << "  PASS: no_policies_still_works\n";
}

// ─── Test 5: AutoPolicy_t mixes default into explicit list ───
void test_auto_policy_mixed() {
    // 3-way join: Source 0 = RetainByKey, Source 1 = default, Source 2 = RetainAll
    using L = Layout<Slot<0>, Slot<1>, Slot<2>>;
    using P = Policies<RetainByKey, AutoPolicy_t, RetainAll>;

    int emit_count = 0;

    auto join = make_reactive<L, P>(
        [&](const std::tuple<int, std::string>& /*keyed*/,
            const double& /*latest*/,
            const std::string& /*accumulated*/) {
            ++emit_count;
        });

    using Entry = std::tuple<int, std::string>;
    join.add<0>(Entry{1, "a"});
    join.add<1>(3.14);
    assert(emit_count == 0);  // source 2 empty

    join.add<2>(std::string("x"));
    assert(emit_count == 1);

    // Duplicate in source 0 → rejected
    join.add<0>(Entry{1, "a"});
    assert(emit_count == 1);

    // Same key, new value in source 0 → emits
    join.add<0>(Entry{1, "b"});
    assert(emit_count == 2);

    std::cout << "  PASS: auto_policy_mixed\n";
}

int main() {
    std::cout << "seqjoin Policies tests:\n";
    test_policies_layout_deduced_storage();
    test_policies_all_retain_all();
    test_policies_mixed();
    test_no_policies_still_works();
    test_auto_policy_mixed();
    std::cout << "All Policies tests passed.\n";
    return 0;
}
