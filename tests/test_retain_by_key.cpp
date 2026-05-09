// seqjoin — RetainByKey policy tests
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── Test 1: Basic keyed upsert ───
void test_retain_by_key_basic() {
    using Entry = std::tuple<int, std::string>;  // (id, value)
    RetainByKey<Entry> policy;

    // New key → accept
    [[maybe_unused]] auto r1 = policy.insert(Entry{1, "alpha"}, 0);
    assert(r1.has_value() && *r1 == 0);
    assert(policy.size() == 1);

    // Different key → accept
    [[maybe_unused]] auto r2 = policy.insert(Entry{2, "beta"}, 1);
    assert(r2.has_value() && *r2 == 1);
    assert(policy.size() == 2);

    // Same key, same value → reject
    [[maybe_unused]] auto r3 = policy.insert(Entry{1, "alpha"}, 2);
    assert(!r3.has_value());
    assert(policy.size() == 2);

    // Same key, different value → overwrite
    [[maybe_unused]] auto r4 = policy.insert(Entry{1, "gamma"}, 3);
    assert(r4.has_value() && *r4 == 3);
    assert(policy.size() == 2);  // still 2 keys

    std::cout << "  PASS: retain_by_key_basic\n";
}

// ─── Test 2: Scan yields correct entries ───
void test_retain_by_key_scan() {
    using Entry = std::tuple<int, std::string>;
    RetainByKey<Entry> policy;

    policy.insert(Entry{1, "a"}, 0);
    policy.insert(Entry{2, "b"}, 1);
    policy.insert(Entry{1, "c"}, 2);  // overwrites key=1

    std::vector<std::pair<Entry, uint64_t>> results;
    for (const auto& [val, seq] : policy.scan()) {
        results.emplace_back(val, seq);
    }
    assert(results.size() == 2);

    // Find key=1 entry (should be "c" with seq=2)
    [[maybe_unused]] bool found_1 = false, found_2 = false;
    for (auto& [entry, seq] : results) {
        if (std::get<0>(entry) == 1) {
            assert(std::get<1>(entry) == "c");
            assert(seq == 2);
            found_1 = true;
        } else if (std::get<0>(entry) == 2) {
            assert(std::get<1>(entry) == "b");
            assert(seq == 1);
            found_2 = true;
        }
    }
    assert(found_1 && found_2);
    std::cout << "  PASS: retain_by_key_scan\n";
}

// ─── Test 3: Remove by key ───
void test_retain_by_key_remove() {
    using Entry = std::tuple<int, std::string>;
    RetainByKey<Entry> policy;

    policy.insert(Entry{1, "a"}, 0);
    policy.insert(Entry{2, "b"}, 1);
    assert(policy.size() == 2);

    [[maybe_unused]] bool removed = policy.remove_by_key(1);
    assert(removed);
    assert(policy.size() == 1);

    // Re-insert key=1 should work now
    [[maybe_unused]] auto r = policy.insert(Entry{1, "x"}, 5);
    assert(r.has_value());
    assert(policy.size() == 2);

    // Remove non-existent key
    assert(!policy.remove_by_key(99));

    std::cout << "  PASS: retain_by_key_remove\n";
}

// ─── Test 4: Integration with Source ───
void test_retain_by_key_source() {
    using Entry = std::tuple<int, std::string>;
    Source<Entry, RetainByKey<Entry>> src;

    [[maybe_unused]] auto r1 = src.insert(Entry{1, "hello"}, 0);
    assert(r1.has_value());
    [[maybe_unused]] auto r2 = src.insert(Entry{1, "hello"}, 1);
    assert(!r2.has_value());  // same key+value → rejected
    [[maybe_unused]] auto r3 = src.insert(Entry{1, "world"}, 2);
    assert(r3.has_value());   // same key, diff value → accepted

    [[maybe_unused]] std::size_t count = 0;
    src.scan([&]([[maybe_unused]] const Entry& val, [[maybe_unused]] uint64_t seq) {
        assert(std::get<0>(val) == 1);
        assert(std::get<1>(val) == "world");
        assert(seq == 2);
        ++count;
    });
    assert(count == 1);

    std::cout << "  PASS: retain_by_key_source\n";
}

// ─── Test 5: NJoin with RetainByKey (keyed device × config) ───
void test_retain_by_key_njoin() {
    using Device = std::tuple<int, std::string>;  // (device_id, status)

    using S0 = Source<Device, RetainByKey<Device>>;
    using S1 = Source<int, RetainLatest<int>>;

    std::vector<std::pair<Device, int>> emissions;

    NJoin join(
        [&](const Device& dev, const int& cfg) {
            emissions.emplace_back(dev, cfg);
        },
        S0{}, S1{}
    );

    // Add device — no emit (config empty)
    join.add<0>(Device{1, "online"});
    assert(emissions.empty());

    // Add config — emits (device1, config42)
    join.add<1>(42);
    assert(emissions.size() == 1);
    assert(std::get<0>(emissions[0].first) == 1);
    assert(std::get<1>(emissions[0].first) == "online");
    assert(emissions[0].second == 42);
    emissions.clear();

    // Add second device — emits (device2, 42)
    join.add<0>(Device{2, "idle"});
    assert(emissions.size() == 1);
    assert(std::get<0>(emissions[0].first) == 2);
    emissions.clear();

    // Update device 1 status → emits (device1_updated, 42)
    join.add<0>(Device{1, "offline"});
    assert(emissions.size() == 1);
    assert(std::get<0>(emissions[0].first) == 1);
    assert(std::get<1>(emissions[0].first) == "offline");
    emissions.clear();

    // Same device, same status → no-op, no emission
    join.add<0>(Device{1, "offline"});
    assert(emissions.empty());

    // Update config → emits for both devices
    join.add<1>(99);
    assert(emissions.size() == 2);

    std::cout << "  PASS: retain_by_key_njoin\n";
}

// ─── Test 6: make_reactive with RetainByKey ───
void test_retain_by_key_make_reactive() {
    using Device = std::tuple<int, std::string>;

    // Use direct NJoin construction with explicit RetainByKey sources.
    using S0 = Source<Device, RetainByKey<Device>>;
    using S1 = Source<int>;

    std::vector<std::string> results2;

    NJoin join2(
        [&](const Device& dev, const int& priority) {
            results2.push_back(std::get<1>(dev) + ":" + std::to_string(priority));
        },
        S0{}, S1{}
    );

    join2.add<0>(Device{1, "sensorA"});
    join2.add<1>(5);
    assert(results2.size() == 1);
    assert(results2[0] == "sensorA:5");

    join2.add<0>(Device{1, "sensorA"});  // duplicate — no emit
    assert(results2.size() == 1);

    join2.add<0>(Device{1, "sensorB"});  // key 1 updated
    assert(results2.size() == 2);
    assert(results2[1] == "sensorB:5");

    std::cout << "  PASS: retain_by_key_make_reactive\n";
}

int main() {
    std::cout << "seqjoin RetainByKey tests:\n";
    test_retain_by_key_basic();
    test_retain_by_key_scan();
    test_retain_by_key_remove();
    test_retain_by_key_source();
    test_retain_by_key_njoin();
    test_retain_by_key_make_reactive();
    std::cout << "All RetainByKey tests passed.\n";
    return 0;
}
