// seqjoin — RetainByKeyCOW policy tests
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

using Entry = std::tuple<int, std::string>;  // (id, value)

// ─── Test 1: Basic keyed upsert ───
void test_cow_keyed_basic() {
    RetainByKeyCOW<Entry> policy;

    // New key → accept
    auto r1 = policy.insert(Entry{1, "alpha"}, 0);
    assert(r1.has_value() && *r1 == 0);
    assert(policy.size() == 1);

    // Different key → accept
    auto r2 = policy.insert(Entry{2, "beta"}, 1);
    assert(r2.has_value() && *r2 == 1);
    assert(policy.size() == 2);

    // Same key, same value → reject
    auto r3 = policy.insert(Entry{1, "alpha"}, 2);
    assert(!r3.has_value());
    assert(policy.size() == 2);

    // Same key, different value → overwrite
    auto r4 = policy.insert(Entry{1, "gamma"}, 3);
    assert(r4.has_value() && *r4 == 3);
    assert(policy.size() == 2);

    std::cout << "  PASS: cow_keyed_basic\n";
}

// ─── Test 2: Snapshot immutability ───
void test_cow_keyed_snapshot_immutability() {
    RetainByKeyCOW<Entry> policy;
    policy.insert(Entry{1, "a"}, 0);
    policy.insert(Entry{2, "b"}, 1);

    auto snap = policy.snapshot();
    assert(snap.size() == 2);

    // Mutate — snapshot must not change
    policy.insert(Entry{3, "c"}, 2);
    policy.insert(Entry{1, "z"}, 3);  // overwrite key 1
    policy.remove(Entry{2, "b"});

    assert(snap.size() == 2);

    std::vector<Entry> values;
    for (const auto& entry : snap) {
        values.push_back(entry.value);
    }
    assert(values.size() == 2);

    // Must contain original entries
    bool has_1a = false, has_2b = false;
    for (const auto& v : values) {
        if (std::get<0>(v) == 1 && std::get<1>(v) == "a") has_1a = true;
        if (std::get<0>(v) == 2 && std::get<1>(v) == "b") has_2b = true;
    }
    assert(has_1a && has_2b);

    std::cout << "  PASS: cow_keyed_snapshot_immutability\n";
}

// ─── Test 3: Scan compatibility ───
void test_cow_keyed_scan() {
    RetainByKeyCOW<Entry> policy;
    policy.insert(Entry{1, "x"}, 0);
    policy.insert(Entry{2, "y"}, 1);
    policy.insert(Entry{1, "z"}, 2);  // overwrite key 1

    std::vector<std::pair<Entry, uint64_t>> results;
    for (const auto& [val, seq] : policy.scan()) {
        results.emplace_back(val, seq);
    }
    assert(results.size() == 2);

    // Key 1 should have "z" (overwritten)
    for (const auto& [entry, seq] : results) {
        if (std::get<0>(entry) == 1) {
            assert(std::get<1>(entry) == "z");
            assert(seq == 2);
        }
    }

    std::cout << "  PASS: cow_keyed_scan\n";
}

// ─── Test 4: Remove ───
void test_cow_keyed_remove() {
    RetainByKeyCOW<Entry> policy;
    policy.insert(Entry{1, "a"}, 0);
    policy.insert(Entry{2, "b"}, 1);
    policy.insert(Entry{3, "c"}, 2);

    assert(policy.remove(Entry{2, "b"}));
    assert(policy.size() == 2);
    assert(!policy.remove(Entry{99, "x"}));  // not found

    // remove_by_key
    assert(policy.remove_by_key(3));
    assert(policy.size() == 1);
    assert(!policy.remove_by_key(99));

    std::cout << "  PASS: cow_keyed_remove\n";
}

// ─── Test 5: Clear ───
void test_cow_keyed_clear() {
    RetainByKeyCOW<Entry> policy;
    policy.insert(Entry{1, "a"}, 0);
    policy.insert(Entry{2, "b"}, 1);

    auto snap = policy.snapshot();
    policy.clear();
    assert(policy.empty());

    // Snapshot still has old data
    assert(snap.size() == 2);

    std::cout << "  PASS: cow_keyed_clear\n";
}

// ─── Test 6: Source integration ───
void test_cow_keyed_source() {
    Source<Entry, RetainByKeyCOW<Entry>> src;

    src.insert(Entry{1, "alice"}, 0);
    src.insert(Entry{2, "bob"}, 1);

    auto snap = src.snapshot();
    assert(!snap.empty());
    assert(snap.size() == 2);

    // Remove via Source::remove()
    src.remove(Entry{1, "alice"});
    auto snap2 = src.snapshot();
    assert(snap2.size() == 1);

    // Original snapshot unchanged
    assert(snap.size() == 2);

    std::cout << "  PASS: cow_keyed_source\n";
}

// ─── Test 7: NJoin with COW keyed policy ───
void test_cow_keyed_njoin() {
    using L = Layout<Group<0>, Group<1>>;
    using P = Policies<RetainByKeyCOW, AutoPolicy_t>;

    using Device = std::tuple<int, std::string>;
    std::vector<std::pair<Device, int>> emissions;

    auto join = make_reactive<L, P>(
        [&](const Device& dev, int config) {
            emissions.emplace_back(dev, config);
        });

    join.template add<0>(Device{1, "online"});
    assert(emissions.empty());

    join.template add<1>(42);
    assert(emissions.size() == 1);
    assert(std::get<0>(emissions[0].first) == 1);
    assert(emissions[0].second == 42);

    // Add another device
    join.template add<0>(Device{2, "idle"});
    assert(emissions.size() == 2);

    // Duplicate → rejected
    join.template add<0>(Device{1, "online"});
    assert(emissions.size() == 2);

    // Upsert key 1 with new value
    join.template add<0>(Device{1, "busy"});
    assert(emissions.size() == 3);
    assert(std::get<1>(emissions[2].first) == "busy");

    std::cout << "  PASS: cow_keyed_njoin\n";
}

// ─── Test 8: Multiple snapshots are independent ───
void test_cow_keyed_multiple_snapshots() {
    RetainByKeyCOW<Entry> policy;
    policy.insert(Entry{1, "a"}, 0);

    auto snap1 = policy.snapshot();
    policy.insert(Entry{2, "b"}, 1);
    auto snap2 = policy.snapshot();
    policy.insert(Entry{1, "z"}, 2);  // overwrite key 1
    auto snap3 = policy.snapshot();

    assert(snap1.size() == 1);
    assert(snap2.size() == 2);
    assert(snap3.size() == 2);  // still 2 keys, but key 1 has new value

    std::cout << "  PASS: cow_keyed_multiple_snapshots\n";
}

// ─── Test 9: Move insert ───
void test_cow_keyed_move_insert() {
    RetainByKeyCOW<Entry> policy;

    Entry val{1, "moved"};
    auto r = policy.insert(std::move(val), 0);
    assert(r.has_value());
    assert(policy.size() == 1);

    auto snap = policy.snapshot();
    bool found = false;
    for (const auto& entry : snap) {
        if (std::get<0>(entry.value) == 1 && std::get<1>(entry.value) == "moved")
            found = true;
    }
    assert(found);

    std::cout << "  PASS: cow_keyed_move_insert\n";
}

// ─── Test 10: Concurrent snapshot + insert ───
void test_cow_keyed_concurrent() {
    Source<Entry, RetainByKeyCOW<Entry>> src;

    for (int i = 0; i < 100; ++i)
        src.insert(Entry{i, "v"}, static_cast<uint64_t>(i));

    std::atomic<bool> done{false};
    std::atomic<int> snapshot_count{0};

    std::thread reader([&] {
        while (!done.load()) {
            auto snap = src.snapshot();
            std::size_t count = 0;
            for (const auto& entry : snap) {
                (void)entry.value;
                ++count;
            }
            assert(count == snap.size());
            snapshot_count.fetch_add(1);
        }
    });

    std::thread writer([&] {
        for (int i = 0; i < 100; ++i) {
            // Upsert existing keys with new values
            src.insert(Entry{i, "updated"}, static_cast<uint64_t>(100 + i));
        }
        done.store(true);
    });

    writer.join();
    reader.join();

    assert(snapshot_count.load() > 0);
    auto final_snap = src.snapshot();
    assert(final_snap.size() == 100);

    std::cout << "  PASS: cow_keyed_concurrent (snapshots=" << snapshot_count.load() << ")\n";
}

int main() {
    std::cout << "=== RetainByKeyCOW Tests ===\n";
    test_cow_keyed_basic();
    test_cow_keyed_snapshot_immutability();
    test_cow_keyed_scan();
    test_cow_keyed_remove();
    test_cow_keyed_clear();
    test_cow_keyed_source();
    test_cow_keyed_njoin();
    test_cow_keyed_multiple_snapshots();
    test_cow_keyed_move_insert();
    test_cow_keyed_concurrent();
    std::cout << "=== All RetainByKeyCOW tests passed ===\n";
    return 0;
}
