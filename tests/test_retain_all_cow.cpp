// seqjoin — RetainAllCOW policy tests
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── Test 1: Basic insert / dedup ───
void test_cow_basic() {
    RetainAllCOW<std::string> policy;

    auto r1 = policy.insert("alpha", 0);
    assert(r1.has_value() && *r1 == 0);
    assert(policy.size() == 1);

    auto r2 = policy.insert("beta", 1);
    assert(r2.has_value() && *r2 == 1);
    assert(policy.size() == 2);

    // Duplicate → rejected
    auto r3 = policy.insert("alpha", 2);
    assert(!r3.has_value());
    assert(policy.size() == 2);

    std::cout << "  PASS: cow_basic\n";
}

// ─── Test 2: Snapshot immutability ───
// Snapshot must be decoupled from future mutations.
void test_cow_snapshot_immutability() {
    RetainAllCOW<int> policy;
    policy.insert(10, 0);
    policy.insert(20, 1);

    // Take a snapshot
    auto snap = policy.snapshot();
    assert(snap.size() == 2);

    // Mutate the policy — snapshot must not change
    policy.insert(30, 2);
    policy.remove(10);

    // Snapshot still sees the old state
    assert(snap.size() == 2);
    std::vector<int> values;
    for (const auto& entry : snap) {
        values.push_back(entry.value);
    }
    assert(values.size() == 2);
    // Must contain 10 and 20 (not 30, and 10 not removed)
    bool has_10 = false, has_20 = false;
    for (int v : values) {
        if (v == 10) has_10 = true;
        if (v == 20) has_20 = true;
    }
    assert(has_10 && has_20);

    std::cout << "  PASS: cow_snapshot_immutability\n";
}

// ─── Test 3: Scan compatibility ───
void test_cow_scan() {
    RetainAllCOW<std::string> policy;
    policy.insert("x", 0);
    policy.insert("y", 1);

    std::vector<std::pair<std::string, uint64_t>> results;
    for (const auto& [val, seq] : policy.scan()) {
        results.emplace_back(val, seq);
    }
    assert(results.size() == 2);

    std::cout << "  PASS: cow_scan\n";
}

// ─── Test 4: Remove (COW) ───
void test_cow_remove() {
    RetainAllCOW<int> policy;
    policy.insert(1, 0);
    policy.insert(2, 1);
    policy.insert(3, 2);

    assert(policy.remove(2));
    assert(policy.size() == 2);
    assert(!policy.remove(99));  // not found

    // Verify remaining
    std::vector<int> vals;
    for (const auto& [v, s] : policy.scan()) {
        vals.push_back(v);
    }
    assert(vals.size() == 2);

    std::cout << "  PASS: cow_remove\n";
}

// ─── Test 5: Clear ───
void test_cow_clear() {
    RetainAllCOW<int> policy;
    policy.insert(1, 0);
    policy.insert(2, 1);

    // Snapshot before clear
    auto snap = policy.snapshot();

    policy.clear();
    assert(policy.empty());
    assert(policy.size() == 0);

    // Snapshot still has old data
    assert(snap.size() == 2);

    std::cout << "  PASS: cow_clear\n";
}

// ─── Test 6: Source<T, RetainAllCOW<T>> integration ───
void test_cow_source() {
    Source<std::string, RetainAllCOW<std::string>> src;

    src.insert("alice", 0);
    src.insert("bob", 1);

    // snapshot() should work via the COW path
    auto snap = src.snapshot();
    assert(!snap.empty());
    assert(snap.size() == 2);

    // Verify entries
    std::vector<std::string> names;
    for (const auto& entry : snap) {
        names.push_back(entry.value);
    }
    assert(names.size() == 2);

    std::cout << "  PASS: cow_source\n";
}

// ─── Test 7: NJoin with COW policy via make_reactive + Policies ───
void test_cow_njoin() {
    using L = Layout<Group<0>, Group<1>>;
    using P = Policies<RetainAllCOW, AutoPolicy_t>;

    std::vector<std::pair<std::string, int>> emissions;

    auto join = make_reactive<L, P>(
        [&](const std::string& name, int score) {
            emissions.emplace_back(name, score);
        });

    join.template add<0>(std::string("alice"));
    assert(emissions.empty());  // no partner yet

    join.template add<1>(100);
    assert(emissions.size() == 1);
    assert(emissions[0].first == "alice");
    assert(emissions[0].second == 100);

    // Add another COW entry — should cross-product with existing score
    join.template add<0>(std::string("bob"));
    assert(emissions.size() == 2);
    assert(emissions[1].first == "bob");
    assert(emissions[1].second == 100);

    // Duplicate on COW source → rejected, no emission
    join.template add<0>(std::string("alice"));
    assert(emissions.size() == 2);

    std::cout << "  PASS: cow_njoin\n";
}

// ─── Test 8: Multiple snapshots from same source are independent ───
void test_cow_multiple_snapshots() {
    RetainAllCOW<int> policy;
    policy.insert(1, 0);

    auto snap1 = policy.snapshot();

    policy.insert(2, 1);
    auto snap2 = policy.snapshot();

    policy.insert(3, 2);
    auto snap3 = policy.snapshot();

    assert(snap1.size() == 1);
    assert(snap2.size() == 2);
    assert(snap3.size() == 3);

    std::cout << "  PASS: cow_multiple_snapshots\n";
}

// ─── Test 9: Move semantics on insert ───
void test_cow_move_insert() {
    RetainAllCOW<std::string> policy;

    std::string val = "moved";
    auto r = policy.insert(std::move(val), 0);
    assert(r.has_value());
    assert(policy.size() == 1);

    // val should be moved-from
    // (not guaranteed to be empty, but moved-from is valid)

    // Verify the stored value
    auto snap = policy.snapshot();
    bool found = false;
    for (const auto& entry : snap) {
        if (entry.value == "moved") found = true;
    }
    assert(found);

    std::cout << "  PASS: cow_move_insert\n";
}

// ─── Test 10: Concurrent snapshot + insert safety ───
void test_cow_concurrent() {
    Source<int, RetainAllCOW<int>> src;

    // Pre-populate
    for (int i = 0; i < 100; ++i) {
        src.insert(i, static_cast<uint64_t>(i));
    }

    // Thread 1: takes snapshots and iterates them
    // Thread 2: inserts new values concurrently
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
        for (int i = 100; i < 200; ++i) {
            src.insert(i, static_cast<uint64_t>(i));
        }
        done.store(true);
    });

    writer.join();
    reader.join();

    assert(snapshot_count.load() > 0);

    // Final state should have 200 entries
    auto final_snap = src.snapshot();
    assert(final_snap.size() == 200);

    std::cout << "  PASS: cow_concurrent (snapshots=" << snapshot_count.load() << ")\n";
}

int main() {
    std::cout << "=== RetainAllCOW Tests ===\n";
    test_cow_basic();
    test_cow_snapshot_immutability();
    test_cow_scan();
    test_cow_remove();
    test_cow_clear();
    test_cow_source();
    test_cow_njoin();
    test_cow_multiple_snapshots();
    test_cow_move_insert();
    test_cow_concurrent();
    std::cout << "=== All RetainAllCOW tests passed ===\n";
    return 0;
}
