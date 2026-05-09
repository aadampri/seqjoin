// seqjoin — basic correctness tests
// SPDX-License-Identifier: MIT

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── Test 1: SeqCounter monotonicity ───
void test_seq_counter() {
    SeqCounter counter;
    uint64_t prev = counter.next();
    for (int i = 0; i < 1000; ++i) {
        uint64_t cur = counter.next();
        assert(seq_after(cur, prev));
        prev = cur;
    }
    std::cout << "  PASS: seq_counter\n";
}

// ─── Test 2: RetainLatest ───
void test_retain_latest() {
    RetainLatest<int> policy;
    assert(!policy.has_value());
    auto r1 = policy.insert(42, 0);
    assert(r1.has_value() && *r1 == 0);
    assert(policy.value() == 42);
    auto r2 = policy.insert(99, 1);
    assert(r2.has_value() && *r2 == 1);
    assert(policy.value() == 99);  // overwrote 42
    std::cout << "  PASS: retain_latest\n";
}

// ─── Test 3: RetainAll with dedup ───
void test_retain_all() {
    RetainAll<std::string> policy;
    auto r1 = policy.insert("hello", 0);
    assert(r1.has_value());
    auto r2 = policy.insert("world", 1);
    assert(r2.has_value());
    auto r3 = policy.insert("hello", 2);  // duplicate
    assert(!r3.has_value());               // rejected
    assert(policy.size() == 2);
    std::cout << "  PASS: retain_all\n";
}

// ─── Test 4: SubscriberList COW ───
void test_subscriber_list() {
    SubscriberList<int, std::string> subs;
    std::vector<std::string> log;
    subs.subscribe([&](const int& n, const std::string& s) {
        log.push_back(std::to_string(n) + ":" + s);
    });
    subs.subscribe([&](const int& n, const std::string& /*s*/) {
        log.push_back("second:" + std::to_string(n));
    });
    subs.fire(42, "hello");
    assert(log.size() == 2);
    assert(log[0] == "42:hello");
    assert(log[1] == "second:42");
    std::cout << "  PASS: subscriber_list\n";
}

// ─── Test 5: 2-way NJoin with RetainLatest ───
void test_njoin_2way_latest() {
    std::vector<std::string> emissions;

    // Source<T> defaults to RetainLatest<T>
    using S0 = Source<std::string>;
    using S1 = Source<int>;

    auto join = NJoin(
        [&](const std::string& s, const int& n) {
            emissions.push_back(s + ":" + std::to_string(n));
        },
        S0{}, S1{}
    );

    // Insert into source 0 — no emission yet (source 1 is empty)
    join.add<0>(std::string("hello"));
    assert(emissions.empty());

    // Insert into source 1 — now both sources have a value → emit
    join.add<1>(42);
    assert(emissions.size() == 1);
    assert(emissions[0] == "hello:42");

    // Insert new value into source 0 — re-emit with latest from source 1
    join.add<0>(std::string("world"));
    assert(emissions.size() == 2);
    assert(emissions[1] == "world:42");

    std::cout << "  PASS: njoin_2way_latest\n";
}

// ─── Test 6: 3-way NJoin with RetainLatest ───
void test_njoin_3way_latest() {
    int emit_count = 0;
    std::string last_emission;

    using S0 = Source<std::string>;
    using S1 = Source<int>;
    using S2 = Source<double>;

    auto join = NJoin(
        [&](const std::string& s, const int& n, const double& d) {
            last_emission = s + ":" + std::to_string(n) + ":" + std::to_string(d);
            ++emit_count;
        },
        S0{}, S1{}, S2{}
    );

    join.add<0>(std::string("a"));
    assert(emit_count == 0);  // need all 3

    join.add<1>(1);
    assert(emit_count == 0);  // still missing source 2

    join.add<2>(3.14);
    assert(emit_count == 1);  // all 3 present → emit

    join.add<1>(2);
    assert(emit_count == 2);  // source 1 updated → re-emit

    std::cout << "  PASS: njoin_3way_latest\n";
}

// ─── Test 7: 2-way NJoin with RetainAll (cross-product) ───
void test_njoin_2way_retainall() {
    std::vector<std::pair<std::string, int>> emissions;

    using S0 = Source<std::string, RetainAll<std::string>>;
    using S1 = Source<int, RetainAll<int>>;

    auto join = NJoin(
        [&](const std::string& s, const int& n) {
            emissions.emplace_back(s, n);
        },
        S0{}, S1{}
    );

    join.add<0>(std::string("a"));   // no emit (source 1 empty)
    join.add<0>(std::string("b"));   // no emit
    assert(emissions.empty());

    // Now add to source 1 — cross-product: {a,b} × {1} = 2 tuples
    // But seq-ownership: only tuples where source 1's element has max seq.
    // "a" has seq=0, "b" has seq=1, and "1" has seq=2 (the newest).
    // So both (a,1) and (b,1) should emit because seq=2 is max in both tuples.
    join.add<1>(1);
    assert(emissions.size() == 2);

    // Add another to source 1 — cross-product with {a,b}: emits (a,2) and (b,2)
    join.add<1>(2);
    assert(emissions.size() == 4);

    // Add duplicate to source 0 — rejected by RetainAll
    join.add<0>(std::string("a"));
    assert(emissions.size() == 4);  // no new emission

    std::cout << "  PASS: njoin_2way_retainall\n";
}

int main() {
    std::cout << "seqjoin basic tests:\n";
    test_seq_counter();
    test_retain_latest();
    test_retain_all();
    test_subscriber_list();
    test_njoin_2way_latest();
    test_njoin_3way_latest();
    test_njoin_2way_retainall();
    std::cout << "All basic tests passed.\n";
    return 0;
}
