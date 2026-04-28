// seqjoin — concurrent stress tests
// SPDX-License-Identifier: MIT

#include <atomic>
#include <cassert>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "seqjoin/seqjoin.hpp"

using namespace seqjoin;

// ─── Test 1: Concurrent add to different sources ───
void test_concurrent_2way() {
    std::atomic<int> emit_count{0};

    using S = Source<int>;  // defaults to RetainLatest<int>

    auto join = NJoin(
        [&](const int&, const int&) {
            emit_count.fetch_add(1, std::memory_order_relaxed);
        },
        S{}, S{}
    );

    constexpr int ITERS = 10'000;

    std::thread t0([&] {
        for (int i = 0; i < ITERS; ++i) {
            join.add<0>(i);
        }
    });

    std::thread t1([&] {
        for (int i = 0; i < ITERS; ++i) {
            join.add<1>(i);
        }
    });

    t0.join();
    t1.join();

    // With RetainLatest on both sources, every add after the first pair
    // should emit. The exact count depends on interleaving, but it must be >= 1.
    assert(emit_count.load() >= 1);
    std::cout << "  PASS: concurrent_2way (emitted " << emit_count.load() << " times)\n";
}

// ─── Test 2: Concurrent add to same source ───
void test_concurrent_same_source() {
    std::atomic<int> emit_count{0};

    using S = Source<int>;

    auto join = NJoin(
        [&](const int&, const int&) {
            emit_count.fetch_add(1, std::memory_order_relaxed);
        },
        S{}, S{}
    );

    // Pre-populate source 1 so every add to source 0 can emit
    join.add<1>(999);

    constexpr int ITERS = 10'000;
    constexpr int THREADS = 4;

    std::vector<std::thread> threads;
    for (int t = 0; t < THREADS; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < ITERS; ++i) {
                join.add<0>(t * ITERS + i);
            }
        });
    }

    for (auto& t : threads) t.join();

    // RetainLatest: each add overwrites. Total adds = THREADS * ITERS.
    // Each add should emit because source 1 is populated.
    assert(emit_count.load() >= 1);
    std::cout << "  PASS: concurrent_same_source (emitted " << emit_count.load()
              << " / " << THREADS * ITERS << " adds)\n";
}

// ─── Test 3: Concurrent SubscriberList ───
void test_concurrent_subscriber_list() {
    SubscriberList<int> subs;
    std::atomic<int> fire_count{0};

    // Subscribe from one thread while firing from another
    constexpr int FIRES = 10'000;

    std::thread subscriber([&] {
        for (int i = 0; i < 10; ++i) {
            subs.subscribe([&](const int&) {
                fire_count.fetch_add(1, std::memory_order_relaxed);
            });
        }
    });

    std::thread firer([&] {
        for (int i = 0; i < FIRES; ++i) {
            subs.fire(i);
        }
    });

    subscriber.join();
    firer.join();

    // Some fires happened before any subscriber was added, some after.
    // We just verify no crash/race.
    std::cout << "  PASS: concurrent_subscriber_list (fired " << fire_count.load()
              << " callbacks)\n";
}

// ─── Test 4: 3-way concurrent join ───
void test_concurrent_3way() {
    std::atomic<int> emit_count{0};

    using S = Source<int>;

    auto join = NJoin(
        [&](const int&, const int&, const int&) {
            emit_count.fetch_add(1, std::memory_order_relaxed);
        },
        S{}, S{}, S{}
    );

    constexpr int ITERS = 5'000;

    std::thread t0([&] { for (int i = 0; i < ITERS; ++i) join.add<0>(i); });
    std::thread t1([&] { for (int i = 0; i < ITERS; ++i) join.add<1>(i); });
    std::thread t2([&] { for (int i = 0; i < ITERS; ++i) join.add<2>(i); });

    t0.join();
    t1.join();
    t2.join();

    assert(emit_count.load() >= 1);
    std::cout << "  PASS: concurrent_3way (emitted " << emit_count.load() << " times)\n";
}

int main() {
    std::cout << "seqjoin concurrent tests:\n";
    test_concurrent_2way();
    test_concurrent_same_source();
    test_concurrent_subscriber_list();
    test_concurrent_3way();
    std::cout << "All concurrent tests passed.\n";
    return 0;
}
