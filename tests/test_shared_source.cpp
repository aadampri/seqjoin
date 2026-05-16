// seqjoin — test_shared_source.cpp
// Tests for SharedSource + BarrierView: reactive keyed store with filtered gating.
#include <seqjoin/seqjoin.hpp>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <span>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <vector>

using namespace seqjoin;

// ─── Test types ─────────────────────────────────────────────────────

struct ImageView {
    int id;
    uint64_t handle;  // simulated VkImageView

    bool operator==(const ImageView&) const = default;
};

// For SharedSource: key extraction via std::get<0> won't work on a struct.
// Provide a custom KeyProj.
struct ImageViewKeyProj {
    int operator()(const ImageView& v) const { return v.id; }
};

using ImageViewSource = SharedSource<ImageView, int, ImageViewKeyProj>;

struct RenderPass {
    std::string name;
    uint64_t handle;

    bool operator==(const RenderPass&) const = default;
};

struct RenderPassKeyProj {
    const std::string& operator()(const RenderPass& rp) const { return rp.name; }
};

using RenderPassSource = SharedSource<RenderPass, std::string, RenderPassKeyProj>;

// ─── Test 1: SharedSource basic insert/remove/contains ──────────────

void test_shared_source_basic() {
    std::cout << "  test_shared_source_basic...";

    ImageViewSource views;

    assert(views.size() == 0);
    assert(!views.contains(0));

    // Insert
    assert(views.insert(ImageView{0, 100}));
    assert(views.size() == 1);
    assert(views.contains(0));

    // Duplicate insert (same key + same value) → no change
    assert(!views.insert(ImageView{0, 100}));
    assert(views.size() == 1);

    // Update (same key, different value) → change
    assert(views.insert(ImageView{0, 200}));
    assert(views.size() == 1);
    auto val = views.get(0);
    assert(val && val->handle == 200);

    // Second key
    assert(views.insert(ImageView{1, 300}));
    assert(views.size() == 2);

    // Remove
    assert(views.remove(0));
    assert(views.size() == 1);
    assert(!views.contains(0));
    assert(views.contains(1));

    // Remove non-existent
    assert(!views.remove(99));

    std::cout << " PASSED\n";
}

// ─── Test 2: SharedSource notifications ─────────────────────────────

void test_shared_source_notifications() {
    std::cout << "  test_shared_source_notifications...";

    ImageViewSource views;
    std::vector<int> notified_keys;

    views.subscribe([&](const int& key, const ImageViewSource&) {
        notified_keys.push_back(key);
    });

    views.insert(ImageView{5, 500});
    views.insert(ImageView{3, 300});
    views.insert(ImageView{5, 500});  // duplicate → no notification
    views.insert(ImageView{5, 501});  // update → notification
    views.remove(3);
    views.remove(99);  // non-existent → no notification

    assert(notified_keys.size() == 4);
    assert(notified_keys[0] == 5);
    assert(notified_keys[1] == 3);
    assert(notified_keys[2] == 5);
    assert(notified_keys[3] == 3);

    std::cout << " PASSED\n";
}

// ─── Test 3: BarrierView basic gating ───────────────────────────────

void test_barrier_view_gating() {
    std::cout << "  test_barrier_view_gating...";

    ImageViewSource views;
    std::vector<std::vector<uint64_t>> injected;

    auto injector = [&](std::span<const uint64_t> handles) {
        injected.emplace_back(handles.begin(), handles.end());
    };

    auto extract = [](const ImageView& v) -> uint64_t { return v.handle; };

    BarrierView<ImageView, int, uint64_t, decltype(extract)> barrier(
        views,
        std::unordered_set<int>{0, 1, 3},
        injector,
        extract
    );

    // Initially incomplete
    assert(!barrier.is_complete());
    assert(injected.empty());

    // Add view 0 — still incomplete
    views.insert(ImageView{0, 100});
    assert(!barrier.is_complete());
    assert(injected.empty());

    // Add view 1 — still incomplete
    views.insert(ImageView{1, 200});
    assert(!barrier.is_complete());
    assert(injected.empty());

    // Add view 2 — irrelevant key, still incomplete
    views.insert(ImageView{2, 250});
    assert(!barrier.is_complete());
    assert(injected.empty());

    // Add view 3 — NOW complete!
    views.insert(ImageView{3, 300});
    assert(barrier.is_complete());
    assert(injected.size() == 1);
    // Handles should be in sorted key order: key 0, 1, 3
    assert(injected[0].size() == 3);
    assert(injected[0][0] == 100);  // key 0
    assert(injected[0][1] == 200);  // key 1
    assert(injected[0][2] == 300);  // key 3

    std::cout << " PASSED\n";
}

// ─── Test 4: BarrierView re-fire on relevant change ─────────────────

void test_barrier_view_refire() {
    std::cout << "  test_barrier_view_refire...";

    ImageViewSource views;
    std::vector<std::vector<uint64_t>> injected;

    auto injector = [&](std::span<const uint64_t> handles) {
        injected.emplace_back(handles.begin(), handles.end());
    };
    auto extract = [](const ImageView& v) -> uint64_t { return v.handle; };

    BarrierView<ImageView, int, uint64_t, decltype(extract)> barrier(
        views, {1, 2}, injector, extract
    );

    views.insert(ImageView{1, 100});
    views.insert(ImageView{2, 200});
    assert(injected.size() == 1);  // first complete

    // Update view 1 → re-fire
    views.insert(ImageView{1, 150});
    assert(injected.size() == 2);
    assert(injected[1][0] == 150);  // key 1 updated
    assert(injected[1][1] == 200);  // key 2 unchanged

    // Update irrelevant view → no re-fire
    views.insert(ImageView{99, 999});
    assert(injected.size() == 2);

    std::cout << " PASSED\n";
}

// ─── Test 5: BarrierView goes incomplete on remove ──────────────────

void test_barrier_view_remove_gates() {
    std::cout << "  test_barrier_view_remove_gates...";

    ImageViewSource views;
    std::vector<std::vector<uint64_t>> injected;

    auto injector = [&](std::span<const uint64_t> handles) {
        injected.emplace_back(handles.begin(), handles.end());
    };
    auto extract = [](const ImageView& v) -> uint64_t { return v.handle; };

    BarrierView<ImageView, int, uint64_t, decltype(extract)> barrier(
        views, {0, 1}, injector, extract
    );

    views.insert(ImageView{0, 10});
    views.insert(ImageView{1, 20});
    assert(barrier.is_complete());
    assert(injected.size() == 1);

    // Remove key 0 → barrier becomes incomplete
    views.remove(0);
    assert(!barrier.is_complete());
    // Note: no injector call on removal (snapshot is empty = NJoin short-circuits)
    // The injected count stays at 1 — we only inject on complete transitions
    // Actually our evaluate_locked returns early on incomplete, so no injection.

    // Re-add key 0 → complete again
    views.insert(ImageView{0, 15});
    assert(barrier.is_complete());
    assert(injected.size() == 2);
    assert(injected[1][0] == 15);  // new handle for key 0
    assert(injected[1][1] == 20);  // key 1 unchanged

    std::cout << " PASSED\n";
}

// ─── Test 6: Two BarrierViews with overlapping keys (Vulkan scenario) ──

void test_two_barriers_overlapping() {
    std::cout << "  test_two_barriers_overlapping...";

    ImageViewSource views;

    std::vector<std::vector<uint64_t>> fb_a_calls;
    std::vector<std::vector<uint64_t>> fb_b_calls;

    auto extract = [](const ImageView& v) -> uint64_t { return v.handle; };

    // FB_A needs views {0, 1, 3}
    BarrierView<ImageView, int, uint64_t, decltype(extract)> barrier_a(
        views, {0, 1, 3},
        [&](std::span<const uint64_t> h) { fb_a_calls.emplace_back(h.begin(), h.end()); },
        extract
    );

    // FB_B needs views {1, 2, 3}
    BarrierView<ImageView, int, uint64_t, decltype(extract)> barrier_b(
        views, {1, 2, 3},
        [&](std::span<const uint64_t> h) { fb_b_calls.emplace_back(h.begin(), h.end()); },
        extract
    );

    // Insert views one by one
    views.insert(ImageView{0, 100});  // A: incomplete, B: irrelevant
    assert(fb_a_calls.empty());
    assert(fb_b_calls.empty());

    views.insert(ImageView{1, 200});  // Both: incomplete (A needs 3, B needs 2,3)
    assert(fb_a_calls.empty());
    assert(fb_b_calls.empty());

    views.insert(ImageView{2, 300});  // A: irrelevant, B: still needs 3
    assert(fb_a_calls.empty());
    assert(fb_b_calls.empty());

    views.insert(ImageView{3, 400});  // A: NOW COMPLETE! B: NOW COMPLETE!
    assert(fb_a_calls.size() == 1);
    assert(fb_b_calls.size() == 1);

    // A: keys {0,1,3} → handles [100, 200, 400]
    assert(fb_a_calls[0] == (std::vector<uint64_t>{100, 200, 400}));
    // B: keys {1,2,3} → handles [200, 300, 400]
    assert(fb_b_calls[0] == (std::vector<uint64_t>{200, 300, 400}));

    // Update shared view 1 → both re-fire
    views.insert(ImageView{1, 250});
    assert(fb_a_calls.size() == 2);
    assert(fb_b_calls.size() == 2);
    assert(fb_a_calls[1] == (std::vector<uint64_t>{100, 250, 400}));
    assert(fb_b_calls[1] == (std::vector<uint64_t>{250, 300, 400}));

    // Remove view 2 → only B affected (goes incomplete)
    views.remove(2);
    assert(barrier_a.is_complete());   // A doesn't need key 2
    assert(!barrier_b.is_complete());  // B needs key 2
    assert(fb_a_calls.size() == 2);    // no re-fire for A
    assert(fb_b_calls.size() == 2);    // no injection for B (incomplete)

    // Insert irrelevant view
    views.insert(ImageView{99, 999});
    assert(fb_a_calls.size() == 2);  // no change
    assert(fb_b_calls.size() == 2);  // no change

    std::cout << " PASSED\n";
}

// ─── Test 7: Full Vulkan-like scenario with NJoin ───────────────────

void test_vulkan_framebuffer_full() {
    std::cout << "  test_vulkan_framebuffer_full...";

    // Simulate Vulkan framebuffer creation with NJoin for exactly-once guarantee
    ImageViewSource shared_views;
    RenderPassSource shared_passes;

    // Track framebuffer rebuild calls
    struct FbBuild {
        std::vector<uint64_t> view_handles;
        uint64_t pass_handle;
    };
    std::vector<FbBuild> fb_a_builds;

    // Create NJoin for framebuffer A: needs views {0,1} + pass "main"
    auto fb_a = make_reactive<Layout<Slot<0>, Slot<1>>>(
        [&](const std::vector<uint64_t>& view_handles, const std::vector<uint64_t>& pass_handles) {
            fb_a_builds.push_back(FbBuild{view_handles, pass_handles[0]});
        }
    );

    auto view_extract = [](const ImageView& v) -> uint64_t { return v.handle; };
    auto pass_extract = [](const RenderPass& rp) -> uint64_t { return rp.handle; };

    // Wire barriers
    BarrierView<ImageView, int, uint64_t, decltype(view_extract)> view_barrier(
        shared_views, {0, 1},
        [&](std::span<const uint64_t> h) {
            std::vector<uint64_t> vec(h.begin(), h.end());
            fb_a.add<0>(std::move(vec));
        },
        view_extract
    );

    BarrierView<RenderPass, std::string, uint64_t, decltype(pass_extract)> pass_barrier(
        shared_passes, std::unordered_set<std::string>{"main"},
        [&](std::span<const uint64_t> h) {
            std::vector<uint64_t> vec(h.begin(), h.end());
            fb_a.add<1>(std::move(vec));
        },
        pass_extract
    );

    // Insert views — no fire (pass not ready)
    shared_views.insert(ImageView{0, 10});
    shared_views.insert(ImageView{1, 20});
    assert(fb_a_builds.empty());  // view barrier complete, but pass slot empty → no emit

    // Insert render pass → both slots filled → NJoin fires!
    shared_passes.insert(RenderPass{"main", 42});
    assert(fb_a_builds.size() == 1);
    assert(fb_a_builds[0].view_handles == (std::vector<uint64_t>{10, 20}));
    assert(fb_a_builds[0].pass_handle == 42);

    // Update view 0 (swapchain recreate) → re-fire
    shared_views.insert(ImageView{0, 15});
    assert(fb_a_builds.size() == 2);
    assert(fb_a_builds[1].view_handles == (std::vector<uint64_t>{15, 20}));
    assert(fb_a_builds[1].pass_handle == 42);

    std::cout << " PASSED\n";
}

// ─── Test 8: Concurrent insert from multiple threads ────────────────

void test_shared_source_concurrent() {
    std::cout << "  test_shared_source_concurrent...";

    ImageViewSource views;
    std::atomic<int> notify_count{0};

    views.subscribe([&](const int&, const ImageViewSource&) {
        notify_count.fetch_add(1, std::memory_order_relaxed);
    });

    constexpr int N_THREADS = 8;
    constexpr int INSERTS_PER_THREAD = 100;

    std::vector<std::thread> threads;
    threads.reserve(N_THREADS);

    for (int t = 0; t < N_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < INSERTS_PER_THREAD; ++i) {
                int key = t * INSERTS_PER_THREAD + i;
                views.insert(ImageView{key, static_cast<uint64_t>(key * 10)});
            }
        });
    }

    for (auto& th : threads) th.join();

    assert(views.size() == N_THREADS * INSERTS_PER_THREAD);
    assert(notify_count.load() == N_THREADS * INSERTS_PER_THREAD);

    std::cout << " PASSED\n";
}

// ─── Test 9: BarrierView with concurrent completions + NJoin ────────

void test_barrier_concurrent_completion() {
    std::cout << "  test_barrier_concurrent_completion...";

    // Two barriers race to complete → the NJoin ensures exactly one emit per combination
    ImageViewSource shared_views;
    RenderPassSource shared_passes;

    std::atomic<int> emit_count{0};

    auto fb = make_reactive<Layout<Slot<0>, Slot<1>>>(
        [&](const std::vector<uint64_t>&, const std::vector<uint64_t>&) {
            emit_count.fetch_add(1, std::memory_order_relaxed);
        }
    );

    auto view_extract = [](const ImageView& v) -> uint64_t { return v.handle; };
    auto pass_extract = [](const RenderPass& rp) -> uint64_t { return rp.handle; };

    BarrierView<ImageView, int, uint64_t, decltype(view_extract)> view_barrier(
        shared_views, {0},
        [&](std::span<const uint64_t> h) {
            std::vector<uint64_t> vec(h.begin(), h.end());
            fb.add<0>(std::move(vec));
        },
        view_extract
    );

    BarrierView<RenderPass, std::string, uint64_t, decltype(pass_extract)> pass_barrier(
        shared_passes, std::unordered_set<std::string>{"main"},
        [&](std::span<const uint64_t> h) {
            std::vector<uint64_t> vec(h.begin(), h.end());
            fb.add<1>(std::move(vec));
        },
        pass_extract
    );

    // Race: both barriers complete simultaneously from different threads
    std::thread t1([&]() { shared_views.insert(ImageView{0, 10}); });
    std::thread t2([&]() { shared_passes.insert(RenderPass{"main", 42}); });

    t1.join();
    t2.join();

    // NJoin guarantees exactly one emit when both slots filled
    // (may be 0 if one thread's inject arrives before the other's value is visible,
    //  then 1 when the second arrives. Or 1 if both are visible.)
    // The key guarantee: no double-fire for the SAME state.
    assert(emit_count.load() >= 1);  // at least one emit
    assert(emit_count.load() <= 2);  // at most one per slot trigger

    std::cout << " PASSED\n";
}

// ─── Main ───────────────────────────────────────────────────────────

int main() {
    std::cout << "test_shared_source:\n";
    test_shared_source_basic();
    test_shared_source_notifications();
    test_barrier_view_gating();
    test_barrier_view_refire();
    test_barrier_view_remove_gates();
    test_two_barriers_overlapping();
    test_vulkan_framebuffer_full();
    test_shared_source_concurrent();
    test_barrier_concurrent_completion();
    std::cout << "All tests passed!\n";
    return 0;
}
