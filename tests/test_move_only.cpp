// seqjoin — Move-only type tests
// Verifies that COW policies support move-only (non-copyable) types
// via shared_ptr<const T> internal wrapping.

#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <seqjoin/seqjoin.hpp>
#include <seqjoin/policy/retain_all_cow.hpp>
#include <seqjoin/policy/retain_by_key_cow.hpp>

using namespace seqjoin;

// ─── Move-only resource type ───
struct Resource {
    int id{0};
    std::string name{};

    Resource() = default;
    Resource(int i, std::string n) : id(i), name(std::move(n)) {}

    // Move-only
    Resource(Resource&&) = default;
    Resource& operator=(Resource&&) = default;
    Resource(const Resource&) = delete;
    Resource& operator=(const Resource&) = delete;

    bool operator==(const Resource& o) const { return id == o.id && name == o.name; }
};

// Hash for Resource (needed by RetainAllCOW)
struct ResourceHash {
    std::size_t operator()(const Resource& r) const {
        return std::hash<int>{}(r.id) ^ (std::hash<std::string>{}(r.name) << 1);
    }
};

// ─── Handle: copyable view type constructible from Resource ───
struct Handle {
    int id;
    explicit Handle(const Resource& r) : id(r.id) {}
};

// ─── Test 1: RetainAllCOW with move-only type ───
void test_retain_all_cow_move_only() {
    RetainAllCOW<Resource, ResourceHash> policy;

    // Insert by move
    policy.insert(Resource{1, "texture"}, 1);
    policy.insert(Resource{2, "buffer"}, 2);
    policy.insert(Resource{3, "shader"}, 3);

    assert(policy.size() == 3);

    // Snapshot yields SnapRef — no copy
    auto snap = policy.snapshot();
    assert(snap.size() == 3);

    std::vector<int> ids;
    for (const auto& entry : snap) {
        ids.push_back(entry.value().id);
    }
    assert(ids.size() == 3);

    // Duplicate reject
    [[maybe_unused]] auto dup = policy.insert(Resource{1, "texture"}, 4);
    assert(!dup.has_value());
    assert(policy.size() == 3);

    // Snapshot immutability
    policy.insert(Resource{4, "pipeline"}, 5);
    assert(snap.size() == 3);  // old snapshot unchanged
    assert(policy.size() == 4);

    std::cout << "  PASS: retain_all_cow_move_only\n";
}

// ─── Test 2: RetainByKeyCOW with move-only tuple type ───
void test_retain_by_key_cow_move_only() {
    // Entry = tuple<string, Resource>, keyed by string (get<0>)
    // tuple containing move-only type is itself move-only
    using Entry = std::tuple<std::string, Resource>;
    RetainByKeyCOW<Entry> policy;

    policy.insert(Entry{"tex1", Resource{1, "texture"}}, 1);
    policy.insert(Entry{"buf1", Resource{2, "buffer"}}, 2);

    assert(policy.size() == 2);

    // Snapshot yields SnapRef — no copy of Entry
    auto snap = policy.snapshot();
    assert(snap.size() == 2);

    std::vector<int> ids;
    for (const auto& entry : snap) {
        const auto& [key, res] = entry.value();
        ids.push_back(res.id);
    }
    assert(ids.size() == 2);

    // Keyed upsert: same key, different value → overwrite
    policy.insert(Entry{"tex1", Resource{10, "texture_v2"}}, 3);
    assert(policy.size() == 2);  // still 2 entries, key "tex1" overwritten

    auto snap2 = policy.snapshot();
    [[maybe_unused]] bool found_v2 = false;
    for (const auto& entry : snap2) {
        const auto& [key, res] = entry.value();
        if (key == "tex1" && res.id == 10) found_v2 = true;
    }
    assert(found_v2);

    // Old snapshot still has old data
    [[maybe_unused]] bool found_v1 = false;
    for (const auto& entry : snap) {
        const auto& [key, res] = entry.value();
        if (key == "tex1" && res.id == 1) found_v1 = true;
    }
    assert(found_v1);

    std::cout << "  PASS: retain_by_key_cow_move_only\n";
}

// ─── Test 3: Source<MoveOnly, RetainAllCOW> ───
void test_source_move_only() {
    Source<Resource, RetainAllCOW<Resource, ResourceHash>> src;

    src.insert(Resource{1, "texture"}, 1);
    src.insert(Resource{2, "buffer"}, 2);

    auto snap = src.snapshot();
    assert(snap.size() == 2);

    std::vector<int> ids;
    for (const auto& entry : snap) {
        ids.push_back(entry.value().id);
    }
    assert(ids.size() == 2);

    std::cout << "  PASS: source_move_only\n";
}

// ─── Test 4: NJoin reactive join with move-only GPU resources ───
//
// Scenario: a GPU resource registry (textures, buffers) joined with
// a render-pass name.  Resources are move-only; the emit callback
// reads them through a lightweight Handle.
void test_njoin_move_only_with_handle() {
    // Source 0: keyed resource registry  (move-only Resource, COW)
    // Source 1: current render-pass name (copyable string, latest)
    Source<Resource, RetainAllCOW<Resource, ResourceHash>> resources;
    Source<std::string> render_pass;

    std::vector<std::pair<Handle, std::string>> results;

    NJoin join(
        [&](const Resource& res, const std::string& pass) {
            results.push_back({Handle{res}, pass});
        },
        std::move(resources), std::move(render_pass));

    join.add<0>(Resource{1, "texture"});
    join.add<1>(std::string{"shadow"});

    assert(results.size() == 1);
    assert(results[0].first.id == 1);
    assert(results[0].second == "shadow");

    // Second resource → cross-product emits with existing render-pass
    join.add<0>(Resource{2, "buffer"});
    assert(results.size() == 2);
    assert(results[1].first.id == 2);

    // Duplicate resource → rejected by COW dedup, no emission
    results.clear();
    join.add<0>(Resource{1, "texture"});
    assert(results.empty());

    // New render-pass → emits for every registered resource
    join.add<1>(std::string{"lighting"});
    assert(results.size() == 2);  // texture × lighting, buffer × lighting

    std::cout << "  PASS: njoin_move_only_with_handle\n";
}

// ─── Test 5: RetainAllCOW remove with move-only ───
void test_retain_all_cow_move_only_remove() {
    RetainAllCOW<Resource, ResourceHash> policy;

    policy.insert(Resource{1, "texture"}, 1);
    policy.insert(Resource{2, "buffer"}, 2);
    assert(policy.size() == 2);

    // remove requires constructing a probe — needs const T& overload
    // This works because remove takes const T& and wraps in shared_ptr
    [[maybe_unused]] bool removed = policy.remove(Resource{1, "texture"});
    assert(removed);
    assert(policy.size() == 1);

    std::cout << "  PASS: retain_all_cow_move_only_remove\n";
}

// ─── Test 6: Grouped layout + RetainByKeyCOW + move-only via make_reactive ───
// The real-world pattern: Group<0,1> packs (key, MoveOnlyResource) into one source,
// Policies<RetainByKeyCOW> gives keyed upsert + O(1) snapshot.
// This exercises the full pipeline: make_reactive → NJoin → add<0>(key, move-only)
// → insert(tuple&&) → COW clone → cross-product → layout_unpack → emit.
void test_grouped_move_only_make_reactive() {
    using L = Layout<Group<0, 1>, Slot<2>>;
    using P = Policies<RetainByKeyCOW, AutoPolicy_t>;

    std::vector<std::pair<int, bool>> results;

    auto join = make_reactive<L, P>(
        [&](const std::string& key, const Resource& res, bool active) {
            results.push_back({res.id, active});
        });

    // Source 1 (Slot<2>): set active flag first
    join.add<1>(true);

    // Source 0 (Group<0,1>): move-only Resource, keyed by string
    join.add<0>(std::string("gpu0"), Resource{1, "texture"});
    assert(results.size() == 1);
    assert(results[0].first == 1 && results[0].second == true);

    join.add<0>(std::string("gpu1"), Resource{2, "buffer"});
    assert(results.size() == 2);
    assert(results[1].first == 2);

    // Same key + same value → rejected (dedup)
    results.clear();
    join.add<0>(std::string("gpu0"), Resource{1, "texture"});
    assert(results.empty());

    // Same key, different value → keyed upsert, emits
    join.add<0>(std::string("gpu0"), Resource{10, "texture_v2"});
    assert(results.size() == 1);
    assert(results[0].first == 10);

    // Flip active flag → emits for all registered resources
    results.clear();
    join.add<1>(false);
    assert(results.size() == 2);  // gpu0 + gpu1

    std::cout << "  PASS: grouped_move_only_make_reactive\n";
}

int main() {
    std::cout << "=== Move-Only Type Tests ===\n";

    test_retain_all_cow_move_only();
    test_retain_by_key_cow_move_only();
    test_source_move_only();
    test_njoin_move_only_with_handle();
    test_retain_all_cow_move_only_remove();
    test_grouped_move_only_make_reactive();

    std::cout << "=== All move-only tests passed ===\n";
    return 0;
}
