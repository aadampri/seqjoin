# seqjoin

Lock-free N-way reactive join for C++26 — sequence-owned cross-product emission with per-source spinlocks and pluggable retention policies.

## Table of Contents

- [Overview](#overview)
- [The Seq-Ownership Invariant](#the-seq-ownership-invariant)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Header Map](#header-map)
- [Source Default Policy](#source-default-policy)
- [Retention Policies](#retention-policies)
- [Source Ownership & Lifetime](#source-ownership--lifetime)
- [Callable Traits](#callable-traits)
- [Group Layout](#group-layout)
- [make_reactive](#make_reactive)
- [Source Views](#source-views)
- [Move-Only Types with Grouped COW](#move-only-types-with-grouped-cow-gpu-resource-registry)
- [SharedSource + BarrierView: Vulkan Framebuffer Assembly](#sharedsource--barrierview-vulkan-framebuffer-assembly)
- [Putting It All Together](#putting-it-all-together)
- [Design Model — Compile-Time Incremental View Maintenance](#design-model--compile-time-incremental-view-maintenance)
- [Thread Safety](#thread-safety)
- [Building](#building)
- [Running Tests](#running-tests)
- [Roadmap](#roadmap)
- [License](#license)

## Overview

**seqjoin** is a header-only C++26 library for joining N asynchronous data streams into tuples. When any source receives a new value, seqjoin computes the cross-product of all sources and emits every valid combination — exactly once, with no duplicates, even under concurrent access from multiple threads.

The core primitive is the **NJoin**: given N typed sources and an emit callback, each `add<I>(value)` inserts into source I, snapshots all sources, and emits tuples where the triggering element has the highest sequence number.

```
Source 0 (names)    ──┐
                      ├──  NJoin  ──→  emit(name, age)
Source 1 (ages)     ──┘
```

## The Seq-Ownership Invariant

Every value inserted into any source gets a monotonically increasing **sequence number** from a shared atomic counter. When computing the cross-product, a tuple is emitted **only if** the triggering source's element has the maximum seq in that tuple.

This guarantees exactly-once emission without coordination between threads:

```
add<0>("alice")  → seq=1  │  cross with ages: {}         → nothing (no age yet)
add<1>(30)       → seq=2  │  cross with names: {"alice"}  → emit("alice", 30) ✓
add<0>("bob")    → seq=3  │  cross with ages: {30}        → emit("bob", 30)   ✓
add<1>(25)       → seq=4  │  cross with names: {"alice","bob"} →
                           │    ("alice", 25): trigger seq=4 is max → emit ✓
                           │    ("bob",   25): trigger seq=4 > bob's seq=3 → emit ✓
```

No locks are held during emission. Each source has its own spinlock, acquired only for the brief insert/snapshot window.

## Quick Start

### 1. Simplest: one-liner reactive join

```cpp
#include <seqjoin/seqjoin.hpp>
using namespace seqjoin;

int main() {
    // Everything deduced from the lambda signature:
    // → Source<string> + Source<int>, both RetainLatest, DefaultLayout
    auto join = make_reactive([](const std::string& name, int age) {
        std::cout << name << " is " << age << "\n";
    });

    join.add<0>(std::string("alice"));  // no output yet (age source empty)
    join.add<1>(30);                    // → alice is 30
    join.add<0>(std::string("bob"));    // → bob is 30 (overwrites alice)
}
```

### 2. Real-world: IoT device fleet × configuration

A fleet of devices, each with a unique ID and status. When a device comes online (or its status changes), apply the current config. When config changes, push to all live devices.

```cpp
#include <seqjoin/seqjoin.hpp>
using namespace seqjoin;

struct Config { int version; std::string payload; };
using Device = std::tuple<int, std::string>;  // (device_id, status)

int main() {
    // Layout: all lambda params from one source each (default 1:1)
    // Policies: Source 0 = RetainByKey (one entry per device ID, upserts on change)
    //           Source 1 = AutoPolicy (→ RetainLatest, just the current config)
    auto join = make_reactive<
        Layout<Slot<0>, Slot<1>>,
        Policies<RetainByKey, AutoPolicy_t>
    >([](const Device& dev, const Config& cfg) {
        auto [id, status] = dev;
        std::cout << "Device " << id << " (" << status
                  << ") → config v" << cfg.version << "\n";
    });

    join.add<0>(Device{1, "online"});           // no emit (no config yet)
    join.add<0>(Device{2, "idle"});             // no emit
    join.add<1>(Config{1, "initial"});          // → Device 1 (online) → config v1
                                                // → Device 2 (idle)   → config v1

    join.add<0>(Device{1, "online"});           // duplicate key+value → REJECTED, no emit
    join.add<0>(Device{1, "busy"});             // key 1 updated → Device 1 (busy) → config v1

    join.add<1>(Config{2, "updated"});          // config changed → emits for ALL devices:
                                                // → Device 1 (busy)   → config v2
                                                // → Device 2 (idle)   → config v2
}
```

### 3. Grouped sources: atomic multi-field inserts

```cpp
// Group<0,1> → params 0 and 1 share one source (stored as tuple<string,int>)
// Slot<2>   → param 2 has its own source
auto join = make_reactive<Layout<Group<0, 1>, Slot<2>>>(
    [](const std::string& name, int age, double score) {
        std::cout << name << "(" << age << "): " << score << "\n";
    });

join.add<0>(std::string("alice"), 30);  // inserts ("alice", 30) atomically into source 0
join.add<1>(9.5);                       // → alice(30): 9.5
```

### 4. COW policy: resource registry with O(1) snapshots

Use `RetainAllCOW<T>` when your source is a growing set that is snapshotted frequently
(e.g., a resource registry). Insertions clone the internal map (O(n)), but snapshots
are O(1) — just a shared_ptr refcount bump. The snapshot is immutable and decoupled
from future mutations.

```cpp
#include <seqjoin/seqjoin.hpp>
using namespace seqjoin;

int main() {
    // Source 0: endpoint registry (COW — many entries, frequent snapshots)
    // Source 1: config (RetainLatest — just the current config)
    auto join = make_reactive<
        Layout<Slot<0>, Slot<1>>,
        Policies<RetainAllCOW, AutoPolicy_t>
    >([](const std::string& endpoint, int config_version) {
        std::cout << endpoint << " → config v" << config_version << "\n";
    });

    join.add<0>(std::string("svc-a:8080"));  // no emit (no config yet)
    join.add<0>(std::string("svc-b:9090"));  // no emit
    join.add<1>(1);                          // → svc-a:8080 → config v1
                                             //   svc-b:9090 → config v1

    // Duplicate → rejected by RetainAllCOW (dedup), no emission
    join.add<0>(std::string("svc-a:8080"));

    // Config update → cross-product with all registered endpoints
    join.add<1>(2);                          // → svc-a:8080 → config v2
                                             //   svc-b:9090 → config v2

    // Remove an endpoint
    join.source<0>().remove(std::string("svc-a:8080"));
    join.add<1>(3);                          // → svc-b:9090 → config v3 (only)
}
```

**Trade-off vs `RetainAll`:**

| | `RetainAll` | `RetainAllCOW` |
|---|---|---|
| Insert | O(1) amortized | O(n) — clones map |
| Snapshot | O(n) — copies all values | O(1) — shared_ptr grab |
| Best for | Small sets, write-heavy | Large sets, read/snapshot-heavy |

### 5. Grouped source with RetainByKey: fleet monitoring

A fleet of devices, each identified by a unique hostname. The hostname and port
always arrive together (grouped), and you want one entry per hostname (keyed upsert).
Health status is an independent signal.

```cpp
#include <seqjoin/seqjoin.hpp>
using namespace seqjoin;

int main() {
    // Layout: Group<0,1> → (hostname, port) stored atomically in one source
    //         Slot<2>    → health status in a separate source
    // Policies: Source 0 = RetainByKey (one entry per hostname, upserts on port change)
    //           Source 1 = AutoPolicy (→ RetainLatest, just current health)
    //
    // This is equivalent to:
    //   Source<tuple<string, int>, RetainByKey<tuple<string, int>>> endpoints;
    //   Source<bool> health;

    auto join = make_reactive<
        Layout<Group<0, 1>, Slot<2>>,
        Policies<RetainByKey, AutoPolicy_t>
    >([](const std::string& host, int port, bool healthy) {
        if (healthy)
            std::cout << "LIVE: " << host << ":" << port << "\n";
    });

    join.add<0>(std::string("node-1"), 8080);  // no emit (health unknown)
    join.add<0>(std::string("node-2"), 9090);  // no emit
    join.add<1>(true);                         // → LIVE: node-1:8080
                                               //   LIVE: node-2:9090

    join.add<0>(std::string("node-1"), 8080);  // same key+value → REJECTED
    join.add<0>(std::string("node-1"), 8081);  // port changed → LIVE: node-1:8081

    join.add<1>(false);                        // health flipped → emits for ALL endpoints
}
```

### 6. Move-only types with grouped COW: GPU resource registry

Move-only types (e.g., GPU handles, unique file descriptors) work naturally with COW
policies — the policy wraps each value in `shared_ptr<const T>` internally, so the
map is copyable even when `T` is not. Combined with `Group<0,1>`, you can atomically
insert a key + move-only resource in one call.

```cpp
#include <seqjoin/seqjoin.hpp>
#include <seqjoin/policy/retain_by_key_cow.hpp>
using namespace seqjoin;

struct GpuBuffer {
    int handle;
    std::string label;
    GpuBuffer(int h, std::string l) : handle(h), label(std::move(l)) {}
    GpuBuffer(GpuBuffer&&) = default;
    GpuBuffer& operator=(GpuBuffer&&) = default;
    GpuBuffer(const GpuBuffer&) = delete;              // move-only
    GpuBuffer& operator=(const GpuBuffer&) = delete;
    bool operator==(const GpuBuffer& o) const {
        return handle == o.handle && label == o.label;
    }
};

int main() {
    // Group<0,1> → (id, GpuBuffer) stored as tuple in one source
    // Slot<2>    → render-pass name in a separate source
    // RetainByKeyCOW: keyed by first tuple element (id string), O(1) snapshots
    auto join = make_reactive<
        Layout<Group<0, 1>, Slot<2>>,
        Policies<RetainByKeyCOW, AutoPolicy_t>
    >([](const std::string& id, const GpuBuffer& buf, const std::string& pass) {
        std::cout << pass << ": " << id << " [handle=" << buf.handle << "]\n";
    });

    join.add<1>(std::string("shadow"));              // no emit (no buffers yet)

    GpuBuffer tex{1, "diffuse"};
    join.add<0>(std::string("tex-0"), std::move(tex));  // → shadow: tex-0 [handle=1]

    GpuBuffer vbo{2, "mesh"};
    join.add<0>(std::string("vbo-0"), std::move(vbo));  // → shadow: vbo-0 [handle=2]

    join.add<0>(std::string("tex-0"), GpuBuffer{1, "diffuse"});  // same → REJECTED (dedup)

    join.add<1>(std::string("lighting"));            // → lighting: tex-0 ...
                                                     //   lighting: vbo-0 ...
}
```

Key properties:
- **Zero copies of GpuBuffer** — moved into `shared_ptr<const T>` on insert, read through `const&` on emit
- **Keyed upsert** — same buffer ID overwrites the old entry (no duplicates)
- **O(1) snapshots** — COW map clone only on mutation, snapshot is a shared_ptr bump
- **`const T&` insert overload** is SFINAE'd out — only `T&&` is available for move-only types

### 7. Direct construction (pre-C++26 style)

```cpp
Source<std::string, RetainAll<std::string>> names;
Source<int, RetainAll<int>> ages;

NJoin join(
    [](const std::string& name, int age) {
        std::cout << name << " is " << age << "\n";
    },
    std::move(names), std::move(ages));

join.add<0>(std::string("alice"));
join.add<0>(std::string("bob"));
join.add<1>(30);    // → alice is 30
                    // → bob is 30
```

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                         NJoin                          │
│                                                        │
│  ┌─────────┐  ┌─────────┐            ┌─────────┐       │
│  │Source<0>│  │Source<1>│    ...     │Source<N>│       │
│  │ policy  │  │ policy  │            │ policy  │       │
│  │spinlock │  │spinlock │            │spinlock │       │
│  └────┬────┘  └────┬────┘            └────┬────┘       │
│       │            │                      │            │
│       └─────┬──────┴─────────────┬────────┘            │
│             │                    │                     │
│        SeqCounter          cross_product               │
│       (atomic u64)        (variadic TMP)               │
│             │                    │                     │
│             └─────────┬──────────┘                     │
│                       │                                │
│            emit(v0, v1, ..., vN)                       │
└────────────────────────────────────────────────────────┘
```

**Data flow for `add<I>(value)`:**

1. Assign a monotonic seq from the shared `SeqCounter`
2. Insert `(value, seq)` into source I under source I's spinlock
3. Snapshot all N sources (each under its own spinlock, one at a time)
4. Compute the cross-product of all snapshots
5. For each tuple: emit only if source I's element has the max seq (ownership filter)

Steps 4–5 run **outside all locks**.

## Header Map

```
include/seqjoin/
├── seqjoin.hpp                 ← umbrella header (includes everything)
├── njoin.hpp                   ← NJoin<EmitFn, LayoutT, Sources...> (Option C)
├── make_reactive.hpp           ← make_reactive() factory (4 overloads) + IsLayout concept
├── source.hpp                  ← Source<T, Policy, Liveness> + view<Is...>()
├── source_view.hpp             ← SourceView<Parent, ProjIs...> — projected views
├── cross_product.hpp           ← variadic cross-product with seq-ownership filter
├── group_layout.hpp            ← Slot, Group, Layout, DefaultLayout, source_index_of, layout_unpack
├── core/
│   ├── seq_counter.hpp         ← SeqCounter (atomic uint64_t, fetch_add)
│   ├── spinlock.hpp            ← SpinLock (Rigtorp pattern, portable PAUSE/YIELD)
│   ├── subscriber_list.hpp     ← SubscriberList<Args...> (COW + SpinLock)
│   ├── concepts.hpp            ← RetentionPolicy, RemovablePolicy, LivenessStrategy
│   ├── callable_traits.hpp     ← callable_traits<F> — extract arity, arg types from callables
│   └── snap_entry.hpp          ← SnapEntry<T>, Snapshot<T> — snapshot value types
├── policy/
│   ├── retain_latest.hpp       ← RetainLatest<T> — keeps last value only
│   ├── retain_all.hpp          ← RetainAll<T> — keeps all unique values (dedup)
│   ├── retain_all_cow.hpp      ← RetainAllCOW<T> — COW variant (O(1) snapshot, O(n) insert)
│   ├── retain_by_key.hpp       ← RetainByKey<T> — keyed upsert (one entry per key)
│   └── retain_by_key_cow.hpp   ← RetainByKeyCOW<T> — COW keyed upsert (O(1) snapshot)
└── liveness/
    └── always_alive.hpp        ← AlwaysAlive — values never expire
```

## Source Default Policy

`Source<T>` defaults its retention policy to `RetainLatest<T>` via the `DefaultPolicy<T>` trait:

```cpp
Source<std::string>                          // → Source<std::string, RetainLatest<std::string>>
Source<int, RetainAll<int>>                  // explicit policy override
Source<int, RetainAll<int>, AlwaysAlive>     // explicit policy + liveness
```

Customize the default by specializing `DefaultPolicy<T>` for your types.

## Retention Policies

| Policy | Storage | Insert | Duplicates | Use Case |
|--------|---------|--------|------------|----------|
| `RetainLatest<T>` | `optional<pair<T, seq>>` | Overwrites | N/A (always 1) | Latest sensor reading, current state |
| `RetainAll<T>` | `unordered_map<T, seq>` | Appends | Rejected (returns nullopt) | Set of active users, unique events |
| `RetainAllCOW<T>` | `shared_ptr<const map<shared_ptr<const T>, seq>>` | COW clone + insert | Rejected (returns nullopt) | Resource registries, large sets, move-only types |
| `RetainByKey<T>` | `unordered_map<Key, pair<T, seq>>` | Upserts by key | Same key+value → rejected; same key, new value → overwrites | Keyed entity tracking (devices, sessions) |
| `RetainByKeyCOW<T>` | `shared_ptr<const map<Key, pair<shared_ptr<const T>,seq>>>` | COW clone + upsert | Same key+value → rejected; same key, new value → overwrites | Large keyed registries, move-only types |

All satisfy the `RetentionPolicy` concept defined in `core/concepts.hpp`.

### Move-Only Type Support

The COW policies (`RetainAllCOW`, `RetainByKeyCOW`) wrap stored values in `shared_ptr<const T>`, which means:

- **Map clones copy only shared_ptrs** (O(n) pointer copies, zero T copies)
- **Move-only types work out of the box** — `T` is never copied, only moved into a `shared_ptr` on insert
- **Lookups use aliasing shared_ptr** — `remove(const T&)` wraps the reference in a non-owning `shared_ptr` via the aliasing constructor, avoiding any copy of `T`
- **Snapshots are O(1)** — just a `shared_ptr` refcount bump on the immutable map

```cpp
#include <seqjoin/seqjoin.hpp>
#include <seqjoin/policy/retain_all_cow.hpp>

using namespace seqjoin;

// Move-only GPU resource — cannot be copied, only moved
struct GpuBuffer {
    int handle;
    std::string label;

    GpuBuffer(int h, std::string l) : handle(h), label(std::move(l)) {}
    GpuBuffer(GpuBuffer&&) = default;
    GpuBuffer& operator=(GpuBuffer&&) = default;
    GpuBuffer(const GpuBuffer&) = delete;
    GpuBuffer& operator=(const GpuBuffer&) = delete;

    bool operator==(const GpuBuffer& o) const {
        return handle == o.handle && label == o.label;
    }
};

struct GpuBufferHash {
    std::size_t operator()(const GpuBuffer& b) const {
        return std::hash<int>{}(b.handle);
    }
};

int main() {
    // Source 0: GPU buffer registry (move-only, COW for O(1) snapshots)
    // Source 1: current render-pass name (just the latest)
    Source<GpuBuffer, RetainAllCOW<GpuBuffer, GpuBufferHash>> buffers;
    Source<std::string> render_pass;

    NJoin join(
        [](const GpuBuffer& buf, const std::string& pass) {
            std::cout << "Bind " << buf.label << " (handle "
                      << buf.handle << ") in " << pass << "\n";
        },
        std::move(buffers), std::move(render_pass));

    join.add<0>(GpuBuffer{1, "vertex_buf"});    // no emit yet (no pass)
    join.add<0>(GpuBuffer{2, "index_buf"});     // no emit yet
    join.add<1>(std::string{"shadow"});          // → Bind vertex_buf in shadow
                                                 //   Bind index_buf in shadow

    // Duplicate buffer → rejected by COW dedup, no emission
    join.add<0>(GpuBuffer{1, "vertex_buf"});

    // New render pass → emits for every registered buffer
    join.add<1>(std::string{"lighting"});        // → Bind vertex_buf in lighting
                                                 //   Bind index_buf in lighting

    // Remove a buffer, then re-emit
    join.source<0>().remove(GpuBuffer{2, "index_buf"});
    join.add<1>(std::string{"post"});            // → Bind vertex_buf in post
}
```

### RetainByKey — Keyed Upsert

`RetainByKey<T, KeyProj>` stores **one entry per unique key**, where the key is extracted from `T` via `KeyProj` (default: `std::get<0>`). It implements keyed upsert semantics:

| Scenario | Action | Returns |
|---|---|---|
| New key | Insert | `seq` (triggers emission) |
| Same key, different value | Overwrite | `seq` (triggers emission) |
| Same key, same value | No-op | `nullopt` (no emission) |

```cpp
#include "seqjoin/policy/retain_by_key.hpp"

using Device = std::tuple<int, std::string>;  // (device_id, status)

Source<Device, RetainByKey<Device>> devices;
Source<int> config;  // RetainLatest

NJoin join([](const Device& dev, const int& cfg) {
    auto [id, status] = dev;
    std::cout << "Device " << id << " (" << status << ") → config " << cfg << "\n";
}, std::move(devices), std::move(config));

join.add<1>(42);                          // config arrives, no devices yet
join.add<0>(Device{1, "online"});         // emits: Device 1 (online) → config 42
join.add<0>(Device{2, "idle"});           // emits: Device 2 (idle) → config 42
join.add<0>(Device{1, "online"});         // same key+value → REJECTED, no emission
join.add<0>(Device{1, "offline"});        // key 1 updated → emits: Device 1 (offline) → config 42
join.add<1>(99);                          // config changed → emits for BOTH devices
```

**Key properties:**
- **Atomicity**: the overwrite is a single `insert_or_assign` under the Source spinlock — no intermediate "empty" state visible to concurrent readers
- **No duplicate emissions**: same key+value insertions are suppressed before they reach the cross-product
- **Bounded growth**: if keys are bounded (e.g., device IDs in a fleet), the source is naturally bounded
- **Custom key projection**: pass a custom `KeyProj` callable to extract keys from non-tuple types or use composite keys

```cpp
// Custom key projection: hash by name field of a struct
struct Sensor { std::string name; double reading; };
struct ByName { const std::string& operator()(const Sensor& s) const { return s.name; } };

Source<Sensor, RetainByKey<Sensor, ByName>> sensors;
```

## Source Ownership & Lifetime

### Who Owns What

```
NJoin  ─── owns ──→  tuple<Sources...>
                       │
                       ├── Source<string>         ── owns ──→ RetainLatest<string>
                       │                                       └── optional<pair<string, seq>>
                       │
                       └── Source<int, RetainAll<int>> ── owns ──→ RetainAll<int>
                                                                    └── unordered_map<int, seq>

SourceView  ─── borrows (raw ptr) ──→  Source   (must outlive view)
```

- **NJoin** owns all Sources (stored in a `std::tuple`, move-constructed)
- **Source** owns its retention policy (which owns the stored values)
- **SourceView** borrows its parent Source via raw pointer — zero cost, but the user must ensure the NJoin outlives any views
- **SeqCounter** is owned by NJoin — one per join, shared across all sources

Sources are **move-only** (deleted copy). NJoin is move-only. Once a Source is moved into an NJoin (via constructor or `make_reactive`), the NJoin is the sole owner.

### RetainLatest: Bounded, Constant Cost

`RetainLatest<T>` stores at most **one** value. Each insert overwrites the previous:

```
add<0>("alice")  →  source 0: {("alice", seq=1)}       ← 1 entry
add<0>("bob")    →  source 0: {("bob", seq=2)}         ← still 1 entry
add<0>("carol")  →  source 0: {("carol", seq=3)}       ← still 1 entry
```

Cross-product cost: always `1 × 1 × ... × 1 = 1` tuple per emission (for N RetainLatest sources). Constant time, constant memory.

### RetainAll: Unbounded, Multiplicative Cost

`RetainAll<T>` keeps **every unique value** permanently. The `unordered_map` never shrinks unless you explicitly call `remove()` or `clear()`:

```
add<0>("alice")  →  source 0: {"alice":seq=1}                      ← 1 entry
add<0>("bob")    →  source 0: {"alice":seq=1, "bob":seq=2}         ← 2 entries
add<0>("alice")  →  REJECTED (duplicate) — no emission, no growth
add<0>("carol")  →  source 0: {"alice":1, "bob":2, "carol":3}      ← 3 entries
```

Cross-product cost grows multiplicatively: if source 0 has 100 values and source 1 has 50, each insert triggers up to `100 × 50 = 5,000` tuple evaluations (filtered by seq-ownership).

**When to use RetainAll:**
- The value set is naturally bounded (e.g., set of active service endpoints)
- Values are removed when they become irrelevant (`remove()` on Source)
- You need the full cross-product (e.g., "for every user × every config")

**When to avoid RetainAll:**
- Unbounded streams (use RetainLatest or RetainLatestN instead)
- High-cardinality sources (cross-product explodes)

### RetainAll with Views: The Dedup Problem

When a grouped source uses `RetainAll` and you insert through a view, the read-modify-write carries existing values into the new tuple:

```
// Source<tuple<string, int>> with RetainAll
add<0>("alice", 30)  →  {("alice",30): seq=1}
add<0>("bob",   25)  →  {("alice",30): seq=1, ("bob",25): seq=2}

// View<0> insert (fills int from latest):
view<0>.add("carol") →  {("alice",30):1, ("bob",25):2, ("carol",25):3}
//                                                           ^^^^ carried
```

Projecting view\<1\> now sees: `30(seq=1), 25(seq=2), 25(seq=3)` — the value `25` appears twice. The **lowest-seq dedup rule** resolves this: keep only the first occurrence (lowest seq) of each unique projected value. After dedup: `30(seq=1), 25(seq=2)` — matching flat `RetainAll` semantics exactly.

With `RetainLatest` this problem doesn't arise (one slot, no duplicates).

### Sources as Resource Registries

A `RetainAll` source isn't just a data accumulator — it's a **live set**. Combined with `remove()`, it becomes a resource registry where the NJoin reacts to set membership changes:

```cpp
// Source stores shared_ptr → source owns a refcount → resource stays alive
using ConnPtr = std::shared_ptr<Connection>;

Source<ConnPtr, RetainAll<ConnPtr>> connections;
Source<Config> config;  // RetainLatest — just current config

auto join = NJoin(
    [](const ConnPtr& conn, const Config& cfg) {
        conn->apply(cfg);  // every live connection × latest config
    },
    std::move(connections), std::move(config));

// Register a resource:
auto c = std::make_shared<Connection>("host:8080");
join.add<0>(c);

// Config change → emit fires for every live connection:
join.add<1>(Config{...});

// Deregister — cross-product shrinks, resource may be freed:
join.source<0>().remove(c);
```

**Three storage patterns:**

| What you store | Source acts as | Lifetime semantics |
|---|---|---|
| `T` (value) | Value set | Owned by policy; lives until `remove()` |
| `shared_ptr<T>` | Resource registry | Source holds refcount; `remove()` may free |
| `weak_ptr<T>` (planned) | Observer set | External owner decides lifetime; dead entries auto-evict on scan |

**The mental model:** A `RetainAll` source is a dynamic set. `add()` = register, `remove()` = deregister. The NJoin cross-product means "for every valid combination of currently registered resources, invoke the emit function." Adding or removing from any source triggers re-evaluation.

## Callable Traits

`callable_traits<F>` extracts parameter types from any callable — lambdas, function pointers, `std::function`, or objects with `operator()`.

```cpp
auto fn = [](const std::string& s, int n) { ... };
using Traits = callable_traits<decltype(fn)>;

Traits::arity;            // 2
Traits::arg_t<0>;         // const std::string&
Traits::decay_arg_t<0>;   // std::string
Traits::return_type;      // void
```

This is the foundation for `make_reactive()` type deduction.

## make_reactive

`make_reactive()` is the primary factory — deduces source types, layout, and policies from template parameters and the callable's signature. Returns an `NJoin` directly (no wrapper).

Six overloads, disambiguated via the `IsLayout` and `IsPolicies` concepts:

```cpp
// (1) Everything deduced — simplest
auto j = make_reactive([](const std::string& name, int age) { ... });

// (2) Explicit layout, storage deduced
auto j = make_reactive<Layout<Group<0,1>, Slot<2>>>(fn);

// (3) Layout + explicit storage types (default policies)
auto j = make_reactive<Layout<Group<0,1>, Slot<2>>,
                       std::string, int, double>(fn);

// (4) Explicit storage, default 1:1 layout
auto j = make_reactive<std::string, int>(
    [](std::string_view sv, int n) { ... }  // stores string, not string_view
);

// (5) Layout + Policies, storage deduced ← NEW
auto j = make_reactive<Layout<Group<0,1>, Slot<2>>,
                       Policies<RetainByKey, AutoPolicy_t>>(fn);

// (6) Layout + Policies + explicit storage ← NEW
auto j = make_reactive<Layout<Slot<0>, Slot<1>>,
                       Policies<RetainByKey, RetainAll>,
                       std::tuple<int, std::string>, Config>(fn);
```

### Policies\<...\>

`Policies<P0, P1, ...>` specifies a retention policy template for each source (positional). Each `Pi` is a template `template<class T> class` that will be instantiated with the source's value type.

Use `AutoPolicy_t` as a placeholder meaning "use the default policy for this source" (= `RetainLatest`).

```cpp
// Keyed device registry × latest config × accumulated event log
auto join = make_reactive<
    Layout<Slot<0>, Slot<1>, Slot<2>>,
    Policies<RetainByKey, AutoPolicy_t, RetainAll>
>([](const Device& dev, const Config& cfg, const Event& evt) {
    // For every (live device × current config × accumulated event), fire
});
```

**Static safety:** if the number of policies doesn't match the number of sources (layout entries), you get a `static_assert` at compile time.

## Source Views

`SourceView` projects a subset of indices from a tuple-valued source. Views are non-owning, flatten to the root source (no nesting), and support sub-views and merge.

Compile-time lattice ordering via `is_refinement_of_v<Fine, Coarse>`.

## SharedSource + BarrierView: Vulkan Framebuffer Assembly

When multiple consumers share a pool of keyed resources — and each consumer needs a specific subset to be fully present before firing — use `SharedSource` + `BarrierView`.

**Problem**: Two Vulkan framebuffers require different (overlapping) sets of image views and render passes. Resources arrive asynchronously from pipeline compilation threads. Requirements:
- Fire once when all dependencies are present
- Re-fire when any dependency changes (swapchain recreate)
- Don't fire on irrelevant resources
- Exactly-once emit guarantee under concurrent completion

**Architecture**:
```
SharedSource<ImageView>     SharedSource<RenderPass>
    (one canonical store)       (one canonical store)
         │                           │
    ┌────┴────┐                 ┌────┴────┐
    │         │                 │         │
BarrierView BarrierView    BarrierView BarrierView
 {0,1,3}     {1,2,3}       {"main"}   {"shadow"}
    │           │               │         │
    ▼           ▼               ▼         ▼
 NJoin_A     NJoin_B         (wired)   (wired)
 (fb_a)      (fb_b)
```

```cpp
#include <seqjoin/seqjoin.hpp>
#include <span>
#include <unordered_set>

using namespace seqjoin;

// Resource types
struct ImageView {
    int id;
    uint64_t handle;
    bool operator==(const ImageView&) const = default;
};
struct ImageViewKey {
    int operator()(const ImageView& v) const { return v.id; }
};

struct RenderPass {
    std::string name;
    uint64_t handle;
    bool operator==(const RenderPass&) const = default;
};
struct RenderPassKey {
    const std::string& operator()(const RenderPass& rp) const { return rp.name; }
};

int main() {
    // Shared resource pools — single source of truth
    SharedSource<ImageView, int, ImageViewKey> shared_views;
    SharedSource<RenderPass, std::string, RenderPassKey> shared_passes;

    // Per-framebuffer NJoins (exactly-once emit under concurrency)
    auto fb_a = make_reactive<Layout<Slot<0>, Slot<1>>>(
        [](const std::vector<uint64_t>& views, const std::vector<uint64_t>& passes) {
            // views = assembled handles in key-sorted order
            // passes[0] = the render pass handle
            // → vkCreateFramebuffer(...)
        }
    );

    auto fb_b = make_reactive<Layout<Slot<0>, Slot<1>>>(
        [](const std::vector<uint64_t>& views, const std::vector<uint64_t>& passes) {
            // Different framebuffer, different dependency set
        }
    );

    // Handle extractors
    auto view_ext = [](const ImageView& v) -> uint64_t { return v.handle; };
    auto pass_ext = [](const RenderPass& rp) -> uint64_t { return rp.handle; };

    // Wire: FB_A needs views {0,1,3} + pass "main"
    BarrierView<ImageView, int, uint64_t, decltype(view_ext)> fb_a_views(
        shared_views, {0, 1, 3},
        [&](std::span<const uint64_t> h) {
            fb_a.add<0>(std::vector<uint64_t>(h.begin(), h.end()));
        }, view_ext
    );
    BarrierView<RenderPass, std::string, uint64_t, decltype(pass_ext)> fb_a_pass(
        shared_passes, std::unordered_set<std::string>{"main"},
        [&](std::span<const uint64_t> h) {
            fb_a.add<1>(std::vector<uint64_t>(h.begin(), h.end()));
        }, pass_ext
    );

    // Wire: FB_B needs views {1,2,3} + pass "shadow"
    BarrierView<ImageView, int, uint64_t, decltype(view_ext)> fb_b_views(
        shared_views, {1, 2, 3},
        [&](std::span<const uint64_t> h) {
            fb_b.add<0>(std::vector<uint64_t>(h.begin(), h.end()));
        }, view_ext
    );
    BarrierView<RenderPass, std::string, uint64_t, decltype(pass_ext)> fb_b_pass(
        shared_passes, std::unordered_set<std::string>{"shadow"},
        [&](std::span<const uint64_t> h) {
            fb_b.add<1>(std::vector<uint64_t>(h.begin(), h.end()));
        }, pass_ext
    );

    // Resources arrive from async threads:
    shared_views.insert(ImageView{0, 0xA0});   // FB_A incomplete, FB_B irrelevant
    shared_views.insert(ImageView{1, 0xA1});   // both incomplete
    shared_views.insert(ImageView{2, 0xA2});   // FB_B still needs 3
    shared_views.insert(ImageView{3, 0xA3});   // BOTH view barriers complete!
                                                // But passes not ready → NJoins don't fire

    shared_passes.insert(RenderPass{"shadow", 0xB1});
    // → FB_B: both slots filled → NJoin fires → build FB_B

    shared_passes.insert(RenderPass{"main", 0xB0});
    // → FB_A: both slots filled → NJoin fires → build FB_A

    // Swapchain recreate: view 1 changes
    shared_views.insert(ImageView{1, 0xC1});
    // → Both barriers re-fire (key 1 is shared)
    // → Both NJoins re-emit → rebuild both framebuffers

    // View 2 removed:
    shared_views.remove(2);
    // → FB_A unaffected (doesn't need key 2)
    // → FB_B goes incomplete → gated until view 2 returns
}
```

**Key properties**:
- `SharedSource` holds ONE canonical map — no data duplication
- `BarrierView` filters by runtime key-set, fires only on relevant changes
- The per-framebuffer `NJoin` is a trivial 1×1 cross-product that guarantees exactly-once emission via seq-ownership — preventing double-rebuild when two barriers complete simultaneously on different threads

## Putting It All Together

A realistic example showing how `make_reactive`, groups, `source<I>()`, and views
work as one system:

```cpp
#include <seqjoin/seqjoin.hpp>
#include <iostream>
#include <string>
#include <vector>

using namespace seqjoin;

int main() {
    // Service discovery: endpoint+config arrive together (grouped),
    // health status arrives independently from a different thread.
    //
    // Layout<Group<0,1>, Group<2>>:
    //   Source 0 stores tuple<string, int>   (endpoint, port)
    //   Source 1 stores bool                 (healthy)

    std::vector<std::string> deployed;

    auto join = make_reactive<Layout<Group<0, 1>, Group<2>>>(
        [&](const std::string& endpoint, int port, bool healthy) {
            if (healthy) {
                deployed.push_back(endpoint + ":" + std::to_string(port));
            }
        });

    // --- Insert grouped data (endpoint + port atomically) ---
    join.add<0>(std::string("svc-a.prod"), 8080);
    // No emit yet — source 1 (health) is empty.

    // --- Insert health status → triggers emit ---
    join.add<1>(true);
    // Emits: ("svc-a.prod", 8080, true) → deployed = ["svc-a.prod:8080"]

    // --- Inspect sources via views ---
    auto& ep_src = join.source<0>();     // Source<tuple<string, int>>
    auto v_endpoint = ep_src.view<0>();  // projects string (the endpoint)
    auto v_port     = ep_src.view<1>();  // projects int (the port)

    std::string last_ep;
    v_endpoint.scan([&](const std::string& ep, uint64_t) { last_ep = ep; });
    // last_ep == "svc-a.prod"

    int last_port = 0;
    v_port.scan([&](int p, uint64_t) { last_port = p; });
    // last_port == 8080

    // --- Sub-views flatten to root ---
    auto v_both = ep_src.view<0, 1>();       // projects (string, int)
    auto v_ep2 = v_both.view<0>();           // local 0 → root 0 → string
    // v_ep2 is SourceView<Source, 0> — no nesting.

    // --- Merge views ---
    auto v_merged = v_endpoint.merge(v_port);  // union → view<0, 1>
    // v_merged.scan() yields tuple<string, int>

    // --- Update group → re-emit with existing health ---
    join.add<0>(std::string("svc-b.prod"), 9090);
    // deployed = ["svc-a.prod:8080", "svc-b.prod:9090"]

    // View reflects the update:
    v_endpoint.scan([&](const std::string& ep, uint64_t) { last_ep = ep; });
    // last_ep == "svc-b.prod"

    std::cout << "Deployed " << deployed.size() << " services\n";
}
```

**What happens under the hood:**

```
add<0>("svc-a.prod", 8080)
  → packs into tuple("svc-a.prod", 8080)
  → inserts into Source<tuple<string,int>> with seq=1
  → snapshots: [source0: {("svc-a.prod",8080,seq=1)}], [source1: {}]
  → cross-product: no tuples (source 1 empty)

add<1>(true)
  → inserts into Source<bool> with seq=2
  → snapshots: [source0: {("svc-a.prod",8080,seq=1)}], [source1: {(true,seq=2)}]
  → cross-product: (("svc-a.prod",8080), true) → trigger seq=2 is max ✓
  → layout_unpack: tuple<string,int> + bool → (string, int, bool)
  → emit("svc-a.prod", 8080, true)

source<0>().view<0>().scan(visitor)
  → acquires source 0's spinlock
  → scans tuple("svc-a.prod", 8080) → projects index 0 → "svc-a.prod"
  → calls visitor("svc-a.prod", seq=1)
  → releases lock
```

## Group Layout

GroupLayout maps N lambda parameters to M ≤ N sources at compile time. By default, each parameter gets its own source (1:1). With groups, multiple parameters can share a source that stores them as a `std::tuple`.

```cpp
// Default: 3 params → 3 sources
using L = DefaultLayout<3>;  // Layout<Slot<0>, Slot<1>, Slot<2>>

// Grouped: params 0,1 share source 0 (stored as tuple<A,B>), param 2 → source 1
using L = Layout<Group<0,1>, Slot<2>>;

// Compile-time queries:
source_index_of<0, L>;   // 0 (param 0 → source 0)
source_index_of<1, L>;   // 0 (param 1 → source 0, same group)
source_index_of<2, L>;   // 1 (param 2 → source 1)

// Type deduction:
using Params = std::tuple<std::string, int, double>;
source_value_type<Group<0,1>, Params>;  // std::tuple<std::string, int>
source_value_type<Slot<2>, Params>;     // double
```

`detail::layout_unpack<Layout>(source_values)` converts source-level values back to a flat tuple of lambda arguments for emission.

## Design Model — Compile-Time Incremental View Maintenance

seqjoin is formally an **incremental materialized view** system. Each NJoin maintains a virtual cross-product view over its sources, updating it incrementally on each insert (the "delta rule" from IVM theory):

```
insert into Source A  →  ΔView = {new_row} × snapshot(B) × snapshot(C) × ...
```

| Aspect | seqjoin approach |
|---|---|
| Query definition | C++ type signature + Layout (compile-time fixed) |
| Execution plan | IS the type — no optimizer needed |
| Delta computation | Seq-ownership filter (only new tuples) |
| Latency | Sub-microsecond (inlined, no serialization) |
| Persistence | None (in-memory, in-process) |

Compared to streaming SQL systems (Materialize, Flink, ksqlDB) or IVM compilers (DBToaster), seqjoin trades ad-hoc querying and persistence for zero-overhead type safety and deterministic latency. See [`docs/eventing_framework.md`](docs/eventing_framework.md) for a full comparison table with Differential Dataflow, Noria/ReadySet, and others.

## Thread Safety

- **`add<I>()`** is safe to call concurrently from any thread
- Each source has its own `SpinLock` — no cross-source contention
- Locks are held only during insert and snapshot (~nanoseconds)
- The emit callback runs **outside all locks**
- The `SeqCounter` is a single `atomic<uint64_t>` with `relaxed` ordering — the only global contention point
- `SubscriberList::fire()` is wait-free (atomic load of a COW shared_ptr snapshot)

## Building

Requires **GCC 16+** or any compiler with C++26 support (`std::atomic<std::shared_ptr<T>>`).

```bash
cmake -B build -DCMAKE_CXX_STANDARD=26
cmake --build build
```

**As a dependency** (header-only, no build step):

```cmake
add_subdirectory(seqjoin)
target_link_libraries(your_target PRIVATE seqjoin)
```

Or just add `include/` to your include path.

## Running Tests

```bash
cmake --build build
cd build && ctest
```

Four test binaries:
- **test_basic** — 7 single-threaded correctness tests (seq_counter, policies, subscriber_list, NJoin 2-way/3-way with CTAD)
- **test_concurrent** — 4 multi-threaded stress tests (concurrent adds, subscriber firing, 3-way join)
- **test_phase4** — 12 tests for callable_traits, group_layout, and make_reactive (type deduction, layouts, grouped/3-way make_reactive)
- **test_integration** — 7 end-to-end tests (grouped joins + source access, SourceView projection, sub-view flattening, view merge, is_refinement_of, full workflow)
- **test_retain_by_key** — 6 tests for RetainByKey policy (basic upsert, scan, remove, Source integration, NJoin cross-product, make_reactive)
- **test_policies** — 5 tests for `Policies<...>` (layout+policies deduced storage, all-RetainAll, mixed policies, AutoPolicy_t, backward compat)
- **test_retain_all_cow** — 10 tests for RetainAllCOW policy (basic insert/dedup, snapshot immutability, scan, remove, clear, Source integration, NJoin cross-product, multiple snapshots, move insert, concurrent reader/writer)
- **test_retain_by_key_cow** — 10 tests for RetainByKeyCOW policy (keyed upsert, snapshot immutability, scan, remove, clear, Source integration, NJoin cross-product, multiple snapshots, move insert, concurrent)
- **test_move_only** — 5 tests for move-only type support (RetainAllCOW + RetainByKeyCOW with non-copyable types, Source integration, NJoin cross-product with Handle projection, COW remove with move-only values)

## Roadmap

- [x] **Phase 1–3**: Core primitives, policies, Source, NJoin, cross-product
- [x] **Phase 4a**: `Source<T>` default policy, `callable_traits`, `group_layout` (Slot, Group, Layout)
- [x] **Phase 4b**: NJoin refactored to Option C (`NJoin<EmitFn, LayoutT, Sources...>`), `make_reactive()` factory (6 overloads with `IsLayout`/`IsPolicies` concepts), CTAD backward compat
- [x] **Phase 4c**: `SourceView` (projected views, sub-views, merge, `is_refinement_of`), `Source::view<Is...>()`
- [x] **Phase 4f**: `RetainByKey<T, KeyProj>` — keyed upsert policy (one entry per key, duplicate suppression)
- [x] **Phase 4g**: `Policies<...>` — per-source policy selection in `make_reactive` (6 overloads, `AutoPolicy_t` sentinel, `IsPolicies` concept)
- [x] **Phase 4h**: Framework improvements — move-accepting `insert()` overloads, NJoin move-safety fix, callable_traits consolidation, `Source` concept constraint, `[[no_unique_address]]`, `Source::snapshot()` dispatch
- [x] **Phase 2.7**: `RetainAllCOW<T>` — copy-on-write policy (O(1) snapshot via `shared_ptr<const Map>`, O(n) COW insert, `remove()` support, `CowSnapshot` adapter)
- [x] **Phase 2.8**: `RetainByKeyCOW<T>` — COW keyed upsert policy (O(1) snapshot via `CowKeyedSnapshot`, keyed upsert semantics, `remove()` / `remove_by_key()` support)
- [x] **Phase 2.9**: COW `shared_ptr<const T>` wrapping — values stored as `shared_ptr<const T>` in COW policies; map clone copies only pointers (zero T copies); aliasing shared_ptr probes for remove; move-only type support
- [x] **Phase 5a**: `SharedSource<T>` — reactive keyed store (COW, single canonical state, SubscriberList notification); `BarrierView<T>` — filtered + gated subscription (runtime key-sets, span assembly, exactly-once via downstream NJoin)
- [x] **Phase 5b**: Performance optimizations — cross-product pointer-based SelectedTuple (zero copies during recursion), RetainAllCOW::remove early-out, source_view zero-copy projection
- [ ] **Phase 4d**: Type-dispatch `add(value)`, group-aware `add<Group>()`, return value strategy
- [ ] **Phase 4e**: `rebind` — layout + function replacement with source move
- [ ] **Phase 5**: Extended policies (RetainLatestN, SlidingWindow, Immediate, Barrier)
- [ ] **Phase 5½**: `RetainAllIndexed<T, IndexDims<Is...>>` — secondary indices for per-dimension point queries on grouped sources (O(1) `contains()` without ungrouping)
- [ ] **Phase 6**: Liveness strategies (WeakRef, Predicate)
- [ ] **Phase 6½**: Input adapters (Debounce, Throttle, Batch)
- [ ] **Phase 7**: Comprehensive test suite, benchmarks, examples

## License

[MIT](LICENSE)
