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
- [Callable Traits](#callable-traits)
- [Group Layout](#group-layout)
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

```cpp
#include <seqjoin/seqjoin.hpp>
#include <iostream>
#include <string>

using namespace seqjoin;

int main() {
    // Source<T> defaults to RetainLatest<T> — no need to spell it out
    Source<std::string> names;
    Source<int> ages;

    NJoin join(
        [](const std::string& name, int age) {
            std::cout << name << " is " << age << "\n";
        },
        std::move(names), std::move(ages));

    join.add<0>(std::string("alice"));  // no output (no age yet)
    join.add<1>(30);                    // prints: alice is 30
    join.add<0>(std::string("bob"));    // prints: bob is 30
}
```

**RetainAll** keeps all unique values per source:

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
join.add<1>(30);
// prints: alice is 30
// prints: bob is 30
```

## Architecture

```
┌───────────────────────────────────────────────────┐
│                       NJoin                       │
│                                                   │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐      │
│  │ Source<0> │  │ Source<1> │  │ Source<N> │ ...  │
│  │ policy    │  │ policy    │  │ policy    │      │
│  │ spinlock  │  │ spinlock  │  │ spinlock  │      │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘      │
│        │              │              │            │
│        └─────┬────────┴──────┬───────┘            │
│              │               │                    │
│         SeqCounter     cross_product              │
│        (atomic u64)    (variadic TMP)             │
│              │               │                    │
│              └───────┬───────┘                    │
│                      │                            │
│            emit(v0, v1, ..., vN)                  │
└───────────────────────────────────────────────────┘
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
│   └── callable_traits.hpp     ← callable_traits<F> — extract arity, arg types from callables
├── policy/
│   ├── retain_latest.hpp       ← RetainLatest<T> — keeps last value only
│   └── retain_all.hpp          ← RetainAll<T> — keeps all unique values (dedup)
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

Both satisfy the `RetentionPolicy` concept defined in `core/concepts.hpp`.

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

`make_reactive()` is the primary factory — deduces source types, layout, and storage from the callable's signature. Returns an `NJoin` directly (no wrapper).

Four overloads, disambiguated via the `IsLayout` concept:

```cpp
// (1) Everything deduced — most common
auto j = make_reactive([](const std::string& name, int age) {
    std::cout << name << " is " << age << "\n";
});
j.add<0>(std::string("alice"));
j.add<1>(30);  // prints: alice is 30

// (2) Explicit layout, storage deduced
auto j = make_reactive<Layout<Group<0,1>, Group<2>>>(
    [](const std::string& name, int age, double score) { ... }
);
j.add<0>(std::string("alice"), 30);  // group insert → source 0
j.add<1>(99.5);                       // source 1 → triggers emit

// (3) Layout + explicit storage types
auto j = make_reactive<Layout<Group<0,1>, Group<2>>,
                       std::string, int, double>(fn);

// (4) Explicit storage, default 1:1 layout
auto j = make_reactive<std::string, int>(
    [](std::string_view sv, int n) { ... }  // stores string, not string_view
);
```

## Source Views

`SourceView` projects a subset of indices from a tuple-valued source. Views are non-owning, flatten to the root source (no nesting), and support sub-views and merge.

Compile-time lattice ordering via `is_refinement_of_v<Fine, Coarse>`.

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

## Roadmap

- [x] **Phase 1–3**: Core primitives, policies, Source, NJoin, cross-product
- [x] **Phase 4a**: `Source<T>` default policy, `callable_traits`, `group_layout` (Slot, Group, Layout)
- [x] **Phase 4b**: NJoin refactored to Option C (`NJoin<EmitFn, LayoutT, Sources...>`), `make_reactive()` factory (4 overloads with `IsLayout` concept), CTAD backward compat
- [x] **Phase 4c**: `SourceView` (projected views, sub-views, merge, `is_refinement_of`), `Source::view<Is...>()`
- [ ] **Phase 4d**: Type-dispatch `add(value)`, group-aware `add<Group>()`, return value strategy
- [ ] **Phase 4e**: `rebind` — layout + function replacement with source move
- [ ] **Phase 5**: Extended policies (RetainLatestN, SlidingWindow, Immediate, Barrier)
- [ ] **Phase 6**: Liveness strategies (WeakRef, Predicate)
- [ ] **Phase 6½**: Input adapters (Debounce, Throttle, Batch)
- [ ] **Phase 7**: Comprehensive test suite, benchmarks, examples

## License

[MIT](LICENSE)
