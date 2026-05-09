# SeqJoin — Implementation Plan

*Standalone C++26 header-only framework for sequence-owned N-way reactive joins.*

## Table of Contents

1. [Class Taxonomy & Template Parameters](#1-class-taxonomy--template-parameters)
2. [File Layout](#2-file-layout)
3. [Implementation Order](#3-implementation-order)
4. [Design Observations: Group Views as Transposition](#4-design-observations-group-views-as-transposition)
4½. [Source Partition Lattice & View Algebra](#4-source-partition-lattice--view-algebra)
5. [Key Design Decisions to Lock Down](#5-key-design-decisions-to-lock-down)
6. [Type Deduction Strategy](#6-type-deduction-strategy)
7. [Compile-Time Specialization Points](#7-compile-time-specialization-points)
8. [C++26 Features That Improve the Framework](#8-c26-features-that-improve-the-framework)
9. [Open Questions](#9-open-questions)

---

## Project Name: `seqjoin`

Header-only, single-namespace (`seqjoin::`), zero external dependencies.
Requires C++26 (concepts, `constexpr if`, fold expressions, `std::atomic`,
pack indexing, contracts, reflection, `inplace_vector`).

---

## 1. Class Taxonomy & Template Parameters

### Layer 0: Primitives

```cpp
namespace seqjoin {

// Monotonic sequence counter — the single source of truth
class SeqCounter {
    std::atomic<uint64_t> counter_{0};
public:
    uint64_t next() { return counter_.fetch_add(1, std::memory_order_relaxed); }
};
// Note: 0 is a valid seq (first value). No sentinel — insert() returns
// optional<uint64_t> to distinguish success from rejection. This is correct
// even after overflow (counter wraps to 0, which is still a valid seq).

// Per-source lock — never held during emit
class SpinLock { ... };

// COW subscriber list — fire() is wait-free, subscribe() uses single-bit spinlock
template <class... Args>
class SubscriberList {
    using Fn = std::function<void(const Args&...)>;
    std::atomic<std::shared_ptr<const std::vector<Fn>>> subs_;
    alignas(64) std::atomic_flag write_lock_;
public:
    void fire(const Args&... args);       // atomic_load snapshot, iterate — no lock
    void subscribe(auto&& fn);            // single-bit spinlock, COW copy+swap
};

}
```

### Layer 1: Retention Policies

Each policy manages **what** is stored and **how**. All satisfy the `RetentionPolicy` concept.

```cpp
template <class P>
concept RetentionPolicy = requires(P p, typename P::value_type v, uint64_t seq) {
    { p.tryInsert(v, seq) } -> std::convertible_to<bool>;
    { p.scan()             };   // returns iterable of (ref, seq)
};

// Optional capability — NOT a separate axis
template <class P>
concept RemovablePolicy = RetentionPolicy<P> &&
    requires(P p, typename P::value_type v) {
        { p.remove(v) } -> std::same_as<bool>;
    };
```

| Class | Template params | `value_type` | `scan()` returns | `remove()` | Notes |
|---|---|---|---|---|---|
| `RetainAll<T, Hash, Eq>` | `T`, optional hash/eq | `T` | `vector<pair<const T*, seq>>` | No | Deduplicates by value |
| `RetainAllCOW<T, Hash, Eq>` | `T`, optional hash/eq | `T` | `shared_ptr<const Map>` | **Yes** | O(1) scan, O(n) mutate |
| `RetainAllWeak<T>` | `T` (stored as `weak_ptr<T>`) | `weak_ptr<T>` | `vector<pair<shared_ptr<T>, seq>>` | No (auto-evict) | Ownership-based lifetime |
| `RetainLatest<T>` | `T` | `T` | `vector<pair<const T*, seq>>` (0 or 1) | **Yes** (trivial) | `optional<pair<T,seq>>` |
| `RetainLatestN<T, N>` | `T`, capacity `N` | `T` | `vector<pair<const T*, seq>>` | No | Ring buffer, evicts oldest |
| `SlidingWindow<T, Dur>` | `T`, `chrono::duration` | `T` | `vector<pair<const T*, seq>>` | No (TTL evict) | Time-based eviction |
| `Immediate<T>` | `T` | `T` | (special) | No | M=1 degenerate — no storage |
| `BarrierPolicy<T, Yield>` | `T`, yield strategy | `pair<size_t, T>` | `vector<pair<const composite*, seq>>` | **Yes** (clear sub-slot) | Runtime N sub-slots |

### Layer 2: Liveness Strategies

Orthogonal to retention — scan-time value filtering.

```cpp
template <class L>
concept LivenessStrategy = requires(L l) {
    { l.isAlive(std::declval<SomeValue>()) } -> std::convertible_to<bool>;
};
```

| Class | Template params | `isAlive()` logic | Notes |
|---|---|---|---|
| `AlwaysAlive` | — | `return true;` | Default. Eliminated at compile time |
| `WeakRef` | — | `return !v.expired();` | For `ExpirableValue` types |
| `Predicate<F>` | `F` (callable) | `return f(v);` | Custom health check, TTL, etc. |

`RetainAllWeak` integrates liveness directly — no separate strategy needed.

### Layer 3: Yield Strategies (BarrierSource only)

```cpp
struct YieldAll;     // vector<T> — all sub-slot values
struct YieldLast;    // T — value that completed the barrier
struct YieldSignal;  // monostate — just a readiness gate
```

### Layer 3½: Input Adapters — Debounce, Throttle, Batch

Callable wrappers that sit between a raw event and `join.add()`. They control
*when* the wrapped callback fires, not what the join stores or emits.

```
raw event → InputAdapter → join.add(value)
```

The framework does **not** own a timer. Adapters that need one (debounce, batch)
take a caller-provided **Scheduler** — a callable `(duration, callback) → cancel_handle`.
This keeps the library agnostic to the caller's async runtime (asio, Qt, game loop, etc.).

```cpp
template <class F>
concept Scheduler = requires(F f, std::chrono::milliseconds d, std::function<void()> cb) {
    { f(d, std::move(cb)) } -> /* cancel handle, destroyed to cancel */;
};

// Debounce: wait D after last event, reset on each new event.
// Emits latest value when silence ≥ D. Optional max-wait cap M.
template <class T, class Scheduler, class Callback>
class Debounce {
    Scheduler schedule_;
    Callback callback_;
    std::chrono::milliseconds delay_, max_wait_;
    std::mutex mu_;
    std::optional<T> pending_;
    time_point first_arrival_;
    /* cancel handle */ cancel_;
public:
    void operator()(const T& value);  // store latest, restart timer
    void flush();                     // force-emit pending (shutdown)
};

// Throttle (leading-edge): forward at most once per interval.
// Synchronous — no scheduler needed.
template <class T, class Callback>
class Throttle {
    Callback callback_;
    std::chrono::milliseconds interval_;
    time_point last_forward_;
public:
    void operator()(const T& value);  // forward or drop
};

// Batch: collect values for duration D, then forward as vector<T>.
template <class T, class Scheduler, class Callback>
class Batch {
    Scheduler schedule_;
    Callback callback_;
    std::chrono::milliseconds window_;
    std::mutex mu_;
    std::vector<T> buffer_;
    bool timer_running_ = false;
public:
    void operator()(const T& value);  // accumulate, start timer if not running
    void flush();                     // force-emit buffer (shutdown)
};
```

Factory helpers:
```cpp
auto d = seqjoin::debounce(200ms, scheduler, callback);       // basic
auto d = seqjoin::debounce(200ms, 2s, scheduler, callback);   // with cap
auto t = seqjoin::throttle(1s, callback);                     // no scheduler
auto b = seqjoin::batch(500ms, scheduler, callback);
```

Thread safety: adapter's internal `mutex` is independent of the join's spinlock.
The callback (which calls `join.add()`) is always invoked **outside** the adapter's lock.

### Layer 4: Source Wrapper

Combines a retention policy + liveness strategy + spinlock. This is what `NJoin` holds.
For grouped params, `T` is `std::tuple<ParamTypes...>` — the Source doesn't know or care.

```cpp
template <class T,
          class Policy   = RetainAll<T>,
          class Liveness = AlwaysAlive>
class Source {
public:
    using value_type = T;
    using policy_type = Policy;
    using liveness_type = Liveness;

    // Insert under lock; seq allocated atomically.
    // Returns seq on success, nullopt if rejected (duplicate/policy).
    // 0 is a valid seq (not a sentinel — overflow wraps to 0).
    std::optional<uint64_t> insert(const T& value, SeqCounter& counter);

    // Scan: policy_.scan() filtered by liveness
    auto scan();

    // Conditional: only exists if policy supports it
    bool remove(const T& value) requires RemovablePolicy<Policy>;

private:
    SpinLock lock_;
    Policy   policy_;
    Liveness liveness_;
};
```

**Type deduction guide** — users don't spell out `Source<T, Policy, Liveness>`:

```cpp
// CTAD: deduce T from Policy::value_type
template <class Policy, class Liveness = AlwaysAlive>
Source(Policy, Liveness = {}) -> Source<typename Policy::value_type, Policy, Liveness>;

// Convenience: just a type → defaults to RetainAll
template <class T>
using SimpleSource = Source<T, RetainAll<T>, AlwaysAlive>;
```

### Layer 5: GroupLayout — Mapping Lambda Params to Sources

The core problem: the lambda has N params, but there are M ≤ N sources (some params
are grouped into one source). The `GroupLayout` is a compile-time map from lambda
param indices to source indices, resolved entirely at compile time.

#### The Default: No Groups (1:1 mapping)

```cpp
// Lambda: [](const Endpoint& e, const Config& c, const Health& h)
// No groups → 3 sources, 1:1 mapping:
//   param 0 (Endpoint) → source 0
//   param 1 (Config)   → source 1
//   param 2 (Health)   → source 2

// GroupLayout = Layout<Slot<0>, Slot<1>, Slot<2>>
// Each Slot<I> maps one param to one source.
```

#### With Groups: M:1 mapping

```cpp
// Lambda: [](const Endpoint& e, const Config& c, const Health& h)
// Group<0,1> → params 0,1 are ONE source (tuple<Endpoint, Config>):
//   param 0 (Endpoint) ─┐
//   param 1 (Config)   ─┤→ source 0: Source<tuple<Endpoint, Config>>
//   param 2 (Health)    → source 1: Source<Health>

// GroupLayout = Layout<Group<0,1>, Slot<2>>
```

#### Core Types

```cpp
// A single lambda param → its own source
template <size_t ParamIdx>
struct Slot {
    static constexpr size_t param_count = 1;
    static constexpr size_t param_indices[] = { ParamIdx };
};

// Multiple lambda params → one source (stored as tuple)
template <size_t... ParamIdxs>
struct Group {
    static constexpr size_t param_count = sizeof...(ParamIdxs);
    static constexpr size_t param_indices[] = { ParamIdxs... };
};

// The full layout — a sequence of Slots and Groups
// sizeof...(Entries) == number of sources (M)
// sum of all param_counts == lambda arity (N)
template <class... Entries>
struct Layout {
    static constexpr size_t source_count = sizeof...(Entries);
    // param_total computed at compile time from entries
};
```

#### How It Affects Source Types

```cpp
// Slot<I> → Source stores the param type directly
//   Slot<2> with param type Health → Source<Health, Policy>

// Group<I,J> → Source stores tuple of param types
//   Group<0,1> with param types (Endpoint, Config)
//     → Source<std::tuple<Endpoint, Config>, Policy>
```

#### How It Affects Emit

When the cross-product engine calls `emit_()`, it must unpack group tuples:

```cpp
// Without groups:
//   emit_(endpoint, config, health)  — one source value per param

// With Group<0,1>:
//   source 0 scan result: tuple<Endpoint, Config>
//   source 1 scan result: Health
//   Must unpack: emit_(get<0>(src0_val), get<1>(src0_val), src1_val)
//                       ^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^  ^^^^^^^^
//                       param 0           param 1           param 2
```

This is a compile-time `std::apply`-like expansion, resolved by the layout:

```cpp
template <class Layout, class EmitFn, class... ScanVals>
void emitUnpacked(EmitFn& fn, const ScanVals&... scan_vals) {
    // For each Layout entry:
    //   If Slot<I>:   pass scan_val directly as one arg
    //   If Group<I,J,...>: std::apply to expand tuple into individual args
    // All resolved at compile time via if constexpr + fold expressions
    std::apply([&fn](auto&&... args) {
        fn(std::forward<decltype(args)>(args)...);
    }, unpackAll<Layout>(scan_vals...));
}
```

#### How It Affects `add()`

```cpp
// Without groups — type dispatch works naturally:
join.add(endpoint);   // Endpoint → source 0
join.add(config);     // Config → source 1

// With Group<0,1> — group insertion packs into tuple, ONE seq:
join.add<Group<0,1>>(endpoint, config);  // packs into tuple, one insert, one seq
join.add(health);                         // Health → source 1 (after group)

// Convenience: if types are unique, multi-arg add auto-detects group:
join.add(endpoint, config);  // Endpoint+Config match Group<0,1> → atomic insert
```

The group-aware `add()`:

```cpp
// Insert into a specific group — packs args into tuple, single seq
template <class GroupTag, class... Args>
void add(const Args&... args)
    requires IsGroupEntry<GroupTag, Layout>
{
    constexpr size_t src_idx = sourceIndexOf<GroupTag, Layout>();
    auto& src = std::get<src_idx>(sources_);
    auto packed = std::make_tuple(args...);
    auto sx = src.insert(packed, counter_);
    if (!sx) return;
    auto snaps = snapshotAllExcept<src_idx>();
    emitTuples<src_idx>(packed, *sx, snaps);
}
```

#### Layout Deduction in `make_reactive`

```cpp
// No groups — default: each param is its own Slot
auto j = make_reactive(
    [](const Endpoint& e, const Config& c, const Health& h) { ... }
);
// Layout = Layout<Slot<0>, Slot<1>, Slot<2>>
// Sources = Source<Endpoint>, Source<Config>, Source<Health>

// With groups — user provides layout descriptor:
auto j = make_reactive(
    [](const Endpoint& e, const Config& c, const Health& h) { ... },
    group<0,1>(RetainLatest{}),   // params 0,1 grouped, RetainLatest policy
    RetainAll<Health>{}           // param 2, separate source
);
// Layout = Layout<Group<0,1>, Slot<2>>
// Sources = Source<tuple<Endpoint,Config>, RetainLatest<tuple<Endpoint,Config>>>,
//           Source<Health, RetainAll<Health>>

// The group<Is...>(policy) helper:
template <size_t... Is, class Policy>
auto group(Policy p) {
    return GroupDescriptor<Policy, Is...>{std::move(p)};
}
```

#### Degenerate Cases with Groups

| Layout | Sources | Equivalent to |
|---|---|---|
| `Layout<Slot<0>, Slot<1>, Slot<2>>` | 3 sources | Default (no groups) |
| `Layout<Group<0,1,2>>` | 1 source (`tuple<A,B,C>`) | M=1 classic event |
| `Layout<Group<0,1>, Slot<2>>` | 2 sources | 2-way join, first source packs 2 params |
| `Layout<Slot<0>, Group<1,2>>` | 2 sources | 2-way join, second source packs 2 params |

When M=1 (all params in one group), the entire join collapses to a direct function
call — same as the `operator()` degenerate case, resolved at compile time.

### Layer 6: NJoin — The Core

```cpp
template <class EmitFn, class LayoutT, class... Sources>
class NJoin {
public:
    // --- Construction ---
    explicit NJoin(EmitFn fn);                          // sources default-constructed
    NJoin(EmitFn fn, Sources... srcs);                  // explicit sources

    // --- Insert by source index ---
    template <size_t I>
    void add(const auto& value);

    // --- Insert by type (unique types, ungrouped only) ---
    template <class T>
    void add(const T& value);

    // --- Insert into group (atomic, single seq) ---
    template <class GroupTag, class... Args>
    void add(const Args&... args)
        requires IsGroupEntry<GroupTag, LayoutT>;

    // --- Multi-add: auto-detect group from arg types ---
    template <class... Ts>
    void add(const Ts&... values);

    // --- Remove by source index (conditional) ---
    template <size_t I>
    void remove(const auto& value)
        requires RemovablePolicy<typename Sources...[I]::policy_type>;

    // --- Remove by type ---
    template <class T>
    void remove(const T& value);

    // --- operator() for M=1 degenerate case ---
    template <class... Args>
    void operator()(Args&&... args)
        requires (sizeof...(Sources) == 1);

private:
    std::tuple<Sources...> sources_;
    SeqCounter             counter_;
    EmitFn                 emit_;
    // LayoutT is a type, not stored — used at compile time only

    template <size_t Exclude> auto snapshotAllExcept();

    // Group-aware emit: unpacks tuples from grouped sources into individual lambda params
    template <size_t I>
    void emitTuples(const auto& trigger, uint64_t seq, const auto& snaps) {
        // ... cross-product with seq filter ...
        // When calling emit_: use LayoutT to unpack group tuples into flat args
        // emitUnpacked<LayoutT>(emit_, scan_val_0, scan_val_1, ...);
    }
};
```

### Layer 7: `make_reactive` — The Primary Entry Point

Maximum type deduction — the user never writes `NJoin<...>`:

```cpp
// Simplest: all RetainAll, no groups, deduce source types from function signature
auto join = make_reactive([](const Endpoint& e, const Config& c) {
    deploy(e, c);
});
// Layout = Layout<Slot<0>, Slot<1>>
// Deduced: NJoin<lambda, Layout<...>, Source<Endpoint>, Source<Config>>

// With explicit policies per source (no groups):
auto join = make_reactive(
    [](const Endpoint& e, const Config& c) { deploy(e, c); },
    RetainAllCOW<Endpoint>{},   // source 0 policy
    RetainLatest<Config>{}      // source 1 policy
);
// Layout = Layout<Slot<0>, Slot<1>>
// Deduced: NJoin<lambda, Layout<...>,
//                Source<Endpoint, RetainAllCOW<Endpoint>>,
//                Source<Config, RetainLatest<Config>>>

// WITH GROUPS: params 0,1 are one source
auto join = make_reactive(
    [](const Endpoint& e, const Config& c, const Health& h) { ... },
    group<0,1>(RetainLatest{}),  // params 0,1 → source 0 (tuple<Endpoint, Config>)
    RetainAll<Health>{}          // param 2   → source 1
);
// Layout = Layout<Group<0,1>, Slot<2>>
// Deduced: NJoin<lambda, Layout<Group<0,1>, Slot<2>>,
//                Source<tuple<Endpoint, Config>, RetainLatest<tuple<Endpoint, Config>>>,
//                Source<Health, RetainAll<Health>>>
```

**Signature deduction**: extract parameter types from the callable, then apply layout:

```cpp
template <class F, class... Descriptors>
auto make_reactive(F&& fn, Descriptors&&... descriptors) {
    using traits = callable_traits<std::decay_t<F>>;
    // traits::args = type_list<Endpoint, Config, Health>

    // 1. Separate GroupDescriptors from plain policies
    // 2. Build Layout from descriptors:
    //    - group<0,1>(policy) → Group<0,1> entry
    //    - plain policy at position K → Slot<K> entry (auto-assigned to unclaimed params)
    //    - unclaimed params → Slot with default RetainAll
    // 3. For each Layout entry, build the Source type:
    //    - Slot<I> → Source<traits::arg<I>, Policy>
    //    - Group<I,J,...> → Source<tuple<traits::arg<I>, traits::arg<J>, ...>, Policy>
    // 4. Construct NJoin<F, Layout, Sources...>

    return NJoin<F, LayoutType, SourceTypes...>(...);
}
```

---

## 2. File Layout

```
seqjoin/
├── include/
│   └── seqjoin/
│       ├── seqjoin.hpp              # single-include umbrella header
│       ├── core/
│       │   ├── seq_counter.hpp      # SeqCounter (atomic monotonic counter)
│       │   ├── spinlock.hpp         # SpinLock
│       │   ├── subscriber_list.hpp  # SubscriberList<Args...> (COW + single-bit spinlock)
│       │   └── concepts.hpp         # RetentionPolicy, RemovablePolicy, LivenessStrategy, ExpirableValue
│       ├── policy/
│       │   ├── retain_all.hpp       # RetainAll<T>
│       │   ├── retain_all_cow.hpp   # RetainAllCOW<T>
│       │   ├── retain_all_weak.hpp  # RetainAllWeak<T>
│       │   ├── retain_latest.hpp    # RetainLatest<T>
│       │   ├── retain_latest_n.hpp  # RetainLatestN<T, N>
│       │   ├── sliding_window.hpp   # SlidingWindow<T, Dur>
│       │   ├── immediate.hpp        # Immediate<T> (M=1 degenerate)
│       │   └── barrier.hpp          # BarrierPolicy<T, Yield>
│       ├── liveness/
│       │   ├── always_alive.hpp     # AlwaysAlive
│       │   ├── weak_ref.hpp         # WeakRef
│       │   └── predicate.hpp        # Predicate<F>
│       ├── yield/
│       │   ├── yield_all.hpp        # YieldAll
│       │   ├── yield_last.hpp       # YieldLast
│       │   └── yield_signal.hpp     # YieldSignal
│       ├── input/
│       │   ├── debounce.hpp         # Debounce<T, Scheduler, Callback>
│       │   ├── throttle.hpp         # Throttle<T, Callback> (no scheduler)
│       │   └── batch.hpp            # Batch<T, Scheduler, Callback>
│       ├── source.hpp               # Source<T, Policy, Liveness>
│       ├── group_layout.hpp         # Slot, Group, Layout, group<>() helper, emitUnpacked
│       ├── njoin.hpp                # NJoin<EmitFn, Layout, Sources...>
│       ├── make_reactive.hpp        # make_reactive() + callable_traits + layout deduction
│       └── cross_product.hpp        # compile-time cross-product emit engine (group-aware)
├── tests/
│   ├── test_retain_all.cpp
│   ├── test_retain_all_cow.cpp
│   ├── test_retain_all_weak.cpp
│   ├── test_retain_latest.cpp
│   ├── test_barrier.cpp
│   ├── test_source.cpp
│   ├── test_njoin_2way.cpp
│   ├── test_njoin_3way.cpp
│   ├── test_njoin_degenerate.cpp    # M=1 (classic event)
│   ├── test_type_dispatch.cpp       # add(value) by type deduction
│   ├── test_make_reactive.cpp       # signature deduction
│   ├── test_groups.cpp              # GroupLayout, atomic co-insertion, tuple unpacking
│   ├── test_removal.cpp             # COW + weak_ptr removal paths
│   ├── test_liveness.cpp            # AlwaysAlive, WeakRef, Predicate
│   ├── test_debounce.cpp            # Debounce timing, flush, cap
│   ├── test_throttle.cpp            # Throttle drop behavior
│   ├── test_batch.cpp               # Batch accumulation + flush
│   ├── test_concurrent.cpp          # multi-threaded correctness
│   └── test_stress.cpp              # throughput benchmarks
├── examples/
│   ├── service_discovery.cpp        # Endpoint × Config join
│   ├── countdown_barrier.cpp        # N-way barrier
│   ├── dag_orchestration.cpp        # Multi-stage DAG
│   ├── debounce_config_watcher.cpp  # Config file watcher with debounce
│   └── weak_ptr_lifecycle.cpp       # Ownership-based lifetime
├── benchmarks/
│   └── throughput.cpp               # Google Benchmark harness
├── CMakeLists.txt
└── README.md
```

---

## 3. Implementation Order

### Phase 1: Core Primitives
1. `seq_counter.hpp` — trivial, atomic wrapper
2. `spinlock.hpp` — `std::atomic_flag` based
3. `subscriber_list.hpp` — COW + single-bit spinlock, wait-free `fire()`, `subscribe()` serialized
4. `concepts.hpp` — `RetentionPolicy`, `RemovablePolicy`, `LivenessStrategy`, `ExpirableValue`

### Phase 2: Retention Policies (simplest first)
5. `retain_latest.hpp` — `optional<pair<T, seq>>`, simplest policy
6. `retain_all.hpp` — `unordered_map<T, seq>`
7. ✅ `retain_all_cow.hpp` — `shared_ptr<const Map>`, O(1) snapshot, O(n) COW mutate.
   `CowSnapshot<T>` wraps shared_ptr for cross-product compatibility. `Source::snapshot()`
   dispatches to COW path when policy provides `snapshot()` method.
7½. ✅ `retain_by_key_cow.hpp` — COW keyed upsert. `CowKeyedSnapshot<T, Key>` for
   cross-product compatibility. Same upsert semantics as `RetainByKey`, COW mechanics
   from `RetainAllCOW`. `remove()`, `remove_by_key()`, `clear()` all COW-safe.
8. `retain_all_weak.hpp` — `weak_ptr` storage, lock-on-scan

### Phase 3: Source + Join
9. `source.hpp` — `Source<T, Policy, Liveness>`, conditional `remove()`
10. `liveness/always_alive.hpp` — identity (compiled out)
11. `cross_product.hpp` — variadic template cross-product with seq filter
12. `njoin.hpp` — core `NJoin<EmitFn, Layout, Sources...>`, index-based `add<I>()`

### Phase 4: Source API Cleanup + Type Deduction + Groups
13. ✅ **Source default policy** — `Source<T>` defaults to `RetainLatest<T>` via
    `DefaultPolicy<T>` trait. Specializable by users. `Source<std::string>` instead
    of `Source<std::string, RetainLatest<std::string>>`. Tests updated.
14. ✅ `callable_traits.hpp` — extract arity, `arg_t<I>`, `decay_arg_t<I>`, `return_type`
    from lambdas, function pointers, member function pointers (const/non-const/noexcept).
15. ✅ `make_reactive()` — **four overloads** (disambiguated via `IsLayout` concept):
    - **(1)** `make_reactive(fn)` — layout + storage deduced from callable
    - **(2)** `make_reactive<LayoutT>(fn)` — layout explicit, storage deduced
    - **(3)** `make_reactive<LayoutT, Ts...>(fn)` — layout + storage explicit
    - **(4)** `make_reactive<Ts...>(fn)` — storage explicit, default layout
16. ✅ `group_layout.hpp` — `Slot<I>`, `Group<I,J,...>`, `Layout<Entries...>`,
    `DefaultLayout<N>`, `source_index_of<ParamIdx, Layout>`, `source_value_type<Entry, Params>`,
    `detail::layout_unpack<Layout>(values)`. All compile-time, consteval lookups.
17. Type-dispatch `add(value)` — compile-time type→source lookup (group-aware)
18. Group-aware `add<Group<I,J>>(a, b)` — atomic co-insertion with single seq
19. **Return value strategy** — `add<I>()` return type dispatches on `EmitFn::return_type`:
    - `void` → returns `std::size_t` (emission count; 0 = no partner data yet)
    - `R` → returns `std::vector<R>` (collected results, one per cross-product emission)
    - Optional fold overload: `add<I>(value, init, binary_op)` or named `fold<I>(...)` —
      accumulates results without vector allocation (semantically `std::accumulate` over
      the would-be vector). Return order = scan order (lowest-seq first → deterministic).
    - Implementation: `cross_product` already iterates combinations; just needs a
      `if constexpr (!std::is_void_v<R>)` branch to push_back / fold each result.
20. ✅ **NJoin ← Reactive collapse (Option C)** — NJoin owns Layout as template param.
    `DefaultLayout<N>` for 1:1. Packing/unpacking logic moves into NJoin. Reactive wrapper
    removed. `make_reactive()` returns NJoin directly. CTAD preserved for backward compat.
21. ✅ **SourceView** — `source_view.hpp` with `SourceView<ParentSource, ProjIndices...>`.
    `view<Is...>()` method on Source (constrained to tuple types). Views flatten to root
    via `compose_index`. Projected scan, sub-view creation. `is_refinement_of_v<Fine, Coarse>`
    for compile-time lattice ordering. Non-owning (raw pointer to parent).
22. ✅ **View merge** — `v_a.merge(v_b)` → union of index sets. Compile-time only,
    same parent required. Self-similar: split of split traces to root.
23. **`rebind`** — `std::move(join).rebind<NewLayout>(new_fn)`. Moves source ownership,
    replaces projection lens + emit function. Both lattice directions (split/merge) free.
    Physical source type unchanged.

### Phase 4h: Framework Improvements
24½. ✅ **Move-accepting insert** — all policies (`RetainLatest`, `RetainAll`, `RetainByKey`,
    `RetainAllCOW`) and `Source` accept `T&&` overloads. Reduces copies on insert path;
    prerequisite for move-only types via COW.
25½. ✅ **NJoin move safety** — `EmitWrapper` no longer stored as member. Constructed on-the-fly
    in `add()` to avoid dangling pointer after `NJoin(NJoin&&) = default`.
26½. ✅ **callable_traits consolidation** — 7 specializations collapsed into
    `callable_traits_base<R, Args...>` + single-line inheriting specializations.
27½. ✅ **Source concept constraint** — `requires RetentionPolicy<Policy>` on `Source` class
    template. `#include "core/concepts.hpp"` added.
28½. ✅ **`[[no_unique_address]]`** on `Source::liveness_` — eliminates 1-byte waste for
    empty liveness strategies (`AlwaysAlive`).
29½. ✅ **`resolve_source` specialization** — Replaced `std::conditional_t` (which
    instantiated `Source<T, AutoPolicy_t<T>>` — failing the concept constraint) with
    template specialization for `AutoPolicy_t`.
30½. ✅ **SnapEntry factored** — `core/snap_entry.hpp` extracted from `cross_product.hpp`
    for reuse by COW snapshot types.
31½. ✅ **`Source::snapshot()`** — policy-aware snapshot method. COW policies return O(1)
    snapshot handle; others collect into `Snapshot<T>` vector. `NJoin::snapshot_one()` now
    delegates to `source.snapshot()` for uniform snapshot dispatch.

### Phase 5: Extended Policies
24. `retain_latest_n.hpp` — ring buffer variant
25. `sliding_window.hpp` — time-based eviction
26. `immediate.hpp` — M=1 optimized (no storage)
27. `barrier.hpp` + yield strategies — runtime N sub-slots
28. **Dedup mode for policies** — optional value-equality suppression on insert.
    Currently RetainLatest re-emits on every insert (even same value), while
    RetainAll silently deduplicates. Behaviour is policy-dependent and correct,
    but users may want configurable dedup (e.g. `RetainLatest<T, Dedup=true>`
    or a `DedupFilter` adapter). Add as a policy-level concern, not in NJoin.

### Phase 6: Extended Liveness
29. `weak_ref.hpp`, `predicate.hpp`

### Phase 6½: Input Adapters
30. `debounce.hpp` — Debounce + capped variant, Scheduler concept
31. `throttle.hpp` — Throttle (leading-edge, synchronous)
32. `batch.hpp` — Batch accumulation with timer flush

### Phase 7: Tests & Examples (continuous, but bulk after Phase 4)
33. Unit tests per policy
34. Integration tests (NJoin correctness, with and without groups)
35. Concurrent stress tests
36. Examples (including debounce + fan-out wiring)

---

## 4. Design Observations: Group Views as Transposition

### The View Model

A grouped `Source<A,B,C>` can expose projected views:

```
Source<A,B,C>            ← maximal group (one lock, atomic store)
    ├── view<0>  → A writer (read-modify-write under parent's lock)
    ├── view<1>  → B writer
    └── view<2>  → C writer
```

Views project **downward only** (split a group into sub-views). You cannot merge
separate sources upward into a group without restructuring storage.

The user decides the atomicity boundary upfront:
- Grouped interface: all dimensions change together (same seq)
- Flat view: dimensions can change independently (different seqs, RMW)

### The Transpose Analogy

Row-major storage (grouped source) vs column-major (views) is a transposition:

```
Row (grouped):  (a1,b1,c1,seq=1), (a2,b2,c2,seq=2), (a3,b2,c2,seq=3)
Column (views): view<0>={a1,a2,a3}, view<1>={b1,b2,b2}, view<2>={c1,c2,c2}
```

Row-dedup (full tuple uniqueness) ≠ column-dedup (per-dimension uniqueness).
Projections on denormalized data produce duplicates unless filtered.

### The Uniqueness Problem with RetainAll

Given `Source<A,B>` RetainAll with views:

```
grouped add(a1,b1) → (a1,b1,seq=1)
grouped add(a2,b2) → (a2,b2,seq=2)
view<0>.add(a3)    → (a3,b2,seq=3)   ← RMW carries b2
view<0>.add(a4)    → (a4,b2,seq=4)   ← RMW carries b2
```

Projected virtual `Source<B>`: b1(1), b2(2), b2(3), b2(4) ← b2 appears 3×.
Cross-product on trigger emits `(a4,b2)` three times. Uniqueness violated.

A truly flat `Source<B>` with RetainAll would have **rejected** duplicate b2 inserts.

**Resolution**: scan-time dedup on views — keep **lowest seq** per unique projected value.
This exactly mimics flat RetainAll semantics: value's seq is assigned at first appearance,
later duplicates are rejected. Analogous to `SELECT DISTINCT` on a projected column.

**Correctness**: keep-lowest-seq rule preserves the seq-ownership invariant. The trigger
entry's seq=T is compared against other entries' first-occurrence seqs; since the first
occurrence always has a lower seq than any later duplicate, ownership resolution is
identical to the flat case.

**Why NOT highest-seq**: if `a3` appears at seq=3,4,5 and we keep seq=5, then a trigger
at seq=5 on view<2> would see `a3(5)` tied for max → ownership check may fail or assign
to wrong source. Lowest-seq avoids this entirely.

### Verified 3D Example with Dedup

Storage: `Source<A,B,C>` RetainAll with view inserts:
```
(a1, b1, c1, seq=1)   ← grouped
(a2, b2, c2, seq=2)   ← grouped
(a3, b2, c2, seq=3)   ← view<0>.add(a3)
(a3, b3, c2, seq=4)   ← view<1>.add(b3)
(a3, b3, c3, seq=5)   ← view<2>.add(c3)
```

Scan-time dedup (lowest seq per unique value):
```
view<0>: a1(1), a2(2), a3(3)     ← a3 first at seq=3
view<1>: b1(1), b2(2), b3(4)     ← b2 first at seq=2, b3 first at seq=4
view<2>: c1(1), c2(2), c3(5)     ← c2 first at seq=2, c3 at seq=5
```

Emissions (trigger = c3, seq=5, trigger_source=view<2>):
- 3×3×1 = 9 tuples, all have c3(5) as max → all emitted ✓

Flat equivalent: `Source<A>:{a1(1),a2(2),a3(3)}`, `Source<B>:{b1(1),b2(2),b3(4)}`,
`Source<C>:{c1(1),c2(2),c3(5)}` → same 9 emissions ✓

### Cost Matrix: View vs Native Grouped vs Flat

| | Native flat | Native grouped | Grouped + views |
|---|---|---|---|
| **Sources (for N params)** | N sources, N locks | M≤N sources, M locks | M sources, M locks |
| **Insert (single param)** | 1 lock | N/A (must insert group) | 1 lock (RMW under parent lock) |
| **Insert (group of K)** | K locks, K seqs | 1 lock, 1 seq | 1 lock, 1 seq |
| **Snapshot** | N lock/unlock cycles | M lock/unlock cycles | M lock/unlock cycles |
| **Scan (RetainLatest)** | O(1) per source | O(1) per source | O(1) — no dedup needed |
| **Scan (RetainAll)** | O(entries) | O(entries) | O(entries) + dedup hash O(entries) |
| **Storage (RetainAll)** | no redundancy | no redundancy | redundant (carried values) |
| **Atomicity** | none between sources | full within group | full within group |
| **Flexibility** | max (each param independent) | fixed groups | can split groups via views |
| **Cross-product size** | \|A\|×\|B\|×... (unique) | \|groups\|×... (unique) | same as flat (after dedup) |
| **Layout conversion** | impossible (upward) | impossible (merge) | free (just view change) |

### Implications

- Views are **zero-cost with RetainLatest** (one slot, overwrite, no dedup needed)
- Views with RetainAll require scan-time dedup (O(n) filter) for correctness
- The grouped representation trades storage redundancy for atomic consistency
- Convertibility between layouts is purely a view/interface concern, not storage restructuring
- User decides atomicity boundary at construction; views relax it without restructuring

> **Note:** The "transpose" metaphor used above is the informal name for moving to the
> **floor** of the partition lattice (full decomposition into singletons). Section 4½
> formalizes this: the grouped source defines a bounded partition lattice Π_K, "transposing"
> is just one extreme (discrete partition), and intermediate lattice points represent
> partial decompositions. The dedup rule, cost trade-offs, and correctness arguments
> generalize to every lattice level — not just the ceiling↔floor binary.

---

## 4½. Source Partition Lattice & View Algebra

### The Lattice Structure

Each source in an NJoin defines a **bounded partition lattice** over its tuple indices.
The NJoin's initial grouping is the lattice ceiling; full singletons are the floor:

```
Source<tuple<A,B,C>> in NJoin with Group<0,1,2>:

           {0,1,2}              ← ceiling (physical storage = tuple<A,B,C>)
          /   |   \
  {0,1}{2}  {0,2}{1}  {1,2}{0}   ← intermediate partitions (views)
          \   |   /
           {0}{1}{2}              ← floor (singleton views)
```

**Split** (down): always valid — just a projection of the stored tuple.
**Merge** (up): valid only between views **from the same parent source**.
Cross-source merge is impossible (different seq domains, different locks).

### API: `source<I>()` and `view<Is...>()`

```cpp
auto join = make_reactive<Layout<Group<0,1,2>>>(fn);

join.source<0>()            // → ref to Source<tuple<A,B,C>>
    .view<0,1>()            // → SourceView projecting (A,B)
    .view<0>()              // → SourceView projecting A only (self-similar)

// Single-index projection unwraps: view<I>() → value_type = A (not tuple<A>)
// Multi-index projection: view<I,J>() → value_type = tuple<A_I, A_J>
```

### Views Are Writable (Read-Modify-Write)

A view insert fills missing dimensions from the parent's latest value:

```cpp
// view<0,1> of Source<tuple<A,B,C>>:
view.add(a, b):
    lock parent
    latest_c = parent.current().get<2>()
    parent.insert(tuple(a, b, latest_c), seq_.next())
    unlock parent
    // dedup handles duplicated C naturally (lowest-seq rule)
```

This is safe because:
- RMW is atomic (under parent's spinlock)
- Dedup ensures projected views stay consistent
- Notifications propagate from parent to all consumers

### View Flattening

All views trace to the **root source** — no nesting:

```cpp
auto v_ab  = source.view<0,1>();    // SourceView<Source<tuple<A,B,C>>, 0, 1>
auto v_a   = v_ab.view<0>();        // SourceView<Source<tuple<A,B,C>>, 0>  ← NOT nested!

// Composition: local index i → parent_indices[i]
// v_ab has indices (0,1), v_a requests local index 0 → parent index 0
```

This ensures sibling merge always works:
```cpp
auto v_a = source.view<0>();     // SourceView<Parent, 0>
auto v_b = source.view<1>();     // SourceView<Parent, 1>
auto v_ab = v_a.merge(v_b);     // SourceView<Parent, 0, 1>  ← union of index sets
// Compile-time only — no runtime state change
```

### Ownership Model

Three modes, same `SourceLike` concept:

| Mode | Syntax | Internal pointer | Use case |
|---|---|---|---|
| **Borrow** (default) | `join.source<0>().view<0,1>()` | raw pointer | Intra-join, same scope |
| **Shared** (opt-in) | `make_shared_source<T>()` | `shared_ptr` | Cross-join views |
| **Owning** (move) | `std::move(join).source<0>().project<0,1>()` | `unique_ptr` | Transfer ownership |

**Default (borrow):** zero-cost. User guarantees NJoin outlives views. Covers the common
case where views are local or held by downstream joins with shorter lifetime.

**Shared:** For cross-join wiring where destruction order is non-deterministic. Source
lives as long as any holder (NJoin or view) exists. `shared_ptr` overhead is per-source
construction only — the hot insert path uses raw pointer through `.get()`.

**Owning:** Source is moved out of the NJoin (consuming it). The owning projection
becomes the sole owner. Useful for dependency injection / adapter patterns.

### `rebind`: Layout + Function Replacement

`rebind` moves source ownership to a new NJoin with a different layout and emit function:

```cpp
// Original: single source tuple<A,B,C>, emit f(a,b,c)
auto join1 = make_reactive<Layout<Group<0,1,2>>>(f);

// Rebind: same source, finer layout, different emitter
auto join2 = std::move(join1).rebind<Layout<Group<0,1>, Group<2>>>(g);
// join1 consumed. join2 owns Source<tuple<A,B,C>>.
// join2 views it through lens: Group<0,1> + Group<2>
// join2 emits: g(tuple<A,B>, C) — or g(A, B, C) with unpack

// Can move in either direction — layout is a lens, not storage:
auto join3 = std::move(join2).rebind<Layout<Group<0,1,2>>>(h);
// ✓ — physical source unchanged, just widens the projection back
```

**Key insight:** The source's physical type is fixed at construction (`tuple<A,B,C>`).
Layout changes are purely how the NJoin **observes** the source — not data restructuring.
Both lattice directions (split and merge) are free (zero data copy).

**Constraint for multi-source NJoins:** rebind can only restructure within each source's
sub-lattice independently. It cannot merge across physical source boundaries (those are
separate storage with separate seqs).

### `SourceLike` Concept

All sources and views satisfy one concept:

```cpp
template <typename S>
concept SourceLike = requires(const S& s, typename S::value_type v, uint64_t seq) {
    typename S::value_type;
    { s.scan() };            // returns range of (value, seq) pairs
};

template <typename S>
concept WritableSource = SourceLike<S> && requires(S& s, typename S::value_type v, uint64_t seq) {
    { s.insert(std::move(v), seq) };
};
```

NJoin's sources satisfy `WritableSource`. Views from writable parents also satisfy
`WritableSource` (via RMW). Views from const/external parents satisfy only `SourceLike`.

---

## 5. Key Design Decisions to Lock Down

| Decision | Chosen approach | Rationale |
|---|---|---|
| Header-only vs compiled | **Header-only** | Zero friction adoption, templates require it |
| Namespace | `seqjoin::` | Short, distinctive, `grep`-friendly |
| C++ standard | **C++26** | C++20 base + pack indexing, contracts, hazard_pointer, reflection |
| Exception policy | **No exceptions** | Lock-free-adjacent code; contracts + `static_assert` only |
| Allocator support | **Deferred** (Phase 2+) | Premature; `std::allocator` default first |
| `scan()` return type | **Policy-dependent** | Each policy returns what's cheapest (pointer, shared_ptr, vector) |
| Cross-product engine | **Recursive variadic template** | Compile-time unrolled for N≤4, generic for N>4 |
| Seq comparison | **Signed wraparound** | `(int64_t)(sx - sy) > 0` — handles overflow |
| Thread sanitizer | **TSAN-clean from day 1** | Annotate spinlock if needed |
| Group storage | **Tuple in single source** | Atomic consistency at both insert and snapshot time |
| `add()` return type | **Dispatch on `return_type`** | void→`size_t` (count), R→`vector<R>` (collected). Optional `fold` avoids alloc. |
| Group views | **Writable via RMW** | Read-modify-write under parent lock; lowest-seq dedup for consistency |
| NJoin + Layout | **Option C: NJoin owns Layout** | Single user-facing type; `DefaultLayout<N>` is identity (zero-cost). No separate Reactive wrapper. |
| Storage deduction | **decay_arg_t default, explicit override** | Covers owning types (common) and view/ref/polymorphic (explicit) |
| View flattening | **All views trace to root source** | No nesting; enables sibling merge; self-similar algebra |
| View ownership | **Borrow default, shared opt-in** | Zero-cost common case; `shared_ptr` for cross-join safety |
| Layout conversion | **`rebind` moves sources, replaces lens + fn** | Both lattice directions free (no data copy); physical source type fixed |
| View lattice | **Bounded partition per source** | Ceiling = initial grouping; merge only within same parent |

---

## 6. Type Deduction Strategy

The goal: **the user writes the lambda and (optionally) policies. Everything else is deduced.**

### Level 1: Deduce source types from lambda

```cpp
auto j = make_reactive([](const Endpoint& e, const Config& c) { ... });
//                      ↑ callable_traits extracts: <Endpoint, Config>
//                      ↑ default policies: RetainAll<Endpoint>, RetainAll<Config>
//                      → NJoin<λ, Source<Endpoint, RetainAll<Endpoint>>,
//                                 Source<Config, RetainAll<Config>>>
```

### Level 2: Override policies positionally

```cpp
auto j = make_reactive(
    [](const Endpoint& e, const Config& c) { ... },
    RetainAllCOW<Endpoint>{},      // overrides source 0
    RetainLatest<Config>{}         // overrides source 1
);
```

### Level 3: Override policies by type match

```cpp
auto j = make_reactive(
    [](const Endpoint& e, const Config& c, const Health& h) { ... },
    cow<Endpoint>(),               // matches source whose T = Endpoint
    latest<Config>()               // matches source whose T = Config
                                   // Health gets default RetainAll
);
```

### Level 4: Deduce `add()` dispatch from value type

```cpp
j.add(endpoint);     // Endpoint → source 0 (compile-time lookup)
j.add(config);       // Config → source 1
j.add(health);       // Health → source 2
```

### Level 5: `operator|` pipe syntax (future, nice-to-have)

```cpp
endpoints | configs | on_join([](auto& e, auto& c) { ... });
```

### `make_reactive` Overload Taxonomy (Option C — NJoin owns Layout)

NJoin always carries a Layout (defaulting to 1:1). `make_reactive` disambiguates
the first template argument via an `IsLayout` concept:

```cpp
template <typename T>
concept IsLayout = /* tag or structural check on Layout<Entries...> */;
```

**Four overloads:**

```cpp
// (1) Everything deduced — common case
make_reactive(fn)
// Layout = DefaultLayout<arity>
// Storage = decay_arg_t<I> per param

// (2) Layout only — storage deduced
make_reactive<LayoutT>(fn)            // requires IsLayout<LayoutT>
// Storage = decay_arg_t<I> per param, grouped per layout
// e.g. make_reactive<Layout<Group<0,1>, Group<2>>>(fn)

// (3) Layout + explicit storage types
make_reactive<LayoutT, Ts...>(fn)     // requires IsLayout<LayoutT>
// Storage = Ts..., grouped per layout
// e.g. make_reactive<Layout<Group<0,1>, Group<2>>, string, int, double>(fn)

// (4) Explicit storage types, default layout
make_reactive<Ts...>(fn)              // requires (!IsLayout<Ts> && ...)
// Layout = DefaultLayout<sizeof...(Ts)>
// e.g. make_reactive<string, int>(fn)  — when decay doesn't match (string_view→string)
```

**Why explicit storage is needed:**

| Lambda param | `decay_arg_t` | Correct storage | Reason |
|---|---|---|---|
| `std::string_view` | `string_view` | `std::string` | Non-owning view, source must own |
| `std::span<int>` | `span<int>` | `vector<int>` | Non-owning view |
| `const Base&` | `Base` | `Derived` | Polymorphic storage |
| `int` | `int` | `int` | ✓ deduction works |
| `const string&` | `string` | `string` | ✓ deduction works |

Emit path: `Source<StorageT> → stored value → implicit conversion → lambda param`.
Verified at compile time: `static_assert(std::is_convertible_v<const StorageT&, ParamT>)`.

For groups: `Source<tuple<string, int>>` stores the tuple; emit unpacks each element
and converts to the corresponding lambda param type.

---

## 6. `callable_traits` — The Deduction Engine

> **C++26 note**: Static reflection (P2996) can replace this entirely. The manual
> implementation below is the C++20-compatible fallback. If targeting C++26-only,
> use `parameters_of(^F::operator())` instead (see §8).

```cpp
// Primary: lambda / functor
template <class T>
struct callable_traits : callable_traits<decltype(&T::operator())> {};

// Member function pointer
template <class C, class R, class... Args>
struct callable_traits<R(C::*)(Args...) const> {
    using return_type = R;
    using args = std::tuple<std::decay_t<Args>...>;
    static constexpr size_t arity = sizeof...(Args);

    template <size_t I>
    using arg = std::tuple_element_t<I, args>;
};

// Function pointer
template <class R, class... Args>
struct callable_traits<R(*)(Args...)> {
    using return_type = R;
    using args = std::tuple<std::decay_t<Args>...>;
    static constexpr size_t arity = sizeof...(Args);

    template <size_t I>
    using arg = std::tuple_element_t<I, args>;
};

// Strip const& / && from arg types to get the stored value type:
//   [](const Endpoint& e, Config c) → <Endpoint, Config>
```

---

## 7. Compile-Time Specialization Points

The compiler eliminates dead code for degenerate cases:

| Condition | What's eliminated |
|---|---|
| `sizeof...(Sources) == 1` | Cross-product, seq comparison, other-source scan |
| `AlwaysAlive` liveness | `isAlive()` check in scan |
| `RetainLatest` for all sources | Cross-product is at most 1×1×...×1 — single tuple |
| `Immediate` policy | All storage — direct passthrough |
| All params in one `Group` | M=1 degenerate — cross-product, seq, scan all eliminated |
| Homogeneous types | Type-dispatch ambiguity → require `add<I>()` (compile error) |

These are not runtime branches — they are `if constexpr` / SFINAE / concepts that
produce different compiled code per instantiation.

---

## 8. C++26 Features That Improve the Framework

Targeting C++26 unlocks several features that directly benefit this design:

### Pack Indexing (P2662)

Access the I-th element of a parameter pack directly: `Args...[I]`.
Eliminates `tuple_element_t` gymnastics in the cross-product engine:

```cpp
// C++20: painful
template <size_t I, class... Sources>
using source_value_t = typename std::tuple_element_t<I, std::tuple<Sources...>>::value_type;

// C++26: direct
template <size_t I, class... Sources>
using source_value_t = Sources...[I]::value_type;
```

This simplifies `NJoin::add<I>()`, `snapshotOne<I>()`, and the entire cross-product expansion.

### Contracts (P2900)

Replace `assert()` with language-level preconditions and postconditions:

```cpp
uint64_t insert(const T& value, SeqCounter& counter)
    pre(/* value is valid for this policy */)
    post(r: r == 0 || r > 0)  // 0 = rejected, >0 = assigned seq
{
    std::lock_guard<SpinLock> g(lock_);
    ...
}

bool remove(const T& value)
    requires RemovablePolicy<Policy>
    pre(/* value was previously inserted */)
    post(r: !r || !policy_.contains(value))  // if removed, it's gone
{ ... }
```

Contracts are checked at build level (off / default / audit) — zero cost in release,
full validation in debug. Replaces our `assert()` calls with structured, tool-visible contracts.

### ~~`std::hazard_pointer` (P2530)~~ — Not Applicable

Initially considered as an alternative to `shared_ptr` COW to avoid atomic refcount
cache-line bouncing. **However, this is a non-problem in our architecture.**

The `shared_ptr` copy (refcount increment) happens **under the source spinlock**:

```cpp
auto scan() {
    std::lock_guard<SpinLock> g(lock_);
    return data_;   // shared_ptr copy — atomic inc, but serialized by spinlock
}
```

Only one thread holds the spinlock at a time → refcount increments are already
serialized. The decrements (scan result destruction after `emit()`) happen outside
the lock, but at different times on different threads — no contention.

Hazard pointers solve concurrent atomic inc/dec on the same refcount. Our spinlock
already prevents that. **Removed from the implementation plan.**

### Static Reflection (P2996)

Reflection replaces our hand-written `callable_traits`:

```cpp
// C++20: manual specialization for lambda, function ptr, member function ptr
template <class T>
struct callable_traits : callable_traits<decltype(&T::operator())> {};
// ... 30 lines of specializations ...

// C++26: reflection
template <class F>
consteval auto extract_param_types() {
    constexpr auto params = parameters_of(^F::operator());
    return []<size_t... Is>(std::index_sequence<Is...>) {
        return type_list<std::decay_t<[:params[Is]:]>...>{};
    }(std::make_index_sequence<params.size()>{});
}
```

More importantly, reflection enables **automatic policy matching by type name**:

```cpp
auto j = make_reactive(
    [](const Endpoint& e, const Config& c) { ... },
    cow<Endpoint>()    // reflection matches Endpoint → source 0
);
// No positional guessing — the policy's T is matched against the lambda's param types
```

### `std::inplace_vector<T, N>` (P0843)

Fixed-capacity vector without heap allocation. Perfect for scan results from
bounded policies:

```cpp
// RetainLatest::scan() returns 0 or 1 elements — no heap needed
auto scan() const {
    std::inplace_vector<std::pair<const T*, uint64_t>, 1> result;
    if (current_) result.push_back({&current_->first, current_->second});
    return result;
}

// RetainLatestN<T, 5>::scan() — at most 5 elements, stack-allocated
auto scan() const {
    std::inplace_vector<std::pair<const T*, uint64_t>, N> result;
    ...
    return result;
}
```

Eliminates heap allocation on the scan hot path for bounded policies.

### Summary: What C++26 Buys Us

| Feature | Replaces | Benefit |
|---|---|---|
| Pack indexing | `tuple_element_t` chains | Cleaner variadic code |
| Contracts | `assert()` / manual checks | Structured, build-level toggled validation |
| Reflection | Hand-written `callable_traits` | Automatic, correct, extensible type deduction |
| `inplace_vector` | `vector` for bounded scans | Zero heap allocation on scan hot path |
| ~~`hazard_pointer`~~ | ~~`shared_ptr` refcount~~ | Not applicable — spinlock serializes increments |

---

## 9. Open Questions

1. **`scan()` return type unification** — Should all policies return the same type
   (`vector<pair<T, seq>>`) for a uniform `NJoin`, or keep policy-specific returns
   for maximum efficiency? Current: policy-specific. Risk: cross-product engine
   must be generic over scan result types.

2. **Allocator injection** — `RetainAll` uses `unordered_map`, which allocates.
   Should we template on allocator from day 1, or add later? Recommendation: later.

3. **`shared_ptr` refcount overhead** — The refcount increment is under the spinlock
   (serialized, no contention). The decrement is outside (scattered, no contention).
   Not a problem. `shared_ptr` is the right choice for COW.

4. **Cross-product iteration order** — Does the emit order within a single `add()` call
   matter? Current: unspecified. Recommendation: keep unspecified, document it.

5. **~~`make_reactive` with groups~~** — **Resolved.** Groups are a first-class feature via
   `GroupLayout` (Layer 5). The `group<Is...>(policy)` helper lets users declare which
   lambda params share a source. `make_reactive` infers the layout from descriptors.
   Ungrouped params default to `Slot<I>` (1:1 mapping). Groups provide atomic
   co-insertion (single seq) and tuple unpacking at emit time. Implemented in Phase 4.
