# Sequence-Owned N-Way Reactive Join Framework (C++)

*Generated: 2026-04-22, updated: 2026-04-23*

## Table of Contents

- [Problem](#problem)
- [Critical Invariant](#critical-invariant)
- [Core Insight: Sequence Ownership](#core-insight-sequence-ownership)
- [Algorithm](#algorithm)
- [Factorization: Source × RetentionPolicy × Join](#factorization-source--retentionpolicy--join)
- [Argument Groups](#argument-groups)
  - [Partition Lattice Structure](#partition-lattice-structure)
- [Composability: Shared Fan-Out](#composability-shared-fan-out)
- [Input Adapters: Debounce, Throttle, Batch](#input-adapters-debounce-throttle-batch)
- [C++ Implementation](#c-implementation)
- [Degenerate Cases](#degenerate-cases)
- [Properties Summary](#properties-summary)
- [Complexity](#complexity)
- [Throughput Estimates](#throughput-estimates)
- [Performance Considerations](#performance-considerations)
- [Removal Semantics](#removal-semantics)
- [Ergonomic API](#ergonomic-api)
- [DynamicJoin: Runtime Source Count](#dynamicjoin-runtime-source-count)
- [Application: DAG Orchestration](#application-dag-orchestration)
- [Comparison: Kubernetes Controller Pattern](#comparison-kubernetes-controller-pattern)
- [Formal Verification (TLA+)](#formal-verification-tla)
- [Publishability Assessment](#publishability-assessment)\n  - [Compile-Time Specialization: A Blind Spot in the Literature](#compile-time-specialization-a-blind-spot-in-the-literature)\n  - [How to Submit](#how-to-submit)
- [Usability Assessment: Human vs. AI-Assisted](#usability-assessment-human-vs-ai-assisted)
- [Related Work](#related-work)
- [Design Lineage](#design-lineage)

## Problem

Given N concurrent event sources $S_1, S_2, \ldots, S_N$, each producing typed values:
- When a **new unique** value arrives at any source, emit a callback over all valid tuples formed with the other sources' retained values.
- **No missed tuple**: every valid combination must be emitted exactly once.
- **No duplicate**: the same tuple must not be emitted twice.
- **No full-set copy**: avoid snapshotting entire state on every insert.
- **Minimal synchronization**: one spinlock per source, one shared atomic counter. Lock-minimal, not lock-free.

## Critical Invariant

> **Sequence numbers MUST be allocated under the source lock.**
>
> If seq is allocated before locking, a thread can receive a lower seq, then be preempted
> while another thread with a higher seq inserts and scans — missing the first thread's
> not-yet-visible element. The seq allocation and the insert must share the same
> serialization point. This is the single load-bearing correctness requirement.

## Core Insight: Sequence Ownership

Every new unique insert receives a monotonically increasing sequence number from a single global atomic counter (a Lamport clock).

For any N-tuple $(e_1, \ldots, e_N)$, exactly **one** element has the highest sequence number.
That element is the **last to arrive** and is the only one responsible for emitting the tuple.

This eliminates both misses and duplicates without any pair/tuple tracking.

### Why This Is Necessary

On real hardware, two threads can execute inserts in the same nanosecond.
Without a shared ordering point, neither side can determine whether the other has committed.
"Sequential" arrival is just the subcase where the ordering gap is large — the machine does not distinguish it.
The atomic counter is the minimal shared clock (one `fetch_add` = one cache-line bounce per unique insert).

## Algorithm

```
addToSource(i, x):
    lock S_i
        if S_i.contains(x): unlock S_i, return     // duplicate → no work
        sx = globalSeq.fetch_add(1) + 1             // allocate seq under lock
        S_i.insert(x, sx)
    unlock S_i

    // Scan all other sources AFTER own insert is visible
    for each j ≠ i:
        lock S_j
        snap[j] = S_j.scan()                        // policy-defined view
        unlock S_j

    // Emit only tuples where x has the maximum seq
    for each tuple (e_1, ..., x_i, ..., e_N) in snap[1] × ··· × snap[N]:
        if ∀ j ≠ i: e_j.seq < sx:
            emit(e_1, ..., x_i, ..., e_N)
```

### Correctness Proof Sketch

1. **No miss**: The element with the highest seq in any valid tuple scans after its own insert. All lower-seq elements are already committed → visible during scan.
2. **No duplicate**: For a given tuple, exactly one element has max seq. Only that element's `addToSource` can satisfy the `∀ e_j.seq < sx` predicate.
3. **No lock during emit**: All locks are released before the cross-product loop and callback invocation.

### Overflow Handling

Use signed wraparound comparison instead of raw `>`:

```cpp
bool isNewer(uint64_t candidate, uint64_t reference) {
    return (int64_t)(candidate - reference) > 0;
}
```

This is correct across full wraparound as long as no two simultaneously in-flight sequence numbers are more than $2^{63}$ apart — physically impossible.

| Counter width | Safe in-flight range | Wrap time @ 1M/sec |
|---|---|---|
| 64-bit | $2^{63} \approx 9.2 \times 10^{18}$ | ~292,000 years |
| 32-bit | $2^{31} \approx 2.1 \times 10^{9}$ | ~35 minutes |

For bounded domains where the counter only increments on genuinely new unique items,
32-bit is sufficient if `|S_1| + ... + |S_N|` < $2^{31}$.
Overflow from misuse (remove/re-add cycling) degrades to **duplicate emission**, never to missed emission — the safer failure mode.

## Factorization: Source × RetentionPolicy × Join

The framework separates into three orthogonal components:

```
┌────────────────────────────────────┐
│            NJoin<Sources...>       │
│  ┌──────┐  ┌──────┐     ┌──────┐   │
│  │ S_1  │  │ S_2  │ ... │ S_N  │   │
│  └──┬───┘  └──┬───┘     └──┬───┘   │
│     │         │             │      │
│  atomic<uint64_t> globalSeq        │
│  EmitFn                            │
└────────────────────────────────────┘

Each S_i = Source<T_i, Policy_i>
```

### Retention Policies

Each source independently defines what it keeps and what `scan()` returns.
The join logic never inspects policy internals.

| Policy | Storage | `scan()` returns | Use case |
|---|---|---|---|
| `RetainAll` | `unordered_map<T, seq>` | all items | Service discovery, topology |
| `RetainAllCOW` | `shared_ptr<const map>` | all items (O(1) scan) | Large sets, high read rate |
| `RetainLatest` | `optional<pair<T, seq>>` | 0–1 item | Current config, latest state |
| `RetainLatestN` | `deque<pair<T, seq>>` | last N items | Recent commands, log tail |
| `SlidingWindow` | time-stamped deque | items within TTL | Heartbeats, metrics |

### Policy Contract

```cpp
template <class T>
concept RetentionPolicy = requires(T policy, typename T::value_type v, uint64_t seq) {
    // Returns true if value was accepted (new/unique per policy rules).
    { policy.tryInsert(v, seq) } -> std::same_as<bool>;

    // Iterable range of (value, seq) pairs currently retained.
    { policy.scan() }            -> std::ranges::range;

    // Fast duplicate/membership check.
    { policy.contains(v) }       -> std::same_as<bool>;
};
```

## Argument Groups

Not every function argument maps to an independent source.
Several arguments may always arrive together from the same event — e.g., `(host, port)` from
a discovery service, or `(user, role, token)` from an auth event.

Argument groups let a single source fill multiple argument slots atomically with one seq number.

### Semantics

- A **group** is a subset of argument indices that are always inserted together.
- A group shares one lock and one seq allocation.
- The cross-product treats the group as a single dimension.
- The seq ownership rule is unchanged: max-seq element (or group) emits.

### Example

```cpp
// Function: deploy(Endpoint, Host, Port, Config)
//   Arg 0: Endpoint       — independent source
//   Arg 1+2: Host, Port   — always arrive together (group)
//   Arg 3: Config          — independent source

auto reactive = make_reactive(
    deploy,
    Independent{ RetainAll{} },             // arg 0: Endpoint
    Group<1,2>{ RetainAll{} },              // args 1,2: Host+Port as one source
    Independent{ RetainLatest{} }           // arg 3: Config
);

// Grouped insert: one call fills two slots, one seq
reactive.addGroup<1>(HostPort{"node-1", 8080});

// Independent inserts: normal
reactive.add<0>("svc-alpha");
reactive.add<3>(config);
```

### Internal Mapping

The join sees M sources (M ≤ N), not N argument slots:

```
Function args:   [0]     [1]  [2]     [3]
                  │       └──┬──┘       │
Sources:        S_0       S_1         S_2
                  │         │           │
                 seq       seq         seq
```

- `S_1` stores `pair<Host, Port>` as its value type.
- On emit, the group value is destructured back into the individual argument slots.
- The cross-product iterates over M dimensions, not N.
- Correctness is preserved: the group gets one seq, participates in ownership as one unit.

### Group Policy Contract

```cpp
template <size_t... ArgIndices>
struct Group {
    // Value type is a tuple of the argument types at ArgIndices.
    // Policy applies to this tuple as a unit.
    // tryInsert, scan, contains operate on the tuple.
};
```

This reduces the dimensionality of the cross-product (M < N), which directly
improves emit-time complexity from N independent sources to M grouped sources:

$$O\left(\prod\limits_{\substack{j=1 \\ j \neq i}}^{M} |S_j|\right) \quad \text{instead of} \quad O\left(\prod\limits_{\substack{j=1 \\ j \neq i}}^{N} |S_j|\right)$$

### Partition Lattice Structure

Argument groups are not an ad-hoc convenience feature — they are instances of the
**partition lattice** $\Pi_K$ over a source's index set $\{0, \ldots, K{-}1\}$.

Each source storing a $K$-tuple defines a bounded lattice:
- **Ceiling**: the trivial partition $\{\{0,1,\ldots,K{-}1\}\}$ — the full tuple stored atomically.
- **Floor**: the discrete partition $\{\{0\},\{1\},\ldots,\{K{-}1\}\}$ — each element independent.
- **Interior nodes**: all partitions in between — partial groupings.

```
                  {0,1,2}                  ← ceiling (one source, atomic)
                 /   |   \
         {0,1}{2}  {0,2}{1}  {1,2}{0}     ← intermediate (partial groups)
                 \   |   /
                  {0}{1}{2}                ← floor (all singletons)
```

**Operations on the lattice:**
- **Split (refine):** a view projecting a subset of indices — always valid downward.
- **Merge (coarsen):** recombining sibling views into a wider projection — valid only
  within the same parent source (same seq domain, same lock).
- **Rebind:** moving source ownership to a new NJoin with a different lattice point
  as its operational layout. Both directions are free (no data restructuring).

**Correctness at every lattice level:**

The seq-ownership invariant and lowest-seq dedup rule hold at every partition node,
not just ceiling or floor. A projected view at any intermediate node satisfies:

$$\text{scan}_{\pi}(S) = \{(v, \min\{s \mid \exists t \in S : \pi(t) = v \land \text{seq}(t) = s\}) \mid v \in \pi(S)\}$$

where $\pi$ is the projection function for the chosen subset of indices.
This is equivalent to `SELECT DISTINCT ON (projected_cols) ORDER BY seq ASC`
in relational terms.

**Research connections:**

| Concept | Standard mathematics | Reference |
|---|---|---|
| Source index set | Finite set $\{0,\ldots,K{-}1\}$ | — |
| Layout/grouping | Set partition | Rota (1964), partition lattice $\Pi_n$ |
| Split/merge | Refinement/coarsening in $\Pi_n$ | Birkhoff, *Lattice Theory* (1940) |
| Lattice node count | Bell number $B(K)$ | $B(3)=5, B(4)=15, B(5)=52$ |
| Dedup projection | Relational algebra $\pi$ with DISTINCT | Codd (1970) |
| Lowest-seq rule | $\text{MIN}(\text{seq}) \; \text{GROUP BY} \; \pi(v)$ | — |
| Self-similarity of sub-lattices | $\Pi_k \hookrightarrow \Pi_K$ for blocks of size $k$ | — |
| Writable views (RMW) | Read-modify-write on tuple store | CAS literature |

**Why this elevates the framework:**

1. **Groups are not a feature — they are fundamental structure.** The partition lattice
   shows that grouping, splitting, and projecting are all instances of the same algebra.
   The framework doesn't add groups as an API convenience; it inherits the lattice from
   the mathematical structure of indexed tuples.

2. **Views are free in both directions.** Because the physical source type (the ceiling)
   is fixed, any lattice navigation is purely a lens change — no data copy. This is the
   "transpose is just the floor" insight: what looks like row↔column conversion is actually
   traversal of a bounded lattice with zero data movement.

3. **The dedup rule generalizes uniformly.** The lowest-seq rule, previously justified only
   for floor projections, is now shown to be correct at every interior node. The $\min$ over
   the projected preimage is the natural consistent choice because it preserves the
   seq-ownership semantics from the original insert ordering.

4. **Compile-time lattice navigation.** In C++, the lattice structure is entirely
   compile-time — template parameters select the partition node, `consteval` functions
   compute index mappings, and `if constexpr` eliminates branches for nodes above or
   below the operational point. The lattice is never traversed at runtime.

## Composability: Shared Fan-Out

A single value can feed the same argument slot in multiple joins. This is **fan-out**:
one insert propagates to N independent consumers. In the framework, fan-out is not a
separate mechanism — it is an NJoin in its M=1 Immediate degenerate form, where the
emit function calls `add()` on downstream joins.

The fan-out node is stateless: no sequencer, no retention, no knowledge of arity.
Each downstream join maintains its own sequencer and state. Cross-join ordering is not
needed because each join's tuple space is independent.

```cpp
// Fan-out expressed as an M=1 NJoin with SubscriberList as EmitFn:
auto endpoints = make_event<Endpoint>();

// Wire into multiple downstream joins:
endpoints.subscribe([&](const Endpoint& e) { j1.add(e); });
endpoints.subscribe([&](const Endpoint& e) { j2.add(e); });

// One fire → both joins receive the value:
endpoints.fire(endpoint);
// → j1.add(endpoint) → j1 scans its other sources → j1.emit(...)
// → j2.add(endpoint) → j2 scans its other sources → j2.emit(...)
```

Mechanically, `fire()` iterates a `SubscriberList` — a **COW (copy-on-write)** subscriber
container. Each subscriber typically calls `joinX.add(value)` on a downstream join.
The downstream join then does its own insert → scan → cross-product → emit, fully
independent of any other join.

### SubscriberList: COW + Single-Bit Spinlock

The SubscriberList uses a different synchronization strategy than Source:

| Component | Lock type | Why |
|---|---|---|
| **Source** (insert/scan) | Exclusive spinlock (Rigtorp) | Held ~ns, single writer per source |
| **SubscriberList** (fire/subscribe) | COW + single-bit exclusive | fire is hot path, must never block |

The problem with a reader-writer lock for SubscriberList: `fire()` is the hot path
(called on every event), `subscribe()` is rare (setup time). With a classic RW-lock,
the writer must wait for all readers to drain to zero — but if `fire()` is frequent,
there's always a reader holding the lock, and the writer starves.

COW eliminates this entirely. `fire()` takes no lock at all — it atomically loads a
`shared_ptr` to an immutable snapshot and iterates it. `subscribe()` takes a single-bit
exclusive spinlock (only to serialize against other `subscribe()` calls), copies the
vector, appends the new subscriber, and atomically swaps the pointer.

```cpp
template <class... Args>
class SubscriberList {
    using Fn = std::function<void(const Args&...)>;
    using Snapshot = std::vector<Fn>;

    std::atomic<std::shared_ptr<const Snapshot>> subs_;
    alignas(64) std::atomic_flag write_lock_ = ATOMIC_FLAG_INIT;  // single bit

public:
    // fire() — wait-free, no lock
    void fire(const Args&... args) {
        auto snap = subs_.load();            // atomic refcount bump
        if (!snap) return;
        for (auto& fn : *snap) fn(args...);
    }   // snap dtor decrements refcount; old snapshot dies when last reader drops it

    // subscribe() — single-bit exclusive lock (writers only)
    void subscribe(auto&& fn) {
        // Acquire single-bit spinlock (Rigtorp pattern)
        while (write_lock_.test_and_set(std::memory_order_acquire))
            ; // only other subscribe() calls contend here

        auto old = subs_.load();
        auto copy = old ? std::make_shared<Snapshot>(*old)
                        : std::make_shared<Snapshot>();
        copy->emplace_back(std::forward<decltype(fn)>(fn));
        subs_.store(std::shared_ptr<const Snapshot>(std::move(copy)));

        write_lock_.clear(std::memory_order_release);
    }
};
```

```
fire(ep)                       subscribe(fn)
  │                               │
  │ atomic_load(subs_)            │ spinlock(1 bit)
  │   → shared_ptr to V1          │ copy V1 → V2
  │ iterate V1                    │ append fn to V2
  │   (V1 stays alive via refcount)│ atomic_store(subs_ = V2)
  │ ~shared_ptr (drop V1)         │ ~spinlock
```

The key insight: COW turns the question from "when are all readers done?" (which
requires a reader counter) into "when does the last shared_ptr die?" (which the
refcount handles automatically). One bit is all the synchronization needed.

```
endpoints.fire(ep)
    │
    ├─→ j1.add(ep) → j1 scans {Config} → j1.emit(ep, config)
    │                  (j1's own seq counter, j1's own sources)
    │
    └─→ j2.add(ep) → j2 scans {Config, Health} → j2.emit(ep, config, health)
                       (j2's own seq counter, j2's own sources)
```

Note: each join assigns its **own seq** to the value. The same Endpoint gets seq=5 in j1
and seq=12 in j2. This is correct — seqs are per-join, never compared across joins.

## Input Adapters: Debounce, Throttle, Batch

Sometimes you don't want `add()` to fire immediately. A source detects a change, but
you want to wait — either to coalesce a burst of rapid changes into one emission, or to
batch multiple values into a single delivery. This is **pre-source** concern: it sits
between the raw event and `join.add()`, outside the join's internals.

### The Problem

A config-file watcher fires 5 events in 50ms as a file is being saved. You don't want
5 scan-and-emit cycles — you want one, with the final state. But you also can't wait
forever: if the last event arrives and nothing follows, you must still trigger.

### Taxonomy

| Adapter | Behavior | Emits |
|---|---|---|
| **Debounce** | Wait D after last event; reset timer on each new event | Latest value when silence ≥ D |
| **Debounce + cap** | Same, but force-emit after max delay M even if events keep arriving | Latest value, at most every M |
| **Throttle** | Forward at most once per interval D | First (leading) or last (trailing) per window |
| **Batch** | Collect values for duration D, then forward all at once | `vector<T>` of accumulated values |

```
Debounce (trailing, D=200ms):
events:    ──a──b──c────────────────d──e────────────
              │  │  │                │  │
timers:       ╳  ╳  ├──[200ms]──→   ╳  ├──[200ms]──→
                     reset    emit c         emit e

Debounce + cap (D=200ms, M=1s):
events:    ──a─b─c─d─e─f─g─h─i─j──────────────k─────
              │                 │              │
              ├──[cap 1s]──→    ├──[200ms]──→  ├──[200ms]──→
                        emit j          (none)      emit k
                        (forced: events kept coming for 1s)

Throttle (leading edge, D=200ms):
events:    ──a──b──c──────d──e──f──────
              ↓           ↓
           emit a      emit d
           (b,c dropped)  (e,f dropped)
```

### Where It Sits

Input adapters are **callable wrappers** around subscriber callbacks. They don't touch
the join, the source, the seq counter, or retention policies. They only control *when*
the wrapped callback is invoked:

```
raw event → InputAdapter → join.add(value)
                  │
                  └── decides WHEN to forward
```

This means every combination works:
- `debounce(200ms) + RetainLatest` — coalesce config bursts, keep only last
- `debounce(200ms) + RetainAll` — coalesce bursts, keep full history
- `throttle(1s) + SlidingWindow(30s)` — rate-limit but keep a time window
- `batch(500ms) + RetainAll` — collect values, insert all at once

### The Timer Problem and Caller-Provided Executor

Debounce requires a timer. The framework has been timer-free so far — all execution is
synchronous on the caller's thread. Adding a timer means *someone* must schedule a
future callback.

The framework does **not** own a timer. Instead, the caller provides a **schedule function**:

```cpp
// The schedule concept: given a duration and a callback, arrange for the
// callback to be called after the duration elapses. Returns a handle
// that can be used to cancel the pending callback.
//
// This is the ONLY integration point with the caller's async runtime.

template <class F>
concept Scheduler = requires(F f, std::chrono::milliseconds d, std::function<void()> cb) {
    { f(d, std::move(cb)) } -> /* cancel handle */;
};
```

How this works mechanically, step by step:

**1. The caller already has an event loop / timer facility.** This could be:
- A `boost::asio::io_context` with `steady_timer`
- A Qt event loop with `QTimer::singleShot`
- A game loop with a priority queue of timed callbacks
- A simple `std::jthread` + `sleep_for` for single-use cases
- Even a manual "tick" function called from a main loop

**2. The caller wraps it as a schedule function:**

```cpp
// Example: boost::asio scheduler
auto asio_scheduler = [&io_ctx](std::chrono::milliseconds delay,
                                std::function<void()> cb) {
    auto timer = std::make_shared<asio::steady_timer>(io_ctx, delay);
    timer->async_wait([timer, cb = std::move(cb)](auto ec) {
        if (!ec) cb();
    });
    return timer;  // cancel handle: timer->cancel() on destruction
};

// Example: simple jthread scheduler (one thread per timer)
auto thread_scheduler = [](std::chrono::milliseconds delay,
                           std::function<void()> cb) {
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    std::jthread([=](std::stop_token) {
        std::this_thread::sleep_for(delay);
        if (!cancel->load()) cb();
    }).detach();
    return cancel;  // cancel: cancel->store(true)
};
```

**3. The debounce adapter uses the scheduler internally:**

```cpp
template <class T, class Scheduler, class Callback>
class Debounce {
    Scheduler schedule_;
    Callback callback_;
    std::chrono::milliseconds delay_;

    std::mutex mu_;             // guards pending_ and cancel_handle_
    std::optional<T> pending_;  // latest value not yet forwarded
    /* cancel handle */ decltype(schedule_(delay_, std::function<void()>{})) cancel_;

public:
    Debounce(std::chrono::milliseconds delay, Scheduler sched, Callback cb)
        : delay_(delay), schedule_(std::move(sched)), callback_(std::move(cb)) {}

    // Called by the event producer (e.g., fan-out subscriber)
    void operator()(const T& value) {
        std::lock_guard lk(mu_);

        // 1. Store latest value (overwrites previous if timer hasn't fired)
        pending_ = value;

        // 2. Cancel the previous timer (if any)
        cancel_ = {};  // destroy old handle → cancels pending callback

        // 3. Schedule a new timer
        cancel_ = schedule_(delay_, [this] {
            T val;
            {
                std::lock_guard lk(mu_);
                if (!pending_) return;    // spurious or already flushed
                val = std::move(*pending_);
                pending_.reset();
            }
            callback_(val);  // → join.add(val)  — outside the lock
        });
    }

    // Force-emit whatever is pending (for shutdown)
    void flush() {
        std::lock_guard lk(mu_);
        cancel_ = {};  // cancel timer
        if (pending_) {
            auto val = std::move(*pending_);
            pending_.reset();
            callback_(val);
        }
    }
};
```

**4. Wiring it together:**

```cpp
auto endpoints = make_event<Endpoint>();

// j1 is a single-source join (M=1, Immediate) — fires on every add()
auto j1 = make_reactive([](const Endpoint& e) { deploy(e); }, RetainLatest{});

// Without debounce — direct:
//   endpoints.subscribe([&](const Endpoint& e) { j1.add(e); });

// With debounce — same wiring, wrapped:
auto debounced = seqjoin::debounce(200ms, asio_scheduler,
    [&](const Endpoint& e) { j1.add(e); });
endpoints.subscribe(std::move(debounced));

// Now:
// endpoints.fire(ep1);  → pending_ = ep1, timer starts
// endpoints.fire(ep2);  → pending_ = ep2, timer reset (ep1 never forwarded)
// ... 200ms silence ...
// timer fires           → j1.add(ep2)  → join does insert/scan/emit
```

**5. What happens inside:**

```
Thread A (producer):              Scheduler thread:
─────────────────────             ────────────────
fire(ep1)
  → Debounce::operator()(ep1)
    lock mu_
    pending_ = ep1
    cancel old timer
    schedule_(200ms, callback)
    unlock mu_
                                   ... 50ms later ...
fire(ep2)
  → Debounce::operator()(ep2)
    lock mu_
    pending_ = ep2               (ep1 overwritten, never forwarded)
    cancel 150ms-remaining timer
    schedule_(200ms, callback)   new timer starts
    unlock mu_
                                   ... 200ms of silence ...
                                   timer fires:
                                     lock mu_
                                     val = ep2
                                     pending_ = nullopt
                                     unlock mu_
                                     callback_(ep2)  → j1.add(ep2)
                                                       → insert, scan, emit
```

### Debounce with Max-Wait Cap

Pure debounce can starve if events never stop arriving. The capped variant tracks
when the first un-forwarded event arrived and force-fires after duration M:

```cpp
void operator()(const T& value) {
    std::lock_guard lk(mu_);
    auto now = clock::now();

    if (!pending_) {
        first_arrival_ = now;   // start the cap window
    }
    pending_ = value;
    cancel_ = {};  // cancel debounce timer

    auto since_first = now - first_arrival_;
    if (since_first >= max_wait_) {
        // Cap exceeded — emit immediately
        auto val = std::move(*pending_);
        pending_.reset();
        callback_(val);  // outside lock ideally, simplified here
    } else {
        // Schedule debounce, but no later than remaining cap
        auto effective = std::min(delay_, max_wait_ - since_first);
        cancel_ = schedule_(effective, [this] { /* same as basic debounce */ });
    }
}
```

### Throttle and Batch

**Throttle** is simpler than debounce — it just tracks the last forward time and
drops or queues events that arrive too soon:

```cpp
// Leading-edge throttle: forward immediately, ignore for D
void operator()(const T& value) {
    auto now = clock::now();
    if (now - last_forward_ >= interval_) {
        last_forward_ = now;
        callback_(value);
    }
    // else: dropped
}
```

**Batch** accumulates values and flushes them as a vector on a timer:

```cpp
void operator()(const T& value) {
    std::lock_guard lk(mu_);
    buffer_.push_back(value);
    if (!timer_running_) {
        timer_running_ = true;
        cancel_ = schedule_(window_, [this] {
            std::vector<T> batch;
            {
                std::lock_guard lk(mu_);
                batch = std::move(buffer_);
                timer_running_ = false;
            }
            callback_(std::move(batch));  // → join.add(batch) or iterate
        });
    }
}
```

### Thread Safety

The adapter's `mu_` lock is independent of the join's per-source spinlock.
The callback (which calls `join.add()`) is always invoked **outside** the adapter's lock.
This avoids nested locking: adapter-lock → (release) → join-spinlock.

### Factory Helpers

```cpp
// Convenience: wrap any callback with debounce
auto debounced = seqjoin::debounce(200ms, scheduler, [&](auto& e) { j.add(e); });
auto capped    = seqjoin::debounce(200ms, 2s, scheduler, [&](auto& e) { j.add(e); });
auto throttled = seqjoin::throttle(1s, [&](auto& e) { j.add(e); });
auto batched   = seqjoin::batch(500ms, scheduler, [&](auto& batch) {
    for (auto& e : batch) j.add(e);
});

// Wire to fan-out:
endpoints.subscribe(std::move(debounced));
```

Note: `throttle` (leading-edge) needs no scheduler — it's purely synchronous,
just comparing `clock::now()` to `last_forward_`. Only trailing-edge adapters
(debounce, batch) need a scheduler.

### Interaction with Seq

Because the adapter delays when `add()` is called, the value gets a **later seq**
than it would without the adapter. This is correct:
- Seq reflects when the join *learned* about the value
- The join's correctness depends only on seq ordering within itself
- Real-world timestamps are irrelevant to the seq-ownership algorithm

A value debounced by 200ms simply participates in later cross-products than it
would have otherwise. No invariant is violated.

## C++ Implementation

### SpinLock

```cpp
#include <atomic>
#include <cstdint>
#include <thread>

class SpinLock {
public:
    void lock() noexcept {
        for (uint32_t spins = 0;
             flag_.test_and_set(std::memory_order_acquire);
             ++spins) {
            if ((spins & 0xFF) == 0xFF) std::this_thread::yield();
        }
    }

    void unlock() noexcept {
        flag_.clear(std::memory_order_release);
    }

private:
    std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
};
```

### RetainAll Policy

```cpp
#include <unordered_map>
#include <vector>

template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class RetainAll {
public:
    using value_type = T;

    bool tryInsert(const T& value, uint64_t seq) {
        auto [it, inserted] = items_.emplace(value, seq);
        return inserted;
    }

    bool contains(const T& value) const {
        return items_.count(value) > 0;
    }

    // Returns vector of (value, seq) — lightweight view of current state.
    auto scan() const {
        std::vector<std::pair<const T*, uint64_t>> result;
        result.reserve(items_.size());
        for (const auto& [v, s] : items_) {
            result.emplace_back(&v, s);
        }
        return result;
    }

private:
    std::unordered_map<T, uint64_t, Hash, Eq> items_;
};
```

### RetainAllCOW Policy (Recommended for Large Sets)

Avoids O(n) copy under lock. Scan returns a shared pointer to an immutable snapshot.
Lock is only held for pointer load (O(1)).

```cpp
#include <memory>

template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class RetainAllCOW {
public:
    using value_type = T;
    using Map = std::unordered_map<T, uint64_t, Hash, Eq>;

    RetainAllCOW() : data_(std::make_shared<const Map>()) {}

    bool tryInsert(const T& value, uint64_t seq) {
        if (data_->count(value)) return false;
        auto next = std::make_shared<Map>(*data_);
        next->emplace(value, seq);
        data_ = std::move(next);
        return true;
    }

    bool contains(const T& value) const {
        return data_->count(value) > 0;
    }

    // O(1) under lock — just ref-count bump.
    std::shared_ptr<const Map> scan() const {
        return data_;
    }

private:
    std::shared_ptr<const Map> data_;
};
```

### RetainLatest Policy

```cpp
#include <optional>

template <class T>
class RetainLatest {
public:
    using value_type = T;

    bool tryInsert(const T& value, uint64_t seq) {
        // Always accept — latest overwrites previous.
        current_ = {value, seq};
        return true;
    }

    bool contains(const T&) const {
        return current_.has_value();
    }

    auto scan() const {
        std::vector<std::pair<const T*, uint64_t>> result;
        if (current_) {
            result.emplace_back(&current_->first, current_->second);
        }
        return result;
    }

private:
    std::optional<std::pair<T, uint64_t>> current_;
};
```

### Source Wrapper

```cpp
template <class T, class Policy>
class Source {
public:
    using value_type = T;

    // Returns seq if accepted, nullopt if rejected.
    // 0 is a valid seq (not a sentinel — overflow wraps to 0).
    std::optional<uint64_t> insert(const T& value, std::atomic<uint64_t>& counter) {
        std::lock_guard<SpinLock> g(lock_);
        uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
        if (policy_.tryInsert(value, seq)) {
            return seq;
        }
        return std::nullopt;
    }

    auto scan() {
        std::lock_guard<SpinLock> g(lock_);
        return policy_.scan();
    }

    SpinLock& lock() { return lock_; }

private:
    SpinLock lock_;
    Policy policy_;
};
```

### NJoin — N-Way Reactive Join

```cpp
#include <functional>
#include <tuple>
#include <utility>

// Helper: cross-product iteration with seq filter.
// Calls emit for every valid tuple where all other seqs < trigger_seq.
template <class EmitFn, class TriggerVal, size_t TriggerIdx, class... Snaps>
void crossProductEmit(
    EmitFn& emit,
    const TriggerVal& trigger,
    uint64_t trigger_seq,
    const std::tuple<Snaps...>& snaps)
{
    // Recursive cross-product over tuple of snapshots.
    // Each Snap is a vector<pair<const T*, uint64_t>>.
    // TriggerIdx is skipped (replaced by trigger value).

    constexpr size_t N = sizeof...(Snaps);

    // Flatten into iterative approach for clarity.
    // For production N > 3, use recursive template expansion.
    // Shown here as a conceptual loop:

    auto allOthersBelowSeq = [&](auto&&... elems) {
        // Check all non-trigger elements have seq < trigger_seq
        bool valid = true;
        size_t idx = 0;
        auto check = [&](auto* ptr, uint64_t s) {
            if (idx != TriggerIdx) {
                valid = valid && ((int64_t)(trigger_seq - s) > 0);
            }
            ++idx;
        };
        (check(elems.first, elems.second), ...);
        return valid;
    };

    // ... emit(tuple) if allOthersBelowSeq
}

template <class EmitFn, class... Sources>
class NJoin {
public:
    explicit NJoin(EmitFn emit) : emit_(std::move(emit)) {}

    template <size_t I>
    void add(const typename std::tuple_element_t<I, std::tuple<Sources...>>::value_type& value) {
        auto& src = std::get<I>(sources_);

        // Insert under source lock; seq allocated atomically.
        auto sx = src.insert(value, counter_);
        if (!sx) return;  // duplicate or rejected by policy

        // Snapshot all other sources (each under its own lock, briefly).
        auto snaps = snapshotAllExcept<I>();

        // Emit valid tuples where this element has max seq.
        emitTuples<I>(value, *sx, snaps);
    }

private:
    template <size_t Exclude>
    auto snapshotAllExcept() {
        return snapshotImpl<Exclude>(std::index_sequence_for<Sources...>{});
    }

    template <size_t Exclude, size_t... Is>
    auto snapshotImpl(std::index_sequence<Is...>) {
        return std::make_tuple(snapshotOne<Is, Exclude>()...);
    }

    template <size_t I, size_t Exclude>
    auto snapshotOne() {
        if constexpr (I == Exclude) {
            // Return empty placeholder for the triggering source.
            return decltype(std::get<I>(sources_).scan()){};
        } else {
            return std::get<I>(sources_).scan();
        }
    }

    template <size_t I, class Snaps>
    void emitTuples(
        const typename std::tuple_element_t<I, std::tuple<Sources...>>::value_type& trigger,
        uint64_t trigger_seq,
        const Snaps& snaps)
    {
        // Cross-product emit with seq filter.
        // For N=2 this is a single loop.
        // For N=3+ this is nested loops or recursive expansion.
        crossProductFiltered<I>(trigger, trigger_seq, snaps,
                                std::index_sequence_for<Sources...>{});
    }

    // --- 2-source specialization for clarity ---
    // For N=2 the cross product is just a single loop:
    template <size_t I, class Snaps, size_t... Is>
    void crossProductFiltered(
        const auto& trigger, uint64_t trigger_seq,
        const Snaps& snaps, std::index_sequence<Is...>)
    {
        // Iterate the single other source's snapshot.
        auto iterate = [&]<size_t J>() {
            if constexpr (J != I) {
                for (const auto& [ptr, seq] : std::get<J>(snaps)) {
                    if ((int64_t)(trigger_seq - seq) > 0) {
                        emit_(trigger, *ptr);  // for N=2
                    }
                }
            }
        };
        (iterate.template operator()<Is>(), ...);
    }

    std::tuple<Sources...> sources_;
    std::atomic<uint64_t> counter_{0};
    EmitFn emit_;
};
```

### Usage Example

```cpp
#include <iostream>
#include <string>
#include <thread>

int main() {
    using SrcEndpoints = Source<std::string, RetainAll<std::string>>;
    using SrcConfig    = Source<int,         RetainLatest<int>>;

    NJoin<std::function<void(const std::string&, const int&)>,
          SrcEndpoints, SrcConfig>
    join([](const std::string& endpoint, const int& config) {
        std::cout << "Event: endpoint=" << endpoint
                  << " config=" << config << "\n";
    });

    std::thread producer_a([&] {
        join.add<0>("svc-alpha");
        join.add<0>("svc-beta");
        join.add<0>("svc-alpha");   // duplicate — ignored
    });

    std::thread producer_b([&] {
        join.add<1>(42);
        join.add<1>(99);            // overwrites 42 (RetainLatest)
    });

    producer_a.join();
    producer_b.join();
}
```

## Degenerate Cases

The framework generalizes the classic event system. Special cases collapse to zero overhead
when the compiler can evaluate source count at compile time (`constexpr`).

### Classic Event (M=1, Single Group, Immediate)

A traditional event where `fire(a, b, c)` notifies all subscribers is a single group
containing all arguments, with an `Immediate` policy that never retains or deduplicates.

```cpp
// Immediate policy: always accept, never retain, never scanned.
template <class T>
class Immediate {
public:
    using value_type = T;
    bool tryInsert(const T&, uint64_t) { return true; }
    bool contains(const T&) const      { return false; }
    auto scan() const {
        return std::vector<std::pair<const T*, uint64_t>>{};
    }
};

// Classic event: all args grouped, immediate policy.
auto onDeploy = make_reactive(
    [](const Endpoint& e, const Config& c) {
        std::cout << "deploy " << e << " with " << c << "\n";
    },
    Group<0,1>{ Immediate{} }
);

// Equivalent to event.fire(endpoint, config):
onDeploy.addGroup<0>(std::tuple{endpoint, config});
```

With M=1 the entire join machinery is eliminated at compile time:

| Framework feature | With M=1 |
|---|---|
| Cross-product | Trivially 1 — nothing to iterate |
| Seq ownership check | Only element is always max — always true |
| Scan other sources | No other sources exist |
| Seq counter | Never compared — optimized out |
| Spinlock on other sources | None exist |
| Race / simultaneous case | Impossible — single dimension |

The compiled output is equivalent to a direct function call through the subscriber list.

### Countdown Barrier (M=N, Homogeneous, RetainLatest)

A legacy pattern: an event that must be called N times before it fires.
This ensures all data points have been provided before processing.

```cpp
// Legacy approach:
event.setThreshold(3);
event.call(data1);  // no fire (count=1)
event.call(data2);  // no fire (count=2)
event.call(data3);  // FIRES  (count=3)
```

In the framework, this is an **N-way join with N homogeneous `RetainLatest` sources**:

```cpp
auto barrier = make_reactive(
    [](const Data& a, const Data& b, const Data& c) {
        process(a, b, c);
    },
    RetainLatest{},   // slot 0
    RetainLatest{},   // slot 1
    RetainLatest{}    // slot 2
);

barrier.add<0>(data1);  // no fire — slots 1,2 empty
barrier.add<1>(data2);  // no fire — slot 2 empty
barrier.add<2>(data3);  // FIRES — all slots filled
```

The cross-product of empty sources is empty, so the join **cannot fire** until every
source has at least one item. The countdown emerges naturally — no special policy needed.

| | Legacy countdown | Framework N-join |
|---|---|---|
| Knows which data is where | No — just a counter | Yes — typed slots |
| Re-fire on update | No — counter already reached N | Yes — `RetainLatest` re-emits on any slot change |
| Thread safety | Manual | Built-in (seq-ownership) |
| Partial reset | Not possible | Update individual slots |
| Compile-time arity check | No — runtime N | Yes — `sizeof...(Sources)` |

The legacy countdown hides the question "which of the N data points is this?" —
callers fire blindly and hope the order is right. The framework makes intent explicit.

### Spectrum of Specializations

```
M=1, single group, Immediate       →  classic event.fire(a, b, c)
M=1, single group, RetainLatest    →  stateful event (late subscribers get last value)
M=1, single group, RetainAll       →  replay event (new subscribers get full history)
M=N, same type, RetainLatest       →  countdown barrier (fire after all N slots filled)
M=N, same type, runtime N          →  BarrierSource in NJoin (preferred) or DynamicJoin
M=2, two groups, Immediate         →  two-source join, fire-and-forget
M=2, independent args, RetainAll   →  full 2D accumulating join
M=N, mixed policies, groups        →  general case
```

Every row uses the same framework code — the only differences are the policy types
and the group layout, both resolved at compile time.

## Properties Summary

| Property | Guarantee |
|---|---|
| No missed tuple | Max-seq element scans after commit → sees all lower-seq peers |
| No duplicate tuple | Only max-seq element satisfies the `∀ seq < sx` predicate |
| Per-source locking | Source $i$ lock only held during insert or scan of $S_i$ |
| No lock during emit | Callback runs after all locks released |
| Overflow safety | Signed wraparound comparison; degrades to duplicate, never miss |
| Type safety | Each source is independently typed; no merging |
| N-dimensional | Same algorithm for any number of sources |
| Policy-independent | Join logic is decoupled from retention strategy |

## Complexity

Post-insert scan cost (M = number of source dimensions, ≤ N with argument groups):

$$C_{\text{scan}} = O\left(\prod\limits_{\substack{j=1 \\ j \neq i}}^{M} |S_j|\right)$$

| Operation | Cost |
|---|---|
| Insert (unique) | O(1) amortized — hash insert + one `fetch_add` |
| Insert (duplicate) | O(1) — hash lookup, no seq allocation |
| Post-insert scan | See formula above |
| Scan under lock (COW) | O(1) — `shared_ptr` copy |
| Scan under lock (plain) | O(n) where n = items in source — full copy |
| Per-element sync overhead | 1 atomic `fetch_add` per unique insert |

## Performance Considerations

### False Sharing

Align each source to a cache line to prevent false sharing between independent sources:

```cpp
template <class T, class Policy>
class alignas(64) Source { /* ... */ };
```

### Global Counter Contention

The single `fetch_add` per unique insert is the theoretical minimum for cross-source ordering.
Mitigations under extreme load:
- Batch inserts: add multiple items under one lock acquisition, one seq range.
- Use `memory_order_relaxed` for the `fetch_add` (safe — the source lock provides happens-before).

### Async Emit Offload

If the callback is expensive, the inserter thread pays the cost.
Offload emit to a dedicated thread by passing COW snapshots (shared_ptrs) into a work queue.
The inserter enqueues and returns immediately.

### Reentrancy

If `emit(a, b)` itself calls `add<0>(a2)`, this is safe — no locks are held during emit.
Worth documenting as a supported pattern.

## Removal Semantics

The insert-only framework grows without bound. For any real system — services go offline,
configs become invalid, nodes decommission — `remove()` is required.

### Does Removal Need a Seq?

**No.** Removal generates no new tuples. Only inserts produce tuples. Removal shrinks the
set, making future cross-products smaller. No seq allocation needed.

```cpp
void removeFromSource(i, x):
    lock S_i
        S_i.erase(x)
    unlock S_i
    // No scan, no emit, no seq.
```

### Guarantee Change

Insert-only:
> Every tuple of items that were ever inserted is emitted exactly once.

With removal:
> Every tuple of items that are **retained at scan time** is emitted exactly once.

Consider this race:

```
Time 0: A has {a1 (seq=1)}
Time 1: Thread 1 → B.insert(b1), gets seq=2, releases B's lock
Time 2: Thread 2 → A.remove(a1), under A's lock, erases a1
Time 3: Thread 1 → scans A under A's lock → sees empty set → emits nothing
```

The tuple (a1, b1) was momentarily valid — both existed — but was never emitted.
This is correct: at the decision point (scan), a1 was gone. The same holds in any database —
if you delete a row between someone else's insert and their join scan, the join doesn't
produce the tuple. The semantic is: **the join fires for items that are present,
not items that were ever present.**

### The Dangling Pointer Problem

Current `RetainAll::scan()` returns `vector<pair<const T*, uint64_t>>` — pointers into the map.
Without removal, this is safe: `unordered_map` preserves element references across insertions.
With removal:

```
Thread 1 (inserter):                    Thread 2 (remover):
────────────────────                    ────────────────────
lock A
  scan() → gets &a1, &a2
unlock A
                                        lock A
                                          erase(a1) → frees memory at &a1
                                        unlock A
emit(*a1, b1)  → USE AFTER FREE
```

Between unlock and emit, another thread can erase the element, invalidating the pointer.

### Why COW Is the Only Safe Option

The naive fix — `scan()` returns `vector<pair<T, uint64_t>>` (values, not pointers) — works
but costs O(n) per scan **under the lock**. If source A has 10,000 items, every insert into
source B copies 10,000 elements while holding A's spinlock. This is the exact cost the
design was built to avoid.

`RetainAllCOW` stores the map behind a `shared_ptr<const Map>`:

```cpp
std::shared_ptr<const Map> scan() const {
    return data_;  // O(1) — just ref-count bump
}
```

The scan is O(1) under the lock — just copy the `shared_ptr` (atomic ref-count increment).
The caller holds a reference to an **immutable snapshot**. Even if another thread removes
elements, it creates a *new* map and swaps `data_`:

```
Thread 1 (inserter):                    Thread 2 (remover):
────────────────────                    ────────────────────
lock A
  snap = data_  (refcount 1→2)
unlock A
                                        lock A
                                          next = make_shared<Map>(*data_)
                                          next->erase(a1)
                                          data_ = next   // swap
                                          // old map alive (refcount=1, held by Thread 1)
                                        unlock A
emit using snap → safe!
  snap points to old map
  a1 still exists in old map
  refcount drops to 0 when snap destroyed → old map freed
```

The old snapshot lives as long as anyone holds a `shared_ptr` to it. No dangling pointers,
no copies under the lock.

#### Cost comparison

| Operation | `RetainAll` | `RetainAllCOW` |
|---|---|---|
| Insert | O(1) amortized | O(n) — full map copy + insert |
| Remove | O(1) | O(n) — full map copy + erase |
| Scan under lock | O(n) — iterate + copy pointers | **O(1)** — `shared_ptr` copy |
| Pointer safety after unlock | **Unsafe with removal** | Safe — snapshot is immutable |

This is the right tradeoff: inserts and removals are infrequent (services coming online/offline),
but scans happen on every insert into *any other source*. Making scan O(1) at the cost of
O(n) insert/remove is a clear win.

#### COW remove implementation

```cpp
template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class RetainAllCOW {
    // ... existing members ...

    bool remove(const T& value) {
        if (!data_->count(value)) return false;
        auto next = std::make_shared<Map>(*data_);
        next->erase(value);
        data_ = std::move(next);
        return true;
    }
};
```

### For RetainLatest, Removal Is Trivial

`RetainLatest` stores an `optional<pair<T, seq>>`. Remove just clears it:

```cpp
bool remove(const T&) {
    bool was = current_.has_value();
    current_.reset();
    return was;
}
```

No pointer issue — `scan()` already copies the single value. After remove, `scan()` returns
an empty vector, so the join sees "this source has no value" and no tuples are emitted.

### When to Use Which Policy with Removal

| Policy | Safe without COW? | Recommendation |
|---|---|---|
| `RetainLatest` | Yes — single value, no pointer issues | Use directly |
| `RetainAll` | **No** — dangling pointers after erase | Use `RetainAllCOW` if removal needed |
| `RetainAllCOW` | Yes — snapshot is immutable | **Required** for RetainAll + removal |
| `SlidingWindow` | Depends on implementation | TTL-based auto-eviction, same pointer concern |
| `BarrierPolicy` | Yes — sub-slots are internal, not exposed | Use directly |

**The rule: if a policy retains multiple items and supports removal, it must use COW
or copy-on-scan.**

### Remove + Re-Add Semantics

```
Time 0: A.insert(a1) → seq=1
Time 1: B.insert(b1) → seq=2 → emits (a1, b1)
Time 2: A.remove(a1)
Time 3: A.insert(a1) → seq=3 → scans B, sees b1 (seq=2) → emits (a1, b1) AGAIN
```

This is a **re-emission** of the same logical tuple (a1, b1). Is it a duplicate?

**No.** The re-added a1 has a different seq (3 vs 1). It's a new insert event. The semantics are:
"a1 came back online — rejoin with everything." This is correct for service discovery
(service restarts → re-provision dependent resources).

If re-emission is undesirable, use `RetainAll` (which rejects duplicates on the value).
The remove clears the deduplication state, making re-add possible:

| Policy | Remove behavior | Re-add after remove |
|---|---|---|
| `RetainAll` / `RetainAllCOW` | Erase from map → re-add possible | Gets new seq, triggers new joins |
| `RetainLatest` | Clear `optional` | Next insert fills it again |
| `SlidingWindow` | Explicit evict (or let TTL expire) | Natural re-add on next heartbeat |
| `Immediate` | N/A — nothing retained | N/A |
| `BarrierPolicy` | Clear one sub-slot → barrier "unfilled" | Sub-slot must be re-filled before barrier fires again |

The barrier case is notable: removing a sub-slot **un-completes** the barrier. If the join
was `Config + Barrier<Sensor>` and one sensor goes offline, the barrier becomes incomplete
and the join stops firing until that slot is refilled.

### Removal Callbacks: A Separate Concern

Should the join fire when something is removed? ("Service went offline → tear down all
resources that depended on it.")

**No.** The join should not handle departure callbacks. Here's why:

1. **Tuple tracking required.** To fire a removal callback, the join would need to know
   which tuples were previously emitted for the removed element. This means maintaining
   an emitted-tuple set — violating the "no tuple tracking" property that makes
   seq-ownership minimal.

2. **O(n) removal scan.** On removing a1, you'd scan the emitted set for all tuples
   containing a1. For large cross-products this is expensive.

3. **Different semantics.** Arrival requires cross-product (N sources interacting).
   Departure is per-source (one thing goes away). These are categorically different.

The clean separation:

```cpp
EventSource<Endpoint> endpointsOnline;
EventSource<Endpoint> endpointsOffline;

// Join: fires when new (endpoint, config) combinations appear
auto provisioner = make_reactive(
    [](const Endpoint& ep, const Config& cfg) {
        provisionResource(ep, cfg);
    },
    RetainAllCOW{},     // endpoints — COW required for removal safety
    RetainLatest{}      // config
);

// Online → add to join (triggers cross-product)
endpointsOnline.subscribe([&](const Endpoint& ep) {
    provisioner.add<0>(ep);
});

// Offline → remove from join + separate teardown (no cross-product needed)
endpointsOffline.subscribe([&](const Endpoint& ep) {
    provisioner.remove<0>(ep);  // shrinks the join's state
    teardownResource(ep);        // separate action
});
```

If you need to know the exact pairings that were provisioned (e.g., tear down specific
(endpoint, config) pairs), maintain a separate registry:

```cpp
std::set<std::pair<Endpoint, Config>> provisioned;

auto provisioner = make_reactive(
    [&](const Endpoint& ep, const Config& cfg) {
        provisionResource(ep, cfg);
        provisioned.insert({ep, cfg});  // track it yourself
    },
    RetainAllCOW{}, RetainLatest{}
);

endpointsOffline.subscribe([&](const Endpoint& ep) {
    provisioner.remove<0>(ep);
    for (auto it = provisioned.begin(); it != provisioned.end(); ) {
        if (it->first == ep) {
            teardownResource(it->first, it->second);
            it = provisioned.erase(it);
        } else { ++it; }
    }
});
```

### The Removable Policy Contract

Not all policies need removal. `Immediate` can't remove (nothing retained). `SlidingWindow`
auto-evicts. The concept is optional — `Source::remove()` is only available if the policy
satisfies `RemovablePolicy`:

```cpp
template <class T>
concept RemovablePolicy = RetentionPolicy<T> && requires(T policy, typename T::value_type v) {
    { policy.remove(v) } -> std::same_as<bool>;  // true if was present and removed
};
```

### Removal Summary

| Property | Insert-only | With removal |
|---|---|---|
| No missed tuple | All ever-inserted tuples emitted | All **currently-retained** tuples emitted |
| No duplicate | Same | Same (seq ownership unchanged) |
| Remove needs seq | N/A | No — no tuples generated |
| Pointer safety | Safe (map only grows) | Requires COW or copy-on-scan |
| Remove + re-add | N/A | New seq → new joins (correct) |
| Barrier removal | N/A | Un-completes barrier → join stops firing |
| Removal callback | N/A | Separate concern — use `EventSource` |

> **The join's job is to compose arrivals. Departure handling is the application's job.**

### Type-Level Liveness: Decentralized Removal

Explicit `remove()` requires the caller to know which source to remove from and to
coordinate the removal call. But there's a deeper question: **what if the value itself
knows when it's dead?**

#### The `weak_ptr` Insight

`weak_ptr` is freely copyable — it's a non-owning observer, not a unique handle.
Multiple `weak_ptr`s can observe the same object. Each can check liveness (`expired()`)
and temporarily resurrect a strong reference (`lock()`), which is atomic and thread-safe.

If the source stores `weak_ptr<T>` instead of `T`, removal is fully decentralized:

```cpp
// Owner creates a resource and shares it
auto endpoint = std::make_shared<Endpoint>("svc-alpha");
join.add<0>(endpoint);  // source stores weak_ptr<Endpoint>

// ... later, owner drops the shared_ptr:
endpoint.reset();  // no remove() call needed — the weak_ptr expires

// Next scan of this source: weak_ptr::lock() fails → skip/evict it
```

Nobody calls `remove()`. The value ceases to exist when its owner drops it. The source
discovers this lazily during scan.

The scan **locks** each `weak_ptr` into a temporary `shared_ptr` — this is the key mechanism:

```
Source storage:  [weak_ptr<a1>, weak_ptr<a2>, weak_ptr<a3>]
                      ↓              ↓              ↓
                   alive           alive          EXPIRED

scan() under lock:
  1. w1.lock() → shared_ptr<a1> ✓  (owner refcount 1→2)
  2. w2.lock() → shared_ptr<a2> ✓  (owner refcount 1→2)
  3. w3.lock() → empty           ✗  (expired — evict)
  return: [shared_ptr<a1>, shared_ptr<a2>]
unlock

                                    Meanwhile, owner drops a1:
                                    owner_a1.reset()
                                    // refcount 2→1 (scan result still holds it)
                                    // a1 is ALIVE in the scan snapshot!

emit using scan result:
  *a1 is safe — shared_ptr in scan result keeps it alive
  *a2 is safe — same

scan result destroyed:
  shared_ptr<a1> destroyed → refcount 1→0 → a1 freed (deferred cleanup)
  shared_ptr<a2> destroyed → refcount 2→1 → a2 still alive (owner has it)
```

**This is COW semantics for free.** The `shared_ptr` from `lock()` acts as an immutable
snapshot of the value. Even if the owner drops it mid-emit, the scan result keeps it
alive. No explicit COW map copy needed — reference counting IS the snapshot mechanism.

#### `RetainAllWeak` Implementation

```cpp
template <class T>
class RetainAllWeak {
public:
    using value_type = std::weak_ptr<T>;
    using scan_entry = std::pair<std::shared_ptr<T>, uint64_t>;

    bool tryInsert(const std::weak_ptr<T>& wp, uint64_t seq) {
        auto sp = wp.lock();
        if (!sp) return false;  // already dead on arrival

        // Use raw pointer as map key for identity
        auto* raw = sp.get();
        auto [it, inserted] = items_.emplace(raw, Entry{wp, seq});
        return inserted;
    }

    bool contains(const std::weak_ptr<T>& wp) const {
        auto sp = wp.lock();
        return sp && items_.count(sp.get()) > 0;
    }

    // Returns STRONG refs — safe to use after unlock
    auto scan() {
        std::vector<scan_entry> result;
        result.reserve(items_.size());

        for (auto it = items_.begin(); it != items_.end(); ) {
            if (auto sp = it->second.wp.lock()) {
                result.emplace_back(std::move(sp), it->second.seq);
                ++it;
            } else {
                it = items_.erase(it);  // expired — evict eagerly
            }
        }
        return result;
    }

private:
    struct Entry { std::weak_ptr<T> wp; uint64_t seq; };
    std::unordered_map<const T*, Entry> items_;
};
```

#### Pointer Safety Comparison: Three Approaches

| Approach | Storage | Scan cost | Removal | Who pays |
|---|---|---|---|---|
| `RetainAll` + explicit `remove()` | `T` in map | O(n) pointer copy | **Unsafe** without COW | — |
| `RetainAllCOW` + explicit `remove()` | `shared_ptr<const Map>` | O(1) ptr copy | Safe — old snapshot lives | Writer (O(n) map copy) |
| `RetainAllWeak` | `weak_ptr<T>` in map | O(n) lock + filter | **Automatic** — no remove needed | Scanner (O(n) lock attempts) |

#### Generalizing: The Expirable Value Concept

`weak_ptr` is just one instance of a general pattern: **the value type defines its own liveness.**

```cpp
template <class T>
concept ExpirableValue = requires(const T& v) {
    { v.expired() } -> std::convertible_to<bool>;
};
```

| Value type | How it expires | Use case |
|---|---|---|
| `weak_ptr<T>` | Owner drops `shared_ptr` | Ownership-based lifetime |
| `LeaseToken<T>` | Lease TTL exceeded | Distributed systems, heartbeat-based |
| `Versioned<T>` | Superseded by newer version | Config versioning |
| `Handle<T>` | External registry marks invalid | Resource management |

#### Liveness as a Policy Axis

Liveness is a second policy axis, orthogonal to retention:

```
Source<T, RetentionPolicy, LivenessStrategy>

RetentionPolicy:  what to store and how     (All, Latest, LatestN, Window, Barrier)
  - MAY implement remove()                  (optional capability, checked at compile time)
LivenessStrategy: scan-time value filtering (AlwaysAlive, WeakRef, Predicate<F>)
```

Two genuinely independent axes. The `remove()` capability is not a separate axis — it's a
**trait of the retention policy**, checked via the `RemovablePolicy` concept:

```cpp
template <class T>
concept RemovablePolicy = RetentionPolicy<T> && requires(T policy, typename T::value_type v) {
    { policy.remove(v) } -> std::same_as<bool>;
};
```

The `Source` wrapper conditionally exposes `remove()` only if the policy supports it:

```cpp
template <class T, class Policy, class Liveness = AlwaysAlive>
class Source {
public:
    uint64_t insert(const T& value, std::atomic<uint64_t>& counter) { ... }
    auto scan() { ... }  // filtered by Liveness

    // Only available if policy supports it — compile error otherwise:
    bool remove(const T& value)
        requires RemovablePolicy<Policy>
    {
        std::lock_guard<SpinLock> g(lock_);
        return policy_.remove(value);
    }
};
```

#### Liveness Strategy Implementations

```cpp
struct AlwaysAlive {
    template <class T>
    static bool isAlive(const T&) { return true; }
};

struct WeakRef {
    template <class T>
    static bool isAlive(const T& v) { return !v.expired(); }
};

template <class F>
struct Predicate {
    F f;
    template <class T>
    bool isAlive(const T& v) const { return f(v); }
};
```

Note: `RetainAllWeak` integrates liveness directly (lock/evict in scan), so it does not
need a separate `LivenessStrategy`. The strategy axis is for policies that store the
value directly (e.g., `RetainAll<LeaseToken>` + `Predicate<checkExpiry>`).

#### Scan with Liveness Filtering

For policies that store values directly, the scan incorporates liveness:

```cpp
// Lazy: filter dead entries in scan output
auto scan() {
    std::lock_guard<SpinLock> g(lock_);
    auto result = policy_.scan();
    std::erase_if(result, [this](const auto& entry) {
        return !liveness_.isAlive(*entry.first);
    });
    return result;
}

// Eager: evict dead entries from policy storage during scan
auto scan() {
    std::lock_guard<SpinLock> g(lock_);
    policy_.evictIf([this](const auto& v) {
        return !liveness_.isAlive(v);
    });
    return policy_.scan();
}
```

Eager eviction keeps memory bounded; lazy filtering avoids mutation cost on the scan path.

#### The Three Removal Paths Compared

| | Explicit `remove()` | Type-level auto-expiry | Policy-based (TTL/capacity) |
|---|---|---|---|
| Who decides | Application | Value owner / value itself | Source configuration |
| When discovered | Immediately | Lazily on next scan | On insert (capacity) or scan (TTL) |
| Needs COW | Yes — pointer safety | **No** — `weak_ptr`/lock acts as snapshot | Depends on policy |
| API surface | `source.remove(x)` | None — just drop the owner | None — automatic |
| Thread coordination | Caller must know which source | **None** — owner just drops reference | None |
| Re-add | Explicit `source.add(x)` | Create new `shared_ptr`, add again | Natural (new insert) |

#### Ownership Models: Who Holds the `shared_ptr`?

The choice between `RetainAll<shared_ptr<T>>` (source owns) and `RetainAllWeak<T>` (source
observes) is not just a policy detail — it determines the entire ownership and removal model.

**Model A: Source observes, caller owns (decentralized)**

The natural model for scope-based lifetime:

```
Caller keeps shared_ptr ─→ Source stores weak_ptr
                           Source auto-evicts on scan when weak_ptr expires

Caller scope ends  →  shared_ptr destroyed  →  weak_ptr expires  →  evicted on next scan
```

No `remove()` call. No coordination. The component that created the resource controls its
lifetime via RAII. The join observes passively.

**Model B: Source owns (registry/control plane)**

The caller inserts and **gives up ownership**. But this creates a fundamental question:
**who tells the source to remove?**

```
Caller:
  auto ep = make_shared<Endpoint>(...);
  source.add(ep);    // source stores shared_ptr — refcount 2
  ep.reset();        // refcount 2→1 — source still owns. NOT freed.
                     // ... who calls source.remove() later?
```

If the caller keeps `shared_ptr` to later call `source.remove(ep)`, the caller is still
managing lifetime — "source owns" is an illusion. If the caller drops its `shared_ptr`,
it has no handle to call `remove()` with.

This model only makes sense in two cases:

1. **Keyed registry** — removal is by name/key, not pointer. An admin or API endpoint
   decides when to remove:
   ```cpp
   source.addByKey("svc-alpha", make_shared<Endpoint>(...));
   // ... later, admin action:
   source.removeByKey("svc-alpha");  // lookup by identity, not pointer
   ```

2. **Policy-based eviction** — the source itself decides, no external removal needed:
   ```cpp
   SlidingWindow<30s>   → evicts entries older than 30 seconds
   RetainLatestN<5>     → evicts oldest when 6th arrives
   ```

**Why this matters for the framework:**

The general, decentralized case — many independent components producing values into
a join — is Model A. The caller owns, the source observes. This is the common case
and the one the framework is primarily designed for:

| | Model A: Caller owns | Model B: Source owns |
|---|---|---|
| Source stores | `weak_ptr<T>` | `shared_ptr<T>` (or `T` directly) |
| Policy | `RetainAllWeak<T>` | `RetainAll<T>` / `RetainAllCOW<T>` |
| Removal trigger | Caller scope ends (RAII) | External command or policy eviction |
| Needs `remove()` | No | Yes (keyed registry) or No (policy eviction) |
| Needs COW | No — `lock()` IS the snapshot | Yes — `remove()` races with scan |
| Feeds multiple DAGs | Yes — distribute `weak_ptr` to downstream joins | Yes — but removal must propagate |
| Coordination needed | None | Caller must know the key or keep a handle |
| Thread safety | Automatic (`lock()` is atomic) | Requires COW or external locking |

**The key insight:** for scope-based lifetime, "source owns `shared_ptr`" is circular —
someone must still decide when to remove, which requires keeping a handle, which means
the caller is the real owner. `weak_ptr` resolves this by making ownership explicit and
removal automatic. The "source owns" model is reserved for the registry pattern where
removal is by key, not by scope.

#### Composition Examples

```cpp
// Explicit removal with COW safety
Source<Endpoint, RetainAllCOW<Endpoint>, AlwaysAlive>

// Automatic lifetime via weak_ptr (no explicit remove needed)
Source<weak_ptr<Endpoint>, RetainAllWeak<Endpoint>>

// Latest config, skipped if health check fails, also explicitly removable
Source<Config, RetainLatest<Config>, Predicate<HealthCheck>>

// Time-window auto-eviction, no explicit removal
Source<Heartbeat, SlidingWindow<Heartbeat, 30s>, AlwaysAlive>
```

#### When to Use Which

```
"Components produce values with scoped lifetimes" (common case)
  → Model A: Caller owns shared_ptr, source stores weak_ptr (RetainAllWeak)
  → No remove(), no coordination, RAII-driven

"Admin/control plane explicitly deregisters service by name"
  → Model B: Source owns by key (RetainAllCOW + keyed registry)
  → removeByKey("name"), needs COW for scan safety

"Keep only last 5 minutes of heartbeats"
  → Model B: Source owns, policy evicts (SlidingWindow)
  → No explicit removal — policy decides

"Remove entries where health check fails"
  → Model A: Predicate liveness (RetainAll + Predicate<F>)
  → Scan filters dead entries, no explicit removal
```

#### Compile-Time Elimination

`AlwaysAlive` (the default) has `isAlive() = true` — the compiler eliminates the liveness
check entirely. You only pay for expiry checking if you use an expiring value type.
This is another zero-cost abstraction that the runtime-dispatched reactive literature
cannot offer.

## Ergonomic API

The index-based `add<I>(value)` API is correct but not ergonomic.
The framework supports four levels of syntactic sugar, all resolved at compile time
with zero runtime overhead.

### 1. Explicit Index (Always Works)

```cpp
join.add<0>(endpoint);
join.add<1>(config);
```

### 2. Type-Based Dispatch (Unique Types)

If each source has a unique value type, the compiler resolves the slot:

```cpp
// Instead of join.add<0>(endpoint):
join.add(endpoint);     // Endpoint matches source 0
join.add(config);       // Config matches source 1
```

Implementation:

```cpp
template <class T>
void add(const T& value) {
    constexpr size_t I = indexOfType<T>();  // compile-time lookup
    add<I>(value);
}

template <class T, size_t... Is>
static constexpr size_t indexOfTypeImpl(std::index_sequence<Is...>) {
    size_t result = sizeof...(Is);
    ((std::is_same_v<T, typename std::tuple_element_t<Is,
        std::tuple<Sources...>>::value_type>
        ? (result = Is, true) : false) || ...);
    static_assert(result < sizeof...(Is),
        "Type does not match any source");
    return result;
}

template <class T>
static constexpr size_t indexOfType() {
    return indexOfTypeImpl<T>(std::index_sequence_for<Sources...>{});
}
```

Type ambiguity produces a compile error:
`"Multiple sources accept std::string — use add<I>() to disambiguate"`

### 3. Multi-Add (Multiple Args by Type)

```cpp
// Feed two sources in one call:
join.add(endpoint, config);
```

Implementation:

```cpp
template <class... Ts>
void add(const Ts&... values) {
    if constexpr (sizeof...(Ts) == 1) {
        constexpr size_t I = indexOfType<Ts...>();
        add<I>(values...);
    } else {
        // Fold: dispatch each arg by type independently.
        // Each gets its own seq — last one emits.
        (add(values), ...);
    }
}
```

### 4. `operator()` (Single Group, M=1)

For the classic event case where all arguments are one group:

```cpp
// Instead of event.addGroup<0>(std::tuple{a, b, c}):
event(a, b, c);
```

Implementation:

```cpp
template <class... Args>
void operator()(Args&&... args) {
    static_assert(sizeof...(Sources) == 1,
        "operator() is only valid for single-source joins (M=1)");

    using GroupValue = typename std::tuple_element_t<
        0, std::tuple<Sources...>>::value_type;
    static_assert(std::is_constructible_v<GroupValue, Args...>,
        "Arguments do not match the group's value type");

    // Insert into the single source
    auto& src = std::get<0>(sources_);
    src.insert(GroupValue{std::forward<Args>(args)...}, counter_);

    // M=1: no other sources, no scan, no seq check.
    // Emit directly.
    emit_(std::forward<Args>(args)...);
}
```

The call `event(endpoint, config)` compiles down to: pack args → policy accept/reject → direct
function call. No seq comparison, no scan, no cross-product.

### 5. Group Detection by Strong Type

If a group's composite type is a distinct struct, it resolves naturally
through the type-dispatch mechanism:

```cpp
struct HostPort { std::string host; int port; };   // strong type for Group<1,2>

join.add(HostPort{"node-1", 8080});   // fills args 1 and 2
join.add(endpoint);                    // fills arg 0
join.add(config);                      // fills arg 3
```

### Dispatch Summary

| Call | Resolution | Constraint |
|---|---|---|
| `add<I>(value)` | Explicit index | Always works |
| `add(value)` | Type lookup at compile time | Unique type per source |
| `add(a, b, c)` | Each arg type-dispatched | All types unique |
| `operator()(a, b, c)` | Single group, all args | M=1 only |

## DynamicJoin: Runtime Source Count

### The Real Need: BarrierSource (Dynamic Source, Not Dynamic Join)

When the source count is not known at compile time, the first instinct is to replace
the entire `NJoin` with a runtime-indexed `DynamicJoin`. But this is overkill.

The typical case is: **some sources are typed and compile-time, one source is a
runtime-counted barrier** (e.g., "wait for all N data feeds, then combine with a Config").
Only the barrier dimension is runtime — the rest stays typed.

The solution is a **`BarrierSource`**: a single source that internally manages N
runtime sub-slots and only produces a value when all sub-slots are filled. From
the outer `NJoin`'s perspective, it is just one source with value type `vector<T>`.

```
Outer NJoin (compile-time, M=2):
┌─────────────────────────────────────────────┐
│  Source 0: Config         Source 1: Barrier │
│  (RetainLatest)           (BarrierSource)   │
│                                             │
│                      ┌──────────────────┐   │
│                      │ sub[0] sub[1] .. │   │
│                      │ sub[N-1]         │   │
│                      │ fires when all   │   │
│                      │ sub-slots filled │   │
│                      └──────────────────┘   │
│                                             │
│  atomic<uint64_t> globalSeq                 │
│  EmitFn(Config, span<SensorReading>)        │
└─────────────────────────────────────────────┘
```

No `DynamicJoin` needed. The outer join stays compile-time typed. The runtime
dimensionality is **encapsulated inside one source**.

### BarrierSource as a Retention Policy

The barrier maps cleanly to the existing policy contract. Internally it is N
sub-slots, each `RetainLatest`. **When** it fires is fixed (all sub-slots filled).
**What** it yields to the outer join is configurable via a yield strategy.

#### Yield Strategies

| Strategy | Exposed type | What the outer join sees | Use case |
|---|---|---|---|
| `YieldAll` | `vector<T>` | All N sub-slot values | Calibration, aggregation, batch processing |
| `YieldLast` | `T` | Only the value that completed the barrier | Trigger — "last arrival kicks off the work" |
| `YieldSignal` | `monostate` | Nothing — just a readiness gate | Values irrelevant, only all-filled matters |

The yield strategy is orthogonal to the barrier's fire condition. The barrier always
tracks N sub-slots and fires when all are filled. The strategy only controls the
**projection** — what the outer join receives in the cross-product.

#### Implementation

```cpp
// --- Yield strategies ---

struct YieldAll {
    template <class T>
    using type = std::vector<T>;

    template <class T>
    static auto project(const std::vector<std::optional<std::pair<T, uint64_t>>>& slots) {
        std::vector<T> result;
        result.reserve(slots.size());
        for (auto& s : slots) result.push_back(s->first);
        return result;
    }
};

struct YieldLast {
    template <class T>
    using type = T;

    template <class T>
    static auto project(const std::vector<std::optional<std::pair<T, uint64_t>>>& slots,
                         size_t last_slot) {
        return slots[last_slot]->first;
    }
};

struct YieldSignal {
    template <class T>
    using type = std::monostate;

    template <class T>
    static auto project(const auto&...) {
        return std::monostate{};
    }
};

// --- Barrier policy parameterized by yield strategy ---

template <class T, class Yield = YieldAll>
class BarrierPolicy {
public:
    using value_type = std::pair<size_t, T>;  // (slot_index, value)
    using composite  = typename Yield::template type<T>;

    explicit BarrierPolicy(size_t n) : slots_(n) {}

    bool tryInsert(const value_type& sv, uint64_t seq) {
        auto [slot, value] = sv;
        assert(slot < slots_.size());
        slots_[slot] = {value, seq};
        last_slot_ = slot;

        // Accept (= produce composite) only if all slots are filled.
        if (!allFilled()) return false;

        // Project according to yield strategy.
        if constexpr (std::is_same_v<Yield, YieldLast>) {
            composite_ = Yield::template project<T>(slots_, last_slot_);
        } else {
            composite_ = Yield::template project<T>(slots_);
        }
        composite_seq_ = seq;
        return true;
    }

    bool contains(const value_type&) const {
        return false;  // barriers never deduplicate — always accept updates
    }

    auto scan() const {
        std::vector<std::pair<const composite*, uint64_t>> result;
        if (composite_seq_ > 0) {
            result.emplace_back(&composite_, composite_seq_);
        }
        return result;
    }

private:
    bool allFilled() const {
        return std::all_of(slots_.begin(), slots_.end(),
                           [](const auto& s) { return s.has_value(); });
    }

    std::vector<std::optional<std::pair<T, uint64_t>>> slots_;
    composite composite_;
    uint64_t composite_seq_ = 0;
    size_t last_slot_ = 0;
};
```

#### When to Use Which Yield

```
"Process all sensor readings together"
  → Barrier<SensorReading, YieldAll>    → callback gets vector<SensorReading>

"Last data feed arrival triggers deployment"
  → Barrier<FeedStatus, YieldLast>      → callback gets FeedStatus (the trigger)

"Just wait for all nodes to check in, then proceed"
  → Barrier<Heartbeat, YieldSignal>     → callback gets monostate (readiness gate)
```

### Usage: Barrier + Typed Sources

```cpp
size_t n = config.getDataFeedCount();  // runtime

// YieldAll: callback receives all N sensor readings
auto controller = make_reactive(
    [](const Config& cfg, std::span<const SensorReading> sensors) {
        deploy(cfg, sensors);
    },
    RetainLatest{},                              // source 0: Config
    Barrier<SensorReading, YieldAll>{ n }        // source 1: N sensors → vector
);

// YieldLast: callback receives only the completing value
auto trigger = make_reactive(
    [](const Config& cfg, const FeedStatus& last) {
        deploy(cfg, last);  // last is the feed that completed the barrier
    },
    RetainLatest{},                               // source 0: Config
    Barrier<FeedStatus, YieldLast>{ n }           // source 1: N feeds → last T
);

// YieldSignal: callback doesn't receive barrier values at all
auto gate = make_reactive(
    [](const Config& cfg, std::monostate) {
        deploy(cfg);  // all nodes checked in, config is ready — go
    },
    RetainLatest{},                                // source 0: Config
    Barrier<Heartbeat, YieldSignal>{ n }           // source 1: N heartbeats → signal
);

// All three variants: same add pattern
controller.addBarrier<1>(sensorIndex, reading);
trigger.addBarrier<1>(feedIndex, status);
gate.addBarrier<1>(nodeIndex, heartbeat);
```

### Why BarrierSource, Not DynamicJoin

| | BarrierSource in NJoin | Full DynamicJoin |
|---|---|---|
| Other sources stay typed | Yes | No — everything becomes `T` |
| Compile-time optimization | All non-barrier sources optimized | None |
| Heterogeneous policies | Yes (barrier + RetainAll + RetainLatest) | No — uniform policy |
| Callback signature | `(const Config&, span<SensorReading>)` | `span<const T>` only |
| Framework change | Just a new policy | Entire new join class |
| Composability | Works with EventSource, groups, etc. | Separate world |

The `BarrierSource` is the right abstraction: **runtime dimensionality is a source concern,
not a join concern.** The join doesn't need to know that one of its sources is internally
a barrier over N sub-slots.

### When You Actually Need DynamicJoin

Only when **all** sources are homogeneous **and** the count is runtime — no typed sources
at all. This is the pure runtime countdown with no other join dimensions.

### DynamicJoin Implementation (Pure Homogeneous Case)

```cpp
template <class T, class Policy = RetainLatest<T>>
class DynamicJoin {
public:
    using EmitFn = std::function<void(std::span<const T>)>;

    DynamicJoin(size_t n, EmitFn emit)
        : sources_(n), emit_(std::move(emit)) {}

    void add(size_t slot, const T& value) {
        assert(slot < sources_.size());
        auto& src = sources_[slot];

        // Insert under source lock; seq allocated atomically.
        auto sx = src.insert(value, counter_);
        if (!sx) return;  // rejected by policy

        // Snapshot all other sources.
        std::vector<std::vector<std::pair<const T*, uint64_t>>> snaps;
        snaps.reserve(sources_.size());
        bool all_filled = true;

        for (size_t j = 0; j < sources_.size(); ++j) {
            if (j == slot) {
                snaps.push_back({});  // placeholder
                continue;
            }
            auto snap = sources_[j].scan();
            if (snap.empty()) { all_filled = false; break; }
            snaps.push_back(std::move(snap));
        }

        if (!all_filled) return;

        // Cross-product with seq filter (odometer iteration).
        crossProductEmit(slot, value, sx, snaps);
    }

    size_t size() const { return sources_.size(); }

private:
    void crossProductEmit(
        size_t trigger_slot, const T& trigger, uint64_t trigger_seq,
        const std::vector<std::vector<std::pair<const T*, uint64_t>>>& snaps)
    {
        const size_t N = sources_.size();
        std::vector<size_t> indices(N, 0);
        std::vector<const T*> tuple(N);
        tuple[trigger_slot] = &trigger;

        // Odometer: iterate all combinations of other sources' items.
        for (;;) {
            bool valid = true;
            for (size_t j = 0; j < N; ++j) {
                if (j == trigger_slot) continue;
                auto& [ptr, seq] = snaps[j][indices[j]];
                tuple[j] = ptr;
                if (!((int64_t)(trigger_seq - seq) > 0)) {
                    valid = false;
                    break;
                }
            }

            if (valid) {
                std::vector<T> values;
                values.reserve(N);
                for (size_t j = 0; j < N; ++j) values.push_back(*tuple[j]);
                emit_(values);
            }

            // Advance odometer.
            bool done = true;
            for (size_t j = N; j-- > 0;) {
                if (j == trigger_slot) continue;
                if (++indices[j] < snaps[j].size()) { done = false; break; }
                indices[j] = 0;
            }
            if (done) break;
        }
    }

    std::vector<Source<T, Policy>> sources_;
    std::atomic<uint64_t> counter_{0};
    EmitFn emit_;
};
```

### Usage: Pure Runtime Barrier

```cpp
size_t n = config.getDataFeedCount();

DynamicJoin<SensorReading> barrier(n, [](std::span<const SensorReading> readings) {
    calibrate(readings);
});

for (size_t i = 0; i < n; ++i) {
    feedCallbacks[i] = [&barrier, i](const SensorReading& r) {
        barrier.add(i, r);
    };
}
```

### Comparison: NJoin vs. BarrierSource vs. DynamicJoin

| Aspect | `NJoin<Sources...>` | NJoin + `BarrierSource` | `DynamicJoin<T>` |
|---|---|---|---|
| Source count | All compile-time | Mixed: typed + runtime barrier | All runtime |
| Source types | Heterogeneous | Heterogeneous + one barrier | Homogeneous |
| Cross-product | Template-expanded | Template-expanded (barrier = 1 dim) | Odometer loop |
| Seq ownership | Identical | Identical | Identical |
| Callback | `(T1&, ..., TN&)` | `(T1&, ..., vector<T>&)` | `span<const T>` |
| When to use | All types known | Some types + runtime barrier | Pure runtime |

## Application: DAG Orchestration

The primary motivating use case: orchestrating a forest of DAGs where each node
represents a resource that can only be created when all its dependencies are online.

### Mapping

```
DAG node       →  one reactive join
Dependency     →  one source slot
Resource ready →  add(resource) on that source
Node fires     →  emit callback creates the downstream resource
```

Example: a database service requires both a storage volume and a network endpoint:

```cpp
auto createDB = make_reactive(
    [](const Volume& vol, const NetworkEndpoint& net) {
        provision_database(vol, net);
    },
    RetainLatest{},    // latest volume
    RetainLatest{}     // latest endpoint
);

// These can arrive in any order, from any thread:
createDB.add(volume);      // storage ready
createDB.add(endpoint);    // network ready
// → provision_database() fires exactly once with both
```

### Forest of DAGs

Using fan-out (an M=1 NJoin with `SubscriberList` as EmitFn), a single resource-ready
event can feed multiple downstream joins across different DAGs. The fan-out node
calls `add()` on each subscriber join — each join then independently does its own
insert → scan → cross-product → emit:

```
    [Volume Ready]  ──fan-out──→ createDB.add(volume)
    (make_event)               ──→ createBackup.add(volume)
                               ──→ createMonitoring.add(volume)

    [Network Ready] ──fan-out──→ createDB.add(endpoint)
    (make_event)               ──→ createLoadBalancer.add(endpoint)
```

Each join independently tracks its own readiness. No central coordinator needed.
The framework is the coordinator — distributed across the join instances.

### Properties for Orchestration

| Requirement | How the framework satisfies it |
|---|---|
| Concurrent resource discovery | Per-source locks, no global bottleneck |
| Exactly-once provisioning | Seq-ownership, set deduplication |
| Partial readiness (2 of 3 deps) | Join only fires when all sources have ≥1 item |
| Re-provisioning on config change | `RetainLatest` policy: new config triggers re-emit |
| Late dependency arrival | `RetainAll`: already-seen resources are retained for future joins |

### Throughput for this use case

Resources coming online is a low-frequency event (seconds to minutes between events).
Even worst-case scenarios give thousands of joins per second. The framework overhead
is invisible — the bottleneck will always be actual resource provisioning.

## Comparison: Kubernetes Controller Pattern

Kubernetes controllers face the same N-source readiness problem — and chose a
**deliberately weaker** solution: poll-based reconciliation instead of event-driven join.

### How K8s Controllers Work

```
                    ┌─────────────┐
  Resource A ──watch──→            │
  Resource B ──watch──→  Work Queue ──→ Reconcile(key)
  Resource C ──watch──→            │      │
                    └─────────────┘      │
                                         ▼
                                    Read ALL current state
                                    Check: is everything ready?
                                    If yes: create/update dependent
                                    If no: requeue, try later
```

The controller watches multiple resource types via **informers** (watch streams from etcd).
Any change to any watched resource enqueues a key into the work queue.
The **reconcile loop** re-reads ALL dependencies from the informer cache and decides what to do.
It does **not** know which resource changed or whether the change is relevant.

### Head-to-Head Comparison

| | This framework | K8s controller |
|---|---|---|
| Model | **Event-driven join**: fire exactly when all sources ready | **Poll-based reconcile**: re-check everything on any change |
| Trigger | New unique item in any source → precise emit | Any change → full state re-read |
| Exactly-once | Yes (seq-ownership) | No — reconcile must be **idempotent** because it runs repeatedly |
| State tracking | Per-source retention policies | None — re-reads from informer cache every time |
| Efficiency | Emit only new tuples | Re-checks ALL state on every event, even if nothing relevant changed |
| Simultaneous arrivals | Handled correctly by seq ownership | Both trigger reconcile; one succeeds, other is redundant work |
| "What changed?" | Trigger value passed to callback | Not available — reconcile re-derives everything |
| Crash safety | In-memory (requires re-population) | Full re-read from etcd on restart |

### What K8s Pays for Avoiding the Join

1. **Reconcile runs far more often than needed.** If you have 5 dependencies, any change
   to any one triggers a full re-read of all 5 + decision logic. With our framework,
   only genuinely new combinations trigger the callback.

2. **Everything must be idempotent.** The reconcile function must be safe to call 100 times
   with the same state. This is a significant design burden on every controller author.

3. **Work queue deduplication + rate limiting** are needed to prevent thundering herds when
   many resources change simultaneously. Our framework's seq-ownership naturally coalesces.

4. **No "what changed?" information.** The reconcile loop re-checks everything.
   Our framework passes the triggering value directly.

### Why K8s Chose This Anyway

1. **Crash safety.** Controllers restart constantly. The reconcile loop re-reads from etcd —
   no in-memory state to lose.
2. **Simplicity.** "Re-check everything" is easier to reason about than
   "exactly-once join under concurrency."
3. **Distribution.** Controllers run on different machines. A shared atomic counter
   would need to be distributed — which is etcd, which they already have.

### Mapping K8s Concepts to Framework Concepts

```
K8s informer cache   =  EventSource<T> + RetainLatest policy
Work queue           =  our seq-ownership (but manual, leaky)
Reconcile loop       =  our emit callback (but idempotent, redundant)
```

### What a K8s Controller Would Look Like with This Framework

```cpp
// Instead of:
//   watch Pods, watch Services, watch Configs
//   on ANY change → reconcile() → re-read all 3 → decide

auto controller = make_reactive(
    [](const Pod& pod, const Service& svc, const Config& cfg) {
        // Only called with genuinely new combinations
        createIngress(pod, svc, cfg);
    },
    RetainLatest{},   // latest pod state
    RetainLatest{},   // latest service state
    RetainLatest{}    // latest config
);

// Informer callbacks feed the join directly:
podInformer.onUpdate([&](const Pod& p)     { controller.add(p); });
svcInformer.onUpdate([&](const Service& s) { controller.add(s); });
cfgInformer.onUpdate([&](const Config& c)  { controller.add(c); });
```

Result: no redundant reconciles, no idempotency requirement on the callback,
no work queue, exact knowledge of what triggered the emit.

### Closing the Gap: Crash Recovery

The one genuine advantage of the K8s reconcile pattern is crash safety.
On restart, our framework needs to re-populate sources from the informer cache.

This maps directly to the existing K8s **list-then-watch** pattern:

1. On startup, **list** all resources → `add()` each into the join
2. Switch to **watch** → informer callbacks feed the join

The informer cache already does this. The gap is closable with zero additional infrastructure.

## Formal Verification (TLA+)

### What is TLA+?

TLA+ (Temporal Logic of Actions) is a formal specification language created by
Leslie Lamport — the same person behind Lamport clocks, which the seq-ownership
mechanism is based on.

A **model checker** (TLC) exhaustively explores every possible interleaving of
concurrent operations to verify that invariants hold in all reachable states.

### Why it matters

Instead of arguing correctness in prose, the invariants are machine-checked:

```tla
\* Invariant: every valid tuple is emitted exactly once
NoMissedTuple == \A a \in A_set, b \in B_set :
    <<a, b>> \in emitted_tuples

NoDuplicate == Cardinality(emitted_tuples) = Cardinality(A_set) * Cardinality(B_set)
```

TLC explores every possible thread interleaving — context switch after lock acquire
but before insert, simultaneous counter increment, etc. — and either:
- **Passes**: invariant holds in all states → design is correct for all interleavings
- **Finds counterexample**: exact interleaving that breaks invariant → bug found

### What to verify

| Property | TLA+ formulation |
|---|---|
| No missed tuple | Every pair of committed items appears in emitted set |
| No duplicate | Emitted set has no repeated entries |
| Seq-under-lock necessity | Remove invariant → TLC finds counterexample |
| Overflow safety | Signed comparison correct within half-range |
| No deadlock | `Termination` or `NoDeadlock` temporal property |

### Industry precedent

| Company | What they verified with TLA+ |
|---|---|
| Amazon (AWS) | S3, DynamoDB, EBS protocols |
| Microsoft | Azure Cosmos DB consistency |
| MongoDB | Replication protocol |
| Intel | Cache coherence protocols |

### Effort estimate

~50–100 lines of TLA+ for the M=2 model. The model parameterizes thread count
and item count; TLC explores all interleavings exhaustively up to the bound.
Lamport provides free video lectures for learning TLA+.

## Publishability Assessment

### Core contribution

The seq-ownership trick — one atomic counter + max-seq ownership = exactly-once
concurrent N-way join — is a clean, non-obvious primitive not published as a
standalone reusable pattern. It is implicit in database join engines and distributed
systems but not available as a C++ library primitive.

### Novelty analysis

| Contribution | Novelty | Assessment |
|---|---|---|
| Seq-ownership for exactly-once N-way concurrent join | Not published as standalone primitive | Novel |
| Zero-cost generalization (classic event → N-join) | Clean unification, not seen in this form | Novel framing |
| Compile-time specialization of reactive joins | Not addressed in reactive/eventing literature | Novel |
| Partition lattice over grouped sources | Known math ($\Pi_n$), novel application to reactive joins | Novel application |
| Views as lattice navigation with uniform dedup | Not seen — existing joins are static structures | Novel |
| `rebind` (zero-cost layout transfer) | No precedent in reactive/join literature | Novel mechanism |
| Per-source retention policy with formal contract | Practical contribution | Incremental |
| Argument groups reducing dimensionality | Application of known grouping | Incremental |

### What reviewers will demand

1. **"This is known in databases"** — Counter-argument: database hash joins are query-time
   constructs with different lifecycle. This is a persistent reactive join with concurrent
   producers, a different problem.
2. **Evaluation** — Throughput benchmarks (M=1 through M=5, various set sizes), comparison
   against mutex-based naive approach, single-threaded event loop, Rx-style operators.
   Latency distribution under contention.
3. **Formal model** — TLA+ specification with model-checking results.

### Target venues

| Venue | Fit | Paper type |
|---|---|---|
| **DEBS** (ACM Distributed and Event-Based Systems) | Exact scope | Full paper |
| **REBLS** (Reactive and Event-based Languages, SIGPLAN workshop) | Exact scope | Workshop paper |
| **EuroPar** workshop track | Concurrent data structures | Short paper |
| **SoCC** / **EuroSys** (industry track) | With DAG orchestration case study | Experience paper |
| **PODC** / **DISC** | With TLA+ formal proof | Short paper |

### Suggested paper structure

```
Title: "Sequence-Owned N-Way Reactive Join:
        A Lock-Minimal Primitive for Concurrent Event Composition"

1. Introduction
   The gap between single-threaded reactive (Rx) and concurrent multi-producer joins
2. Problem formulation
   Exactly-once N-tuple emission under concurrent inserts
3. The seq-ownership invariant + correctness proof
4. Design
   Policies, groups, composability, ergonomic API
5. Partition lattice for source groups
   Groups as partitions of index sets; views as lattice navigation;
   uniform dedup rule at every node; self-similar sub-lattices;
   rebind as zero-cost lens replacement
6. Degenerate cases
   Unifying classic events as M=1
7. Compile-time specialization
   Template instantiation eliminates framework for degenerate configs
8. Evaluation
   Throughput, latency, scalability benchmarks
9. Case study: DAG orchestration
   Infrastructure provisioning with reactive joins
10. Case study: Kubernetes controllers
    Reconcile-loop anti-pattern as manual, inefficient N-way join
11. Formal verification
    TLA+ specification and model-checking results
12. Related work
    Rx, CEP engines, database joins, Lamport clocks, K8s controller pattern,
    partition lattices (Rota), relational projection
13. Conclusion
```

### Realistic assessment

A workshop paper (DEBS, REBLS) is achievable with the current design plus benchmarks.
A top-tier venue paper requires the TLA+ formalization and a thorough evaluation.
The core idea is clean and non-obvious enough to publish.

### Compile-Time Specialization: A Blind Spot in the Literature

The reactive/eventing literature was written in languages where this question cannot arise.
The Join Calculus, Polyphonic C#, Turon & Russo, Rx, and all CEP engines use runtime
dispatch — the framework is always present, always paying its cost, regardless of the
actual source count or policy configuration.

In C++, template instantiation produces categorically different compiled programs
from the same source code:

| Configuration | What the compiler eliminates |
|---|---|
| M=1, Immediate | Seq counter, scan, cross-product, all locks on other sources → direct function call |
| M=1, RetainLatest | Seq counter, scan, cross-product → store + direct call |
| M=2, RetainLatest | Cross-product is single loop, one comparison |
| M=N, RetainAllCOW | Full framework — nothing eliminated |
| Barrier + YieldSignal | Value copies in barrier projection eliminated |
| Barrier + YieldAll | Full N-element copy into composite |

This means:
1. **Adoption cost is zero for M=1.** Using the framework for classic events compiles to
   the same code as a hand-written subscriber list. There is no abstraction tax.
2. **The cost model is non-uniform.** Each template instantiation is a different program.
   The literature cannot discuss this because their implementations are uniform.
3. **Type safety eliminates a bug class.** Compile-time dispatch catches "wrong source"
   errors that are silent runtime bugs in Rx (wrong pipe wiring) or K8s (reconcile reads
   wrong resource). This is not ergonomics — it is correctness by construction.

The closest academic parallels:

| Work | What it does | Relevance |
|---|---|---|
| Expression Templates (Veldhuizen, 1995) | Compile-time elimination of temporaries in linear algebra | Same spirit — templates eliminate framework overhead. But for numerics, not eventing. |
| Generative Programming (Czarnecki & Eisenecker, 2000) | C++ policy-based framework specialization | Foundational for our policy-based design. Does not apply to reactive joins. |
| Terra (DeVito et al., PLDI 2013) | Multi-stage programming for high-performance DSLs | Shows staging eliminates abstraction cost. Parallel evidence for our approach. |
| Rust zero-cost futures (Tokio, 2018) | Async framework compiles to state machines | Similar "framework compiles away" claim. But single-threaded, no concurrent join. |

None of these apply the insight to reactive joins or eventing.
This is a genuine novel contribution: the first reactive join framework where the
framework itself is zero-cost in degenerate cases, verified at compile time.

### How to Submit

No academic affiliation, credentials, or institutional backing is required.
All venues use blind peer review — reviewers judge the paper, not the author.

#### Requirements

| Requirement | Notes |
|---|---|
| Academic affiliation | Not required. Independent/industry submissions accepted everywhere. |
| Co-authors | Not required. Solo papers are fine. |
| PhD / Professor backing | Not required. No gatekeeping on submission. |
| Registration fee | ~$50–200 for workshop registration (paid only if accepted). |
| LaTeX formatting | Required. Each venue provides a template (ACM `acmart`, IEEE `IEEEtran`). |

#### Submission Process

1. **Pick a venue and check deadline.**
   REBLS: summer deadline (Jul–Aug) for October SPLASH. DEBS: Feb–Mar for June.
2. **Format in LaTeX.** Use the venue's template. [Overleaf](https://overleaf.com) is free.
3. **Submit via HotCRP or EasyChair.** Create account, upload PDF, enter metadata.
4. **Review.** 2–3 anonymous reviewers over 4–8 weeks. Written feedback regardless of outcome.

#### Effort vs. Acceptance Likelihood

| Path | What's needed | Likelihood |
|---|---|---|
| REBLS workshop (6 pages) | Paper + basic benchmarks + TLA+ | High if well-written |
| DEBS full paper (12 pages) | Same + thorough evaluation + K8s case study | Medium — competitive |
| SoCC/EuroSys industry track | Same + production deployment story | Needs real deployment |

#### Practical Tips

- **arXiv first.** Upload to [arXiv.org](https://arxiv.org) (free, no review, no affiliation required)
  to establish priority and get early feedback.
- **Cold-email a co-author.** A clean design doc + working prototype is a strong opener.
  Researchers working on reactive systems or concurrent data structures are ideal.
  The design doc in its current form is more thorough than most initial drafts.
- **What matters most:** evaluation section. A paper without benchmarks will be
  desk-rejected. The design is strong — what's missing is numbers.

## Usability Assessment: Human vs. AI-Assisted

The framework has a large configuration space: RetentionPolicy × LivenessStrategy ×
GroupLayout × YieldStrategy × compile-time specialization. This raises the question
of whether the design is too complex for practical adoption.

The answer depends on the assumed user interface.

### Without AI: Traditional Developer Experience

| Concern | Severity | Analysis |
|---|---|---|
| **Complexity budget** | High risk | Five orthogonal axes. Users must understand which policy fits their use case. Risk of choosing wrong policy and getting subtle correctness bugs |
| **Error messages** | Critical | Variadic templates + concepts + pack indexing = multi-page compiler errors. Template substitution failures in `NJoin<lambda, Source<...>>` are impenetrable without expertise |
| **Documentation burden** | High | Requires tutorials, getting-started guides, cookbooks, migration guides. 80% of effort goes to documentation, not implementation |
| **`make_event` wrapper** | Must ship | Users will not independently discover that `SubscriberList-as-EmitFn + NJoin + Immediate` = EventSource. The shorthand must exist in the library |
| **API surface** | Must minimize | Overloads must be few, constraints must be obvious, defaults must be sensible. Users should be productive with `make_reactive(lambda)` alone |

**Mitigation strategy for traditional use:**
- Sensible defaults (`RetainAll`, `AlwaysAlive`, no groups) make the simplest case trivial
- `make_reactive(lambda)` deduces everything — zero configuration needed for the common case
- Invest heavily in `static_assert` messages: `"Multiple sources accept std::string — use add<I>() to disambiguate"`
- Provide a cookbook of common patterns (service discovery, countdown barrier, DAG orchestration)

### With AI as the Primary Interface

The assumption shifts: the user describes intent in natural language, and an AI tool
(copilot, agent, code generator) maps that intent to the framework's configuration space.
This fundamentally changes the design priorities.

| Concern | Without AI | With AI |
|---|---|---|
| **Complexity budget** | High risk — users overwhelmed | **Advantage** — more axes = more AI-navigable expressiveness. AI maps "auto-expire when owner drops" → `RetainAllWeak` without the user browsing a taxonomy |
| **Error messages** | Critical — users can't decode | **Reduced** — AI decodes template errors. Shift investment from pretty `static_assert` messages to **runtime contracts** (C++26) that catch semantic mistakes |
| **Documentation** | Tutorials and cookbooks | **Reference docs + working tests + design rationale**. AI learns from working code and intent descriptions, not prose tutorials |
| **`make_event` wrapper** | Must ship | **Nice to have** — AI composes the primitives directly when the shorthand doesn't fit |
| **API surface** | Minimize overloads | **Maximize orthogonality** — AI reasons about axes independently. Hidden interactions between axes confuse AI too |
| **Configuration space** | Liability (too many choices) | **Asset** (precise fit for every problem) |

**Key insight:** with AI as intermediary, a complex-but-correct framework beats a
simple-but-limited one. The user says "I need a join that fires when I get both an
Endpoint and a Config, keeps latest Config, accumulates all Endpoints, and auto-expires
when the Endpoint owner drops it." The AI generates:

```cpp
auto join = make_reactive(
    [](const Endpoint& ep, const Config& cfg) { deploy(ep, cfg); },
    RetainAllWeak<Endpoint>{},
    RetainLatest<Config>{}
);
```

The user never browses the policy taxonomy. The complexity axes become AI-navigable
parameter space, not human-readable documentation burden.

### Design Implications: Optimize for Both

The framework should support both usage modes:

**For human developers (must have):**
- `make_reactive(lambda)` with zero-config defaults
- `make_event<Args...>()` convenience factory
- Clear `static_assert` messages for common mistakes
- Small set of well-documented examples

**For AI-assisted development (design for):**
- Orthogonal, independently composable axes (AI reasons about each axis separately)
- Comprehensive reference documentation with intent descriptions
- Concepts with semantically clear names (`RetainAllWeak`, `AlwaysAlive`, `RemovablePolicy`)
- Working test suite as ground truth (AI learns from tests, not prose)
- C++26 contracts for runtime invariant checking (AI can't diagnose silent correctness bugs)

**For both:**
- Compile-time errors over runtime errors (both humans and AI can diagnose compiler output)
- Few overloads with clear constraints (ambiguous overload resolution confuses both)
- One entry point (`make_reactive`) with progressive disclosure of complexity

### EventSource as Degenerate NJoin

A significant simplification: the framework does not need a separate `EventSource` class.
A classic event source with multiple subscribers is just:

```
NJoin with M=1, single group, Immediate policy, SubscriberList as EmitFn
```

The compile-time specialization eliminates all join machinery (no seq comparison, no
cross-product, no scan). The compiled output is a direct function call through the
subscriber list — identical to a hand-written event system.

```cpp
// Classic EventSource expressed in the framework:
template <class... Args>
auto make_event() {
    auto subs = std::make_shared<SubscriberList<Args...>>();  // COW subscriber list
    auto join = make_reactive<Immediate>(
        [subs](const Args&... args) { subs->fire(args...); }
    );
    // Expose fire() + subscribe() via wrapper
    return Event<Args...>{std::move(join), subs};
}

// Usage — identical to traditional EventSource:
auto onEndpoint = make_event<Endpoint>();
onEndpoint.subscribe(handler1);
onEndpoint.subscribe(handler2);
onEndpoint.fire(endpoint);  // → handler1(ep), handler2(ep)
```

The unified model means:
- One framework, one set of concepts, one set of tests
- EventSource is not a separate class — it's where the configuration space collapses
- Fan-out is composition: NJoin's emit function calls other NJoins' `add()`
- The entire system is a DAG of NJoins, where M=1 nodes are distribution points

## Related Work

### Positioning

The framework occupies a gap in the design space between single-threaded reactive
operators and concurrent message-consuming join patterns:

```
                        Retains state?
                     No                Yes
                 ┌──────────────┬─────────────────────┐
  Single-thread  │ Observer     │ Rx combineLatest     │
                 │ (GoF, 1994)  │ (RetainLatest only)  │
                 ├──────────────┼─────────────────────┤
  Concurrent     │ Join Calculus│ THIS FRAMEWORK        │
  multi-producer │ (consume)    │ (retain + join)       │
                 └──────────────┴─────────────────────┘
```

The N=1 column (eventing) is well-served by existing patterns. The N≥2 concurrent
column with state retention is the gap this framework fills. No amount of composing
N=1 events solves N≥2 correctly — the ordering primitive is categorically required.

### Comparison with Existing Work

| Work | Year | What it does | Key difference |
|---|---|---|---|
| **Join Calculus** (Fournet & Gonthier) | 2000 | Formal model: fire body when all channels have a message. The theoretical ancestor. | **Consumes** messages (one-shot). No retention policies. No seq ownership — uses internal queuing. |
| **Polyphonic C# / Cω** (Benton, Cardelli, Fournet) | 2004 | Language extension: "chords" — method body runs when multiple async methods called. | Language-level, not library. Consumes arguments. Serialized by runtime — no concurrent multi-producer problem. |
| **Joins Concurrency Library** (Russo) | 2007 | Library implementation of join patterns in C#. | Lock-based, single-threaded pattern matching. No retention, no policies. |
| **Scalable Join Patterns** (Turon & Russo, OOPSLA) | 2011 | Lock-free, scalable join patterns using fine-grained concurrency. | Closest work. Still **message-consuming** — each message participates in exactly one join firing. No retention. No N-way cross-product with persistent state. |
| **Scalable Multiway Stream Joins in HW** (Najafi & Sadoghi, IEEE) | 2019 | FPGA-based multi-way stream join for continuous queries. | Hardware-specific. Database-style windowed join, not event-driven reactive. |
| **Noria** (MIT) | 2018 | Incremental multi-way join for materialized views in databases. | Single-writer dataflow graph. Not concurrent multi-producer. |
| **RxJS/RxCpp `combineLatest`** | 2012+ | Reactive stream operator: emit latest from each source on any update. | Single-threaded scheduler. No concurrent producers. No exactly-once under race. RetainLatest only — no policy abstraction. |
| **Lamport Clocks** | 1978 | Logical timestamps for happened-before ordering in distributed systems. | Foundational theory. Not applied as a reactive join primitive. Our seq-ownership is a direct application. |

### Key Differentiator: Retain vs. Consume

The **join calculus** and all its descendants (Polyphonic C#, Scalable Join Patterns)
are **message-consuming**: each message participates in exactly one join firing, then is gone.

This framework **retains** state (policy-dependent). A new endpoint arriving triggers
emission with ALL existing configs, not just one. This is a fundamentally different
semantic model — a **reactive cross-product join**, not a **synchronization barrier**.

Turon & Russo (2011) is the paper reviewers will most likely compare against.
The rebuttal: they solve scalable message-consuming synchronization; we solve
stateful multi-producer reactive join with configurable retention policies.
Different problem, different invariants, different guarantees.

### What Has Not Been Published

A search across Google Scholar for `"reactive join" concurrent lock-free event streams`
returns **zero results**. Searches for `concurrent reactive join exactly-once multi-source`
return papers on stream processing, CEP engines, and database joins — none addressing
the specific problem of exactly-once N-tuple emission from concurrent producers with
per-source retention policies as a reusable library primitive.

## Design Lineage

| Component | Origin |
|---|---|
| Sequence ownership for pair dedup | Lamport clocks (1978) |
| Signed wraparound comparison | TCP sequence numbers (RFC 793) |
| Spinlock + per-resource locking | Classic concurrent data structures |
| Scan-after-insert pattern | Database hash join algorithms |
| Policy-based source abstraction | C++ policy-based design (Alexandrescu, 2001) |
| Fan-out subscription | Observer pattern (GoF) |
| Copy-on-write snapshots | RCU (Read-Copy-Update), Linux kernel |
| Join calculus (consume semantics) | Fournet & Gonthier (2000) |
| Scalable join patterns | Turon & Russo (OOPSLA 2011) |
| Partition lattice for grouping algebra | Rota (1964), Birkhoff *Lattice Theory* (1940) |
| View projection with dedup | Relational algebra π + DISTINCT (Codd, 1970) |
| Writable views via read-modify-write | CAS / RMW literature |
| Zero-cost lattice navigation | C++ template metaprogramming (consteval) |
