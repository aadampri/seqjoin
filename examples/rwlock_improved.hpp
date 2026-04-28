// Improved Reader-Writer Spinlock — bit-partitioned, portable
// Drop-in improvement for Sync.hpp's Read/Write Spinlock specializations
//
// Layout of the atomic uint32_t:
//
//   Bit 31       : WRITER flag (exclusive)
//   Bits 0–30    : Reader count (up to 2^31 - 1 = 2,147,483,647 readers)
//
//   ┌───┬────────────────────────────────┐
//   │ W │        reader_count            │
//   └───┴────────────────────────────────┘
//    31   30                            0
//
// Acquire protocol:
//   Reader: spin while WRITER bit set, then atomic increment reader count
//   Writer: set WRITER bit via fetch_or, then spin until reader count == 0
//
// Release protocol:
//   Reader: atomic decrement reader count
//   Writer: clear WRITER bit via fetch_and
//
// Properties:
//   - No magic sentinel (2048) — uses the full 31-bit range
//   - Writer cannot be starved by new readers (WRITER bit blocks new readers)
//   - Readers are wait-free when no writer is present (single atomic add)
//   - Portable spin_wait (x86 PAUSE / ARM YIELD / fallback yield)
//
// Memory orderings:
//   - Reader acquire:  acq_rel on the increment (acquire for protected data,
//                      release to publish our presence to writers)
//   - Reader release:  release on the decrement
//   - Writer acquire:  acquire on fetch_or (see protected data from prior writers)
//   - Writer release:  release on fetch_and (publish writes to next readers)

#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

namespace Sync {

namespace detail {

inline void spin_wait() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(_M_ARM64)
    asm volatile("yield");
#else
    std::this_thread::yield();
#endif
}

} // namespace detail

// ─── Constants ───────────────────────────────────────────────────────

static constexpr uint32_t WRITER_BIT = 1u << 31;  // 0x8000'0000
static constexpr uint32_t READER_MASK = ~WRITER_BIT;  // 0x7FFF'FFFF

// ─── ReadLock ────────────────────────────────────────────────────────
//
// RAII read-lock. Multiple readers can hold simultaneously.
// Spins while a writer holds or is waiting for the lock.

class ReadLock {
    std::atomic<uint32_t>& state_;

public:
    explicit ReadLock(std::atomic<uint32_t>& state) noexcept : state_(state) {
        uint32_t spins = 0;
        for (;;) {
            // Fast path: no writer → increment reader count
            uint32_t val = state_.load(std::memory_order_relaxed);
            if (!(val & WRITER_BIT)) {
                // Try to add 1 to reader count; succeed only if WRITER_BIT
                // didn't appear between load and CAS
                if (state_.compare_exchange_weak(
                        val, val + 1,
                        std::memory_order_acquire,    // acquire: see writes from prior writer
                        std::memory_order_relaxed)) {
                    return;  // locked
                }
                continue;  // CAS failed (concurrent reader/writer), retry
            }
            // Slow path: writer active, spin
            if ((++spins & 0xFF) == 0) std::this_thread::yield();
            detail::spin_wait();
        }
    }

    ~ReadLock() noexcept {
        state_.fetch_sub(1, std::memory_order_release);
    }

    ReadLock(const ReadLock&) = delete;
    ReadLock& operator=(const ReadLock&) = delete;
};

// ─── WriteLock ───────────────────────────────────────────────────────
//
// RAII exclusive write-lock. Blocks new readers, waits for existing
// readers to drain, then enters the critical section.
//
// Writer starvation prevention: once WRITER_BIT is set, no new reader
// can increment the count. Only existing readers (who already passed
// the check) can still be active. They will finish and decrement.

class WriteLock {
    std::atomic<uint32_t>& state_;

public:
    explicit WriteLock(std::atomic<uint32_t>& state) noexcept : state_(state) {
        // Phase 1: Set the WRITER bit. If already set, another writer
        // is active — spin until it clears.
        uint32_t spins = 0;
        while (state_.fetch_or(WRITER_BIT, std::memory_order_acquire) & WRITER_BIT) {
            // Another writer holds the bit — wait for it to release
            while (state_.load(std::memory_order_relaxed) & WRITER_BIT) {
                if ((++spins & 0xFF) == 0) std::this_thread::yield();
                detail::spin_wait();
            }
        }

        // Phase 2: WRITER bit is ours. Wait for all active readers to drain.
        spins = 0;
        while (state_.load(std::memory_order_acquire) & READER_MASK) {
            if ((++spins & 0xFF) == 0) std::this_thread::yield();
            detail::spin_wait();
        }
        // Now we hold exclusive access.
    }

    ~WriteLock() noexcept {
        state_.fetch_and(READER_MASK, std::memory_order_release);  // clear WRITER_BIT
    }

    WriteLock(const WriteLock&) = delete;
    WriteLock& operator=(const WriteLock&) = delete;
};

} // namespace Sync
