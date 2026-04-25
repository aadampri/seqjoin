// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

namespace seqjoin {

/// Exclusive spinlock using the Rigtorp pattern.
/// RAII-compatible via lock()/unlock() for use with std::lock_guard.
/// Never held during emit — only protects per-source insert/scan.
class SpinLock {
public:
    SpinLock() noexcept = default;

    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;

    void lock() noexcept {
        for (uint32_t spins = 0;
             flag_.test_and_set(std::memory_order_acquire);
             ++spins) {
            // Inner loop: spin on load (no cache-line bouncing) until released
            while (flag_.test(std::memory_order_relaxed)) {
                if ((spins & 0xFF) == 0xFF) {
                    std::this_thread::yield();  // back off after 255 spins
                }
                spin_wait();
                ++spins;
            }
        }
    }

    void unlock() noexcept {
        flag_.clear(std::memory_order_release);
    }

private:
    std::atomic_flag flag_ = ATOMIC_FLAG_INIT;

    static void spin_wait() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
        __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(_M_ARM64)
        asm volatile("yield");
#else
        std::this_thread::yield();
#endif
    }
};

} // namespace seqjoin
