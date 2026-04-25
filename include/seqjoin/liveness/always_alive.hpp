// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

namespace seqjoin {

/// Default liveness strategy — values are always alive.
/// Compiled out entirely (empty struct, is_alive returns constexpr true).
struct AlwaysAlive {
    template <class T>
    [[nodiscard]] constexpr bool is_alive(const T&) const noexcept { return true; }
};

} // namespace seqjoin
