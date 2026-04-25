// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>

namespace seqjoin {

/// Retains all unique values, each with its assigned seq.
/// Duplicate values (by Hash/Eq) are rejected — insert returns nullopt.
/// scan() yields all stored (value, seq) pairs.
template <class T, class Hash = std::hash<T>, class Eq = std::equal_to<T>>
class RetainAll {
public:
    using value_type = T;

    /// Insert value with seq. Returns nullopt if value already exists (dedup).
    std::optional<uint64_t> insert(const T& value, uint64_t seq) {
        auto [it, inserted] = data_.emplace(value, seq);
        if (!inserted) return std::nullopt;  // duplicate — rejected
        return seq;
    }

    /// Returns a const reference to the map for iteration.
    /// Caller holds the source spinlock.
    [[nodiscard]] const auto& scan() const noexcept { return data_; }

    /// Number of stored values.
    [[nodiscard]] std::size_t size() const noexcept { return data_.size(); }

    [[nodiscard]] bool empty() const noexcept { return data_.empty(); }

    /// Remove a specific value. Returns true if it was present.
    bool remove(const T& value) { return data_.erase(value) > 0; }

    void clear() { data_.clear(); }

private:
    std::unordered_map<T, uint64_t, Hash, Eq> data_;
};

} // namespace seqjoin
