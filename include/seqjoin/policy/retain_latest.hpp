// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <optional>
#include <utility>

namespace seqjoin {

/// Retains only the latest value. Each insert overwrites the previous.
/// scan() yields at most one element.
template <class T>
class RetainLatest {
public:
    using value_type = T;

    /// Store the value with its seq. Always succeeds.
    std::optional<uint64_t> insert(const T& value, uint64_t seq) {
        data_ = {value, seq};
        return seq;
    }
    std::optional<uint64_t> insert(T&& value, uint64_t seq) {
        data_ = {std::move(value), seq};
        return seq;
    }

    /// Returns a pointer range of 0 or 1 elements for iteration.
    /// Caller holds the source spinlock.
    [[nodiscard]] const auto& scan() const noexcept { return data_; }

    /// True if a value is stored.
    [[nodiscard]] bool has_value() const noexcept { return data_.has_value(); }

    /// Get the stored value (caller must check has_value()).
    [[nodiscard]] const T& value() const noexcept { return data_->first; }

    /// Get the stored seq (caller must check has_value()).
    [[nodiscard]] uint64_t seq() const noexcept { return data_->second; }

    void clear() { data_.reset(); }

private:
    std::optional<std::pair<T, uint64_t>> data_;
};

} // namespace seqjoin
