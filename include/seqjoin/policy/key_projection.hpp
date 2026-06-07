// seqjoin — Concurrent N-way Reactive Join Framework
// SPDX-License-Identifier: MIT
#pragma once

#include <tuple>

namespace seqjoin {

/// Default key projector: extracts std::get<0> from a tuple-like type.
/// Used as the default KeyProj for keyed retention policies (RetainByKey, RetainByKeyCOW).
struct ProjectFirst {
    template <class T>
    decltype(auto) operator()(const T& t) const { return std::get<0>(t); }
};

} // namespace seqjoin
