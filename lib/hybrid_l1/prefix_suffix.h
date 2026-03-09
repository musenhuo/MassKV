#pragma once

#include "db/table.h"

#include <cstdint>

namespace flowkv::hybrid_l1 {

using RoutePrefix = uint64_t;
using RouteSuffix = uint64_t;

inline RoutePrefix ExtractPrefix(const KeyType& key) {
#if defined(FLOWKV_KEY16)
    return key.hi;
#else
    return 0;
#endif
}

inline RouteSuffix ExtractSuffix(const KeyType& key) {
#if defined(FLOWKV_KEY16)
    return key.lo;
#else
    return __builtin_bswap64(key);
#endif
}

inline KeyType ComposeKey(RoutePrefix prefix, RouteSuffix suffix) {
#if defined(FLOWKV_KEY16)
    return Key16{prefix, suffix};
#else
    (void)prefix;
    return __builtin_bswap64(suffix);
#endif
}

}  // namespace flowkv::hybrid_l1
