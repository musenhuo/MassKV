#pragma once

#include "db/table.h"

#include <cstdint>
#include <type_traits>

namespace flowkv::hybrid_l1 {

using RoutePrefix = uint64_t;
using RouteSuffix = uint64_t;

static_assert(sizeof(RoutePrefix) == 8, "RoutePrefix must be 8 bytes");
static_assert(sizeof(RouteSuffix) == 8, "RouteSuffix must be 8 bytes");
#if defined(FLOWKV_KEY16)
static_assert(sizeof(KeyType) == 16, "FLOWKV_KEY16 mode requires 16-byte KeyType");
#endif

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
