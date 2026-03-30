#pragma once

#include "db.h"
#include "lib/hybrid_l1/prefix_suffix.h"

#include <cstddef>
#include <string>

namespace flowkv::experiments {

enum class PrefixDistribution {
    kUniform,
    kPrefixSkew,
    kPrefixSkewExtreme,
};

struct FastBulkL1BuildOptions {
    std::string pool_path;
    size_t pool_size_bytes = 0;
    bool use_direct_io = true;
    size_t key_count = 0;
    size_t prefix_count = 0;
    PrefixDistribution distribution = PrefixDistribution::kUniform;
};

FixedValue16 DeterministicValue16(hybrid_l1::RoutePrefix prefix,
                                  hybrid_l1::RouteSuffix suffix);

size_t UsedCountForPrefix(const FastBulkL1BuildOptions& options,
                          hybrid_l1::RoutePrefix prefix);

KeyType KeyForLogicalIndex(const FastBulkL1BuildOptions& options, size_t logical_idx);

bool BuildFastBulkL1Dataset(const FastBulkL1BuildOptions& options,
                            std::string* error_message = nullptr);

}  // namespace flowkv::experiments
