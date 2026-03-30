#pragma once

#include "prefix_suffix.h"

#include <algorithm>
#include <cstdint>
#include <vector>

namespace flowkv::hybrid_l1 {

enum class L1DeltaOpType : uint8_t {
    kAdd = 0,
    kDelete = 1,
    kReplace = 2,
};

struct L1DeltaOp {
    L1DeltaOpType type = L1DeltaOpType::kAdd;
    RouteSuffix suffix_begin = 0;
    RouteSuffix suffix_end = 0;
    uint64_t kv_block_ptr = 0;
    uint16_t offset = 0;
    uint8_t count = 0;
};

struct L1PrefixDelta {
    RoutePrefix prefix = 0;
    uint64_t old_root_page_ptr = 0;
    uint32_t old_record_count = 0;
    std::vector<L1DeltaOp> ops;
};

struct L1DeltaBatch {
    uint64_t batch_id = 0;
    uint32_t l1_seq = 0;
    uint64_t checksum = 0;
    std::vector<L1PrefixDelta> deltas;

    bool Empty() const { return deltas.empty(); }

    void SortAndUniquePrefixes() {
        std::sort(deltas.begin(), deltas.end(),
                  [](const L1PrefixDelta& lhs, const L1PrefixDelta& rhs) {
                      return lhs.prefix < rhs.prefix;
                  });
        size_t write_idx = 0;
        for (size_t i = 0; i < deltas.size(); ++i) {
            if (write_idx == 0 || deltas[write_idx - 1].prefix != deltas[i].prefix) {
                if (write_idx != i) {
                    deltas[write_idx] = std::move(deltas[i]);
                }
                ++write_idx;
                continue;
            }
            auto& target = deltas[write_idx - 1];
            target.ops.insert(target.ops.end(), deltas[i].ops.begin(), deltas[i].ops.end());
            if (target.old_root_page_ptr == 0 && deltas[i].old_root_page_ptr != 0) {
                target.old_root_page_ptr = deltas[i].old_root_page_ptr;
            }
            if (target.old_record_count == 0 && deltas[i].old_record_count != 0) {
                target.old_record_count = deltas[i].old_record_count;
            }
        }
        deltas.resize(write_idx);
    }

    std::vector<KeyType> ToChangedRouteKeys() const {
        std::vector<KeyType> keys;
        keys.reserve(deltas.size());
        for (const auto& delta : deltas) {
            keys.push_back(ComposeKey(delta.prefix, 0));
        }
        return keys;
    }
};

}  // namespace flowkv::hybrid_l1
