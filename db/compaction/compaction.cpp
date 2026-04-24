#include "compaction.h"
#include "manifest.h"
#include "version.h"
#include "db/blocks/fixed_size_block.h"
#include "lib/hybrid_l1/l1_delta_batch.h"
#include "lib/hybrid_l1/prefix_suffix.h"
#include "lib/hybrid_l1/subtree_record.h"
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <deque>
#include <limits>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>

size_t total_L1_num = 0;

struct KeyWithRowId
{
	KeyType key;
	int row_id;
};

struct UintKeyComparator
{
	bool operator()(const KeyWithRowId l, const KeyWithRowId r) const
	{
		if (unlikely(CompareKeyType(l.key, r.key) == 0))
		{
			return l.row_id < r.row_id;
		}
		return CompareKeyType(l.key, r.key) > 0;
	}
} cmp;

namespace {

using flowkv::hybrid_l1::ExtractPrefix;
using flowkv::hybrid_l1::ExtractSuffix;
using flowkv::hybrid_l1::L1DeltaBatch;
using flowkv::hybrid_l1::L1DeltaOp;
using flowkv::hybrid_l1::L1DeltaOpType;
using flowkv::hybrid_l1::L1PrefixDelta;
using flowkv::hybrid_l1::RoutePrefix;
using flowkv::hybrid_l1::RouteSuffix;
using flowkv::hybrid_l1::SubtreeRecord;
using flowkv::hybrid_l1::ComposeKey;
constexpr size_t kInvalidManifestPosition = std::numeric_limits<size_t>::max();

struct PrefixWindow {
    RoutePrefix prefix = 0;
    RouteSuffix suffix_begin = 0;
    RouteSuffix suffix_end = 0;
    uint16_t offset = 0;
    uint8_t count = 0;
};

bool ParseEnvBool(const char* name, bool& out) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) {
        return false;
    }
    const std::string value(raw);
    if (value == "1" || value == "true" || value == "TRUE") {
        out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "FALSE") {
        out = false;
        return true;
    }
    return false;
}

bool IsL1RangeScanRecordsEnabled() {
    static const bool enabled = []() {
        bool parsed = true;
        if (ParseEnvBool("FLOWKV_L1_RANGE_SCAN_RECORDS", parsed)) {
            return parsed;
        }
        return true;
    }();
    return enabled;
}

bool IsL1DeleteCoveredOnlyEnabled() {
    static const bool enabled = []() {
        bool parsed = true;
        if (ParseEnvBool("FLOWKV_L1_DELETE_FULLY_COVERED_ONLY", parsed)) {
            return parsed;
        }
        return true;
    }();
    return enabled;
}

bool IsCompactionCleanTraceEnabled() {
    static const bool enabled = []() {
        bool parsed = false;
        if (ParseEnvBool("FLOWKV_COMPACTION_CLEAN_TRACE", parsed)) {
            return parsed;
        }
        if (ParseEnvBool("FLOWKV_COMPACTION_TRACE", parsed)) {
            return parsed;
        }
        return false;
    }();
    return enabled;
}

uint64_t ReadProcessRSSBytesFromProc() {
    std::FILE* file = std::fopen("/proc/self/statm", "r");
    if (file == nullptr) {
        return 0;
    }
    unsigned long total_pages = 0;
    unsigned long resident_pages = 0;
    const int scanned = std::fscanf(file, "%lu %lu", &total_pages, &resident_pages);
    std::fclose(file);
    if (scanned != 2) {
        return 0;
    }
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return 0;
    }
    return static_cast<uint64_t>(resident_pages) * static_cast<uint64_t>(page_size);
}

std::atomic<uint64_t> g_clean_trace_invocation_id{0};

bool AddEntryToBuilder(PSTBuilder* builder, const KeyType& key, const FixedValue16& value) {
#if defined(FLOWKV_KEY16)
    uint8_t key_bytes[16];
    key.ToBigEndianBytes(key_bytes);
    return builder->AddEntry(Slice(reinterpret_cast<const char*>(key_bytes), 16),
                             Slice(reinterpret_cast<const char*>(&value), sizeof(value)));
#else
    return builder->AddEntry(Slice(&key),
                             Slice(reinterpret_cast<const char*>(&value), sizeof(value)));
#endif
}

void PersistL1HybridStateOrExit(Version* version, Manifest* manifest) {
    std::vector<uint8_t> l1_hybrid_state_bytes;
    uint32_t current_l1_seq_no = 0;
    if (!version->ExportL1HybridState(l1_hybrid_state_bytes, current_l1_seq_no) ||
        !manifest->PersistL1HybridState(l1_hybrid_state_bytes, current_l1_seq_no)) {
        ERROR_EXIT("L1 hybrid state persist failed, cannot continue");
    }
}

void ReclaimObsoletePstsAfterCommit(PSTDeleter* pst_deleter,
                                    const std::vector<TaggedPstMeta>& l1_tables_to_delete,
                                    const std::vector<std::vector<TaggedPstMeta>>& inputs,
                                    int tree_num) {
    for (const auto& pst : l1_tables_to_delete) {
        if (pst.level != NotOverlappedMark) {
            pst_deleter->DeletePST(pst.meta);
        }
    }
    for (int i = 0; i < tree_num; ++i) {
        for (const auto& pst : inputs[i]) {
            if (pst.level != NotOverlappedMark) {
                assert(pst.level == 0);
                pst_deleter->DeletePST(pst.meta);
            }
        }
    }
    pst_deleter->PersistCheckpoint();
}

class StepCPatchRecorder {
public:
    void AddKey(const KeyType& key) {
        const RoutePrefix prefix = ExtractPrefix(key);
        const RouteSuffix suffix = ExtractSuffix(key);
        if (!has_active_) {
            active_prefix_ = prefix;
            active_begin_ = suffix;
            active_end_ = suffix;
            active_offset_ = entry_cursor_;
            active_count_ = 1;
            has_active_ = true;
            ++entry_cursor_;
            return;
        }
        if (prefix == active_prefix_ &&
            active_count_ < std::numeric_limits<uint8_t>::max()) {
            ++active_count_;
            active_end_ = suffix;
            ++entry_cursor_;
            return;
        }
        SealActiveWindow();
        active_prefix_ = prefix;
        active_begin_ = suffix;
        active_end_ = suffix;
        active_offset_ = entry_cursor_;
        active_count_ = 1;
        has_active_ = true;
        ++entry_cursor_;
    }

    void EmitAndReset(const PSTMeta& table_meta, std::vector<SubtreeRecord>& out) {
        SealActiveWindow();
        if (!table_meta.Valid() || windows_.empty()) {
            Reset();
            return;
        }
        const uint64_t kv_block_ptr = SubtreeRecord::EncodeKvBlockPtr(table_meta.datablock_ptr_);
        for (const auto& window : windows_) {
            SubtreeRecord record{};
            record.route_prefix = window.prefix;
            record.route_min_suffix = window.suffix_begin;
            record.route_max_suffix = window.suffix_end;
            record.min_key = ComposeKey(window.prefix, window.suffix_begin);
            record.max_key = ComposeKey(window.prefix, window.suffix_end);
            record.table_idx = INVALID_PTR;
            record.seq_no = table_meta.seq_no_;
            record.SetLeafWindowByBlockPtr(kv_block_ptr, window.offset, window.count);
            out.push_back(record);
        }
        Reset();
    }

private:
    struct Window {
        RoutePrefix prefix = 0;
        RouteSuffix suffix_begin = 0;
        RouteSuffix suffix_end = 0;
        uint16_t offset = 0;
        uint8_t count = 0;
    };

    void SealActiveWindow() {
        if (!has_active_) {
            return;
        }
        windows_.push_back(Window{
            active_prefix_,
            active_begin_,
            active_end_,
            active_offset_,
            active_count_});
        has_active_ = false;
    }

    void Reset() {
        windows_.clear();
        has_active_ = false;
        active_prefix_ = 0;
        active_begin_ = 0;
        active_end_ = 0;
        active_offset_ = 0;
        active_count_ = 0;
        entry_cursor_ = 0;
    }

    std::vector<Window> windows_;
    bool has_active_ = false;
    RoutePrefix active_prefix_ = 0;
    RouteSuffix active_begin_ = 0;
    RouteSuffix active_end_ = 0;
    uint16_t active_offset_ = 0;
    uint8_t active_count_ = 0;
    uint16_t entry_cursor_ = 0;
};

class RecordIterator {
public:
    RecordIterator(PSTReader* pst_reader,
                   const std::vector<SubtreeRecord>& records)
        : pst_reader_(pst_reader), records_(records) {
        current_valid_ = AdvanceToNextOutput();
    }

    ~RecordIterator() {
        for (auto& state : states_) {
            if (state.iter != nullptr) {
                delete state.iter;
                state.iter = nullptr;
            }
        }
    }

    bool Valid() const {
        return current_valid_;
    }

    KeyType GetCurrentKey() const {
        assert(Valid());
        return current_key_;
    }

    FixedValue16 GetCurrentValue() const {
        assert(Valid());
        return current_value_;
    }

    bool NextKey() {
        current_valid_ = AdvanceToNextOutput();
        return current_valid_;
    }

    bool MoveTo(const KeyType& key) {
        while (Valid() && CompareKeyType(GetCurrentKey(), key) < 0) {
            if (!NextKey()) {
                return false;
            }
        }
        return Valid();
    }

private:
    struct ActiveState {
        PSTReader::Iterator* iter = nullptr;
        uint16_t end_exclusive = 0;
        size_t record_idx = 0;
        KeyType route_min_key{};
        KeyType route_max_key{};
    };

    struct HeapEntry {
        KeyType key{};
        size_t state_idx = 0;
    };

    struct HeapEntryComparator {
        bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const {
            const int cmp = CompareKeyType(lhs.key, rhs.key);
            if (cmp != 0) {
                return cmp > 0;
            }
            return lhs.state_idx > rhs.state_idx;
        }
    };

    bool IsRecordNewer(size_t lhs_record_idx, size_t rhs_record_idx) const {
        const SubtreeRecord& lhs = records_[lhs_record_idx];
        const SubtreeRecord& rhs = records_[rhs_record_idx];
        if (lhs.seq_no != rhs.seq_no) {
            return lhs.seq_no > rhs.seq_no;
        }
        return lhs_record_idx < rhs_record_idx;
    }

    bool AdvanceState(size_t state_idx) {
        ActiveState& state = states_[state_idx];
        if (state.iter == nullptr) {
            return false;
        }
        uint16_t next_idx =
            static_cast<uint16_t>(state.iter->current_record_index_ + 1);
        while (next_idx < state.end_exclusive) {
            state.iter->current_record_index_ = next_idx;
            const KeyType next_key = state.iter->Key();
            if (CompareKeyType(next_key, state.route_min_key) < 0) {
                next_idx = static_cast<uint16_t>(next_idx + 1);
                continue;
            }
            if (CompareKeyType(next_key, state.route_max_key) > 0) {
                break;
            }
            heap_.push(HeapEntry{next_key, state_idx});
            return true;
        }
        delete state.iter;
        state.iter = nullptr;
        return false;
    }

    bool HasPendingWindowWithMinLE(const KeyType& key) const {
        for (size_t idx = next_record_idx_; idx < records_.size(); ++idx) {
            const auto window = records_[idx].LeafWindow();
            if (window.count == 0) {
                continue;
            }
            return CompareKeyType(records_[idx].RouteMinKey(), key) <= 0;
        }
        return false;
    }

    bool ActivateNextWindow() {
        while (next_record_idx_ < records_.size()) {
            const size_t record_idx = next_record_idx_++;
            const SubtreeRecord& record = records_[record_idx];
            const auto window = record.LeafWindow();
            if (window.count == 0) {
                continue;
            }
            const uint64_t block_offset = SubtreeRecord::DecodeKvBlockOffset(window.kv_block_ptr);
            PSTReader::Iterator* iter = pst_reader_->GetIterator(block_offset);
            if (iter == nullptr) {
                continue;
            }
            const uint16_t entry_begin = window.offset;
            const uint16_t records_size = static_cast<uint16_t>(iter->RecordsSize());
            if (entry_begin >= records_size) {
                delete iter;
                continue;
            }
            const uint16_t entry_end = static_cast<uint16_t>(
                std::min<uint32_t>(records_size,
                                   static_cast<uint32_t>(window.offset) + window.count));
            if (entry_end <= entry_begin) {
                delete iter;
                continue;
            }
            iter->current_record_index_ = entry_begin;
            const KeyType route_min_key = record.RouteMinKey();
            const KeyType route_max_key = record.RouteMaxKey();
            while (iter->current_record_index_ < entry_end &&
                   CompareKeyType(iter->Key(), route_min_key) < 0) {
                iter->current_record_index_ =
                    static_cast<uint16_t>(iter->current_record_index_ + 1);
            }
            if (iter->current_record_index_ >= entry_end ||
                CompareKeyType(iter->Key(), route_max_key) > 0) {
                delete iter;
                continue;
            }
            const size_t state_idx = states_.size();
            states_.push_back(ActiveState{
                iter,
                entry_end,
                record_idx,
                route_min_key,
                route_max_key});
            heap_.push(HeapEntry{iter->Key(), state_idx});
            return true;
        }
        return false;
    }

    bool AdvanceToNextOutput() {
        while (true) {
            if (heap_.empty()) {
                if (!ActivateNextWindow()) {
                    return false;
                }
            }

            KeyType min_key = heap_.top().key;
            while (HasPendingWindowWithMinLE(min_key)) {
                if (!ActivateNextWindow()) {
                    break;
                }
                min_key = heap_.top().key;
            }

            min_key = heap_.top().key;
            std::vector<size_t> same_key_states;
            while (!heap_.empty() && heap_.top().key == min_key) {
                same_key_states.push_back(heap_.top().state_idx);
                heap_.pop();
            }
            if (same_key_states.empty()) {
                continue;
            }

            size_t winner_state_idx = same_key_states[0];
            for (size_t i = 1; i < same_key_states.size(); ++i) {
                const size_t candidate = same_key_states[i];
                if (IsRecordNewer(states_[candidate].record_idx,
                                  states_[winner_state_idx].record_idx)) {
                    winner_state_idx = candidate;
                }
            }

            current_key_ = min_key;
            current_value_ = states_[winner_state_idx].iter->Value();

            for (const size_t state_idx : same_key_states) {
                AdvanceState(state_idx);
            }
            return true;
        }
    }

    PSTReader* pst_reader_ = nullptr;
    const std::vector<SubtreeRecord>& records_;
    std::vector<ActiveState> states_;
    std::priority_queue<HeapEntry,
                        std::vector<HeapEntry>,
                        HeapEntryComparator> heap_;
    size_t next_record_idx_ = 0;
    bool current_valid_ = false;
    KeyType current_key_{};
    FixedValue16 current_value_{};
};

L1PrefixDelta& EnsurePrefixDelta(
    std::vector<L1PrefixDelta>& deltas,
    std::unordered_map<RoutePrefix, size_t>& index_by_prefix,
    RoutePrefix prefix) {
    const auto it = index_by_prefix.find(prefix);
    if (it != index_by_prefix.end()) {
        return deltas[it->second];
    }
    const size_t idx = deltas.size();
    index_by_prefix.emplace(prefix, idx);
    deltas.push_back(L1PrefixDelta{});
    deltas.back().prefix = prefix;
    return deltas.back();
}

KeyType ExtractEntryKey(const PDataBlock::Entry& entry) {
#if defined(FLOWKV_KEY16)
    return KeyType{entry.key_hi, entry.key_lo};
#else
    return entry.key;
#endif
}

bool BuildPrefixWindowsFromTable(const TaggedPstMeta& pst,
                                 SegmentAllocator* seg_allocator,
                                 std::vector<PrefixWindow>& windows_out) {
    windows_out.clear();
    if (!pst.Valid() || seg_allocator == nullptr) {
        return false;
    }
    const auto& table = pst.meta;
    if (table.entry_num_ == 0 || table.entry_num_ > static_cast<uint16_t>(PDataBlock::MAX_ENTRIES)) {
        return false;
    }

    PDataBlock block{};
    const ssize_t ret = pread(seg_allocator->Getfd(),
                              &block,
                              sizeof(PDataBlock),
                              static_cast<off_t>(table.datablock_ptr_));
    if (ret != static_cast<ssize_t>(sizeof(PDataBlock))) {
        return false;
    }

    uint16_t idx = 0;
    while (idx < table.entry_num_) {
        const KeyType first_key = ExtractEntryKey(block.entries[idx]);
        const RoutePrefix prefix = ExtractPrefix(first_key);
        const uint16_t run_begin = idx;
        ++idx;
        while (idx < table.entry_num_) {
            const KeyType key = ExtractEntryKey(block.entries[idx]);
            if (ExtractPrefix(key) != prefix) {
                break;
            }
            ++idx;
        }
        const uint16_t run_size = static_cast<uint16_t>(idx - run_begin);
        uint16_t consumed = 0;
        while (consumed < run_size) {
            const uint16_t chunk_size = static_cast<uint16_t>(std::min<uint16_t>(
                static_cast<uint16_t>(run_size - consumed),
                static_cast<uint16_t>(std::numeric_limits<uint8_t>::max())));
            const uint16_t chunk_begin = static_cast<uint16_t>(run_begin + consumed);
            const uint16_t chunk_last = static_cast<uint16_t>(chunk_begin + chunk_size - 1);
            const KeyType begin_key = ExtractEntryKey(block.entries[chunk_begin]);
            const KeyType end_key = ExtractEntryKey(block.entries[chunk_last]);
            PrefixWindow window;
            window.prefix = prefix;
            window.suffix_begin = ExtractSuffix(begin_key);
            window.suffix_end = ExtractSuffix(end_key);
            window.offset = chunk_begin;
            window.count = static_cast<uint8_t>(chunk_size);
            windows_out.push_back(window);
            consumed = static_cast<uint16_t>(consumed + chunk_size);
        }
    }
    return !windows_out.empty();
}

void AppendDeltaRangeFallback(const TaggedPstMeta& pst,
                              L1DeltaOpType op_type,
                              std::vector<L1PrefixDelta>& deltas,
                              std::unordered_map<RoutePrefix, size_t>& index_by_prefix) {
    if (!pst.Valid()) {
        return;
    }
    const auto& table = pst.meta;
    const RoutePrefix min_prefix = ExtractPrefix(table.MinKey());
    const RoutePrefix max_prefix = ExtractPrefix(table.MaxKey());
    RoutePrefix current = min_prefix;
    const uint64_t kv_block_ptr = SubtreeRecord::EncodeKvBlockPtr(table.datablock_ptr_);
    const uint8_t count = static_cast<uint8_t>(std::min<uint16_t>(
        table.entry_num_, static_cast<uint16_t>(std::numeric_limits<uint8_t>::max())));
    while (true) {
        L1PrefixDelta& prefix_delta = EnsurePrefixDelta(deltas, index_by_prefix, current);
        L1DeltaOp op;
        op.type = op_type;
        op.kv_block_ptr = kv_block_ptr;
        op.offset = 0;
        op.count = count;
        op.suffix_begin = current == min_prefix ? ExtractSuffix(table.MinKey()) : 0;
        op.suffix_end =
            current == max_prefix ? ExtractSuffix(table.MaxKey()) : std::numeric_limits<RouteSuffix>::max();
        prefix_delta.ops.push_back(op);
        if (current == max_prefix) {
            break;
        }
        ++current;
    }
}

void AppendSyntheticRecordsFromTable(const TaggedPstMeta& pst,
                                     std::vector<SubtreeRecord>& out_records) {
    if (!pst.Valid()) {
        return;
    }
    const auto& table = pst.meta;
    const RoutePrefix min_prefix = ExtractPrefix(table.MinKey());
    const RoutePrefix max_prefix = ExtractPrefix(table.MaxKey());
    RoutePrefix current = min_prefix;
    const uint64_t kv_block_ptr = SubtreeRecord::EncodeKvBlockPtr(table.datablock_ptr_);
    const uint8_t count = static_cast<uint8_t>(std::min<uint16_t>(
        table.entry_num_, static_cast<uint16_t>(std::numeric_limits<uint8_t>::max())));
    while (true) {
        SubtreeRecord record{};
        record.route_prefix = current;
        record.route_min_suffix = current == min_prefix ? ExtractSuffix(table.MinKey()) : 0;
        record.route_max_suffix =
            current == max_prefix ? ExtractSuffix(table.MaxKey()) : std::numeric_limits<RouteSuffix>::max();
        record.min_key = ComposeKey(current, record.route_min_suffix);
        record.max_key = ComposeKey(current, record.route_max_suffix);
        record.seq_no = table.seq_no_;
        record.table_idx = INVALID_PTR;
        record.SetLeafWindowByBlockPtr(kv_block_ptr, 0, count);
        out_records.push_back(record);
        if (current == max_prefix) {
            break;
        }
        ++current;
    }
}

std::vector<SubtreeRecord> BuildSyntheticRecordsFromTables(
    const std::vector<TaggedPstMeta>& tables) {
    std::vector<SubtreeRecord> records;
    records.reserve(tables.size());
    for (const auto& table : tables) {
        AppendSyntheticRecordsFromTable(table, records);
    }
    return records;
}

struct RecordPatchSignature {
    RoutePrefix prefix = 0;
    RouteSuffix suffix_begin = 0;
    RouteSuffix suffix_end = 0;
    uint64_t kv_block_ptr = 0;
    uint16_t offset = 0;
    uint8_t count = 0;

    bool operator==(const RecordPatchSignature& rhs) const {
        return prefix == rhs.prefix &&
               suffix_begin == rhs.suffix_begin &&
               suffix_end == rhs.suffix_end &&
               kv_block_ptr == rhs.kv_block_ptr &&
               offset == rhs.offset &&
               count == rhs.count;
    }
};

struct RecordPatchSignatureHash {
    size_t operator()(const RecordPatchSignature& sig) const {
        size_t h = std::hash<uint64_t>{}(sig.prefix);
        h ^= std::hash<uint64_t>{}(sig.suffix_begin) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<uint64_t>{}(sig.suffix_end) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<uint64_t>{}(sig.kv_block_ptr) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<uint16_t>{}(sig.offset) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<uint8_t>{}(sig.count) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

void DedupAddPatchRecords(std::vector<SubtreeRecord>& records) {
    std::unordered_set<RecordPatchSignature, RecordPatchSignatureHash> seen;
    seen.reserve(records.size());
    std::vector<SubtreeRecord> deduped;
    deduped.reserve(records.size());
    for (const auto& record : records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        const RecordPatchSignature sig{
            record.route_prefix,
            record.route_min_suffix,
            record.route_max_suffix,
            window.kv_block_ptr,
            window.offset,
            window.count};
        if (!seen.insert(sig).second) {
            continue;
        }
        deduped.push_back(record);
    }
    records.swap(deduped);
}

std::vector<SubtreeRecord> MergeSubCompactionAddPatchRecords(
    const std::vector<SubtreeRecord> (&partition_patch_records)[RANGE_PARTITION_NUM]) {
    size_t total = 0;
    for (int i = 0; i < RANGE_PARTITION_NUM; ++i) {
        total += partition_patch_records[i].size();
    }
    std::vector<SubtreeRecord> merged;
    merged.reserve(total);
    for (int i = 0; i < RANGE_PARTITION_NUM; ++i) {
        merged.insert(merged.end(),
                      partition_patch_records[i].begin(),
                      partition_patch_records[i].end());
    }
    return merged;
}

std::vector<SubtreeRecord> BuildAddRecordsWithFallback(
    const std::vector<TaggedPstMeta>& outputs,
    const std::vector<SubtreeRecord>& precise_add_records,
    const std::vector<SubtreeRecord>& l1_input_records) {
    std::vector<SubtreeRecord> add_records;
    add_records.reserve(precise_add_records.size() + outputs.size());
    std::unordered_set<uint64_t> covered_kv_block_ptrs;
    covered_kv_block_ptrs.reserve(outputs.size() + precise_add_records.size());
    for (const auto& record : precise_add_records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        add_records.push_back(record);
        covered_kv_block_ptrs.insert(window.kv_block_ptr);
    }

    std::unordered_multimap<uint64_t, SubtreeRecord> l1_records_by_block;
    l1_records_by_block.reserve(l1_input_records.size());
    for (const auto& record : l1_input_records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        l1_records_by_block.emplace(window.kv_block_ptr, record);
    }

    for (const auto& table : outputs) {
        if (!table.Valid()) {
            continue;
        }
        const uint64_t kv_block_ptr = SubtreeRecord::EncodeKvBlockPtr(table.meta.datablock_ptr_);
        if (!covered_kv_block_ptrs.insert(kv_block_ptr).second) {
            continue;
        }
        bool appended_from_l1 = false;
        const auto range = l1_records_by_block.equal_range(kv_block_ptr);
        for (auto it = range.first; it != range.second; ++it) {
            add_records.push_back(it->second);
            appended_from_l1 = true;
        }
        if (!appended_from_l1) {
            AppendSyntheticRecordsFromTable(table, add_records);
        }
    }

    DedupAddPatchRecords(add_records);
    return add_records;
}

std::vector<uint64_t> CollectUniqueBlocksFromRecords(
    const std::vector<SubtreeRecord>& records) {
    std::unordered_set<uint64_t> unique_blocks;
    unique_blocks.reserve(records.size());
    for (const auto& record : records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        unique_blocks.insert(window.kv_block_ptr);
    }
    std::vector<uint64_t> out;
    out.reserve(unique_blocks.size());
    for (const auto block_ptr : unique_blocks) {
        out.push_back(block_ptr);
    }
    std::sort(out.begin(), out.end());
    return out;
}

bool AreAllRecordsWindowReadable(const std::vector<SubtreeRecord>& records) {
    if (records.empty()) {
        return false;
    }
    for (const auto& record : records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            return false;
        }
    }
    return true;
}

void SplitL1RecordsByWindowReadability(
    const std::vector<SubtreeRecord>& records,
    std::vector<SubtreeRecord>& readable_records,
    std::vector<uint64_t>& unreadable_unique_blocks) {
    readable_records.clear();
    unreadable_unique_blocks.clear();
    readable_records.reserve(records.size());

    std::unordered_set<uint64_t> unreadable_blocks;
    unreadable_blocks.reserve(records.size());
    for (const auto& record : records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            if (window.kv_block_ptr != 0) {
                unreadable_blocks.insert(window.kv_block_ptr);
            }
            continue;
        }
        readable_records.push_back(record);
    }

    unreadable_unique_blocks.reserve(unreadable_blocks.size());
    for (const uint64_t block_ptr : unreadable_blocks) {
        unreadable_unique_blocks.push_back(block_ptr);
    }
    std::sort(unreadable_unique_blocks.begin(), unreadable_unique_blocks.end());
}

bool SubtreeRecordMinKeyLess(const SubtreeRecord& lhs, const SubtreeRecord& rhs) {
    const KeyType lhs_route_min = lhs.RouteMinKey();
    const KeyType rhs_route_min = rhs.RouteMinKey();
    const int min_cmp = CompareKeyType(lhs_route_min, rhs_route_min);
    if (min_cmp != 0) {
        return min_cmp < 0;
    }
    const KeyType lhs_route_max = lhs.RouteMaxKey();
    const KeyType rhs_route_max = rhs.RouteMaxKey();
    const int max_cmp = CompareKeyType(lhs_route_max, rhs_route_max);
    if (max_cmp != 0) {
        return max_cmp < 0;
    }
    if (lhs.seq_no != rhs.seq_no) {
        return lhs.seq_no > rhs.seq_no;
    }
    const auto lhs_window = lhs.LeafWindow();
    const auto rhs_window = rhs.LeafWindow();
    if (lhs_window.kv_block_ptr != rhs_window.kv_block_ptr) {
        return lhs_window.kv_block_ptr < rhs_window.kv_block_ptr;
    }
    if (lhs_window.offset != rhs_window.offset) {
        return lhs_window.offset < rhs_window.offset;
    }
    if (lhs_window.count != rhs_window.count) {
        return lhs_window.count < rhs_window.count;
    }
    return lhs.table_idx < rhs.table_idx;
}

bool SubtreeRecordWindowKeyLess(const SubtreeRecord& lhs, const SubtreeRecord& rhs) {
    if (lhs.route_prefix != rhs.route_prefix) {
        return lhs.route_prefix < rhs.route_prefix;
    }
    if (lhs.route_min_suffix != rhs.route_min_suffix) {
        return lhs.route_min_suffix < rhs.route_min_suffix;
    }
    if (lhs.route_max_suffix != rhs.route_max_suffix) {
        return lhs.route_max_suffix < rhs.route_max_suffix;
    }
    if (lhs.seq_no != rhs.seq_no) {
        return lhs.seq_no > rhs.seq_no;
    }
    const auto lhs_window = lhs.LeafWindow();
    const auto rhs_window = rhs.LeafWindow();
    if (lhs_window.kv_block_ptr != rhs_window.kv_block_ptr) {
        return lhs_window.kv_block_ptr < rhs_window.kv_block_ptr;
    }
    if (lhs_window.offset != rhs_window.offset) {
        return lhs_window.offset < rhs_window.offset;
    }
    if (lhs_window.count != rhs_window.count) {
        return lhs_window.count < rhs_window.count;
    }
    return lhs.table_idx < rhs.table_idx;
}

void NormalizeL1InputRecordsForMerge(std::vector<SubtreeRecord>& records) {
    if (records.size() <= 1) {
        return;
    }
    // Collapse duplicated route windows from different versions and keep newest seq.
    std::stable_sort(records.begin(), records.end(), SubtreeRecordWindowKeyLess);
    std::vector<SubtreeRecord> deduped;
    deduped.reserve(records.size());
    for (const auto& record : records) {
        if (deduped.empty()) {
            deduped.push_back(record);
            continue;
        }
        const SubtreeRecord& last = deduped.back();
        if (record.route_prefix == last.route_prefix &&
            record.route_min_suffix == last.route_min_suffix &&
            record.route_max_suffix == last.route_max_suffix) {
            continue;
        }
        deduped.push_back(record);
    }
    records.swap(deduped);
    std::stable_sort(records.begin(), records.end(), SubtreeRecordMinKeyLess);
}

const std::vector<TaggedPstMeta>* ResolveObsoleteL1TablesFromInputs(
    Version* version,
    const std::vector<TaggedPstMeta>& l1_inputs,
    const std::vector<SubtreeRecord>& l1_records,
    const std::vector<uint64_t>& l1_unique_blocks,
    std::vector<TaggedPstMeta>& resolved_out) {
    resolved_out.clear();
    static const std::vector<TaggedPstMeta> kEmpty;
    if (version == nullptr) {
        return &kEmpty;
    }

    if (!l1_inputs.empty()) {
        return &l1_inputs;
    }

    if (!l1_unique_blocks.empty()) {
        version->ResolveL1BlocksToTables(l1_unique_blocks, resolved_out);
    } else if (!l1_records.empty()) {
        version->ResolveL1RecordsToTables(l1_records, resolved_out);
    }
    return resolved_out.empty() ? &kEmpty : &resolved_out;
}

bool MaterializeOverlappedL1TablesFromRecords(Version* version,
                                              const KeyType& min,
                                              const KeyType& max,
                                              std::vector<TaggedPstMeta>& output) {
    output.clear();
    if (version == nullptr) {
        return false;
    }
    std::vector<SubtreeRecord> records;
    std::vector<uint64_t> unique_kv_block_ptrs;
    if (!version->PickOverlappedL1Records(min, max, records, unique_kv_block_ptrs)) {
        return false;
    }
    if (!unique_kv_block_ptrs.empty()) {
        version->ResolveL1BlocksToTables(unique_kv_block_ptrs, output);
    } else if (!records.empty()) {
        version->ResolveL1RecordsToTables(records, output);
    }
    return true;
}

struct WindowInterval {
    uint16_t begin = 0;
    uint16_t end = 0;  // exclusive
};

using DeleteCoverageMap = std::unordered_map<uint64_t, std::vector<WindowInterval>>;

DeleteCoverageMap BuildDeleteCoverageMap(const std::vector<SubtreeRecord>& delete_records) {
    DeleteCoverageMap coverage;
    coverage.reserve(delete_records.size());
    for (const auto& record : delete_records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        const uint32_t begin = window.offset;
        const uint32_t end = static_cast<uint32_t>(window.offset) + static_cast<uint32_t>(window.count);
        if (begin >= end || end > PDataBlock::MAX_ENTRIES) {
            continue;
        }
        coverage[window.kv_block_ptr].push_back(
            WindowInterval{static_cast<uint16_t>(begin), static_cast<uint16_t>(end)});
    }
    return coverage;
}

bool IsEntryRangeFullyCovered(uint16_t entry_num, std::vector<WindowInterval> intervals) {
    if (entry_num == 0) {
        return true;
    }
    if (intervals.empty()) {
        return false;
    }
    std::sort(intervals.begin(), intervals.end(),
              [](const WindowInterval& lhs, const WindowInterval& rhs) {
                  if (lhs.begin != rhs.begin) {
                      return lhs.begin < rhs.begin;
                  }
                  return lhs.end < rhs.end;
              });
    uint16_t covered = 0;
    for (const auto& interval : intervals) {
        if (interval.end <= covered) {
            continue;
        }
        if (interval.begin > covered) {
            return false;
        }
        covered = interval.end;
        if (covered >= entry_num) {
            return true;
        }
    }
    return covered >= entry_num;
}

std::vector<TaggedPstMeta> BuildDeletableL1Tables(
    const std::vector<TaggedPstMeta>& obsolete_l1_tables,
    const std::vector<SubtreeRecord>* delete_records,
    bool enable_coverage_guard) {
    if (obsolete_l1_tables.empty()) {
        return {};
    }
    if (!enable_coverage_guard) {
        return obsolete_l1_tables;
    }
    if (delete_records == nullptr) {
        return obsolete_l1_tables;
    }
    const DeleteCoverageMap coverage = BuildDeleteCoverageMap(*delete_records);
    std::vector<TaggedPstMeta> deletable;
    deletable.reserve(obsolete_l1_tables.size());
    for (const auto& table : obsolete_l1_tables) {
        if (!table.Valid()) {
            continue;
        }
        const uint64_t kv_block_ptr =
            SubtreeRecord::EncodeKvBlockPtr(table.meta.datablock_ptr_);
        const auto it = coverage.find(kv_block_ptr);
        if (it == coverage.end()) {
            continue;
        }
        if (!IsEntryRangeFullyCovered(table.meta.entry_num_, it->second)) {
            continue;
        }
        deletable.push_back(table);
    }
    return deletable;
}

struct DeltaSignature {
    RouteSuffix suffix_begin = 0;
    RouteSuffix suffix_end = 0;
    uint64_t kv_block_ptr = 0;
    uint16_t offset = 0;
    uint8_t count = 0;

    bool operator==(const DeltaSignature& rhs) const {
        return suffix_begin == rhs.suffix_begin && suffix_end == rhs.suffix_end &&
               kv_block_ptr == rhs.kv_block_ptr && offset == rhs.offset && count == rhs.count;
    }
};

struct DeltaSignatureHash {
    size_t operator()(const DeltaSignature& sig) const {
        const size_t h1 = std::hash<uint64_t>{}(sig.suffix_begin);
        const size_t h2 = std::hash<uint64_t>{}(sig.suffix_end);
        const size_t h3 = std::hash<uint64_t>{}(sig.kv_block_ptr);
        const size_t h4 = std::hash<uint16_t>{}(sig.offset);
        const size_t h5 = std::hash<uint8_t>{}(sig.count);
        size_t h = h1;
        h ^= (h2 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        h ^= (h3 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        h ^= (h4 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        h ^= (h5 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        return h;
    }
};

struct DeltaRangeKey {
    RouteSuffix suffix_begin = 0;
    RouteSuffix suffix_end = 0;

    bool operator==(const DeltaRangeKey& rhs) const {
        return suffix_begin == rhs.suffix_begin && suffix_end == rhs.suffix_end;
    }
};

struct DeltaRangeKeyHash {
    size_t operator()(const DeltaRangeKey& key) const {
        const size_t h1 = std::hash<uint64_t>{}(key.suffix_begin);
        const size_t h2 = std::hash<uint64_t>{}(key.suffix_end);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};

int DeltaTypeRank(L1DeltaOpType type) {
    switch (type) {
        case L1DeltaOpType::kDelete:
            return 0;
        case L1DeltaOpType::kReplace:
            return 1;
        case L1DeltaOpType::kAdd:
            return 2;
    }
    return 3;
}

void NormalizePrefixOps(std::vector<L1DeltaOp>& ops) {
    if (ops.empty()) {
        return;
    }

    std::unordered_map<DeltaSignature, std::deque<L1DeltaOp>, DeltaSignatureHash> add_by_sig;
    std::unordered_map<DeltaSignature, std::deque<L1DeltaOp>, DeltaSignatureHash> del_by_sig;
    std::vector<L1DeltaOp> passthrough;
    passthrough.reserve(ops.size());

    for (const auto& op : ops) {
        DeltaSignature sig{op.suffix_begin, op.suffix_end, op.kv_block_ptr, op.offset, op.count};
        if (op.type == L1DeltaOpType::kAdd) {
            add_by_sig[sig].push_back(op);
        } else if (op.type == L1DeltaOpType::kDelete) {
            del_by_sig[sig].push_back(op);
        } else {
            passthrough.push_back(op);
        }
    }

    // Exact add/delete cancellation.
    for (auto& [sig, adds] : add_by_sig) {
        auto del_it = del_by_sig.find(sig);
        if (del_it == del_by_sig.end()) {
            continue;
        }
        auto& dels = del_it->second;
        const size_t paired = std::min(adds.size(), dels.size());
        for (size_t i = 0; i < paired; ++i) {
            adds.pop_back();
            dels.pop_back();
        }
    }

    // Range-level replace conversion: delete(old) + add(new) on the same suffix window.
    std::unordered_map<DeltaRangeKey, std::deque<L1DeltaOp>, DeltaRangeKeyHash> add_by_range;
    std::unordered_map<DeltaRangeKey, std::deque<L1DeltaOp>, DeltaRangeKeyHash> del_by_range;
    for (auto& [sig, adds] : add_by_sig) {
        const DeltaRangeKey range{sig.suffix_begin, sig.suffix_end};
        while (!adds.empty()) {
            add_by_range[range].push_back(adds.front());
            adds.pop_front();
        }
    }
    for (auto& [sig, dels] : del_by_sig) {
        const DeltaRangeKey range{sig.suffix_begin, sig.suffix_end};
        while (!dels.empty()) {
            del_by_range[range].push_back(dels.front());
            dels.pop_front();
        }
    }

    std::vector<L1DeltaOp> normalized;
    normalized.reserve(ops.size());
    normalized.insert(normalized.end(), passthrough.begin(), passthrough.end());

    std::unordered_map<DeltaRangeKey, bool, DeltaRangeKeyHash> all_ranges;
    all_ranges.reserve(add_by_range.size() + del_by_range.size());
    for (const auto& [range, _] : add_by_range) {
        all_ranges.emplace(range, true);
    }
    for (const auto& [range, _] : del_by_range) {
        all_ranges.emplace(range, true);
    }

    for (const auto& [range, _] : all_ranges) {
        auto add_it = add_by_range.find(range);
        auto del_it = del_by_range.find(range);
        std::deque<L1DeltaOp>* adds = add_it == add_by_range.end() ? nullptr : &add_it->second;
        std::deque<L1DeltaOp>* dels = del_it == del_by_range.end() ? nullptr : &del_it->second;
        while (adds != nullptr && dels != nullptr && !adds->empty() && !dels->empty()) {
            // Replace carries the new payload (from add op).
            L1DeltaOp replace = adds->front();
            replace.type = L1DeltaOpType::kReplace;
            normalized.push_back(replace);
            adds->pop_front();
            dels->pop_front();
        }
        if (dels != nullptr) {
            while (!dels->empty()) {
                normalized.push_back(dels->front());
                dels->pop_front();
            }
        }
        if (adds != nullptr) {
            while (!adds->empty()) {
                normalized.push_back(adds->front());
                adds->pop_front();
            }
        }
    }

    std::sort(normalized.begin(), normalized.end(),
              [](const L1DeltaOp& lhs, const L1DeltaOp& rhs) {
                  if (lhs.suffix_begin != rhs.suffix_begin) {
                      return lhs.suffix_begin < rhs.suffix_begin;
                  }
                  if (lhs.suffix_end != rhs.suffix_end) {
                      return lhs.suffix_end < rhs.suffix_end;
                  }
                  const int lt = DeltaTypeRank(lhs.type);
                  const int rt = DeltaTypeRank(rhs.type);
                  if (lt != rt) {
                      return lt < rt;
                  }
                  if (lhs.kv_block_ptr != rhs.kv_block_ptr) {
                      return lhs.kv_block_ptr < rhs.kv_block_ptr;
                  }
                  if (lhs.offset != rhs.offset) {
                      return lhs.offset < rhs.offset;
                  }
                  return lhs.count < rhs.count;
              });
    ops.swap(normalized);
}

void NormalizeDeltaBatch(L1DeltaBatch& batch) {
    std::vector<L1PrefixDelta> compacted;
    compacted.reserve(batch.deltas.size());
    for (auto& prefix_delta : batch.deltas) {
        NormalizePrefixOps(prefix_delta.ops);
        if (prefix_delta.ops.empty()) {
            continue;
        }
        compacted.push_back(std::move(prefix_delta));
    }
    batch.deltas = std::move(compacted);
}

void AppendDeltaFromRecord(const SubtreeRecord& record,
                           L1DeltaOpType op_type,
                           std::vector<L1PrefixDelta>& deltas,
                           std::unordered_map<RoutePrefix, size_t>& index_by_prefix) {
    if (record.route_min_suffix > record.route_max_suffix) {
        return;
    }
    const auto leaf_window = record.LeafWindow();
    if (op_type == L1DeltaOpType::kAdd && leaf_window.count == 0) {
        return;
    }
    L1PrefixDelta& prefix_delta = EnsurePrefixDelta(deltas, index_by_prefix, record.route_prefix);
    L1DeltaOp op;
    op.type = op_type;
    op.suffix_begin = record.route_min_suffix;
    op.suffix_end = record.route_max_suffix;
    if (leaf_window.count != 0) {
        op.kv_block_ptr = leaf_window.kv_block_ptr;
        op.offset = leaf_window.offset;
        op.count = leaf_window.count;
    } else {
        // For delete ops, zero kv_block_ptr means range-level delete fallback.
        op.kv_block_ptr = 0;
        op.offset = 0;
        op.count = 0;
    }
    prefix_delta.ops.push_back(op);
}

void AppendDeltaFromTable(const TaggedPstMeta& pst,
                          L1DeltaOpType op_type,
                          std::vector<L1PrefixDelta>& deltas,
                          std::unordered_map<RoutePrefix, size_t>& index_by_prefix,
                          SegmentAllocator* seg_allocator) {
    if (!pst.Valid()) {
        return;
    }
    const auto& table = pst.meta;
    const uint64_t kv_block_ptr = SubtreeRecord::EncodeKvBlockPtr(table.datablock_ptr_);
    std::vector<PrefixWindow> windows;
    if (!BuildPrefixWindowsFromTable(pst, seg_allocator, windows)) {
        AppendDeltaRangeFallback(pst, op_type, deltas, index_by_prefix);
        return;
    }
    for (const auto& window : windows) {
        L1PrefixDelta& prefix_delta = EnsurePrefixDelta(deltas, index_by_prefix, window.prefix);
        L1DeltaOp op;
        op.type = op_type;
        op.kv_block_ptr = kv_block_ptr;
        op.offset = window.offset;
        op.count = window.count;
        op.suffix_begin = window.suffix_begin;
        op.suffix_end = window.suffix_end;
        prefix_delta.ops.push_back(op);
    }
}

L1DeltaBatch BuildL1DeltaBatchFromRecords(const std::vector<SubtreeRecord>& add_records,
                                          const std::vector<SubtreeRecord>& delete_records,
                                          uint64_t batch_id,
                                          uint32_t l1_seq) {
    L1DeltaBatch batch;
    batch.batch_id = batch_id;
    batch.l1_seq = l1_seq;
    std::unordered_map<RoutePrefix, size_t> index_by_prefix;
    index_by_prefix.reserve(add_records.size() + delete_records.size());
    for (const auto& record : add_records) {
        AppendDeltaFromRecord(record, L1DeltaOpType::kAdd, batch.deltas, index_by_prefix);
    }
    for (const auto& record : delete_records) {
        AppendDeltaFromRecord(record, L1DeltaOpType::kDelete, batch.deltas, index_by_prefix);
    }
    batch.SortAndUniquePrefixes();
    NormalizeDeltaBatch(batch);
    return batch;
}

L1DeltaBatch BuildL1DeltaBatchFromTables(const std::vector<TaggedPstMeta>& add_tables,
                                         const std::vector<TaggedPstMeta>& delete_tables,
                                         SegmentAllocator* seg_allocator,
                                         uint64_t batch_id,
                                         uint32_t l1_seq) {
    L1DeltaBatch batch;
    batch.batch_id = batch_id;
    batch.l1_seq = l1_seq;
    std::unordered_map<RoutePrefix, size_t> index_by_prefix;
    index_by_prefix.reserve(add_tables.size() + delete_tables.size());
    for (const auto& table : add_tables) {
        AppendDeltaFromTable(table, L1DeltaOpType::kAdd, batch.deltas, index_by_prefix, seg_allocator);
    }
    for (const auto& table : delete_tables) {
        AppendDeltaFromTable(table, L1DeltaOpType::kDelete, batch.deltas, index_by_prefix, seg_allocator);
    }
    batch.SortAndUniquePrefixes();
    NormalizeDeltaBatch(batch);
    return batch;
}

std::vector<TaggedPstMeta> GatherSubCompactionOutputs(
    const std::vector<TaggedPstMeta> (&partition_outputs)[RANGE_PARTITION_NUM]) {
    size_t total = 0;
    for (int i = 0; i < RANGE_PARTITION_NUM; ++i) {
        total += partition_outputs[i].size();
    }
    std::vector<TaggedPstMeta> merged;
    merged.reserve(total);
    for (int i = 0; i < RANGE_PARTITION_NUM; ++i) {
        merged.insert(merged.end(), partition_outputs[i].begin(), partition_outputs[i].end());
    }
    return merged;
}

}  // namespace

CompactionJob::CompactionJob(SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest, PartitionInfo *partition_info, ThreadPoolImpl *thread_pool) : seg_allocater_(seg_alloc), version_(target_version), manifest_(manifest), pst_builder_(seg_allocater_), pst_deleter_(seg_allocater_), output_seq_no_(version_->GenerateL1Seq()), use_l1_range_scan_records_(IsL1RangeScanRecordsEnabled()), use_l1_delete_covered_only_(IsL1DeleteCoveredOnlyEnabled()), partition_info_(partition_info), compaction_thread_pool_(thread_pool)
{
}
CompactionJob::~CompactionJob()
{
	outputs_.clear();
	std::vector<TaggedPstMeta>().swap(outputs_);
	add_patch_records_.clear();
	std::vector<flowkv::hybrid_l1::SubtreeRecord>().swap(add_patch_records_);
	for (int i = 0; i < RANGE_PARTITION_NUM; ++i) {
		partition_add_patch_records_[i].clear();
		std::vector<flowkv::hybrid_l1::SubtreeRecord>().swap(partition_add_patch_records_[i]);
	}
	inputs_l1_records_.clear();
	std::vector<flowkv::hybrid_l1::SubtreeRecord>().swap(inputs_l1_records_);
	inputs_l1_unique_blocks_.clear();
	std::vector<uint64_t>().swap(inputs_l1_unique_blocks_);
	for (auto &list : inputs_)
	{
		list.clear();
		std::vector<TaggedPstMeta>().swap(list);
	}
	std::vector<std::vector<TaggedPstMeta>>().swap(inputs_);
}
bool CompactionJob::CheckPmRoomEnough()
{
	// TODO
	return true;
}
size_t CompactionJob::PickCompaction()
{
	// iterate l0 meta
	std::vector<TreeMeta> tree_metas;
	add_patch_records_.clear();
	for (int i = 0; i < RANGE_PARTITION_NUM; ++i) {
		partition_add_patch_records_[i].clear();
	}
	inputs_l1_records_.clear();
	inputs_l1_unique_blocks_.clear();
	force_serial_compaction_ = false;
	version_->PickLevel0Trees(inputs_, tree_metas);
	size_t size = inputs_.size();
	if (size == 0)
		return 0;
	LOG("pick %lu level 0 tree", size);
	for (auto &tree_meta : tree_metas)
	{
		if (KeyTypeGreater(min_key_, tree_meta.min_key))
		{
			min_key_ = tree_meta.min_key;
		}
		if (KeyTypeLess(max_key_, tree_meta.max_key))
		{
			max_key_ = tree_meta.max_key;
		}
		#if defined(FLOWKV_KEY16)
		LOG("min:%lu:%lu,max:%lu:%lu", tree_meta.min_key.hi, tree_meta.min_key.lo, tree_meta.max_key.hi, tree_meta.max_key.lo);
		#else
		LOG("min:%lu,max:%lu", __bswap_64(tree_meta.min_key), __bswap_64(tree_meta.max_key));
		#endif
	}
	#if defined(FLOWKV_KEY16)
	LOG("l0 table range=%lu:%lu~%lu:%lu", min_key_.hi, min_key_.lo, max_key_.hi, max_key_.lo);
	#else
	LOG("l0 table range=%lu~%lu", __bswap_64(min_key_), __bswap_64(max_key_));
	#endif
	inputs_.emplace_back(std::vector<TaggedPstMeta>());
	if (use_l1_range_scan_records_) {
		if (!version_->PickOverlappedL1Records(min_key_,
			                                      max_key_,
			                                      inputs_l1_records_,
			                                      inputs_l1_unique_blocks_)) {
			MaterializeOverlappedL1TablesFromRecords(version_, min_key_, max_key_, inputs_[size]);
		} else {
			NormalizeL1InputRecordsForMerge(inputs_l1_records_);
			if (!AreAllRecordsWindowReadable(inputs_l1_records_)) {
				std::vector<SubtreeRecord> readable_records;
				std::vector<uint64_t> unreadable_unique_blocks;
				SplitL1RecordsByWindowReadability(inputs_l1_records_,
				                                readable_records,
				                                unreadable_unique_blocks);
				LOG("L1 range-record scan mixed fallback: total_records=%lu readable_records=%lu unreadable_blocks=%lu",
				    inputs_l1_records_.size(),
				    readable_records.size(),
				    unreadable_unique_blocks.size());
				inputs_l1_records_.swap(readable_records);
				if (!inputs_l1_records_.empty()) {
					inputs_l1_unique_blocks_ = CollectUniqueBlocksFromRecords(inputs_l1_records_);
				} else {
					inputs_l1_unique_blocks_.clear();
				}
				inputs_[size].clear();
				if (!unreadable_unique_blocks.empty()) {
					version_->ResolveL1BlocksToTables(unreadable_unique_blocks, inputs_[size]);
				}
				if (inputs_l1_records_.empty() && inputs_[size].empty()) {
					LOG("L1 range-record scan fallback produced no usable L1 input");
				}
			} else {
				if (inputs_l1_unique_blocks_.empty()) {
					inputs_l1_unique_blocks_ = CollectUniqueBlocksFromRecords(inputs_l1_records_);
				}
				inputs_[size].clear();
				LOG("L1 range-record scan enabled(record-only): windows=%lu, unique_blocks=%lu",
				    inputs_l1_records_.size(),
				    inputs_l1_unique_blocks_.size());
			}
		}
	} else {
		MaterializeOverlappedL1TablesFromRecords(version_, min_key_, max_key_, inputs_[size]);
	}
	if (use_l1_range_scan_records_ && !inputs_l1_records_.empty() && !inputs_[size].empty()) {
		force_serial_compaction_ = true;
		LOG("L1 mixed-mode input detected: forcing serial compaction merge "
		    "(readable_records=%lu materialized_tables=%lu)",
		    inputs_l1_records_.size(),
		    inputs_[size].size());
	}
	if (inputs_[size].size())
	{
		KeyType l1_min = inputs_[size][0].meta.MinKey();
		KeyType l1_max = inputs_[size][inputs_[size].size() - 1].meta.MaxKey();
		#if defined(FLOWKV_KEY16)
		LOG("level1 tables:%lu %lu:%lu~%lu:%lu", inputs_[size].size(), l1_min.hi, l1_min.lo, l1_max.hi, l1_max.lo);
		#else
		LOG("level1 tables:%lu %lu~%lu", inputs_[size].size(), __bswap_64(l1_min), __bswap_64(l1_max));
		#endif
	} else if (!inputs_l1_records_.empty()) {
		LOG("level1 record inputs:%lu windows, %lu unique blocks (table materialization deferred)",
		    inputs_l1_records_.size(),
		    inputs_l1_unique_blocks_.size());
	}
	return size;
}
bool CompactionJob::RunCompaction()
{
	std::priority_queue<KeyWithRowId, std::vector<KeyWithRowId>, UintKeyComparator> key_heap(cmp);
	std::vector<RowIterator> table_rows;
	std::vector<RecordIterator> record_rows;
	std::deque<std::vector<TaggedPstMeta>> split_l1_table_rows;
	std::vector<SubtreeRecord> partition_l1_records;
	std::vector<PSTReader *> readers;
	struct InputSource {
		bool is_record = false;
		size_t index = 0;
	};
	std::vector<InputSource> sources;
	size_t marked_output = 0;
	add_patch_records_.clear();
	StepCPatchRecorder add_patch_recorder;

	const bool has_l1_record_input =
		use_l1_range_scan_records_ && !inputs_l1_records_.empty() &&
		AreAllRecordsWindowReadable(inputs_l1_records_);
	const int l1_bucket_idx = inputs_.empty() ? -1 : static_cast<int>(inputs_.size() - 1);

	for (int i = 0; i < static_cast<int>(inputs_.size()); i++)
	{
		if (inputs_[i].empty()) {
			if (!(has_l1_record_input && i == l1_bucket_idx)) {
				continue;
			}
		}
		if (has_l1_record_input && i == l1_bucket_idx && inputs_[i].empty()) {
			// Record-only L1 execution: when readable windows exist, do not materialize
			// the L1 input into a table list for the merge loop.
			continue;
		}
		if (has_l1_record_input && i == l1_bucket_idx) {
			std::vector<TaggedPstMeta> split_tables = inputs_[i];
			std::stable_sort(split_tables.begin(), split_tables.end(),
			                 [](const TaggedPstMeta& lhs, const TaggedPstMeta& rhs) {
				                 if (lhs.meta.seq_no_ != rhs.meta.seq_no_) {
					                 return lhs.meta.seq_no_ > rhs.meta.seq_no_;
				                 }
				                 const int min_cmp = CompareKeyType(lhs.meta.MinKey(), rhs.meta.MinKey());
				                 if (min_cmp != 0) {
					                 return min_cmp < 0;
				                 }
				                 const int max_cmp = CompareKeyType(lhs.meta.MaxKey(), rhs.meta.MaxKey());
				                 if (max_cmp != 0) {
					                 return max_cmp < 0;
				                 }
				                 return lhs.meta.datablock_ptr_ < rhs.meta.datablock_ptr_;
			                 });
			for (const auto& table : split_tables) {
				split_l1_table_rows.emplace_back();
				split_l1_table_rows.back().push_back(table);
				auto pr = new PSTReader(seg_allocater_);
				readers.push_back(pr);
				table_rows.emplace_back(pr, split_l1_table_rows.back());
				table_rows.back().ResetPstIter();
				sources.push_back(InputSource{false, table_rows.size() - 1});
			}
			continue;
		}
		auto pr = new PSTReader(seg_allocater_);
		readers.push_back(pr);
		table_rows.emplace_back(pr, inputs_[i]);
		sources.push_back(InputSource{false, table_rows.size() - 1});
	}
	if (has_l1_record_input)
	{
		auto pr = new PSTReader(seg_allocater_);
		readers.push_back(pr);
		record_rows.emplace_back(pr, inputs_l1_records_);
		if (record_rows.back().Valid()) {
			sources.push_back(InputSource{true, record_rows.size() - 1});
		}
	}

	auto SourceValid = [&](int source_id) -> bool {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].Valid();
		}
		return table_rows[source.index].Valid();
	};
	auto SourceCurrentKey = [&](int source_id) -> KeyType {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].GetCurrentKey();
		}
		return table_rows[source.index].GetCurrentKey();
	};
	auto SourceCurrentValue = [&](int source_id) -> FixedValue16 {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].GetCurrentValue();
		}
		return table_rows[source.index].GetCurrentValue();
	};
	auto SourceNextKey = [&](int source_id) -> bool {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].NextKey();
		}
		return table_rows[source.index].NextKey();
	};
	auto PushSourceToHeapIfValid = [&](int source_id) {
		if (!SourceValid(source_id)) {
			return;
		}
		key_heap.push(KeyWithRowId{SourceCurrentKey(source_id), source_id});
	};
	auto AppendEntry = [&](const KeyType& key, const FixedValue16& value) {
		if (!AddEntryToBuilder(&pst_builder_, key, value))
		{
			const PSTMeta meta = pst_builder_.Flush();
			TaggedPstMeta flushed;
			flushed.meta = meta;
			add_patch_recorder.EmitAndReset(meta, add_patch_records_);
			outputs_.emplace_back(flushed);
			if (!AddEntryToBuilder(&pst_builder_, key, value)) {
				ERROR_EXIT("cannot add pst entry in compaction");
			}
		}
		add_patch_recorder.AddKey(key);
	};

	for (int source_id = 0; source_id < static_cast<int>(sources.size()); ++source_id) {
		PushSourceToHeapIfValid(source_id);
	}

	size_t count = 0;
	#if defined(FLOWKV_KEY16)
	KeyWithRowId current_max_key = {Key16{0, 0}, -1};
	#else
	KeyWithRowId current_max_key = {0, -1};
	#endif
	// Get key from heap and get the value from row
	while (!key_heap.empty())
	{
		auto topkey = key_heap.top();
		key_heap.pop();
		count++;
		if (unlikely(CompareKeyType(topkey.key, current_max_key.key) < 0))
		{
			const InputSource& source = sources[topkey.row_id];
			#if defined(FLOWKV_KEY16)
			if (source.is_record) {
				ERROR_EXIT("Reverse order found in Compaction: %lu:%lu(%d)<%lu:%lu(%d), source=record[%zu]",
				           topkey.key.hi, topkey.key.lo, topkey.row_id,
				           current_max_key.key.hi, current_max_key.key.lo, current_max_key.row_id,
				           source.index);
			}
			if (!source.is_record) {
				RowIterator &row = table_rows[source.index];
				const auto pst = row.GetPst();
				auto pr = readers[source.index];
				auto *dbg_iter = pr == nullptr ? nullptr : pr->GetIterator(pst.meta.datablock_ptr_);
				uint64_t k0_hi = 0, k0_lo = 0, k1_hi = 0, k1_lo = 0, k2_hi = 0, k2_lo = 0;
				size_t dbg_size = 0;
				if (dbg_iter != nullptr) {
					dbg_size = dbg_iter->RecordsSize();
					if (dbg_size > 0) {
						auto k = dbg_iter->Key();
						k0_hi = k.hi;
						k0_lo = k.lo;
					}
					if (dbg_size > 1) {
						dbg_iter->current_record_index_ = 1;
						auto k = dbg_iter->Key();
						k1_hi = k.hi;
						k1_lo = k.lo;
					}
					if (dbg_size > 2) {
						dbg_iter->current_record_index_ = 2;
						auto k = dbg_iter->Key();
						k2_hi = k.hi;
						k2_lo = k.lo;
					}
					delete dbg_iter;
				}
				ERROR_EXIT("Reverse order found in Compaction: %lu:%lu(%d)<%lu:%lu(%d), source=table[%zu], pst_idx=%d, datablock_ptr=%lu, min=%lu:%lu, max=%lu:%lu, entry_num=%u, dbg_size=%lu, first_keys=%lu:%lu|%lu:%lu|%lu:%lu",
				           topkey.key.hi, topkey.key.lo, topkey.row_id,
				           current_max_key.key.hi, current_max_key.key.lo, current_max_key.row_id,
				           source.index, row.current_pst_idx_, pst.meta.datablock_ptr_,
				           pst.meta.min_key_hi, pst.meta.min_key_lo,
				           pst.meta.max_key_hi, pst.meta.max_key_lo,
				           pst.meta.entry_num_, dbg_size,
				           k0_hi, k0_lo, k1_hi, k1_lo, k2_hi, k2_lo);
			}
			#else
			if (source.is_record) {
				ERROR_EXIT("Reverse order found in Compaction: %lu(%d)<%lu(%d), source=record[%zu]",
				           __bswap_64(topkey.key), topkey.row_id,
				           __bswap_64(current_max_key.key), current_max_key.row_id,
				           source.index);
			}
			ERROR_EXIT("Reverse order found in Compaction: %lu(%d)<%lu(%d), source=table[%zu], pst_idx=%d",
			           __bswap_64(topkey.key), topkey.row_id,
			           __bswap_64(current_max_key.key), current_max_key.row_id,
			           source.index, table_rows[source.index].current_pst_idx_);
			#endif
		}
		current_max_key = topkey;
		while (!key_heap.empty() && key_heap.top().key == topkey.key)
		{
			#if defined(FLOWKV_KEY16)
			DEBUG("重合key %lu:%lu from row %d with row %d", topkey.key.hi, topkey.key.lo, topkey.row_id, key_heap.top().row_id);
			#else
			DEBUG("重合key %lu from row %d with row %d", __bswap_64(topkey.key), topkey.row_id, key_heap.top().row_id);
			#endif
			// 如果出现重合key，旧key直接next
			if (SourceNextKey(topkey.row_id)) {
				PushSourceToHeapIfValid(topkey.row_id);
			}
			topkey = key_heap.top();
			key_heap.pop();
		}

		const InputSource& source = sources[topkey.row_id];
		if (!source.is_record)
		{
			RowIterator &row = table_rows[source.index];
			bool is_overlapped = 0;
			if (row.pst_iter_ == nullptr)
			{
				// the key is the first key in a pst, now check if the pst is overlapped with other inputs
				KeyType max = row.GetPst().meta.MaxKey();
				for (int i = 0; i < static_cast<int>(sources.size()); i++)
				{
					if (i == topkey.row_id) {
						continue;
					}
					if (!SourceValid(i)) {
						continue;
					}
					if (KeyTypeLess(SourceCurrentKey(i), max))
					{
						is_overlapped = 1;
						break;
					}
				}
				if (!is_overlapped)
				{
					#if defined(FLOWKV_KEY16)
					LOG("jump,topkey=%lu:%lu,max=%lu:%lu, is overlapped=%d", topkey.key.hi, topkey.key.lo, max.hi, max.lo, is_overlapped);
					#else
					LOG("jump,topkey=%lu,max=%lu, is overlapped=%d", __bswap_64(topkey.key), __bswap_64(max), is_overlapped);
					#endif
					// not overlapped: directly reuse the whole pst as output
					TaggedPstMeta flushed;
					flushed.meta = pst_builder_.Flush();
					add_patch_recorder.EmitAndReset(flushed.meta, add_patch_records_);
					outputs_.emplace_back(flushed);
					TaggedPstMeta reused = row.GetPst();
					row.MarkPst();
					marked_output++;
					outputs_.emplace_back(reused);
					if (row.NextPst()) {
						PushSourceToHeapIfValid(topkey.row_id);
					}
					continue;
				}

				// overlapped: read the pst and switch to key-wise merge.
				row.ResetPstIter();
			}
			const KeyType key = row.GetCurrentKey();
			assert(key == topkey.key);
			const FixedValue16 value = row.GetCurrentValue();
			AppendEntry(key, value);
		}
		else
		{
			const KeyType key = SourceCurrentKey(topkey.row_id);
			const FixedValue16 value = SourceCurrentValue(topkey.row_id);
			AppendEntry(key, value);
		}

		if (SourceNextKey(topkey.row_id))
		{
			PushSourceToHeapIfValid(topkey.row_id);
		}
	}
	auto meta = pst_builder_.Flush();
	add_patch_recorder.EmitAndReset(meta, add_patch_records_);
	// add meta into manifest
	TaggedPstMeta tmeta;
	tmeta.meta = meta;
	outputs_.emplace_back(tmeta);
	DEBUG("output=%lu,marked=%lu,rewrite key num=%lu", outputs_.size(), marked_output, count);
	pst_builder_.PersistCheckpoint();
	table_rows.clear();
	record_rows.clear();
	for (auto &pr : readers)
	{
		delete pr;
	}
	return true;
}
struct SubCompactionArgs
{
	CompactionJob *cj_;
	int partition_id_;
	SubCompactionArgs(CompactionJob *cj,int partition_id) : cj_(cj),partition_id_(partition_id) {}
};
bool CompactionJob::RunSubCompactionParallel()
{
	for (int i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		SubCompactionArgs *sca = new SubCompactionArgs(this,i);
		// printf("schedule %d\n",sca->partition_id_);
		compaction_thread_pool_->Schedule(&CompactionJob::TriggerSubCompaction,sca,sca,nullptr);
	}
	compaction_thread_pool_->WaitForJobsAndJoinAllThreads();
	return true;
}

void CompactionJob::RunSubCompaction(int partition_id)
{
	// DEBUG2("sub compaction %d", partition_id);
	PSTBuilder *pst_builder = partition_pst_builder_[partition_id] = new PSTBuilder(seg_allocater_);
	partition_add_patch_records_[partition_id].clear();
	StepCPatchRecorder add_patch_recorder;
	std::priority_queue<KeyWithRowId, std::vector<KeyWithRowId>, UintKeyComparator> key_heap(cmp);
	std::vector<RowIterator> table_rows;
	std::vector<RecordIterator> record_rows;
	std::deque<std::vector<TaggedPstMeta>> split_l1_table_rows;
	std::vector<SubtreeRecord> partition_l1_records;
	std::vector<PSTReader *> readers;
	struct InputSource {
		bool is_record = false;
		size_t index = 0;
	};
	std::vector<InputSource> sources;
	size_t marked_output = 0;
	PartitionInfo &partition = partition_info_[partition_id];

	const bool has_l1_record_input =
		use_l1_range_scan_records_ && !inputs_l1_records_.empty() &&
		AreAllRecordsWindowReadable(inputs_l1_records_);
	const int l1_bucket_idx = inputs_.empty() ? -1 : static_cast<int>(inputs_.size() - 1);

	// initialize: init iterators, move to partition min key, push first key to heap
	for (int i = 0; i < static_cast<int>(inputs_.size()); i++)
	{
		if (inputs_[i].empty()) {
			if (!(has_l1_record_input && i == l1_bucket_idx)) {
				continue;
			}
		}
		if (has_l1_record_input && i == l1_bucket_idx && inputs_[i].empty()) {
			continue;
		}
		if (has_l1_record_input && i == l1_bucket_idx) {
			std::vector<TaggedPstMeta> split_tables = inputs_[i];
			std::stable_sort(split_tables.begin(), split_tables.end(),
			                 [](const TaggedPstMeta& lhs, const TaggedPstMeta& rhs) {
				                 if (lhs.meta.seq_no_ != rhs.meta.seq_no_) {
					                 return lhs.meta.seq_no_ > rhs.meta.seq_no_;
				                 }
				                 const int min_cmp = CompareKeyType(lhs.meta.MinKey(), rhs.meta.MinKey());
				                 if (min_cmp != 0) {
					                 return min_cmp < 0;
				                 }
				                 const int max_cmp = CompareKeyType(lhs.meta.MaxKey(), rhs.meta.MaxKey());
				                 if (max_cmp != 0) {
					                 return max_cmp < 0;
				                 }
				                 return lhs.meta.datablock_ptr_ < rhs.meta.datablock_ptr_;
			                 });
			for (const auto& table : split_tables) {
				split_l1_table_rows.emplace_back();
				split_l1_table_rows.back().push_back(table);
				auto pr = new PSTReader(seg_allocater_);
				readers.push_back(pr);
				table_rows.emplace_back(pr, split_l1_table_rows.back());
				if (!table_rows.back().MoveTo(partition.min_key)) {
					continue;
				}
				sources.push_back(InputSource{false, table_rows.size() - 1});
			}
			continue;
		}
		auto pr = new PSTReader(seg_allocater_);
		readers.push_back(pr);
		table_rows.emplace_back(pr, inputs_[i]);
		if (!table_rows.back().MoveTo(partition.min_key)) {
			continue;
		}
		sources.push_back(InputSource{false, table_rows.size() - 1});
	}
	if (has_l1_record_input)
	{
		partition_l1_records.reserve(inputs_l1_records_.size() / RANGE_PARTITION_NUM + 16);
		for (const auto& record : inputs_l1_records_) {
			if (KeyTypeLess(record.RouteMaxKey(), partition.min_key)) {
				continue;
			}
			if (KeyTypeLess(partition.max_key, record.RouteMinKey())) {
				break;
			}
			partition_l1_records.push_back(record);
		}
		auto pr = new PSTReader(seg_allocater_);
		readers.push_back(pr);
		record_rows.emplace_back(pr, partition_l1_records);
		if (record_rows.back().Valid()) {
			sources.push_back(InputSource{true, record_rows.size() - 1});
		}
	}

	auto SourceValid = [&](int source_id) -> bool {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].Valid();
		}
		return table_rows[source.index].Valid();
	};
	auto SourceCurrentKey = [&](int source_id) -> KeyType {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].GetCurrentKey();
		}
		return table_rows[source.index].GetCurrentKey();
	};
	auto SourceCurrentValue = [&](int source_id) -> FixedValue16 {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].GetCurrentValue();
		}
		return table_rows[source.index].GetCurrentValue();
	};
	auto SourceNextKey = [&](int source_id) -> bool {
		const InputSource& source = sources[source_id];
		if (source.is_record) {
			return record_rows[source.index].NextKey();
		}
		return table_rows[source.index].NextKey();
	};
	auto SourceWithinPartition = [&](int source_id) -> bool {
		if (!SourceValid(source_id)) {
			return false;
		}
		return KeyTypeLessEq(SourceCurrentKey(source_id), partition.max_key);
	};
	auto PushSourceToHeapIfValid = [&](int source_id) {
		if (!SourceWithinPartition(source_id)) {
			return;
		}
		key_heap.push(KeyWithRowId{SourceCurrentKey(source_id), source_id});
	};
	auto AppendEntry = [&](const KeyType& key, const FixedValue16& value) {
		if (!AddEntryToBuilder(pst_builder, key, value))
		{
			const PSTMeta meta = pst_builder->Flush();
			TaggedPstMeta flushed;
			flushed.meta = meta;
			add_patch_recorder.EmitAndReset(meta, partition_add_patch_records_[partition_id]);
			partition_outputs_[partition_id].emplace_back(flushed);
			if (!AddEntryToBuilder(pst_builder, key, value)) {
				ERROR_EXIT("cannot add pst entry in compaction");
			}
		}
		add_patch_recorder.AddKey(key);
	};

	for (int source_id = 0; source_id < static_cast<int>(sources.size()); ++source_id) {
		PushSourceToHeapIfValid(source_id);
	}
	size_t count = 0;
	#if defined(FLOWKV_KEY16)
	KeyWithRowId current_max_key = {Key16{0, 0}, -1};
	#else
	KeyWithRowId current_max_key = {0, -1};
	#endif

	// Debug: check PST list order for each row
	for (int i = 0; i < static_cast<int>(table_rows.size()); i++) {
		auto &pst_list = table_rows[i].pst_list_;
		for (size_t j = 1; j < pst_list.size(); j++) {
			if (KeyTypeLess(pst_list[j].meta.MaxKey(), pst_list[j-1].meta.MaxKey())) {
				#if defined(FLOWKV_KEY16)
				ERROR_EXIT("PST list out of order for row %d: PST[%zu].MaxKey=%lu:%lu < PST[%zu].MaxKey=%lu:%lu",
					i, j, pst_list[j].meta.MaxKey().hi, pst_list[j].meta.MaxKey().lo,
					j-1, pst_list[j-1].meta.MaxKey().hi, pst_list[j-1].meta.MaxKey().lo);
				#else
				ERROR_EXIT("PST list out of order for row %d: PST[%zu].MaxKey < PST[%zu].MaxKey", i, j, j-1);
				#endif
			}
		}
	}

	// Get key from heap and get the value from row
	while (!key_heap.empty())
	{
		auto topkey = key_heap.top();
		key_heap.pop();
		count++;
		if (unlikely(CompareKeyType(topkey.key, current_max_key.key) < 0))
		{
			const InputSource& source = sources[topkey.row_id];
			#if defined(FLOWKV_KEY16)
			if (source.is_record) {
				ERROR_EXIT("Reverse order found in Compaction %lu:%lu(%d)<%lu:%lu(%d), source=record[%zu]",
				           topkey.key.hi, topkey.key.lo, topkey.row_id,
				           current_max_key.key.hi, current_max_key.key.lo, current_max_key.row_id,
				           source.index);
			}
			ERROR_EXIT("Reverse order found in Compaction %lu:%lu(%d)<%lu:%lu(%d), source=table[%zu], pst_idx=%d",
			           topkey.key.hi, topkey.key.lo, topkey.row_id,
			           current_max_key.key.hi, current_max_key.key.lo, current_max_key.row_id,
			           source.index, table_rows[source.index].current_pst_idx_);
			#else
			if (source.is_record) {
				ERROR_EXIT("Reverse order found in Compaction %lu(%d)<%lu(%d), source=record[%zu]",
				           __bswap_64(topkey.key), topkey.row_id,
				           __bswap_64(current_max_key.key), current_max_key.row_id,
				           source.index);
			}
			ERROR_EXIT("Reverse order found in Compaction %lu(%d)<%lu(%d), source=table[%zu], pst_idx=%d",
			           __bswap_64(topkey.key), topkey.row_id,
			           __bswap_64(current_max_key.key), current_max_key.row_id,
			           source.index, table_rows[source.index].current_pst_idx_);
			#endif
		}
		current_max_key = topkey;
		// 如果出现重合key，旧key直接next
		while (!key_heap.empty() && key_heap.top().key == topkey.key)
		{
			#if defined(FLOWKV_KEY16)
			DEBUG("重合key %lu:%lu from row %d with row %d", topkey.key.hi, topkey.key.lo, topkey.row_id, key_heap.top().row_id);
			#else
			DEBUG("重合key %lu from row %d with row %d", __bswap_64(topkey.key), topkey.row_id, key_heap.top().row_id);
			#endif

			if (SourceNextKey(topkey.row_id)) {
				PushSourceToHeapIfValid(topkey.row_id);
			}
			topkey = key_heap.top();
			key_heap.pop();
		}

		const InputSource& source = sources[topkey.row_id];
		if (!source.is_record)
		{
			RowIterator &row = table_rows[source.index];
			bool is_overlapped = 0;
			if (row.pst_iter_ == nullptr)
			{
				// the key is the first key in a pst, now check if the pst is overlapped with other inputs
				KeyType max = row.GetPst().meta.MaxKey();
				for (int i = 0; i < static_cast<int>(sources.size()); i++)
				{
					if (i == topkey.row_id) {
						continue;
					}
					if (!SourceWithinPartition(i)) {
						continue;
					}
					if (KeyTypeLess(SourceCurrentKey(i), max))
					{
						is_overlapped = 1;
						break;
					}
				}
				if (!is_overlapped)
				{
					#if defined(FLOWKV_KEY16)
					LOG("jump,topkey=%lu:%lu,max=%lu:%lu, is overlapped=%d", topkey.key.hi, topkey.key.lo, max.hi, max.lo, is_overlapped);
					#else
					LOG("jump,topkey=%lu,max=%lu, is overlapped=%d", __bswap_64(topkey.key), __bswap_64(max), is_overlapped);
					#endif
					// not overlapped: directly reuse the whole pst as output
					TaggedPstMeta flushed;
					flushed.meta = pst_builder->Flush();
					add_patch_recorder.EmitAndReset(flushed.meta, partition_add_patch_records_[partition_id]);
					partition_outputs_[partition_id].emplace_back(flushed);
					TaggedPstMeta reused = row.GetPst();
					row.MarkPst();
					marked_output++;
					partition_outputs_[partition_id].emplace_back(reused);
					if (row.NextPst()) {
						PushSourceToHeapIfValid(topkey.row_id);
					}
					continue;
				}

				// overlapped: read the pst and switch to key-wise merge.
				row.ResetPstIter();
			}
			const KeyType key = row.GetCurrentKey();
			const FixedValue16 value = row.GetCurrentValue();
			AppendEntry(key, value);
		}
		else
		{
			const KeyType key = SourceCurrentKey(topkey.row_id);
			const FixedValue16 value = SourceCurrentValue(topkey.row_id);
			AppendEntry(key, value);
		}

		if (SourceNextKey(topkey.row_id))
		{
			PushSourceToHeapIfValid(topkey.row_id);
		}
	}
	auto meta = pst_builder->Flush();
	add_patch_recorder.EmitAndReset(meta, partition_add_patch_records_[partition_id]);
	// add meta into manifest
	TaggedPstMeta tmeta;
	tmeta.meta = meta;
	partition_outputs_[partition_id].emplace_back(tmeta);
	// pst_builder.PersistCheckpoint();
	table_rows.clear();
	record_rows.clear();
	for (auto &pr : readers)
	{
		delete pr;
	}
}

void CompactionJob::CleanCompaction()
{
	if (!manifest_->BeginBatchUpdate()) {
		ERROR_EXIT("manifest begin batch failed in CleanCompaction");
	}
	std::vector<TaggedPstMeta> resolved_obsolete_l1_tables;
	const std::vector<TaggedPstMeta>* obsolete_l1_tables_ptr =
		ResolveObsoleteL1TablesFromInputs(version_,
		                                 inputs_.back(),
		                                 inputs_l1_records_,
		                                 inputs_l1_unique_blocks_,
		                                 resolved_obsolete_l1_tables);
	const std::vector<TaggedPstMeta>& obsolete_l1_tables = *obsolete_l1_tables_ptr;
	std::vector<SubtreeRecord> delete_records_storage;
	const std::vector<SubtreeRecord>* delete_records_for_guard = nullptr;
	if (use_l1_range_scan_records_) {
		const std::vector<SubtreeRecord> add_records =
			BuildAddRecordsWithFallback(outputs_, add_patch_records_, inputs_l1_records_);
		delete_records_storage = BuildSyntheticRecordsFromTables(obsolete_l1_tables);
		const std::vector<SubtreeRecord>* delete_records = &delete_records_storage;
		delete_records_for_guard = delete_records;
		version_->SetPendingL1DeltaBatch(
			BuildL1DeltaBatchFromRecords(add_records, *delete_records, output_seq_no_, output_seq_no_));
		LOG("L1 delta(record-path): add_windows=%lu, delete_windows=%lu",
		    add_records.size(), delete_records->size());
	} else {
		version_->SetPendingL1DeltaBatch(
			BuildL1DeltaBatchFromTables(outputs_, obsolete_l1_tables, seg_allocater_, output_seq_no_, output_seq_no_));
	}
	version_->BeginL1BatchUpdate();
	// 1. add outputs to level 1 index
	for (auto &pst : outputs_)
	{
		pst.meta.seq_no_ = output_seq_no_;
		pst.level = 1;
		pst.manifest_position = kInvalidManifestPosition;
		version_->InsertTableToL1(pst);
	}
	pst_builder_.PersistCheckpoint();
	// 2. change version in manifest
	manifest_->UpdateL1Version(output_seq_no_);
	int tree_num = inputs_.size() - 1;
	manifest_->UpdateL0Version(manifest_->GetL0Version() + tree_num);

	// 3. delete obsolute PSTs
	const std::vector<TaggedPstMeta> l1_tables_to_delete = BuildDeletableL1Tables(
		obsolete_l1_tables,
		delete_records_for_guard,
		use_l1_delete_covered_only_);
	// delete inputs[-1](except for .level=1) from level 1 index
	for (const auto &pst : l1_tables_to_delete)
	{
		version_->DeleteTableInL1(pst.meta);
	}
	// delete level 0 trees;
	for (int i = 0; i < tree_num; i++)
	{
		version_->FreeLevel0Tree();

		for (auto &pst : inputs_[i])
		{
			manifest_->DeleteTable(pst.manifest_position, 0);
		}
	}
	version_->EndL1BatchUpdate();
	PersistL1HybridStateOrExit(version_, manifest_);

	if (!manifest_->CommitBatchUpdate()) {
		ERROR_EXIT("manifest batch commit failed in CleanCompaction");
	}
	ReclaimObsoletePstsAfterCommit(&pst_deleter_, l1_tables_to_delete, inputs_, tree_num);

	total_L1_num = total_L1_num + outputs_.size() - l1_tables_to_delete.size();
	LOG("L1 add %lu pst, delete %lu/%lu pst, total %lu pst",
	    outputs_.size(),
	    l1_tables_to_delete.size(),
	    obsolete_l1_tables.size(),
	    total_L1_num);
	if (use_l1_delete_covered_only_ && l1_tables_to_delete.size() < obsolete_l1_tables.size()) {
		LOG("L1 delete guard retained %lu obsolete tables due to partial coverage",
		    obsolete_l1_tables.size() - l1_tables_to_delete.size());
	}
	manifest_->PrintL1Info();
}
void CompactionJob::CleanCompactionWhenUsingSubCompaction()
{
	const bool clean_trace_enabled = IsCompactionCleanTraceEnabled();
	const uint64_t clean_trace_invocation_id =
		g_clean_trace_invocation_id.fetch_add(1, std::memory_order_relaxed) + 1;
	const auto clean_trace_begin = std::chrono::steady_clock::now();
	size_t merged_outputs_count = 0;
	size_t obsolete_l1_tables_count = 0;
	size_t l1_tables_to_delete_count = 0;
	int tree_num = -1;
	const auto emit_clean_trace = [&](const char* stage) {
		if (!clean_trace_enabled) {
			return;
		}
		const auto now = std::chrono::steady_clock::now();
		const double elapsed_ms =
			static_cast<double>(
				std::chrono::duration_cast<std::chrono::microseconds>(now - clean_trace_begin).count()) /
			1000.0;
		std::cout << "[CLEAN_TRACE]"
		          << " invocation_id=" << clean_trace_invocation_id
		          << " stage=" << stage
		          << " elapsed_ms=" << elapsed_ms
		          << " rss_bytes=" << ReadProcessRSSBytesFromProc()
		          << " merged_outputs=" << merged_outputs_count
		          << " obsolete_l1_tables=" << obsolete_l1_tables_count
		          << " l1_tables_to_delete=" << l1_tables_to_delete_count
		          << " tree_num=" << tree_num
		          << "\n";
	};
	emit_clean_trace("entry");

	if (!manifest_->BeginBatchUpdate()) {
		ERROR_EXIT("manifest begin batch failed in CleanCompactionWhenUsingSubCompaction");
	}
	const std::vector<TaggedPstMeta> merged_outputs = GatherSubCompactionOutputs(partition_outputs_);
	merged_outputs_count = merged_outputs.size();
	std::vector<TaggedPstMeta> resolved_obsolete_l1_tables;
	const std::vector<TaggedPstMeta>* obsolete_l1_tables_ptr =
		ResolveObsoleteL1TablesFromInputs(version_,
		                                 inputs_.back(),
		                                 inputs_l1_records_,
		                                 inputs_l1_unique_blocks_,
		                                 resolved_obsolete_l1_tables);
	const std::vector<TaggedPstMeta>& obsolete_l1_tables = *obsolete_l1_tables_ptr;
	obsolete_l1_tables_count = obsolete_l1_tables.size();
	emit_clean_trace("after_collect_inputs");

	std::vector<SubtreeRecord> delete_records_storage;
	const std::vector<SubtreeRecord>* delete_records_for_guard = nullptr;
	if (use_l1_range_scan_records_) {
		const std::vector<SubtreeRecord> merged_add_patch_records =
			MergeSubCompactionAddPatchRecords(partition_add_patch_records_);
		const std::vector<SubtreeRecord> add_records =
			BuildAddRecordsWithFallback(merged_outputs, merged_add_patch_records, inputs_l1_records_);
		delete_records_storage = BuildSyntheticRecordsFromTables(obsolete_l1_tables);
		const std::vector<SubtreeRecord>* delete_records = &delete_records_storage;
		delete_records_for_guard = delete_records;
		version_->SetPendingL1DeltaBatch(
			BuildL1DeltaBatchFromRecords(add_records, *delete_records, output_seq_no_, output_seq_no_));
		LOG("L1 delta(record-path,subcompaction): add_windows=%lu, delete_windows=%lu",
		    add_records.size(), delete_records->size());
	} else {
		version_->SetPendingL1DeltaBatch(
			BuildL1DeltaBatchFromTables(merged_outputs, obsolete_l1_tables, seg_allocater_, output_seq_no_, output_seq_no_));
	}
	emit_clean_trace("after_set_pending_delta");
	version_->BeginL1BatchUpdate();
	emit_clean_trace("after_begin_l1_batch");

	// 1. add outputs to level 1 index
	for (int i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		auto outputs = partition_outputs_[i];
		for (auto &pst : outputs)
		{
			pst.meta.seq_no_ = output_seq_no_;
			pst.level = 1;
			pst.manifest_position = kInvalidManifestPosition;
			version_->InsertTableToL1(pst);
		}
		partition_pst_builder_[i]->PersistCheckpoint();
		delete partition_pst_builder_[i];
	}
	emit_clean_trace("after_add_outputs");

	// 2. change version in manifest
	manifest_->UpdateL1Version(output_seq_no_);
	tree_num = inputs_.size() - 1;
	manifest_->UpdateL0Version(manifest_->GetL0Version() + tree_num);
	emit_clean_trace("after_update_versions");

	// 3. delete obsolute PSTs
	const std::vector<TaggedPstMeta> l1_tables_to_delete = BuildDeletableL1Tables(
		obsolete_l1_tables,
		delete_records_for_guard,
		use_l1_delete_covered_only_);
	l1_tables_to_delete_count = l1_tables_to_delete.size();
	emit_clean_trace("after_build_delete_set");

	// delete inputs[-1](except for .level=1) from level 1 index
	for (const auto &pst : l1_tables_to_delete)
	{
		version_->DeleteTableInL1(pst.meta);
	}
	emit_clean_trace("after_delete_l1_tables");

	// delete level 0 trees;
	for (int i = 0; i < tree_num; i++)
	{
		version_->FreeLevel0Tree();

		for (auto &pst : inputs_[i])
		{
			manifest_->DeleteTable(pst.manifest_position, 0);
		}
	}
	emit_clean_trace("after_delete_l0_tables");
	emit_clean_trace("before_end_l1_batch");
	version_->EndL1BatchUpdate();
	emit_clean_trace("after_end_l1_batch");
	PersistL1HybridStateOrExit(version_, manifest_);
	emit_clean_trace("after_persist_l1_hybrid_state");

	if (!manifest_->CommitBatchUpdate()) {
		ERROR_EXIT("manifest batch commit failed in CleanCompactionWhenUsingSubCompaction");
	}
	emit_clean_trace("after_commit_batch");
	ReclaimObsoletePstsAfterCommit(&pst_deleter_, l1_tables_to_delete, inputs_, tree_num);
	emit_clean_trace("after_delete_checkpoint");
	if (use_l1_delete_covered_only_ && l1_tables_to_delete.size() < obsolete_l1_tables.size()) {
		LOG("L1 delete guard(subcompaction) retained %lu obsolete tables due to partial coverage",
		    obsolete_l1_tables.size() - l1_tables_to_delete.size());
	}
	emit_clean_trace("function_end");
}
bool CompactionJob::RollbackCompaction()
{
	DEBUG("rollback start!");
	return false;
}
void CompactionJob::TriggerSubCompaction(void *arg)
{
	SubCompactionArgs sca = *(reinterpret_cast<SubCompactionArgs *>(arg));
	// printf("sca.id=%d\n",sca.partition_id_);
	delete (reinterpret_cast<SubCompactionArgs *>(arg));
	// printf("trigger bgcompaction\n");
	static_cast<CompactionJob *>(sca.cj_)->RunSubCompaction(sca.partition_id_);
}
