#include "version.h"
#include "manifest.h"
#include "lib/hybrid_l1/l1_hybrid_index.h"
#include "lib/hybrid_l1/prefix_suffix.h"
#include "lib/hybrid_l1/subtree_record.h"
#include "lib/index_masstree.h"
#ifdef HOT_L1
#include "lib/index_hot.h"
#endif
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_set>

namespace {

constexpr uint32_t kL1HybridStateMagic = 0x4C314853u;   // "L1HS"
constexpr uint16_t kL1HybridStateFormat = 5;
constexpr uint32_t kL1SubtreePageSizeBytes = 16 * 1024;

void WriteU16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xFFu));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
}

void WriteU32(std::vector<uint8_t>& out, uint32_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xFFu));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
    out.push_back(static_cast<uint8_t>((value >> 16) & 0xFFu));
    out.push_back(static_cast<uint8_t>((value >> 24) & 0xFFu));
}

void WriteU64(std::vector<uint8_t>& out, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        out.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xFFu));
    }
}

bool ReadU16(const std::vector<uint8_t>& in, size_t& cursor, uint16_t& out) {
    if (cursor + 2 > in.size()) {
        return false;
    }
    out = static_cast<uint16_t>(in[cursor]) |
          static_cast<uint16_t>(in[cursor + 1] << 8);
    cursor += 2;
    return true;
}

bool ReadU32(const std::vector<uint8_t>& in, size_t& cursor, uint32_t& out) {
    if (cursor + 4 > in.size()) {
        return false;
    }
    out = static_cast<uint32_t>(in[cursor]) |
          (static_cast<uint32_t>(in[cursor + 1]) << 8) |
          (static_cast<uint32_t>(in[cursor + 2]) << 16) |
          (static_cast<uint32_t>(in[cursor + 3]) << 24);
    cursor += 4;
    return true;
}

bool ReadU64(const std::vector<uint8_t>& in, size_t& cursor, uint64_t& out) {
    if (cursor + 8 > in.size()) {
        return false;
    }
    out = 0;
    for (size_t i = 0; i < 8; ++i) {
        out |= (static_cast<uint64_t>(in[cursor + i]) << (i * 8));
    }
    cursor += 8;
    return true;
}

void WriteKey(std::vector<uint8_t>& out, const KeyType& key) {
#if defined(FLOWKV_KEY16)
    WriteU64(out, key.hi);
    WriteU64(out, key.lo);
#else
    WriteU64(out, key);
#endif
}

bool ReadKey(const std::vector<uint8_t>& in, size_t& cursor, KeyType& key) {
#if defined(FLOWKV_KEY16)
    uint64_t hi = 0;
    uint64_t lo = 0;
    if (!ReadU64(in, cursor, hi) || !ReadU64(in, cursor, lo)) {
        return false;
    }
    key = KeyType{hi, lo};
    return true;
#else
    uint64_t raw = 0;
    if (!ReadU64(in, cursor, raw)) {
        return false;
    }
    key = raw;
    return true;
#endif
}

uint8_t EncodeGovernance(const flowkv::hybrid_l1::BucketGovernanceState& state) {
    uint8_t flags = 0;
    if (state.hot_prefix) {
        flags |= 0x1u;
    }
    if (state.prefer_cow) {
        flags |= 0x2u;
    }
    if (state.prefer_parallel_scan) {
        flags |= 0x4u;
    }
    return flags;
}

flowkv::hybrid_l1::BucketGovernanceState DecodeGovernance(uint8_t flags) {
    flowkv::hybrid_l1::BucketGovernanceState state;
    state.hot_prefix = (flags & 0x1u) != 0;
    state.prefer_cow = (flags & 0x2u) != 0;
    state.prefer_parallel_scan = (flags & 0x4u) != 0;
    return state;
}

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

bool ParseEnvSize(const char* name, size_t& out) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0') {
        return false;
    }
    out = static_cast<size_t>(parsed);
    return true;
}

uint64_t KvBlockPtrFromTable(const PSTMeta& table) {
    return flowkv::hybrid_l1::SubtreeRecord::EncodeKvBlockPtr(table.datablock_ptr_);
}

}  // namespace

Version::Version(SegmentAllocator *seg_allocator)
    : pst_reader_(seg_allocator)
{
    level0_table_lists_.resize(MAX_L0_TREE_NUM);
    for (int i = 0; i < MAX_L0_TREE_NUM; i++)
    {
        level0_trees_[i] = nullptr;
    }
    size_t l1_block_map_reserve = 0;
    if (ParseEnvSize("FLOWKV_L1_TABLE_META_RESERVE", l1_block_map_reserve) &&
        l1_block_map_reserve > 0) {
        level1_table_by_block_.reserve(l1_block_map_reserve);
    }
    flowkv::hybrid_l1::L1HybridIndex::BuildOptions options;
    options.subtree_page_size = kL1SubtreePageSizeBytes;
    options.segment_allocator = seg_allocator;
    bool enable_subtree_cache = false;
    if (ParseEnvBool("FLOWKV_L1_ENABLE_SUBTREE_CACHE", enable_subtree_cache)) {
        options.enable_subtree_cache = enable_subtree_cache;
    }
    bool enable_leaf_stream_bulk_load = false;
    if (ParseEnvBool("FLOWKV_L1_ENABLE_LEAF_STREAM_BULKLOAD", enable_leaf_stream_bulk_load)) {
        options.update_policy.enable_leaf_stream_bulk_load = enable_leaf_stream_bulk_load;
    }
    size_t subtree_cache_capacity = 0;
    if (ParseEnvSize("FLOWKV_L1_SUBTREE_CACHE_CAPACITY", subtree_cache_capacity)) {
        options.subtree_cache_capacity = subtree_cache_capacity;
    }
    size_t subtree_cache_max_bytes = 0;
    if (ParseEnvSize("FLOWKV_L1_SUBTREE_CACHE_MAX_BYTES", subtree_cache_max_bytes)) {
        options.subtree_cache_max_bytes = subtree_cache_max_bytes;
    }
    size_t route_hot_leaf_budget_bytes = 0;
    if (ParseEnvSize("FLOWKV_L1_ROUTE_HOT_LEAF_BUDGET_BYTES", route_hot_leaf_budget_bytes)) {
        options.route_hot_leaf_budget_bytes = route_hot_leaf_budget_bytes;
    } else if (ParseEnvSize("FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES", route_hot_leaf_budget_bytes)) {
        // Backward-compatible fallback for older experiment scripts.
        options.route_hot_leaf_budget_bytes = route_hot_leaf_budget_bytes;
    }
    if (!options.enable_subtree_cache) {
        options.subtree_cache_capacity = 0;
        options.subtree_cache_max_bytes = 0;
    }
    level1_tree_ = new flowkv::hybrid_l1::L1HybridIndex(options);
}

Version::~Version()
{

    if (level1_tree_)
        delete level1_tree_;

    // TODO: maybe save some metadata?
}

std::vector<PSTMeta>::const_iterator FindTableInLevels(const std::vector<PSTMeta> &level,
                                                       const Slice &key)
{
    // OBSOLUTE
    auto cmp = [&](const PSTMeta &f, const Slice &k) -> bool
    {
        KeyType mk = f.MinKey();
    #if defined(FLOWKV_KEY16)
        uint8_t key_bytes[16];
        mk.ToBigEndianBytes(key_bytes);
        return Slice(reinterpret_cast<const char *>(key_bytes), 16).compare(k);
    #else
        return Slice(reinterpret_cast<const char *>(&mk), sizeof(uint64_t)).compare(k);
    #endif
    };
    auto iter = std::lower_bound(level.begin(), level.end(), key, cmp);

    return iter;
}
int Version::InsertTableToL0(TaggedPstMeta tmeta, int tree_idx)
{
    if (tree_idx < 0 || tree_idx >= MAX_L0_TREE_NUM) {
        return -1;
    }
    // get position
    PSTMeta table = tmeta.meta;
    int idx = level0_table_lists_[tree_idx].size();
    level0_table_lists_[tree_idx].emplace_back(tmeta);
    // update index
    ValueHelper lh((uint64_t)idx);
    KeyType tempk = table.MaxKey();
    if (level0_trees_[tree_idx] == nullptr) {
        return -1;
    }
    level0_trees_[tree_idx]->Put(tempk, lh);
    KeyType maxk = tmeta.meta.MaxKey();
    KeyType mink = tmeta.meta.MinKey();
    if (KeyTypeGreater(maxk, level0_tree_meta_[tree_idx].max_key))
    {
        level0_tree_meta_[tree_idx].max_key = maxk;
    }
    if (KeyTypeLess(mink, level0_tree_meta_[tree_idx].min_key))
    {
        level0_tree_meta_[tree_idx].min_key = mink;
    }
    return idx;
}
int Version::InsertTableToL1(TaggedPstMeta tmeta)
{
    if (tmeta.Valid()) {
        level1_table_by_block_[KvBlockPtrFromTable(tmeta.meta)] = tmeta;
    }
    QueueOrApplyL1Rebuild(CollectChangedRouteKeysForTable(tmeta.meta));
    return -1;
}

void Version::BeginRecoverLevel1(uint32_t next_l1_seq, size_t expected_table_count)
{
    l1_batch_update_depth_ = 0;
    pending_l1_changed_route_keys_.clear();
    has_pending_l1_delta_batch_ = false;
    pending_l1_delta_batch_ = flowkv::hybrid_l1::L1DeltaBatch{};

    level1_table_by_block_.clear();
    if (expected_table_count > 0) {
        level1_table_by_block_.reserve(expected_table_count);
    }

    l1_seq_ = static_cast<int>(next_l1_seq);
}

void Version::RecoverLevel1Table(const TaggedPstMeta& table)
{
    if (!table.Valid()) {
        return;
    }
    level1_table_by_block_[KvBlockPtrFromTable(table.meta)] = table;
}

void Version::FinalizeRecoverLevel1()
{
    RebuildLevel1Tree();
}

void Version::RecoverLevel1Tables(const std::vector<TaggedPstMeta>& tables, uint32_t next_l1_seq)
{
    size_t valid_count = 0;
    for (const auto& table : tables) {
        if (table.Valid()) {
            ++valid_count;
        }
    }
    BeginRecoverLevel1(next_l1_seq, valid_count);
    for (const auto& table : tables) {
        RecoverLevel1Table(table);
    }
    FinalizeRecoverLevel1();
}

// 删除时比对删除的value是否为table的indexblock_ptr，若不是说明已经被同key的其他pst替代了
bool Version::DeleteTableInL1(PSTMeta table)
{
    const uint64_t kv_block_ptr = KvBlockPtrFromTable(table);
    auto map_it = level1_table_by_block_.find(kv_block_ptr);
    if (map_it == level1_table_by_block_.end()) {
        return false;
    }
    const TaggedPstMeta stored = map_it->second;
    const PSTMeta& old = stored.meta;
    if (old.datablock_ptr_ != table.datablock_ptr_ ||
        CompareKeyType(old.MaxKey(), table.MaxKey()) != 0 ||
        CompareKeyType(old.MinKey(), table.MinKey()) != 0) {
        return false;
    }
    level1_table_by_block_.erase(map_it);

    QueueOrApplyL1Rebuild(CollectChangedRouteKeysForTable(table));
    return true;
}

void Version::BeginL1BatchUpdate()
{
    if (l1_batch_update_depth_ == 0) {
        pending_l1_changed_route_keys_.clear();
    }
    ++l1_batch_update_depth_;
}

void Version::EndL1BatchUpdate()
{
    if (l1_batch_update_depth_ <= 0) {
        return;
    }
    --l1_batch_update_depth_;
    if (l1_batch_update_depth_ != 0) {
        return;
    }
    std::vector<KeyType> changed_route_keys;
    const flowkv::hybrid_l1::L1DeltaBatch* delta_batch_ptr = nullptr;
    if (has_pending_l1_delta_batch_) {
        changed_route_keys = pending_l1_delta_batch_.ToChangedRouteKeys();
        delta_batch_ptr = &pending_l1_delta_batch_;
        has_pending_l1_delta_batch_ = false;
    } else {
        changed_route_keys.swap(pending_l1_changed_route_keys_);
    }
    if (changed_route_keys.empty()) {
        if (delta_batch_ptr != nullptr) {
            pending_l1_delta_batch_ = flowkv::hybrid_l1::L1DeltaBatch{};
        }
        return;
    }
    RebuildLevel1Partitions(changed_route_keys, delta_batch_ptr);
    if (!level1_table_by_block_.empty() && level1_tree_->Empty()) {
        // Safety net: if delta-only rebuild doesn't materialize descriptors,
        // fall back to table-driven rebuild to preserve read correctness.
        RebuildLevel1Tree();
    }
    if (delta_batch_ptr != nullptr) {
        pending_l1_delta_batch_ = flowkv::hybrid_l1::L1DeltaBatch{};
    }
}

void Version::SetPendingL1DeltaBatch(flowkv::hybrid_l1::L1DeltaBatch batch)
{
    batch.SortAndUniquePrefixes();
    has_pending_l1_delta_batch_ = !batch.Empty();
    pending_l1_delta_batch_ = std::move(batch);
}

bool Version::ResolveL1BlockToTableMeta(uint64_t kv_block_ptr, TaggedPstMeta& output) const
{
    const auto it = level1_table_by_block_.find(kv_block_ptr);
    if (it == level1_table_by_block_.end()) {
        return false;
    }
    if (!it->second.Valid()) {
        return false;
    }
    if (KvBlockPtrFromTable(it->second.meta) != kv_block_ptr) {
        return false;
    }
    output = it->second;
    return true;
}

bool Version::ResolveL1RecordToTableMeta(
    const flowkv::hybrid_l1::SubtreeRecord& record,
    TaggedPstMeta& output) const
{
    const auto window = record.LeafWindow();
    if (window.count == 0) {
        return false;
    }
    return ResolveL1BlockToTableMeta(window.kv_block_ptr, output);
}

// bool Version::DeleteTable(int idx, int level_id)
// {
//     assert(level_id < max_level_);
//     // TODO: never delete table. when compaction, building new version by copying
//     // find meta in meta array
//     auto table = levels_[level_id][idx];
//     // erase table key in the tree
//     level_trees_[level_id]->Delete(table.meta.max_key_);
//     // append vector idx to the freelist
//     free_lists_[level_id].push(idx);
//     return true;
// }

// Debug counters for L1 lookup failures
static std::atomic<uint64_t> g_l1_idx_not_found_{0};
static std::atomic<uint64_t> g_l1_table_invalid_{0};
static std::atomic<uint64_t> g_l1_range_check_fail_{0};
static std::atomic<uint64_t> g_l1_point_query_fail_{0};
static std::atomic<uint64_t> g_l1_success_{0};

namespace {

bool IsDeletedValueBytes(const char* value_bytes) {
    if (value_bytes == nullptr) {
        return false;
    }
    return IsDeletedFixedValue16Bytes(value_bytes);
}

bool TaggedTableByMaxKeyLess(const TaggedPstMeta& lhs, const TaggedPstMeta& rhs) {
    const int max_cmp = CompareKeyType(lhs.meta.MaxKey(), rhs.meta.MaxKey());
    if (max_cmp != 0) {
        return max_cmp < 0;
    }
    const int min_cmp = CompareKeyType(lhs.meta.MinKey(), rhs.meta.MinKey());
    if (min_cmp != 0) {
        return min_cmp < 0;
    }
    return lhs.meta.datablock_ptr_ < rhs.meta.datablock_ptr_;
}

void SortResolvedTablesByKeyRange(std::vector<TaggedPstMeta>& tables) {
    std::sort(tables.begin(), tables.end(), TaggedTableByMaxKeyLess);
}

}  // namespace

void Version::CollectActiveLevel1Tables(std::vector<TaggedPstMeta>& output) const
{
    output.clear();
    output.reserve(level1_table_by_block_.size());
    for (const auto& [_, table] : level1_table_by_block_) {
        if (!table.Valid()) {
            continue;
        }
        output.push_back(table);
    }
    SortResolvedTablesByKeyRange(output);
}

void Version::RebuildLevel1TableMapFromRecords()
{
    level1_table_by_block_.clear();
    if (level1_tree_ == nullptr || level1_tree_->Empty()) {
        return;
    }

    std::vector<flowkv::hybrid_l1::SubtreeRecord> records;
    level1_tree_->ExportAll(records);
    level1_table_by_block_.reserve(records.size());

    for (const auto& record : records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        const uint64_t kv_block_ptr = window.kv_block_ptr;
        const uint64_t block_off =
            flowkv::hybrid_l1::SubtreeRecord::DecodeKvBlockOffset(kv_block_ptr);
        auto [it, inserted] = level1_table_by_block_.emplace(kv_block_ptr, TaggedPstMeta{});
        TaggedPstMeta& table = it->second;
        if (inserted) {
            table.level = 1;
            table.manifest_position = std::numeric_limits<size_t>::max();
            table.meta = PSTMeta::InvalidTable();
            table.meta.datablock_ptr_ = block_off;
            table.meta.seq_no_ = record.seq_no;
            table.meta.entry_num_ = window.count;
#if defined(FLOWKV_KEY16)
            table.meta.min_key_hi = record.min_key.hi;
            table.meta.min_key_lo = record.min_key.lo;
            table.meta.max_key_hi = record.max_key.hi;
            table.meta.max_key_lo = record.max_key.lo;
#else
            table.meta.min_key_ = record.min_key;
            table.meta.max_key_ = record.max_key;
#endif
            continue;
        }

        if (record.seq_no > table.meta.seq_no_) {
            table.meta.seq_no_ = record.seq_no;
        }
        if (CompareKeyType(record.min_key, table.meta.MinKey()) < 0) {
#if defined(FLOWKV_KEY16)
            table.meta.min_key_hi = record.min_key.hi;
            table.meta.min_key_lo = record.min_key.lo;
#else
            table.meta.min_key_ = record.min_key;
#endif
        }
        if (CompareKeyType(record.max_key, table.meta.MaxKey()) > 0) {
#if defined(FLOWKV_KEY16)
            table.meta.max_key_hi = record.max_key.hi;
            table.meta.max_key_lo = record.max_key.lo;
#else
            table.meta.max_key_ = record.max_key;
#endif
        }
        const uint32_t merged_entry_num =
            static_cast<uint32_t>(table.meta.entry_num_) + static_cast<uint32_t>(window.count);
        table.meta.entry_num_ = static_cast<uint16_t>(
            std::min<uint32_t>(merged_entry_num, std::numeric_limits<uint16_t>::max()));
    }
}

void PrintL1DebugStats() {
    printf("=== L1 Lookup Stats ===\n");
    printf("  idx=-1:       %lu\n", g_l1_idx_not_found_.load());
    printf("  table invalid: %lu\n", g_l1_table_invalid_.load());
    printf("  range fail:   %lu\n", g_l1_range_check_fail_.load());
    printf("  PointQuery fail: %lu\n", g_l1_point_query_fail_.load());
    printf("  success:      %lu\n", g_l1_success_.load());
    printf("=======================\n");
}

void Version::PrintL1IndexStats() {
    printf("=== L1 Index Stats ===\n");
    printf("  Active PST count(block map): %lu\n", level1_table_by_block_.size());
    printf("======================\n");
}

int FindTableByIndex(const KeyType &key, Index *level_index)
{
    std::vector<ValueType> temp;
    level_index->Scan(key, 1, temp);
    if (temp.empty())
    {
#if defined(FLOWKV_KEY16)
        static std::atomic<int> debug_count{0};
        if (debug_count.fetch_add(1) < 10) {
            printf("[DEBUG] FindTableByIndex: Scan returned empty for key %lu:%lu\n", key.hi, key.lo);
        }
#endif
        return -1;
    }
    if (temp[0] == INVALID_PTR)
    {
        return -1;
    }
    return static_cast<int>(temp[0]);
}

bool FindTableBySubtreeIndex(const KeyType &key,
                             flowkv::hybrid_l1::L1HybridIndex *level_index,
                             flowkv::hybrid_l1::SubtreeRecord &record_out)
{
    if (!level_index->LookupCandidate(key, record_out)) {
        return false;
    }
    return true;
}
int FindTableByIndex2(const KeyType &key, Index *level_index)
{
    std::vector<ValueType> temp;
    level_index->Scan(key, 2, temp);
    if (temp.size() < 2)
    {
        return -1;
    }
    if (temp[1] == INVALID_PTR)
    {
        return -1;
    }
    return static_cast<int>(temp[1]);
}
inline void ScanIndexForTables(const KeyType &key, Index *level_index, size_t table_num, std::vector<ValueType> &output_table_ids)
{
    output_table_ids.clear();
    level_index->Scan(key, table_num, output_table_ids);
}

bool Version::Get(Slice key, const char *value_out, int *value_size, PSTReader *pst_reader)
{
    TaggedPstMeta table;
#if defined(FLOWKV_KEY16)
    KeyType int_key = key.ToKey16();
#else
    KeyType int_key = key.ToUint64();
#endif
    // DEBUG("read key=%lu,l0head=%d,l0_read_tail=%d",key.ToUint64Bswap(),l0_head_,l0_read_tail_);
    // search level0
    for (int tree_idx = l0_head_; tree_idx != l0_read_tail_; tree_idx = (tree_idx + 1) % MAX_L0_TREE_NUM)
    {
        Index *tree = level0_trees_[tree_idx];
        // DEBUG("1");
        int idx = FindTableByIndex(int_key, tree);
        if (idx == -1)
            continue;
        // DEBUG("2");
        table = level0_table_lists_[tree_idx][idx];
        if (!table.Valid())
        {
            continue;
        }
        // DEBUG("3");     
#if defined(FLOWKV_KEY16)
        if (!(table.meta.MinKey().hi == MAX_UINT64 && table.meta.MinKey().lo == MAX_UINT64) && KeyTypeGreater(table.meta.MinKey(), int_key))
#else
        if (table.meta.MinKey() != MAX_UINT64 && KeyTypeGreater(table.meta.MinKey(), int_key))
#endif
        {
            continue;
        }
        const bool ret = pst_reader->PointQuery(
            table.meta.datablock_ptr_, key, value_out, value_size, table.meta.entry_num_);
        if (ret)
        {
            if (IsDeletedValueBytes(value_out))
            {
                return false;
            }
            return true;
        }

    }
    // searchlevel1
    flowkv::hybrid_l1::SubtreeRecord l1_record;
    if (!FindTableBySubtreeIndex(int_key, level1_tree_, l1_record))
    {
        g_l1_idx_not_found_.fetch_add(1, std::memory_order_relaxed);
        DEBUG("5");
        return false;
    }
    if (!l1_record.Contains(int_key) || !l1_record.HasLeafWindow()) {
        g_l1_range_check_fail_.fetch_add(1, std::memory_order_relaxed);
        DEBUG("6");
        return false;
    }
    const auto window = l1_record.LeafWindow();
    const uint64_t block_off =
        flowkv::hybrid_l1::SubtreeRecord::DecodeKvBlockOffset(window.kv_block_ptr);
    if (window.count == 0 ||
        window.offset >= static_cast<uint16_t>(PDataBlock::MAX_ENTRIES)) {
        g_l1_table_invalid_.fetch_add(1, std::memory_order_relaxed);
        DEBUG("7");
        return false;
    }
    const uint16_t bounded_entries = static_cast<uint16_t>(std::min<uint32_t>(
        static_cast<uint32_t>(window.count),
        static_cast<uint32_t>(PDataBlock::MAX_ENTRIES) -
            static_cast<uint32_t>(window.offset)));
    if (bounded_entries == 0) {
        g_l1_table_invalid_.fetch_add(1, std::memory_order_relaxed);
        DEBUG("8");
        return false;
    }
    const bool ret = pst_reader->PointQueryWindow(
        block_off,
        key,
        value_out,
        value_size,
        window.offset,
        bounded_entries);

    if (ret && IsDeletedValueBytes(value_out)) {
        return false;
    }

    if (ret) {
        g_l1_success_.fetch_add(1, std::memory_order_relaxed);
    } else {
        g_l1_point_query_fail_.fetch_add(1, std::memory_order_relaxed);
    }
    //ret = pst_reader->PointQuery(table.meta.datablock_ptr_, key, value_out, value_size, table.meta.entry_num_);
    return ret;
}

RowIterator *Version::GetLevel1Iter(Slice key, PSTReader *pst_reader, std::vector<TaggedPstMeta> &table_metas)
{
#if defined(FLOWKV_KEY16)
    KeyType int_key = key.ToKey16();
#else
    KeyType int_key = key.ToUint64();
#endif
    flowkv::hybrid_l1::RangeScanRecordOptions options;
    options.include_window_fragments = true;
    options.include_unique_blocks = true;
    options.dedup_windows = true;
    flowkv::hybrid_l1::RangeScanRecordResult result;
    level1_tree_->RangeScanRecords(int_key, int_key, options, result);
    ResolveL1BlocksToTables(result.unique_kv_block_ptrs, table_metas);
    return new RowIterator(pst_reader, table_metas);
}

bool Version::CheckSpaceForL0Tree()
{
    std::lock_guard<std::mutex> lock(l0_meta_lock_);
    return (l0_tail_ + 1) % MAX_L0_TREE_NUM != l0_head_;
}

int Version::AddLevel0Tree(uint32_t *seq_no_out)
{
    std::lock_guard<std::mutex> lock(l0_meta_lock_);
    if ((l0_tail_ + 1) % MAX_L0_TREE_NUM == l0_head_)
        return -1;
    auto ret = l0_tail_;
    l0_tail_ = (l0_tail_ + 1) % MAX_L0_TREE_NUM;
#ifdef MASSTREE_L1
    level0_trees_[ret] = new MasstreeIndex();
#endif
#ifdef HOT_L1
    level0_trees_[ret] = new HOTIndex(MAX_PST_L1);
#endif
    level0_table_lists_[ret].clear();
    level0_tree_meta_[ret] = TreeMeta();
    l0_tree_ready_[ret] = false;
    if (seq_no_out != nullptr) {
        *seq_no_out = static_cast<uint32_t>(l0_tree_seq_);
    }
    l0_tree_seq_++;
    return ret;
}
uint32_t Version::GetCurrentL0TreeSeq()
{
    return l0_tree_seq_;
}
void Version::SetCurrentL0TreeSeq(uint32_t seq)
{
    l0_tree_seq_ = seq;
}
void Version::SetCurrentL1Seq(uint32_t seq)
{
    l1_seq_ = static_cast<int>(seq);
}
uint32_t Version::GenerateL1Seq()
{
    return l1_seq_++;
}

bool Version::FreeLevel0Tree()
{
    Index *to_free = nullptr;
    {
        std::lock_guard<std::mutex> lock(l0_meta_lock_);
        if (l0_tail_ == l0_head_)
        {
            return false;
        }
        auto idx = l0_head_;
        l0_head_ = (l0_head_ + 1) % MAX_L0_TREE_NUM;
        to_free = level0_trees_[idx];
        level0_trees_[idx] = nullptr;
        level0_tree_meta_[idx] = TreeMeta();
        level0_table_lists_[idx].clear();
        l0_tree_ready_[idx] = false;
    }
    // wait for reader leave the index (just sleep for > 1ms)
    usleep(100);
    delete to_free;
    return true;
}
void Version::PublishLevel0Tree(int tree_idx)
{
    std::lock_guard<std::mutex> lock(l0_meta_lock_);
    if (tree_idx < 0 || tree_idx >= MAX_L0_TREE_NUM) {
        return;
    }
    l0_tree_ready_[tree_idx] = true;
    while (l0_read_tail_ != l0_tail_ && l0_tree_ready_[l0_read_tail_]) {
        l0_read_tail_ = (l0_read_tail_ + 1) % MAX_L0_TREE_NUM;
    }
}

void Version::UpdateLevel0ReadTail()
{
    std::lock_guard<std::mutex> lock(l0_meta_lock_);
    l0_read_tail_ = l0_tail_;
    for (int idx = l0_head_; idx != l0_read_tail_; idx = (idx + 1) % MAX_L0_TREE_NUM) {
        l0_tree_ready_[idx] = true;
    }
}

int Version::PickLevel0Trees(std::vector<std::vector<TaggedPstMeta>> &outputs, std::vector<TreeMeta> &tree_metas, int max_size)
{
    int tail = 0;
    int head = 0;
    {
        std::lock_guard<std::mutex> lock(l0_meta_lock_);
        tail = l0_read_tail_;
        head = l0_head_;
    }
    DEBUG("pick level0 tree from %d to %d", head, tail);
    int tree_num = (tail + MAX_L0_TREE_NUM - head) % MAX_L0_TREE_NUM;
    outputs.resize(tree_num);
    tree_metas.resize(tree_num);
    for (size_t i = 0; i < tree_num && i < max_size; i++)
    {
        int tree_id = (head + i) % MAX_L0_TREE_NUM;
        Index *tree_index = level0_trees_[tree_id];
        TreeMeta tree_meta = level0_tree_meta_[tree_id];
        tree_metas[tree_num - i - 1] = tree_meta;
        std::vector<uint64_t> value_out;
    #if defined(FLOWKV_KEY16)
        tree_index->Scan(KeyType{0, 0}, MAX_INT32, value_out);
    #else
        tree_index->Scan(0, MAX_INT32, value_out);
    #endif
        for (auto &idx : value_out)
        {
            // reverse output to prove less row_id -> newest row
            auto pst = level0_table_lists_[tree_id][idx];
            outputs[tree_num - i - 1].emplace_back(pst);
        }
    }
    // for (auto &tree : outputs)
    // {
    //     LOG("tree tables:%lu", tree.size());
    //     RowIterator *row = new RowIterator(&pst_reader_, tree);
    //     size_t cmax = row->GetCurrentKey();
    //     size_t k;
    //     while (row->NextKey())
    //     {
    //         k = row->GetCurrentKey();
    //         printf("cmx:%lu,key=%lu\n",__bswap_64(cmax),__bswap_64(k));
    //         fflush(stdout);
    //         if (__bswap_64(k) < __bswap_64(cmax))
    //             ERROR_EXIT("error sequence");
    //         cmax = k;
    //     }
    //     delete row;
    // }
    return tree_metas.size();
}
bool Version::PickOverlappedL1Tables(const KeyType &min, const KeyType &max, std::vector<TaggedPstMeta> &output)
{
    std::vector<flowkv::hybrid_l1::SubtreeRecord> records;
    std::vector<uint64_t> unique_kv_block_ptrs;
    if (!PickOverlappedL1Records(min, max, records, unique_kv_block_ptrs)) {
        output.clear();
        return false;
    }
    ResolveL1BlocksToTables(unique_kv_block_ptrs, output);
    return true;
}

bool Version::PickOverlappedL1Records(
    const KeyType &min,
    const KeyType &max,
    std::vector<flowkv::hybrid_l1::SubtreeRecord> &records,
    std::vector<uint64_t> &unique_kv_block_ptrs) const
{
    records.clear();
    unique_kv_block_ptrs.clear();
    if (level1_tree_ == nullptr) {
        return false;
    }
    flowkv::hybrid_l1::RangeScanRecordOptions options;
    options.include_window_fragments = true;
    options.include_unique_blocks = true;
    options.dedup_windows = true;
    flowkv::hybrid_l1::RangeScanRecordResult result;
    level1_tree_->RangeScanRecords(min, max, options, result);
    records = std::move(result.window_fragments);
    unique_kv_block_ptrs = std::move(result.unique_kv_block_ptrs);
    return true;
}

void Version::ResolveL1RecordsToTables(
    const std::vector<flowkv::hybrid_l1::SubtreeRecord> &records,
    std::vector<TaggedPstMeta> &output) const
{
    std::vector<uint64_t> kv_block_ptrs;
    kv_block_ptrs.reserve(records.size());
    std::unordered_set<uint64_t> seen_blocks;
    seen_blocks.reserve(records.size());
    for (const auto& record : records) {
        const auto window = record.LeafWindow();
        if (window.count == 0) {
            continue;
        }
        if (!seen_blocks.insert(window.kv_block_ptr).second) {
            continue;
        }
        kv_block_ptrs.push_back(window.kv_block_ptr);
    }
    ResolveL1BlocksToTables(kv_block_ptrs, output);
}

void Version::ResolveL1BlocksToTables(const std::vector<uint64_t>& kv_block_ptrs,
                                      std::vector<TaggedPstMeta>& output) const
{
    output.clear();
    output.reserve(kv_block_ptrs.size());
    std::unordered_set<uint64_t> seen_blocks;
    seen_blocks.reserve(kv_block_ptrs.size());
    for (const auto kv_block_ptr : kv_block_ptrs) {
        if (!seen_blocks.insert(kv_block_ptr).second) {
            continue;
        }
        TaggedPstMeta resolved;
        if (!ResolveL1BlockToTableMeta(kv_block_ptr, resolved)) {
            continue;
        }
        output.push_back(resolved);
    }
    SortResolvedTablesByKeyRange(output);
}

bool Version::L1TreeConsistencyCheckAndFix(PSTDeleter *pst_deleter,Manifest* manifest)
{
    std::vector<flowkv::hybrid_l1::SubtreeRecord> fragments;
    level1_tree_->ExportLocalFragments(fragments);
    TaggedPstMeta last_pst_meta, current_pst_meta;
    flowkv::hybrid_l1::SubtreeRecord last_fragment;
    bool has_last_fragment = false;
    std::unordered_set<uint64_t> deleted_blocks;
    DEBUG("L1 local fragment size=%lu",fragments.size());
    for (const auto &fragment : fragments)
    {
        TaggedPstMeta resolved;
        if (!ResolveL1RecordToTableMeta(fragment, resolved)) {
            continue;
        }
        if (deleted_blocks.find(resolved.meta.datablock_ptr_) != deleted_blocks.end()) {
            continue;
        }
        current_pst_meta = resolved;

        bool current_deleted = false;
        if (has_last_fragment &&
            last_fragment.route_prefix == fragment.route_prefix &&
            last_fragment.OverlapsLocalSuffixRange(fragment.route_min_suffix, fragment.route_max_suffix))
        {
            if (last_pst_meta.meta.seq_no_ < current_pst_meta.meta.seq_no_)
            {
                DeleteTableInL1(last_pst_meta.meta);
                if (manifest != nullptr &&
                    last_pst_meta.manifest_position != std::numeric_limits<size_t>::max()) {
                    manifest->DeleteTable(static_cast<int>(last_pst_meta.manifest_position), 1);
                }
                deleted_blocks.insert(last_pst_meta.meta.datablock_ptr_);
                has_last_fragment = false;
            }
            else
            {
                DeleteTableInL1(current_pst_meta.meta);
                if (manifest != nullptr &&
                    current_pst_meta.manifest_position != std::numeric_limits<size_t>::max()) {
                    manifest->DeleteTable(static_cast<int>(current_pst_meta.manifest_position), 1);
                }
                deleted_blocks.insert(current_pst_meta.meta.datablock_ptr_);
                current_deleted = true;
            }
        }

        if (!current_deleted) {
            last_fragment = fragment;
            last_pst_meta = current_pst_meta;
            has_last_fragment = true;
        }
    }
    pst_deleter->PersistCheckpoint();
    return true;
}

bool Version::DebugValidateLevel1Structure() const
{
    return level1_tree_->Validate();
}

flowkv::hybrid_l1::L1HybridIndex::MemoryUsageStats Version::DebugEstimateLevel1MemoryUsage() const
{
    if (level1_tree_ == nullptr) {
        return {};
    }
    return level1_tree_->EstimateMemoryUsage();
}

void Version::DebugExportLevel1Records(std::vector<flowkv::hybrid_l1::SubtreeRecord> &output) const
{
    level1_tree_->ExportAll(output);
}

void Version::DebugExportLevel1LocalFragments(
    std::vector<flowkv::hybrid_l1::SubtreeRecord> &output) const
{
    level1_tree_->ExportLocalFragments(output);
}

bool Version::DebugResolveLevel1Record(const flowkv::hybrid_l1::SubtreeRecord &record,
                                       TaggedPstMeta &output) const
{
    return ResolveL1RecordToTableMeta(record, output);
}

void Version::DebugExportActiveLevel1Tables(std::vector<TaggedPstMeta> &output) const
{
    CollectActiveLevel1Tables(output);
}

void Version::DebugCollectLevel1Candidates(const KeyType &key, std::vector<TaggedPstMeta> &output) const
{
    output.clear();
    flowkv::hybrid_l1::RangeScanRecordOptions options;
    options.include_window_fragments = false;
    options.include_unique_blocks = true;
    options.dedup_windows = true;
    flowkv::hybrid_l1::RangeScanRecordResult result;
    level1_tree_->RangeScanRecords(key, key, options, result);
    ResolveL1BlocksToTables(result.unique_kv_block_ptrs, output);
}

void Version::DebugCollectLevel1Overlaps(const KeyType &min,
                                         const KeyType &max,
                                         std::vector<TaggedPstMeta> &output) const
{
    output.clear();
    std::vector<flowkv::hybrid_l1::SubtreeRecord> matches;
    level1_tree_->RangeScan(min, max, matches);
    std::unordered_set<uint64_t> seen_blocks;
    seen_blocks.reserve(matches.size());
    for (const auto &record : matches) {
        TaggedPstMeta resolved;
        if (!ResolveL1RecordToTableMeta(record, resolved)) {
            continue;
        }
        if (!seen_blocks.insert(resolved.meta.datablock_ptr_).second) {
            continue;
        }
        output.push_back(resolved);
    }
}

bool Version::ExportL1HybridState(std::vector<uint8_t>& bytes_out, uint32_t& current_l1_seq_no) const
{
    bytes_out.clear();
    current_l1_seq_no = (l1_seq_ == 0) ? 0u : static_cast<uint32_t>(l1_seq_ - 1);
    if (level1_tree_ == nullptr) {
        return false;
    }

    std::vector<flowkv::hybrid_l1::RoutePartition> partitions;
    size_t logical_size = 0;
    uint64_t generation = 0;
    level1_tree_->ExportPersistedState(partitions, logical_size, generation);

    bytes_out.reserve(64 + partitions.size() * 128);
    WriteU32(bytes_out, kL1HybridStateMagic);
    WriteU16(bytes_out, kL1HybridStateFormat);
    WriteU16(bytes_out, 0);
    WriteU32(bytes_out, current_l1_seq_no);
    WriteU64(bytes_out, static_cast<uint64_t>(logical_size));
    WriteU64(bytes_out, generation);
    WriteU32(bytes_out, static_cast<uint32_t>(partitions.size()));

    for (const auto& partition : partitions) {
        WriteU64(bytes_out, partition.prefix);
        WriteU64(bytes_out, partition.generation);
        WriteU64(bytes_out, static_cast<uint64_t>(partition.record_count));
        bytes_out.push_back(EncodeGovernance(partition.governance));
        bytes_out.push_back(static_cast<uint8_t>(partition.descriptor_mode));
        bytes_out.push_back(partition.tiny_enter_streak);
        WriteU64(bytes_out, partition.tiny_leaf_value);
        WriteU64(bytes_out, partition.pack_page_ptr);
        bytes_out.push_back(partition.pack_slot_id);
        bytes_out.push_back(0);
        WriteU16(bytes_out, 0);
        WriteU32(bytes_out, partition.subtree_store.page_size);
        WriteU32(bytes_out, partition.subtree_store.page_count);
        bytes_out.push_back(partition.subtree_store.flags);
        WriteU64(bytes_out, partition.subtree_store.root_page_ptr);
        WriteU64(bytes_out, partition.subtree_store.manifest_page_ptr);
        WriteU64(bytes_out, partition.subtree_store.record_count);
    }
    return true;
}

bool Version::ImportL1HybridState(const std::vector<uint8_t>& bytes, uint32_t expected_l1_seq_no)
{
    if (level1_tree_ == nullptr || bytes.empty()) {
        return false;
    }

    size_t cursor = 0;
    uint32_t magic = 0;
    uint16_t format = 0;
    uint16_t reserved = 0;
    uint32_t snapshot_seq = 0;
    uint64_t logical_size = 0;
    uint64_t generation = 0;
    uint32_t partition_count = 0;
    if (!ReadU32(bytes, cursor, magic) || !ReadU16(bytes, cursor, format) ||
        !ReadU16(bytes, cursor, reserved) || !ReadU32(bytes, cursor, snapshot_seq) ||
        !ReadU64(bytes, cursor, logical_size) || !ReadU64(bytes, cursor, generation) ||
        !ReadU32(bytes, cursor, partition_count)) {
        return false;
    }
    if (magic != kL1HybridStateMagic || reserved != 0) {
        return false;
    }
    if (format != 3 && format != 4 && format != kL1HybridStateFormat) {
        return false;
    }
    if (snapshot_seq != expected_l1_seq_no) {
        return false;
    }

    std::vector<flowkv::hybrid_l1::RoutePartition> partitions;
    partitions.reserve(partition_count);
    for (uint32_t i = 0; i < partition_count; ++i) {
        flowkv::hybrid_l1::RoutePartition partition;
        uint64_t prefix = 0;
        uint64_t partition_generation = 0;
        uint64_t record_count = 0;
        uint8_t governance_flags = 0;
        uint8_t descriptor_mode = static_cast<uint8_t>(
            flowkv::hybrid_l1::RouteDescriptorMode::kNormalSubtree);
        uint8_t tiny_enter_streak = 0;
        uint64_t tiny_leaf_value = 0;
        uint64_t pack_page_ptr = flowkv::hybrid_l1::kInvalidSubtreePagePtr;
        uint8_t pack_slot_id = 0;
        uint32_t page_size = 0;
        uint32_t page_count = 0;
        uint8_t subtree_store_flags = 0;
        uint64_t subtree_root_page_ptr = flowkv::hybrid_l1::kInvalidSubtreePagePtr;
        uint64_t subtree_manifest_page_ptr = flowkv::hybrid_l1::kInvalidSubtreePagePtr;
        uint64_t subtree_record_count = 0;
        if (!ReadU64(bytes, cursor, prefix) ||
            !ReadU64(bytes, cursor, partition_generation) ||
            !ReadU64(bytes, cursor, record_count)) {
            return false;
        }
        if (cursor + 1 > bytes.size()) {
            return false;
        }
        governance_flags = bytes[cursor++];
        if (format >= 4) {
            if (cursor + 2 > bytes.size()) {
                return false;
            }
            descriptor_mode = bytes[cursor++];
            tiny_enter_streak = bytes[cursor++];
            if (!ReadU64(bytes, cursor, tiny_leaf_value)) {
                return false;
            }
            if (format >= 5) {
                uint16_t descriptor_reserved = 0;
                if (!ReadU64(bytes, cursor, pack_page_ptr)) {
                    return false;
                }
                if (cursor + 2 > bytes.size()) {
                    return false;
                }
                pack_slot_id = bytes[cursor++];
                ++cursor;  // reserved byte
                if (!ReadU16(bytes, cursor, descriptor_reserved)) {
                    return false;
                }
            }
        }
        if (!ReadU32(bytes, cursor, page_size) || !ReadU32(bytes, cursor, page_count)) {
            return false;
        }
        if (cursor + 1 > bytes.size()) {
            return false;
        }
        subtree_store_flags = bytes[cursor++];
        if (!ReadU64(bytes, cursor, subtree_root_page_ptr) ||
            !ReadU64(bytes, cursor, subtree_manifest_page_ptr) ||
            !ReadU64(bytes, cursor, subtree_record_count)) {
            return false;
        }

        partition.prefix = prefix;
        if (partition_generation > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        partition.generation = static_cast<uint32_t>(partition_generation);
        if (record_count > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        partition.record_count = static_cast<uint32_t>(record_count);
        partition.governance = DecodeGovernance(governance_flags);
        if (descriptor_mode > static_cast<uint8_t>(
                                  flowkv::hybrid_l1::RouteDescriptorMode::kNormalPack)) {
            return false;
        }
        partition.descriptor_mode =
            static_cast<flowkv::hybrid_l1::RouteDescriptorMode>(descriptor_mode);
        partition.tiny_enter_streak = tiny_enter_streak;
        partition.tiny_leaf_value = tiny_leaf_value;
        partition.pack_page_ptr = pack_page_ptr;
        partition.pack_slot_id = pack_slot_id;
        if (page_size > std::numeric_limits<uint16_t>::max()) {
            return false;
        }
        partition.subtree_store.page_size = static_cast<uint16_t>(page_size);
        partition.subtree_store.flags = subtree_store_flags;
        partition.subtree_store.page_count = page_count;
        partition.subtree_store.record_count = subtree_record_count;
        partition.subtree_store.root_page_ptr = subtree_root_page_ptr;
        partition.subtree_store.manifest_page_ptr = subtree_manifest_page_ptr;
        partitions.push_back(std::move(partition));
    }
    if (cursor != bytes.size()) {
        return false;
    }

    if (!level1_tree_->ImportPersistedState(partitions,
                                            static_cast<size_t>(logical_size),
                                            generation)) {
        return false;
    }

    if (level1_table_by_block_.empty()) {
        // Tableless recovery mode: synthesize block catalog from persisted records.
        RebuildLevel1TableMapFromRecords();
        return true;
    }

    std::vector<flowkv::hybrid_l1::SubtreeRecord> fragments;
    level1_tree_->ExportLocalFragments(fragments);
    for (const auto& fragment : fragments) {
        TaggedPstMeta resolved;
        if (!ResolveL1RecordToTableMeta(fragment, resolved)) {
            // Compatibility fallback: if manifest-derived map is stale, rebuild
            // map from persisted records once and retry validation.
            RebuildLevel1TableMapFromRecords();
            break;
        }
    }

    level1_tree_->ExportLocalFragments(fragments);
    for (const auto& fragment : fragments) {
        TaggedPstMeta resolved;
        if (!ResolveL1RecordToTableMeta(fragment, resolved)) {
            RebuildLevel1Tree();
            return false;
        }
    }
    return true;
}

void Version::RebuildLevel1Tree()
{
    std::vector<TaggedPstMeta> active_tables;
    CollectActiveLevel1Tables(active_tables);
    level1_tree_->BulkLoadFromTables(active_tables);
}

void Version::RebuildLevel1Partitions(const std::vector<KeyType>& changed_route_keys)
{
    RebuildLevel1Partitions(changed_route_keys, nullptr);
}

void Version::RebuildLevel1Partitions(
    const std::vector<KeyType>& changed_route_keys,
    const flowkv::hybrid_l1::L1DeltaBatch* delta_batch)
{
    if (delta_batch != nullptr && !delta_batch->Empty()) {
        level1_tree_->RebuildPartitionsFromDelta(changed_route_keys, *delta_batch);
        return;
    }
    // Without delta payload, avoid table-driven partial rebuild fallback.
    // Rebuild the full L1 tree from the current block map snapshot instead.
    RebuildLevel1Tree();
}

void Version::QueueOrApplyL1Rebuild(const std::vector<KeyType>& changed_route_keys)
{
    if (changed_route_keys.empty()) {
        return;
    }
    if (has_pending_l1_delta_batch_ && l1_batch_update_depth_ > 0) {
        return;
    }
    if (l1_batch_update_depth_ > 0) {
        pending_l1_changed_route_keys_.insert(pending_l1_changed_route_keys_.end(),
                                              changed_route_keys.begin(),
                                              changed_route_keys.end());
        return;
    }
    RebuildLevel1Partitions(changed_route_keys);
}

std::vector<KeyType> Version::CollectChangedRouteKeysForTable(const PSTMeta& table) const
{
    std::vector<KeyType> changed_route_keys;
    const auto begin = flowkv::hybrid_l1::ExtractPrefix(table.MinKey());
    const auto end = flowkv::hybrid_l1::ExtractPrefix(table.MaxKey());
    auto current = begin;
    while (true) {
        changed_route_keys.push_back(flowkv::hybrid_l1::ComposeKey(current, 0));
        if (current == end) {
            break;
        }
        ++current;
    }
    return changed_route_keys;
}
