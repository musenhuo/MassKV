#include "version.h"
#include "manifest.h"
#include "lib/hybrid_l1/l1_hybrid_index.h"
#include "lib/hybrid_l1/prefix_suffix.h"
#include "lib/hybrid_l1/subtree_record.h"
#if defined(USE_HMASSTREE)
#include "lib/index_hmasstree.h"
#else
#include "lib/index_masstree.h"
#endif
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
constexpr uint16_t kL1HybridStateFormat = 2;

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

}  // namespace

Version::Version(SegmentAllocator *seg_allocator) : pst_reader_(seg_allocator)
{
    level0_table_lists_.resize(MAX_L0_TREE_NUM);
    for (int i = 0; i < MAX_L0_TREE_NUM; i++)
    {
        level0_trees_[i] = nullptr;
    }
    level1_tables_.reserve(6553600); // reserve for at most 6400M records
    flowkv::hybrid_l1::L1HybridIndex::BuildOptions options;
    options.segment_allocator = seg_allocator;
    bool enable_subtree_cache = false;
    if (ParseEnvBool("FLOWKV_L1_ENABLE_SUBTREE_CACHE", enable_subtree_cache)) {
        options.enable_subtree_cache = enable_subtree_cache;
    }
    size_t subtree_cache_capacity = 0;
    if (ParseEnvSize("FLOWKV_L1_SUBTREE_CACHE_CAPACITY", subtree_cache_capacity)) {
        options.subtree_cache_capacity = subtree_cache_capacity;
    }
    size_t subtree_cache_max_bytes = 0;
    if (ParseEnvSize("FLOWKV_L1_SUBTREE_CACHE_MAX_BYTES", subtree_cache_max_bytes)) {
        options.subtree_cache_max_bytes = subtree_cache_max_bytes;
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
    // get position
    PSTMeta table = tmeta.meta;
    int idx = level0_table_lists_[tree_idx].size();
    level0_table_lists_[tree_idx].emplace_back(tmeta);
    // update index
    ValueHelper lh((uint64_t)idx);
    KeyType tempk = table.MaxKey();
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
    int idx;
    // find a position
    if (level1_free_list_.empty())
    {
        idx = level1_tables_.size();
        level1_tables_.emplace_back(tmeta);
    }
    else
    {
        idx = level1_free_list_.back();
        level1_free_list_.pop_back();
        level1_tables_[idx] = tmeta;
    }

    assert(level1_tables_.size() > idx);
    RebuildLevel1Partitions(CollectChangedRouteKeysForTable(tmeta.meta));
    return idx;
}

void Version::RecoverLevel1Tables(std::vector<TaggedPstMeta> tables, uint32_t next_l1_seq)
{
    level1_tables_ = std::move(tables);
    level1_free_list_.clear();
    for (size_t i = 0; i < level1_tables_.size(); ++i) {
        if (!level1_tables_[i].Valid()) {
            level1_free_list_.push_back(i);
        }
    }
    l1_seq_ = static_cast<int>(next_l1_seq);
    RebuildLevel1Tree();
}

// 删除时比对删除的value是否为table的indexblock_ptr，若不是说明已经被同key的其他pst替代了
bool Version::DeleteTableInL1(PSTMeta table)
{
    for (size_t idx = 0; idx < level1_tables_.size(); ++idx) {
        auto &entry = level1_tables_[idx];
        if (!entry.Valid()) {
            continue;
        }
        PSTMeta &old = entry.meta;
        if (old.datablock_ptr_ != table.datablock_ptr_) {
            continue;
        }
        if (CompareKeyType(old.MaxKey(), table.MaxKey()) != 0 ||
            CompareKeyType(old.MinKey(), table.MinKey()) != 0) {
            continue;
        }
        entry.meta = PSTMeta::InvalidTable();
        level1_free_list_.push_back(idx);
        RebuildLevel1Partitions(CollectChangedRouteKeysForTable(table));
        return true;
    }
    return false;
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

}  // namespace

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
    printf("  Total PST entries in vector: %lu\n", level1_tables_.size());
    printf("  Free list size: %lu\n", level1_free_list_.size());
    printf("  Active PST count: %lu\n", level1_tables_.size() - level1_free_list_.size());
    
    // Count valid entries
    int valid_count = 0;
    for (const auto& pst : level1_tables_) {
        if (pst.Valid()) valid_count++;
    }
    printf("  Valid PST count: %d\n", valid_count);
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
    int idx = static_cast<int>(l1_record.table_idx);
    if (idx < 0 || static_cast<size_t>(idx) >= level1_tables_.size()) {
        g_l1_idx_not_found_.fetch_add(1, std::memory_order_relaxed);
        DEBUG("L1 idx out of range: idx=%d, table_size=%lu", idx, level1_tables_.size());
        return false;
    }
    table = level1_tables_[idx];
    if (!table.Valid())
    {
        g_l1_table_invalid_.fetch_add(1, std::memory_order_relaxed);
        DEBUG("6");
        return false;
    }

#if defined(FLOWKV_KEY16)
    if (!(table.meta.MinKey().hi == MAX_UINT64 && table.meta.MinKey().lo == MAX_UINT64) && KeyTypeGreater(table.meta.MinKey(), int_key))
#else
    if (table.meta.MinKey() != MAX_UINT64 && KeyTypeGreater(table.meta.MinKey(), int_key))
#endif
    {
        g_l1_range_check_fail_.fetch_add(1, std::memory_order_relaxed);
#if defined(FLOWKV_KEY16)
        KeyType tmin = table.meta.MinKey();
        KeyType tmax = table.meta.MaxKey();
        DEBUG("7, key=%lu:%lu at %lu (%lu:%lu~%lu:%lu)", int_key.hi, int_key.lo, table.meta.datablock_ptr_, tmin.hi, tmin.lo, tmax.hi, tmax.lo);
#else
        DEBUG("7, key=%lu at %lu (%lu~%lu)", key.ToUint64Bswap(), table.meta.datablock_ptr_, __bswap_64(table.meta.MinKey()), __bswap_64(table.meta.MaxKey()));
#endif
        return false;
    }
    bool ret = false;
    if (l1_record.HasLeafWindow()) {
        const auto window = l1_record.LeafWindow();
        const uint64_t hinted_block_off =
            flowkv::hybrid_l1::SubtreeRecord::DecodeKvBlockOffset(window.kv_block_ptr);
        if (window.count > 0 && hinted_block_off == table.meta.datablock_ptr_ &&
            window.offset < table.meta.entry_num_) {
            const uint16_t remaining = static_cast<uint16_t>(table.meta.entry_num_ - window.offset);
            const uint16_t hinted_entries = std::min<uint16_t>(remaining, window.count);
            if (hinted_entries > 0) {
                ret = pst_reader->PointQueryWindow(
                    table.meta.datablock_ptr_,
                    key,
                    value_out,
                    value_size,
                    table.meta.entry_num_,
                    window.offset,
                    hinted_entries);
            }
        }
    }
    if (!ret) {
        ret = pst_reader->PointQuery(
            table.meta.datablock_ptr_, key, value_out, value_size, table.meta.entry_num_);
    }

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
    std::vector<flowkv::hybrid_l1::SubtreeRecord> matches;
#if defined(FLOWKV_KEY16)
    KeyType int_key = key.ToKey16();
#else
    KeyType int_key = key.ToUint64();
#endif
    level1_tree_->LookupCandidates(int_key, 2, matches);
    for (const auto &record : matches)
    {
        if (record.table_idx < level1_tables_.size()) {
            table_metas.push_back(level1_tables_[record.table_idx]);
        }
    }
    return new RowIterator(pst_reader, table_metas);
}

bool Version::CheckSpaceForL0Tree()
{
    return (l0_tail_ + 1) % MAX_L0_TREE_NUM != l0_head_;
}

int Version::AddLevel0Tree()
{
    if ((l0_tail_ + 1) % MAX_L0_TREE_NUM == l0_head_)
        return -1;
    auto ret = l0_tail_;
    l0_tail_ = (l0_tail_ + 1) % MAX_L0_TREE_NUM;
#ifdef MASSTREE_L1
    #if defined(USE_HMASSTREE)
    level0_trees_[ret] = new HMasstreeIndex();
    #else
    level0_trees_[ret] = new MasstreeIndex();
    #endif
#endif
#ifdef HOT_L1
    level0_trees_[ret] = new HOTIndex(MAX_PST_L1);
#endif
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

    if (l0_tail_ == l0_head_)
    {
        return false;
    }
    auto idx = l0_head_;
    l0_head_ = (l0_head_ + 1) % MAX_L0_TREE_NUM;
    // printf("before FreeLevel0Tree\n");
    // wait for reader leave the index (just sleep for > 1ms)
    usleep(100);
    delete level0_trees_[idx];
    // printf("after FreeLevel0Tree\n");
    level0_trees_[idx] = nullptr;
    level0_tree_meta_[idx] = TreeMeta();
    level0_table_lists_[idx].clear();
    return true;
}
void Version::UpdateLevel0ReadTail()
{
    l0_read_tail_ = l0_tail_;
}

int Version::PickLevel0Trees(std::vector<std::vector<TaggedPstMeta>> &outputs, std::vector<TreeMeta> &tree_metas, int max_size)
{
    int tail = l0_read_tail_;
    int head = l0_head_;
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
    std::vector<flowkv::hybrid_l1::SubtreeRecord> matches;
    level1_tree_->RangeScan(min, max, matches);
    for (const auto &record : matches)
    {
        if (record.table_idx >= level1_tables_.size()) {
            continue;
        }
        output.emplace_back(level1_tables_[record.table_idx]);
    }
    return true;
}

bool Version::L1TreeConsistencyCheckAndFix(PSTDeleter *pst_deleter,Manifest* manifest)
{
    std::vector<flowkv::hybrid_l1::SubtreeRecord> fragments;
    level1_tree_->ExportLocalFragments(fragments);
    TaggedPstMeta last_pst_meta, current_pst_meta;
    flowkv::hybrid_l1::SubtreeRecord last_fragment;
    bool has_last_fragment = false;
    std::unordered_set<uint64_t> deleted_table_idx;
    DEBUG("L1 local fragment size=%lu",fragments.size());
    for (const auto &fragment : fragments)
    {
        if (fragment.table_idx >= level1_tables_.size()) {
            continue;
        }
        if (deleted_table_idx.find(fragment.table_idx) != deleted_table_idx.end()) {
            continue;
        }
        current_pst_meta = level1_tables_[fragment.table_idx];
        if (!current_pst_meta.Valid()) {
            continue;
        }

        bool current_deleted = false;
        if (has_last_fragment &&
            last_fragment.route_prefix == fragment.route_prefix &&
            last_fragment.OverlapsLocalSuffixRange(fragment.route_min_suffix, fragment.route_max_suffix))
        {
            if (last_pst_meta.meta.seq_no_ < current_pst_meta.meta.seq_no_)
            {
                DeleteTableInL1(last_pst_meta.meta);
                manifest->DeleteTable(last_pst_meta.manifest_position,1);
                deleted_table_idx.insert(last_fragment.table_idx);
                has_last_fragment = false;
            }
            else
            {
                DeleteTableInL1(current_pst_meta.meta);
                manifest->DeleteTable(current_pst_meta.manifest_position,1);
                deleted_table_idx.insert(fragment.table_idx);
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
    if (record.table_idx >= level1_tables_.size()) {
        return false;
    }
    if (!level1_tables_[record.table_idx].Valid()) {
        return false;
    }
    output = level1_tables_[record.table_idx];
    return true;
}

void Version::DebugExportActiveLevel1Tables(std::vector<TaggedPstMeta> &output) const
{
    output.clear();
    output.reserve(level1_tables_.size());
    for (const auto &table : level1_tables_) {
        if (table.Valid()) {
            output.push_back(table);
        }
    }
}

void Version::DebugCollectLevel1Candidates(const KeyType &key, std::vector<TaggedPstMeta> &output) const
{
    output.clear();
    std::vector<flowkv::hybrid_l1::SubtreeRecord> matches;
    level1_tree_->LookupCandidates(key, 2, matches);
    for (const auto &record : matches) {
        if (record.table_idx < level1_tables_.size() && level1_tables_[record.table_idx].Valid()) {
            output.push_back(level1_tables_[record.table_idx]);
        }
    }
}

void Version::DebugCollectLevel1Overlaps(const KeyType &min,
                                         const KeyType &max,
                                         std::vector<TaggedPstMeta> &output) const
{
    output.clear();
    std::vector<flowkv::hybrid_l1::SubtreeRecord> matches;
    level1_tree_->RangeScan(min, max, matches);
    for (const auto &record : matches) {
        if (record.table_idx < level1_tables_.size() && level1_tables_[record.table_idx].Valid()) {
            output.push_back(level1_tables_[record.table_idx]);
        }
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
        WriteU32(bytes_out, partition.subtree_store.page_size);
        WriteU32(bytes_out, static_cast<uint32_t>(partition.subtree_store.pages.size()));
        bytes_out.push_back(partition.subtree_store.flags);
        WriteU32(bytes_out, partition.subtree_store.root_page_id);
        WriteU32(bytes_out, partition.subtree_store.page_count);
        WriteU64(bytes_out, partition.subtree_store.record_count);
        for (const auto& page_ref : partition.subtree_store.pages) {
            WriteU64(bytes_out, page_ref.segment_id);
            WriteU32(bytes_out, page_ref.page_id);
        }
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
    if (format != 1 && format != kL1HybridStateFormat) {
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
        uint32_t page_size = 0;
        uint32_t page_count = 0;
        uint8_t subtree_store_flags = 0;
        uint32_t subtree_root_page_id = flowkv::hybrid_l1::kInvalidSubtreePageId;
        uint32_t subtree_page_count = 0;
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
        if (!ReadU32(bytes, cursor, page_size) || !ReadU32(bytes, cursor, page_count)) {
            return false;
        }
        if (format >= 2) {
            if (cursor + 1 > bytes.size()) {
                return false;
            }
            subtree_store_flags = bytes[cursor++];
            if (!ReadU32(bytes, cursor, subtree_root_page_id) ||
                !ReadU32(bytes, cursor, subtree_page_count) ||
                !ReadU64(bytes, cursor, subtree_record_count)) {
                return false;
            }
        }

        partition.prefix = prefix;
        partition.generation = partition_generation;
        if (record_count > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        partition.record_count = static_cast<uint32_t>(record_count);
        partition.governance = DecodeGovernance(governance_flags);
        partition.subtree_store.page_size = page_size;
        partition.subtree_store.flags = subtree_store_flags;
        partition.subtree_store.root_page_id = subtree_root_page_id;
        partition.subtree_store.page_count = subtree_page_count;
        partition.subtree_store.record_count = subtree_record_count;
        partition.subtree_store.pages.reserve(page_count);
        for (uint32_t p = 0; p < page_count; ++p) {
            uint64_t segment_id = 0;
            uint32_t page_id = 0;
            if (!ReadU64(bytes, cursor, segment_id) || !ReadU32(bytes, cursor, page_id)) {
                return false;
            }
            if (segment_id > std::numeric_limits<uint32_t>::max()) {
                return false;
            }
            partition.subtree_store.pages.push_back(
                {static_cast<uint32_t>(segment_id), page_id});
        }
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

    std::vector<flowkv::hybrid_l1::SubtreeRecord> fragments;
    level1_tree_->ExportLocalFragments(fragments);
    for (const auto& fragment : fragments) {
        if (fragment.table_idx >= level1_tables_.size() ||
            !level1_tables_[fragment.table_idx].Valid()) {
            RebuildLevel1Tree();
            return false;
        }
    }
    return true;
}

void Version::RebuildLevel1Tree()
{
    level1_tree_->BulkLoadFromTables(level1_tables_);
}

void Version::RebuildLevel1Partitions(const std::vector<KeyType>& changed_route_keys)
{
    if (level1_tree_->Empty()) {
        RebuildLevel1Tree();
        return;
    }
    level1_tree_->RebuildPartitionsFromTables(level1_tables_, changed_route_keys);
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
