#pragma once

#include "pst_reader.h"
#include "lib/hybrid_l1/subtree_record.h"

#include <algorithm>
#include <cassert>
#include <queue>
#include <vector>

class L1RecordIterator : public KeyValueIterator
{
public:
    using SubtreeRecord = flowkv::hybrid_l1::SubtreeRecord;

    L1RecordIterator(PSTReader *pst_reader,
                     const std::vector<SubtreeRecord> &records)
        : pst_reader_(pst_reader), records_(records)
    {
        current_valid_ = AdvanceToNextOutput();
    }

    ~L1RecordIterator() override
    {
        for (auto &state : states_) {
            if (state.iter != nullptr) {
                delete state.iter;
                state.iter = nullptr;
            }
        }
    }

    KeyType GetCurrentKey() override
    {
        assert(current_valid_);
        return current_key_;
    }

    FixedValue16 GetCurrentValue() override
    {
        assert(current_valid_);
        return current_value_;
    }

    bool NextKey() override
    {
        current_valid_ = AdvanceToNextOutput();
        return current_valid_;
    }

    bool Valid() override
    {
        return current_valid_;
    }

    bool MoveTo(const KeyType &key) override
    {
        while (Valid() && CompareKeyType(GetCurrentKey(), key) < 0) {
            if (!NextKey()) {
                return false;
            }
        }
        return Valid();
    }

private:
    struct ActiveState {
        PSTReader::Iterator *iter = nullptr;
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
        bool operator()(const HeapEntry &lhs, const HeapEntry &rhs) const
        {
            const int cmp = CompareKeyType(lhs.key, rhs.key);
            if (cmp != 0) {
                return cmp > 0;
            }
            return lhs.state_idx > rhs.state_idx;
        }
    };

    bool IsRecordNewer(size_t lhs_record_idx, size_t rhs_record_idx) const
    {
        const SubtreeRecord &lhs = records_[lhs_record_idx];
        const SubtreeRecord &rhs = records_[rhs_record_idx];
        if (lhs.seq_no != rhs.seq_no) {
            return lhs.seq_no > rhs.seq_no;
        }
        return lhs_record_idx < rhs_record_idx;
    }

    bool AdvanceState(size_t state_idx)
    {
        ActiveState &state = states_[state_idx];
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

    bool HasPendingWindowWithMinLE(const KeyType &key) const
    {
        for (size_t idx = next_record_idx_; idx < records_.size(); ++idx) {
            const auto window = records_[idx].LeafWindow();
            if (window.count == 0) {
                continue;
            }
            return CompareKeyType(records_[idx].RouteMinKey(), key) <= 0;
        }
        return false;
    }

    bool ActivateNextWindow()
    {
        while (next_record_idx_ < records_.size()) {
            const size_t record_idx = next_record_idx_++;
            const SubtreeRecord &record = records_[record_idx];
            const auto window = record.LeafWindow();
            if (window.count == 0) {
                continue;
            }
            const uint64_t block_offset = SubtreeRecord::DecodeKvBlockOffset(window.kv_block_ptr);
            PSTReader::Iterator *iter = pst_reader_->GetIterator(block_offset);
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

    bool AdvanceToNextOutput()
    {
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

    PSTReader *pst_reader_ = nullptr;
    const std::vector<SubtreeRecord> &records_;
    std::vector<ActiveState> states_;
    std::priority_queue<HeapEntry,
                        std::vector<HeapEntry>,
                        HeapEntryComparator> heap_;
    size_t next_record_idx_ = 0;
    bool current_valid_ = false;
    KeyType current_key_{};
    FixedValue16 current_value_{};
};
