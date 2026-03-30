/**
 * @file flush.cpp
 * @author your name (you@domain.com)
 * @brief core steps: 1. iterate all items in the memtable index and copy them into a sorted array
 *        2. get the address of log entries and arrange them to 
 *        3. delete index
 *        4. release (or mark with oplog?) the participant log segments for crash-consistentcy
 * @version 0.1
 * @date 2022-09-21
 *
 * @copyright Copyright (c) 2022
 *
 */
#include "flush.h"
#include "manifest.h"
#include "version.h"
#include <queue>
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"
inline void FlushJob::FlushPST()
{
	auto meta = pst_builder_.Flush();
	meta.seq_no_ = tree_seq_no_;
	if (meta.Valid())
	{
		// add meta into manifest
		TaggedPstMeta tmeta;
		tmeta.meta = meta;
		tmeta.level = 0;
		tmeta.manifest_position = manifest_->AddTable(meta, 0);
		version_->InsertTableToL0(tmeta, tree_idx_);
		output_pst_list_.push_back(tmeta);
	}
}
FlushJob::~FlushJob()
{
}
bool FlushJob::run()
{
	LOG("iterate index (ForEachEntry)");
	// build psts and version
	LOG("add level0 tree");
	uint32_t tree_seq = 0;
	tree_idx_ = version_->AddLevel0Tree(&tree_seq);
	while (tree_idx_ == -1)
	{
		INFO("can't addlevel0tree, waiting...");
		usleep(100000);
		tree_idx_ = version_->AddLevel0Tree(&tree_seq);
	}
	tree_seq_no_ = tree_seq;
	LOG("tree_idx=%d, streaming flush", tree_idx_);

	int cur_partition = 0;
	KeyType partition_max_key = partition_info_[cur_partition].max_key;
	size_t flushed_count = 0;

	memtable_index_->ForEachEntry([&](KeyType k, uint64_t val) -> bool {
		// Prepare key slice
#if defined(FLOWKV_KEY16)
		uint8_t k_bytes[16];
		k.ToBigEndianBytes(k_bytes);
		Slice key(reinterpret_cast<const char *>(k_bytes), 16);
#else
		Slice key(&k);
#endif
		// Prepare value slice
		FixedValue16 persisted_value{};
#if defined(INDEX_LOG_MEMTABLE)
		ValuePtr vptr;
		vptr.data_ = val;
		if (!vptr.detail_.valid)
		{
			persisted_value = FixedValue16::Tombstone();
		}
		else
		{
			persisted_value = {};
			if (log_reader_.ReadLogForValue(key, vptr, persisted_value.data()) == 0)
				ERROR_EXIT("flush log value read should succeed");
		}
		Slice value(reinterpret_cast<const char *>(&persisted_value), sizeof(persisted_value));
#else
		uint64_t v = val;
		Slice value(&v);
#endif
		// Partition boundary check
		while (unlikely(KeyTypeGreater(k, partition_max_key)))
		{
			FlushPST();
			cur_partition++;
			if (unlikely(cur_partition >= RANGE_PARTITION_NUM))
				ERROR_EXIT("key > max_key in the largest partition");
			partition_max_key = partition_info_[cur_partition].max_key;
		}

		bool success = pst_builder_.AddEntry(key, value);
		if (!success)
		{
			FlushPST();
			if (!pst_builder_.AddEntry(key, value))
				ERROR_EXIT("cannot add pst entry in flush");
		}
		flushed_count++;
		return true;
	});

	LOG("ForEachEntry flushed %lu entries", flushed_count);
	FlushPST();
	// now the new tree can be read
	version_->PublishLevel0Tree(tree_idx_);
	// delete obsolute index and log segments
	std::vector<uint64_t> segment_list;
	seg_allocater_->GetElementsFromLogGroup(seg_group_id_, &segment_list);
	LOG("delete obsolute index and log segments: %lu ,%lu", segment_list.size(), segment_list[0]);
#ifndef KV_SEPARATE
	manifest_->AddFlushLog(segment_list);
#endif
	for (auto &seg_id : segment_list)
	{
		// TODO: add a new function to free a segment without reopening it
		LOG("ready to delete log segment %lu", seg_id);
		auto log_seg = seg_allocater_->GetLogSegment(seg_id);
#ifdef KV_SEPARATE
		seg_allocater_->CloseSegment(log_seg, true);
#else
		seg_allocater_->FreeSegment(log_seg);
#endif
	}
	LOG("delete over");
#ifndef KV_SEPARATE
	manifest_->ClearFlushLog();
#endif
	return true;
}


bool FlushJob::subrun(int partition_id)
{
	PSTBuilder *pst_builder = partition_pst_builder_[partition_id] = new PSTBuilder(seg_allocater_);
	LOG("streaming flush partition %d", partition_id);

	memtable_index_->ForEachEntryInRange(
		partition_info_[partition_id].min_key,
		partition_info_[partition_id].max_key,
		[&](KeyType k, uint64_t val) -> bool {
#if defined(FLOWKV_KEY16)
			uint8_t k_bytes[16];
			k.ToBigEndianBytes(k_bytes);
			Slice key(reinterpret_cast<const char *>(k_bytes), 16);
#else
			Slice key(&k);
#endif
			FixedValue16 persisted_value{};
#if defined(INDEX_LOG_MEMTABLE)
			ValuePtr vptr;
			vptr.data_ = val;
			if (!vptr.detail_.valid)
			{
				persisted_value = FixedValue16::Tombstone();
			}
			else
			{
				persisted_value = {};
				if (log_reader_.ReadLogForValue(key, vptr, persisted_value.data()) == 0)
					ERROR_EXIT("flush log value read should succeed");
			}
			Slice value(reinterpret_cast<const char *>(&persisted_value), sizeof(persisted_value));
#else
			uint64_t v = val;
			Slice value(&v);
#endif
			bool success = pst_builder->AddEntry(key, value);
			if (!success)
			{
				auto meta = pst_builder->Flush();
				meta.seq_no_ = tree_seq_no_;
				if (meta.Valid())
				{
					TaggedPstMeta tmeta;
					tmeta.meta = meta;
					tmeta.level = 0;
					partition_outputs_[partition_id].emplace_back(tmeta);
				}
				if (!pst_builder->AddEntry(key, value))
					ERROR_EXIT("cannot add pst entry in flush");
			}
			return true;
		});

	auto meta = pst_builder->Flush();
	meta.seq_no_ = tree_seq_no_;
	if (meta.Valid())
	{
		TaggedPstMeta tmeta;
		tmeta.meta = meta;
		tmeta.level = 0;
		partition_outputs_[partition_id].emplace_back(tmeta);
	}
	return true;
}
struct subrunArgs
{
	FlushJob *cj_;
	int partition_id_;
	subrunArgs(FlushJob *cj,int partition_id) : cj_(cj),partition_id_(partition_id) {}
};
bool FlushJob::subrunParallel()
{
	// build psts and version
	LOG("add level0 tree");
	uint32_t tree_seq = 0;
	tree_idx_ = version_->AddLevel0Tree(&tree_seq);
	while (tree_idx_ == -1)
	{
		INFO("can't addlevel0tree, waiting...");
		usleep(100000);
		tree_idx_ = version_->AddLevel0Tree(&tree_seq);
	}
	tree_seq_no_ = tree_seq;
	LOG("tree_idx=%d, read log and build psts", tree_idx_);

	for (int i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		subrunArgs *sca = new subrunArgs(this,i);
		flush_thread_pool_->Schedule(&FlushJob::TriggerSubFlush,sca,sca,nullptr);
	}
	flush_thread_pool_->WaitForJobsAndJoinAllThreads();

	for (int i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		auto outputs = partition_outputs_[i];
		for (auto &pst : outputs)
		{
			pst.manifest_position = manifest_->AddTable(pst.meta, 0);
			version_->InsertTableToL0(pst, tree_idx_);
			output_pst_list_.push_back(pst);
		}
		delete partition_pst_builder_[i];
	}
	// now the new tree can be read
	version_->PublishLevel0Tree(tree_idx_);
	// delete obsolute index and log segments
	std::vector<uint64_t> segment_list;
	seg_allocater_->GetElementsFromLogGroup(seg_group_id_, &segment_list);
	LOG("delete obsolute index and log segments: %lu ,%lu", segment_list.size(), segment_list[0]);
#ifndef KV_SEPARATE
	manifest_->AddFlushLog(segment_list);
#endif
	for (auto &seg_id : segment_list)
	{
		// TODO: add a new function to free a segment without reopening it
		LOG("ready to delete log segment %lu", seg_id);
		auto log_seg = seg_allocater_->GetLogSegment(seg_id);
#ifdef KV_SEPARATE
		seg_allocater_->CloseSegment(log_seg, true);
#else
		seg_allocater_->FreeSegment(log_seg);
#endif
	}
	LOG("delete over");
#ifndef KV_SEPARATE
	manifest_->ClearFlushLog();
#endif
	return true;
}
void FlushJob::TriggerSubFlush(void *arg)
{
	subrunArgs sca = *(reinterpret_cast<subrunArgs *>(arg));
	// printf("sca.id=%d\n",sca.partition_id_);
	delete (reinterpret_cast<subrunArgs *>(arg));
	static_cast<FlushJob *>(sca.cj_)->subrun(sca.partition_id_);
}
