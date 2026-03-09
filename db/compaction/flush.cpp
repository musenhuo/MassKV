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
	// iterate index to get kv list
	LOG("iterate index(scan2)");
	std::vector<KeyType> keys;
	std::vector<uint64_t> values; // TODO: try to avoid memory allocate/free overhead
	#if defined(FLOWKV_KEY16)
	memtable_index_->Scan2(KeyType{0, 0}, MAX_INT32, keys, values);
	#else
	memtable_index_->Scan2(0, MAX_INT32, keys, values);
	#endif
	LOG("scan2 result: size= %lu", keys.size());
	// build psts and version
	LOG("add level0 tree");
	tree_seq_no_ = version_->GetCurrentL0TreeSeq();
	tree_idx_ = version_->AddLevel0Tree();
	while (tree_idx_ == -1)
	{
		INFO("can't addlevel0tree, waiting...");
		usleep(100000);
		tree_idx_ = version_->AddLevel0Tree();
	}
	LOG("tree_idx=%d, read log and build psts", tree_idx_);
	PSTMeta meta;
	ValuePtr vptr;
	Slice key;
	Slice value;
#if defined(FLOWKV_KEY16)
	Key16 k;
	uint8_t k_bytes[16];
#else
	uint64_t k = 0;
#endif
	uint64_t v = 0;
	FixedValue16 persisted_value{};
	#if defined(FLOWKV_KEY16)
	key = Slice(reinterpret_cast<const char *>(k_bytes), 16);
	value = Slice(reinterpret_cast<const char *>(&persisted_value), sizeof(persisted_value));
	#else
	key = Slice(&k);
	value = Slice(&v);
	#endif
#if defined(FLOWKV_KEY16)
	DEBUG("will flush %lu keys", keys.size());
#else
	DEBUG("will flush %lu keys,key = %lu~%lu,", keys.size(), __bswap_64(keys[0]), __bswap_64(keys[keys.size() - 1]));
#endif

	int cur_partition = 0;
	KeyType partition_max_key = partition_info_[cur_partition].max_key;

	for (size_t i = 0; i < keys.size(); i++)
	{
		k = keys[i];
#if defined(FLOWKV_KEY16)
		// 16B key: encode to bytes for Slice
		k.ToBigEndianBytes(k_bytes);
		key = Slice(reinterpret_cast<const char *>(k_bytes), 16);
#else
		// 8B key: use uint64_t directly
		key = Slice(&k);
#endif
		// DEBUG("aa:%lu", __bswap_64(k));
#if defined(INDEX_LOG_MEMTABLE)
		vptr.data_ = values[i];
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
#else
		v = values[i];
#endif
		while (unlikely(KeyTypeGreater(k, partition_max_key)))
		{
			FlushPST();
			cur_partition++;
			if(unlikely(cur_partition >= RANGE_PARTITION_NUM))ERROR_EXIT("key > max_key in the largest partition");
			partition_max_key = partition_info_[cur_partition].max_key;
		}

		bool success = pst_builder_.AddEntry(key, value);
		if (!success)
		{
			FlushPST();
			if (!pst_builder_.AddEntry(key, value))
				ERROR_EXIT("cannot add pst entry in flush");
		}
	}
	FlushPST();
	// now the new tree can be read
	version_->UpdateLevel0ReadTail();
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
	// iterate index to get kv list
	LOG("iterate index(scan2)");
	//INFO("partition_id=%d",partition_id);
	std::vector<KeyType> keys;
	std::vector<uint64_t> values; // TODO: try to avoid memory allocate/free overhead
	//memtable_index_->Scan2(0, MAX_INT32, keys, values);
	memtable_index_->ScanByRange(partition_info_[partition_id].min_key, partition_info_[partition_id].max_key, keys, values);
	//INFO("%lu~%lu",__bswap_64(partition_info_[partition_id].min_key),__bswap_64(partition_info_[partition_id].max_key));
	//INFO("scan2 result: size= %lu", keys.size());
	// // build psts and version
	// LOG("add level0 tree");
	// tree_seq_no_ = version_->GetCurrentL0TreeSeq();
	// tree_idx_ = version_->AddLevel0Tree();
	// while (tree_idx_ == -1)
	// {
	// 	INFO("can't addlevel0tree, waiting...");
	// 	usleep(100000);
	// 	tree_idx_ = version_->AddLevel0Tree();
	// }
	// LOG("tree_idx=%d, read log and build psts", tree_idx_);
	//INFO("OKKKpartition_id=%d",partition_id);

    // meta_ =
    //     {
    //         .datablock_ptr_ = 0,
    //         .max_key_ = 0,
    //         .min_key_ = MAX_UINT64,
    //         .entry_num_ = 0};
	TaggedPstMeta tmeta;




	PSTMeta meta;
	ValuePtr vptr;
	Slice key;
	Slice value;
#if defined(FLOWKV_KEY16)
	Key16 k;
	uint8_t k_bytes[16];
#else
	uint64_t k = 0;
#endif
	uint64_t v = 0;
	FixedValue16 persisted_value{};
	#if defined(FLOWKV_KEY16)
	key = Slice(reinterpret_cast<const char *>(k_bytes), 16);
	value = Slice(reinterpret_cast<const char *>(&persisted_value), sizeof(persisted_value));
	#else
	key = Slice(&k);
	value = Slice(&v);
	#endif
#if defined(FLOWKV_KEY16)
	DEBUG("will flush %lu keys", keys.size());
#else
	DEBUG("will flush %lu keys,key = %lu~%lu,", keys.size(), __bswap_64(keys[0]), __bswap_64(keys[keys.size() - 1]));
#endif

	for (size_t i = 0; i < keys.size(); i++)
	{
		k = keys[i];
#if defined(FLOWKV_KEY16)
		k.ToBigEndianBytes(k_bytes);
		key = Slice(reinterpret_cast<const char *>(k_bytes), 16);
#else
		key = Slice(&k);
#endif
		// DEBUG("aa:%lu", __bswap_64(k));
#if defined(INDEX_LOG_MEMTABLE)
		vptr.data_ = values[i];
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
#else
		v = values[i];
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
				// tmeta.manifest_position = manifest_->AddTable(meta, 0);
				// version_->InsertTableToL0(tmeta, tree_idx_);
				// output_pst_list_.push_back(tmeta);
				partition_outputs_[partition_id].emplace_back(tmeta);
			}
			if (!pst_builder->AddEntry(key, value))
				ERROR_EXIT("cannot add pst entry in flush");
		}
	}
	meta = pst_builder->Flush();
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
	tree_seq_no_ = version_->GetCurrentL0TreeSeq();
	tree_idx_ = version_->AddLevel0Tree();
	while (tree_idx_ == -1)
	{
		INFO("can't addlevel0tree, waiting...");
		usleep(100000);
		tree_idx_ = version_->AddLevel0Tree();
	}
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
	version_->UpdateLevel0ReadTail();
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
