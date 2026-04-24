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
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"

namespace {

std::atomic<uint64_t> g_flush_job_trace_invocation_id{0};

bool FlushJobTraceEnabled() {
    const char* raw = std::getenv("FLOWKV_FLUSH_TRACE");
    return raw != nullptr && raw[0] != '\0' && raw[0] != '0';
}

uint64_t ReadProcessRSSBytesFromProc() {
    std::ifstream status("/proc/self/status");
    if (!status.is_open()) {
        return 0;
    }
    std::string key;
    while (status >> key) {
        if (key == "VmRSS:") {
            uint64_t kb = 0;
            std::string unit;
            status >> kb >> unit;
            return kb * 1024ULL;
        }
        std::string rest;
        std::getline(status, rest);
    }
    return 0;
}

}  // namespace
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
	const bool trace_enabled = FlushJobTraceEnabled();
	const uint64_t trace_invocation_id =
		g_flush_job_trace_invocation_id.fetch_add(1, std::memory_order_relaxed) + 1;
	const auto trace_begin = std::chrono::steady_clock::now();
	const auto emit_flush_job_trace = [&](const char* stage, double phase_time_ms) {
		if (!trace_enabled) {
			return;
		}
		const auto now = std::chrono::steady_clock::now();
		const double elapsed_ms =
			static_cast<double>(
				std::chrono::duration_cast<std::chrono::microseconds>(now - trace_begin).count()) /
			1000.0;
		const uint64_t rss_bytes = ReadProcessRSSBytesFromProc();
		std::cout << "[FLUSH_JOB_TRACE]"
		          << " invocation_id=" << trace_invocation_id
		          << " stage=" << stage
		          << " elapsed_ms=" << elapsed_ms
		          << " phase_time_ms=" << phase_time_ms
		          << " rss_bytes=" << rss_bytes
		          << " seg_group_id=" << seg_group_id_
		          << " tree_idx=" << tree_idx_
		          << " output_pst_count=" << output_pst_list_.size()
		          << "\n";
	};
	emit_flush_job_trace("entry", 0.0);
	LOG("iterate index (ForEachEntry)");
	// build psts and version
	LOG("add level0 tree");
	auto phase_begin = std::chrono::steady_clock::now();
	uint32_t tree_seq = 0;
	tree_idx_ = version_->AddLevel0Tree(&tree_seq);
	while (tree_idx_ == -1)
	{
		INFO("can't addlevel0tree, waiting...");
		usleep(100000);
		tree_idx_ = version_->AddLevel0Tree(&tree_seq);
	}
	tree_seq_no_ = tree_seq;
	auto phase_end = std::chrono::steady_clock::now();
	emit_flush_job_trace(
		"add_l0_tree_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	LOG("tree_idx=%d, streaming flush", tree_idx_);

	int cur_partition = 0;
	KeyType partition_max_key = partition_info_[cur_partition].max_key;
	size_t flushed_count = 0;
	uint64_t trace_log_read_us = 0;
	uint64_t trace_add_entry_us = 0;
	uint64_t trace_partition_flush_us = 0;
	uint64_t trace_capacity_flush_us = 0;
	uint64_t trace_log_read_count = 0;
	uint64_t trace_partition_flush_count = 0;
	uint64_t trace_capacity_flush_count = 0;

	phase_begin = std::chrono::steady_clock::now();
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
			if (trace_enabled) {
				const auto log_read_begin = std::chrono::steady_clock::now();
				if (log_reader_.ReadLogForValue(key, vptr, persisted_value.data()) == 0)
					ERROR_EXIT("flush log value read should succeed");
				trace_log_read_us += static_cast<uint64_t>(
					std::chrono::duration_cast<std::chrono::microseconds>(
						std::chrono::steady_clock::now() - log_read_begin).count());
				trace_log_read_count++;
			} else {
				if (log_reader_.ReadLogForValue(key, vptr, persisted_value.data()) == 0)
					ERROR_EXIT("flush log value read should succeed");
			}
		}
		Slice value(reinterpret_cast<const char *>(&persisted_value), sizeof(persisted_value));
#else
		uint64_t v = val;
		Slice value(&v);
#endif
		// Partition boundary check
		while (unlikely(KeyTypeGreater(k, partition_max_key)))
		{
			if (trace_enabled) {
				const auto flush_begin = std::chrono::steady_clock::now();
				FlushPST();
				trace_partition_flush_us += static_cast<uint64_t>(
					std::chrono::duration_cast<std::chrono::microseconds>(
						std::chrono::steady_clock::now() - flush_begin).count());
				trace_partition_flush_count++;
			} else {
				FlushPST();
			}
			cur_partition++;
			if (unlikely(cur_partition >= RANGE_PARTITION_NUM))
				ERROR_EXIT("key > max_key in the largest partition");
			partition_max_key = partition_info_[cur_partition].max_key;
		}

		bool success = false;
		if (trace_enabled) {
			const auto add_begin = std::chrono::steady_clock::now();
			success = pst_builder_.AddEntry(key, value);
			trace_add_entry_us += static_cast<uint64_t>(
				std::chrono::duration_cast<std::chrono::microseconds>(
					std::chrono::steady_clock::now() - add_begin).count());
		} else {
			success = pst_builder_.AddEntry(key, value);
		}
		if (!success)
		{
			if (trace_enabled) {
				const auto flush_begin = std::chrono::steady_clock::now();
				FlushPST();
				trace_capacity_flush_us += static_cast<uint64_t>(
					std::chrono::duration_cast<std::chrono::microseconds>(
						std::chrono::steady_clock::now() - flush_begin).count());
				trace_capacity_flush_count++;
			} else {
				FlushPST();
			}
			if (trace_enabled) {
				const auto add_begin = std::chrono::steady_clock::now();
				if (!pst_builder_.AddEntry(key, value))
					ERROR_EXIT("cannot add pst entry in flush");
				trace_add_entry_us += static_cast<uint64_t>(
					std::chrono::duration_cast<std::chrono::microseconds>(
						std::chrono::steady_clock::now() - add_begin).count());
			} else {
				if (!pst_builder_.AddEntry(key, value))
					ERROR_EXIT("cannot add pst entry in flush");
			}
		}
		flushed_count++;
		return true;
	});
	phase_end = std::chrono::steady_clock::now();
	emit_flush_job_trace(
		"foreach_entry_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	if (trace_enabled) {
		std::cout << "[FLUSH_JOB_TRACE_DETAIL]"
		          << " invocation_id=" << trace_invocation_id
		          << " flushed_count=" << flushed_count
		          << " log_read_count=" << trace_log_read_count
		          << " partition_flush_count=" << trace_partition_flush_count
		          << " capacity_flush_count=" << trace_capacity_flush_count
		          << " log_read_ms=" << (static_cast<double>(trace_log_read_us) / 1000.0)
		          << " add_entry_ms=" << (static_cast<double>(trace_add_entry_us) / 1000.0)
		          << " partition_flush_ms=" << (static_cast<double>(trace_partition_flush_us) / 1000.0)
		          << " capacity_flush_ms=" << (static_cast<double>(trace_capacity_flush_us) / 1000.0)
		          << "\n";
	}

	LOG("ForEachEntry flushed %lu entries", flushed_count);
	phase_begin = std::chrono::steady_clock::now();
	FlushPST();
	phase_end = std::chrono::steady_clock::now();
	emit_flush_job_trace(
		"final_flush_pst_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	// now the new tree can be read
	phase_begin = std::chrono::steady_clock::now();
	version_->PublishLevel0Tree(tree_idx_);
	phase_end = std::chrono::steady_clock::now();
	emit_flush_job_trace(
		"publish_l0_tree_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	// delete obsolute index and log segments
	std::vector<uint64_t> segment_list;
	phase_begin = std::chrono::steady_clock::now();
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
	phase_end = std::chrono::steady_clock::now();
	emit_flush_job_trace(
		"reclaim_log_segments_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	emit_flush_job_trace("exit", static_cast<double>(
		std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::steady_clock::now() - trace_begin).count()) / 1000.0);
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
