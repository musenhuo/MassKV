#include "db.h"
#include "config.h"
#include <libpmem.h>
#include <malloc.h>
#include "allocator/segment_allocator.h"
#include "compaction/version.h"
#include "compaction/manifest.h"
#include "compaction/flush.h"
#include "compaction/compaction.h"
#include "datablock_reader.h"
#include "lib/index_masstree.h"
#include "util/stopwatch.hpp"
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"
#ifdef HOT_MEMTABLE
#include "lib/index_hot.h"
#endif
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

// Global Get statistics
std::atomic<uint64_t> MYDB::global_get_success_{0};
std::atomic<uint64_t> MYDB::global_get_failure_{0};

namespace {

std::atomic<uint64_t> g_compaction_trace_invocation_id{0};
std::atomic<uint64_t> g_flush_trace_invocation_id{0};

bool CompactionTraceEnabled() {
	const char* raw = std::getenv("FLOWKV_COMPACTION_TRACE");
	return raw != nullptr && raw[0] != '\0' && raw[0] != '0';
}

bool FlushTraceEnabled() {
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

} // namespace

/***********************MYDB*************************/

void BGWorkTrigger(MYDB *db)
{
	while (!db->stop_bgwork_)
	{
		bool ret = db->MayTriggerFlushOrCompaction();
		usleep(100000);
	}
	printf("BGWorkTrigger stopped!\n");
}
// MYDB::MYDB(){
// 	INFO("init MYDBsss");
// }


MYDB::MYDB()
{
	
	INFO("init MYDB1");
	MYDBConfig cfg;
	INFO("init MYDB2");
	db_path_=cfg.pm_pool_path;
	INFO("init MYDB3");
	INFO("pool size: %lu", cfg.pm_pool_size);
	INFO("segment size: %lu", SEGMENT_SIZE);
	segment_allocator_ = new SegmentAllocator(db_path_, cfg.pm_pool_size, cfg.recover, cfg.use_direct_io);
	// SegmentAllocator a;
	// segment_allocator_ = &a;     
	//segment_allocator_ = new SegmentAllocator();
	current_memtable_idx_ = 0;
	for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
	{
		mem_index_[i] = nullptr;
		memtable_states_[i].state = MemTableStates::EMPTY;
	}
#ifdef MASSTREE_MEMTABLE
	mem_index_[current_memtable_idx_] = new MasstreeIndex();
#endif
#ifdef HOT_MEMTABLE
	mem_index_[current_memtable_idx_] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	{
		client_list_[i] = nullptr;
		for (int j = 0; j < MAX_MEMTABLE_NUM; j++)
		{
			memtable_states_[j].thread_write_states[i] = false;
			temp_memtable_size_[j] = 0;
		}
	}
	memtable_states_[current_memtable_idx_].state = MemTableStates::ACTIVE;
	std::string manifest_path = "/dev/nvme0n1";
	//std::string manifest_path = db_path_+"/manifest";
	// NOTE: long-term we want O_DIRECT for manifest; manifest writes are
	// implemented to use full page-aligned 4096-byte buffers to satisfy
	// O_DIRECT requirements.
	int fd = open(manifest_path.c_str(), O_DIRECT | O_RDWR | O_CREAT, 0666);
	INFO("open manifest file %s", manifest_path.c_str());
	current_version_ = new Version(segment_allocator_);
	manifest_ = new Manifest(fd, cfg.recover);
	if (cfg.recover)
	{
		stopwatch_t sw;
		printf("start recovering!\n");
		sw.start();
		INFO("init MYDB7");
		current_version_ = manifest_->RecoverVersion(current_version_, segment_allocator_);
		INFO("init MYDB8");
		printf("manifest recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
		sw.start();
		bool ret = RecoverLogAndMemtable();
		printf("memtable recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
		if (!ret)
			ERROR_EXIT("recover error");
	}
	//Thread pool init: flush threads + compaction threads + flush/compaction controller threads
	thread_pool_ = new ThreadPoolImpl();
	thread_pool_->SetBackgroundThreads(std::max<size_t>(1, cfg.bg_trigger_threads));
	flush_thread_pool_ = new ThreadPoolImpl();
	flush_thread_pool_->SetBackgroundThreads(std::max<size_t>(1, cfg.flush_threads));
	compaction_thread_pool_ = new ThreadPoolImpl();
	compaction_thread_pool_->SetBackgroundThreads(std::max<size_t>(1, cfg.compaction_threads));

	// Initialize partition info
	size_t range=(1UL<<32)/RANGE_PARTITION_NUM << 32;
	for(size_t i=0;i<RANGE_PARTITION_NUM;i++){
		#if defined(FLOWKV_KEY16)
		partition_info_[i].min_key = Key16{0, range * i};
		partition_info_[i].max_key = Key16{0, range * i - 1 + range};
		#else
		partition_info_[i].min_key=__bswap_64(range*i);
		partition_info_[i].max_key=__bswap_64(range*i-1+range);
		#endif
	}
	#if defined(FLOWKV_KEY16)
	partition_info_[RANGE_PARTITION_NUM-1].max_key = Key16{MAX_UINT64, MAX_UINT64};
	#else
	partition_info_[RANGE_PARTITION_NUM-1].max_key=MAX_UINT64;
	#endif

#ifdef BUFFER_WAL_MEMTABLE
	for (size_t i = 0; i < LSN_MAP_SIZE; i++)
	{
		lsn_list_[i] = 0;
	}
#endif

	bgwork_trigger_ = new std::thread(BGWorkTrigger, this);
}

MYDB::MYDB(const MYDBConfig &cfg)
{
	INFO("init MYDB1");
	INFO("init MYDB2");
	db_path_ = cfg.pm_pool_path;
	use_direct_io_ = cfg.use_direct_io;  // 保存配置，供 MYDBClient 使用
	INFO("init MYDB3");
	INFO("pool size: %lu", cfg.pm_pool_size);
	INFO("segment size: %lu", SEGMENT_SIZE);
	segment_allocator_ = new SegmentAllocator(db_path_, cfg.pm_pool_size, cfg.recover, cfg.use_direct_io);
	current_memtable_idx_ = 0;
	for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
	{
		mem_index_[i] = nullptr;
		memtable_states_[i].state = MemTableStates::EMPTY;
	}
#ifdef MASSTREE_MEMTABLE
	mem_index_[current_memtable_idx_] = new MasstreeIndex();
#endif
#ifdef HOT_MEMTABLE
	mem_index_[current_memtable_idx_] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	{
		client_list_[i] = nullptr;
		for (int j = 0; j < MAX_MEMTABLE_NUM; j++)
		{
			memtable_states_[j].thread_write_states[i] = false;
			temp_memtable_size_[j] = 0;
		}
	}
	memtable_states_[current_memtable_idx_].state = MemTableStates::ACTIVE;

	// For file-backed pools used in benchmarks, place the manifest alongside the pool file.
	std::string manifest_path = cfg.pm_pool_path + ".manifest";
	int fd = open(manifest_path.c_str(), O_DIRECT | O_RDWR | O_CREAT, 0666);
	INFO("open manifest file %s", manifest_path.c_str());
	current_version_ = new Version(segment_allocator_);
	manifest_ = new Manifest(fd, cfg.recover);
	if (cfg.recover)
	{
		stopwatch_t sw;
		printf("start recovering!\n");
		sw.start();
		INFO("init MYDB7");
		current_version_ = manifest_->RecoverVersion(current_version_, segment_allocator_);
		INFO("init MYDB8");
		printf("manifest recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
		sw.start();
		bool ret = RecoverLogAndMemtable();
		printf("memtable recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
		if (!ret)
			ERROR_EXIT("recover error");
	}
	thread_pool_ = new ThreadPoolImpl();
	thread_pool_->SetBackgroundThreads(std::max<size_t>(1, cfg.bg_trigger_threads));
	flush_thread_pool_ = new ThreadPoolImpl();
	flush_thread_pool_->SetBackgroundThreads(std::max<size_t>(1, cfg.flush_threads));
	compaction_thread_pool_ = new ThreadPoolImpl();
	compaction_thread_pool_->SetBackgroundThreads(std::max<size_t>(1, cfg.compaction_threads));

	size_t range = (1UL << 32) / RANGE_PARTITION_NUM << 32;
	for (size_t i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		#if defined(FLOWKV_KEY16)
		partition_info_[i].min_key = Key16{0, range * i};
		partition_info_[i].max_key = Key16{0, range * i - 1 + range};
		#else
		partition_info_[i].min_key = __bswap_64(range * i);
		partition_info_[i].max_key = __bswap_64(range * i - 1 + range);
		#endif
	}
	#if defined(FLOWKV_KEY16)
	partition_info_[RANGE_PARTITION_NUM - 1].max_key = Key16{MAX_UINT64, MAX_UINT64};
	#else
	partition_info_[RANGE_PARTITION_NUM - 1].max_key = MAX_UINT64;
	#endif

#ifdef BUFFER_WAL_MEMTABLE
	for (size_t i = 0; i < LSN_MAP_SIZE; i++)
	{
		lsn_list_[i] = 0;
	}
#endif

	bgwork_trigger_ = new std::thread(BGWorkTrigger, this);
}

// MYDB::MYDB_1(MYDBConfig cfg)
// {
// 	INFO("init MYDB");
// 	db_path_=cfg.pm_pool_path;
// 	segment_allocator_ = new SegmentAllocator(db_path_, cfg.pm_pool_size, cfg.recover);
// 	current_memtable_idx_ = 0;
// #ifdef MASSTREE_MEMTABLE
// 	mem_index_[current_memtable_idx_] = new MasstreeIndex();
// 	mem_index_[1] = nullptr;
// #endif
// #ifdef HOT_MEMTABLE
// 	mem_index_[current_memtable_idx_] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
// #endif
// 	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
// 	{
// 		client_list_[i] = nullptr;
// 		for (int j = 0; j < MAX_MEMTABLE_NUM; j++)
// 		{
// 			memtable_states_[j].thread_write_states[i] = false;
// 			temp_memtable_size_[j] = 0;
// 		}
// 	}
// 	memtable_states_[current_memtable_idx_].state = MemTableStates::ACTIVE;
// 	std::string manifest_path = "/dev/nvme0n1";
// 	//std::string manifest_path = db_path_+"/manifest";
// 	// NOTE: O_DIRECT removed in active code above for quick validation.
// 	int fd = open(manifest_path.c_str(), O_RDWR | O_CREAT, 0666);
// 	INFO("open manifest file %s", manifest_path.c_str());
// 	current_version_ = new Version(segment_allocator_);
// 	manifest_ = new Manifest(fd, cfg.recover);
// 	if (cfg.recover)
// 	{
// 		stopwatch_t sw;
// 		printf("start recovering!\n");
// 		sw.start();
// 		current_version_ = manifest_->RecoverVersion(current_version_, segment_allocator_);
// 		printf("manifest recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
// 		sw.start();
// 		bool ret = RecoverLogAndMemtable();
// 		printf("memtable recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
// 		if (!ret)
// 			ERROR_EXIT("recover error");
// 	}
// 	//Thread pool init: flush threads + compaction threads + flush/compaction controller threads
// 	thread_pool_ = new ThreadPoolImpl();
// 	thread_pool_->SetBackgroundThreads(4);
// 	flush_thread_pool_ = new ThreadPoolImpl();
// 	flush_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM);
// 	compaction_thread_pool_ = new ThreadPoolImpl();
// 	compaction_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM);

// 	// Initialize partition info
// 	size_t range=(1UL<<32)/RANGE_PARTITION_NUM << 32;
// 	for(size_t i=0;i<RANGE_PARTITION_NUM;i++){
// 		partition_info_[i].min_key=__bswap_64(range*i);
// 		partition_info_[i].max_key=__bswap_64(range*i-1+range);
// 	}
// 	partition_info_[RANGE_PARTITION_NUM-1].max_key=MAX_UINT64;

// #ifdef BUFFER_WAL_MEMTABLE
// 	for (size_t i = 0; i < LSN_MAP_SIZE; i++)
// 	{
// 		lsn_list_[i] = 0;
// 	}
// #endif

// 	bgwork_trigger_ = new std::thread(BGWorkTrigger, this);
// }
MYDB::~MYDB()
{
	WaitForFlushAndCompaction();
	// segment_allocator_->PrintLogStats();
	stop_bgwork_ = true;
	if (thread_pool_ != nullptr)
	{
		thread_pool_->JoinAllThreads();
	}
	PrintLogGroup(0);
	PrintLogGroup(1);
	if (bgwork_trigger_)
	{
		bgwork_trigger_->join();
		delete bgwork_trigger_;
		bgwork_trigger_ = nullptr;
	}

	if (flush_thread_pool_ != nullptr)
	{
		flush_thread_pool_->JoinAllThreads();
		delete flush_thread_pool_;
		flush_thread_pool_ = nullptr;
	}

	if (compaction_thread_pool_ != nullptr)
	{
		compaction_thread_pool_->JoinAllThreads();
		delete compaction_thread_pool_;
		compaction_thread_pool_ = nullptr;
	}

	delete current_version_;
	delete segment_allocator_;
	delete manifest_;
	if (thread_pool_ != nullptr)
	{
		delete thread_pool_;
		thread_pool_ = nullptr;
	}
	for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
	{
		if (memtable_states_[i].state != MemTableStates::EMPTY && mem_index_[i] != nullptr)
		{
			delete mem_index_[i];
			mem_index_[i] = nullptr;
		}
	}
}

bool MYDB::RecoverLogAndMemtable()
{
	std::vector<uint64_t> seg_id_list;
	// redo flush log
	manifest_->GetFlushLog(seg_id_list);
	DEBUG("Redo flush log %lu", seg_id_list.size());
	segment_allocator_->RedoFlushLog(seg_id_list);
	// get valid log segments
	DEBUG("Get valid log segments");
	seg_id_list.clear();
	bool ret = segment_allocator_->RecoverLogSegmentAndGetId(seg_id_list);
	LogReader *log_reader = new LogReader(segment_allocator_);
	std::vector<char> logbuffer(SEGMENT_SIZE);
	DEBUG("Log segment list:(size=%lu)", seg_id_list.size());
	printf("\t\t\t\t");
	for (auto &seg_id : seg_id_list)
	{
		printf("%lu,", seg_id);
	}
	printf("\n");
	fflush(stdout);
	size_t total_entries_recovered = 0;
	for (auto &seg_id : seg_id_list)
	{
		// get segment header
		auto entry_num = log_reader->ReadLogFromSegment(seg_id, logbuffer.data());
		total_entries_recovered += entry_num;
		for (int i = 0; i < entry_num; i++)
		{
			ValueHelper lh(0);
#if defined(FLOWKV_KEY16)
			auto *entry = reinterpret_cast<LogEntry64 *>(logbuffer.data() + i * LOG_ENTRY_SIZE);
#else
			auto *entry = reinterpret_cast<LogEntry32 *>(logbuffer.data() + i * LOG_ENTRY_SIZE);
#endif
#ifdef INDEX_LOG_MEMTABLE
			// Calculate log_ptr to match the encoding used in Put
			// Entry i is at offset (logbuffersize + i * LOG_ENTRY_SIZE) within segment
			uint64_t log_ptr = seg_id * SEGMENT_SIZE + logbuffersize + i * LOG_ENTRY_SIZE;
			ValuePtr vp{.detail_ = {.valid = entry->valid,
									.ptr = log_ptr/logbuffersize*LogNumperBlock+log_ptr%logbuffersize/LOG_ENTRY_SIZE,
									.lsn = entry->lsn}};
			lh.new_val = vp.data_;
#endif
#ifdef BUFFER_WAL_MEMTABLE
			lh.new_val = entry->valid ? entry->value_addr : INVALID_PTR;
#endif
#if defined(FLOWKV_KEY16)
			KeyType key{entry->key_hi, entry->key_lo};
			mem_index_[0]->Put(key, lh);
#else
			mem_index_[0]->Put(entry->key, lh);
#endif
		}
	}
	printf("[RecoverLogAndMemtable] Recovered %lu entries from %lu log segments\n", 
		   total_entries_recovered, seg_id_list.size());
	delete log_reader;
	return ret;
}

std::unique_ptr<MYDBClient> MYDB::GetClient(int tid)
{
	std::lock_guard<SpinLock> lock(client_lock_);
	std::unique_ptr<MYDBClient> c;
	if (tid == -1)
	{
		for (int i = 1; i <= MAX_USER_THREAD_NUM; i++)
		{
			if (client_list_[i] == nullptr)
			{
				c = std::make_unique<MYDBClient>(this, i);
				client_list_[i] = c.get();
				DEBUG("create client %d", i);
				return c;
			}
		}
		ERROR_EXIT("Not support too much user threads");
		return c;
	}
	c = std::make_unique<MYDBClient>(this, tid);
	client_list_[tid] = c.get();
	DEBUG("create client %d", tid);
	return c;
}

#ifdef INDEX_LOG_MEMTABLE
LSN MYDB::GetLSN(const KeyType &i_key)
{
#if defined(FLOWKV_KEY16)
	size_t idx = (i_key.hi ^ i_key.lo) & (LSN_MAP_SIZE - 1);
#else
	size_t idx = i_key & (LSN_MAP_SIZE - 1);
#endif
	size_t id = lsn_map_[idx].fetch_add(1);
	return LSN{idx, id, 0};
}
#endif
#ifdef BUFFER_WAL_MEMTABLE
// LSN MYDB::LSN_lock(uint64_t i_key)
// {
//     size_t idx = i_key & (LSN_MAP_SIZE - 1);
//     wal_lock_[idx].lock();
//     size_t id = lsn_list_[idx]++;
//     return LSN{idx, id, 0};
// }
// void MYDB::LSN_unlock(size_t epoch)
// {
//     wal_lock_[epoch].unlock();
// }
LSN MYDB::LSN_lock(const KeyType &i_key)
{
#if defined(FLOWKV_KEY16)
	size_t idx = (i_key.hi ^ i_key.lo) & (LSN_MAP_SIZE - 1);
#else
	size_t idx = i_key & (LSN_MAP_SIZE - 1);
#endif
	size_t id = lsn_map_[idx].fetch_add(1);
	return LSN{idx, id, 0};
}
void MYDB::LSN_unlock(size_t epoch)
{
}
#endif

size_t MYDB::GetMemtableSize(int idx)
{
	size_t num = 0;
	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	{
		auto &c = client_list_[i];
		if (c != nullptr)
		{
			num += c->GetMemtablePutCount(idx);
			// printf("get from client %d,num=%lu\n", i, num);
		}
	}
	num += temp_memtable_size_[idx].load();
	return num;
}
void MYDB::ClearMemtableSize(int idx)
{
	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	{
		auto &c = client_list_[i];
		if (c != nullptr)
		{
			c->ClearMemtablePutCount(idx);
		}
	}
	temp_memtable_size_[idx] = 0;
}

bool MYDB::MayTriggerFlushOrCompaction()
{
	if (++workload_detect_sample_ >= 5)
	{
		if (client_list_[1])
		{
			auto c = client_list_[1];
			auto write_ratio = c->GetWriteRatioAndClear();
			if (write_ratio < 0)
			{
			}
			else if (write_ratio > 0.5)
			{
				DisableReadOnlyMode();
				DisableReadOptimizedMode();
			}
			else if (write_ratio == 0)
			{
				EnableReadOnlyMode();
				EnableReadOptimizedMode();
			}
			else
			{
				DisableReadOnlyMode();
				EnableReadOptimizedMode();
			}
		}
		workload_detect_sample_ = 0;
	}
	// Flush
	size_t memtablesize = GetMemtableSize(current_memtable_idx_);
	size_t flush_threashold = read_only_mode_ ? 1 : MAX_MEMTABLE_ENTRIES;
	if (memtablesize >= flush_threashold)
	{
		// Allow concurrent flushes as long as there are free memtable slots
        if (flushing_count_.load() < MAX_MEMTABLE_NUM - 1)
        {
			// Freeze current memtable and switch to next
			int target_memtable_idx = current_memtable_idx_;
			int next_memtable_idx = (target_memtable_idx + 1) % MAX_MEMTABLE_NUM;
			if (memtable_states_[next_memtable_idx].state == MemTableStates::EMPTY)
			{
				flushing_count_.fetch_add(1);
				memtable_states_[target_memtable_idx].state = MemTableStates::FREEZE;
#ifdef MASSTREE_MEMTABLE
				mem_index_[next_memtable_idx] = new MasstreeIndex();
#endif
#ifdef HOT_MEMTABLE
				mem_index_[next_memtable_idx] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
				memtable_states_[next_memtable_idx].state = MemTableStates::ACTIVE;
				current_memtable_idx_ = next_memtable_idx;
				LOG("switch memtable %d -> %d for parallel flush", target_memtable_idx, next_memtable_idx);

				FlushArgs *fa = new FlushArgs(this, target_memtable_idx);
				thread_pool_->Schedule(&MYDB::TriggerBGFlush, fa, fa, nullptr);
				return true;
			}
		}
	}
	// Compaction
	auto ret = false;
	ret = MayTriggerCompaction();
	return ret;
}

bool MYDB::MayTriggerCompaction()
{
	if (!compaction_enabled_)
	{
		return false;
	}
	size_t compaction_threashold = read_optimized_mode_ ? 1 : l0_compaction_tree_num_;
	// Compaction
	if (current_version_->GetLevel0TreeNum() >= compaction_threashold)
	{
		bool expect = false;
		if (is_l0_compacting_.compare_exchange_weak(expect, true))
		{
			DEBUG("trigger compaction because l0 tree num=%d", current_version_->GetLevel0TreeNum());
			CompactionArgs *ca = new CompactionArgs(this);
			thread_pool_->Schedule(&MYDB::TriggerBGCompaction, ca, ca, nullptr);
			return true;
		}
	}
	return false;
}

void MYDB::PrintLogGroup(int id)
{
	DEBUG("current log group = %d, get %d, memtablesize=%lu, level0treenum=%d,table=%d", current_memtable_idx_, id, GetMemtableSize(id), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
	// std::vector<uint64_t> list;
	// segment_allocator_->GetElementsFromLogGroup(id, &list);
	// for (auto &seg : list)
	// {
	//     printf("%lu,", seg);
	// }
	// printf("\n");
}

void MYDB::PrintLogGroupInMYDB(void *arg)
{
	TestParams tp = *(reinterpret_cast<TestParams *>(arg));
	delete (reinterpret_cast<TestParams *>(arg));
	static_cast<MYDB *>(tp.db_)->PrintLogGroup(tp.log_group_id_);
	sleep(1);
	static_cast<MYDB *>(tp.db_)->PrintLogGroup(tp.log_group_id_);
	tp.is_running_->store(false);
}

void MYDB::TriggerBGFlush(void *arg)
{
	FlushArgs fa = *(reinterpret_cast<FlushArgs *>(arg));
	delete (reinterpret_cast<FlushArgs *>(arg));
	static_cast<MYDB *>(fa.db_)->BGFlush(fa.target_memtable_idx_);
}

void MYDB::TriggerBGCompaction(void *arg)
{
	CompactionArgs ca = *(reinterpret_cast<CompactionArgs *>(arg));
	delete (reinterpret_cast<CompactionArgs *>(arg));
	// printf("trigger bgcompaction\n");
	static_cast<MYDB *>(ca.db_)->BGCompaction();
}

bool MYDB::BGFlush()
{
	int target_memtable_idx = current_memtable_idx_;
	int next_memtable_idx = (target_memtable_idx + 1) % MAX_MEMTABLE_NUM;
	flushing_count_.fetch_add(1);
	memtable_states_[target_memtable_idx].state = MemTableStates::FREEZE;
#ifdef MASSTREE_MEMTABLE
	mem_index_[next_memtable_idx] = new MasstreeIndex();
#endif
#ifdef HOT_MEMTABLE
	mem_index_[next_memtable_idx] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
	memtable_states_[next_memtable_idx].state = MemTableStates::ACTIVE;
	current_memtable_idx_ = next_memtable_idx;
	return BGFlush(target_memtable_idx);
}

bool MYDB::BGFlush(int target_memtable_idx)
{
	const bool trace_enabled = FlushTraceEnabled();
	const uint64_t trace_invocation_id =
		g_flush_trace_invocation_id.fetch_add(1, std::memory_order_relaxed) + 1;
	const auto trace_begin = std::chrono::steady_clock::now();
	const auto emit_flush_trace = [&](const char* stage, double phase_time_ms) {
		if (!trace_enabled) {
			return;
		}
		const auto now = std::chrono::steady_clock::now();
		const double elapsed_ms =
			static_cast<double>(
				std::chrono::duration_cast<std::chrono::microseconds>(now - trace_begin).count()) /
			1000.0;
		const uint64_t rss_bytes = ReadProcessRSSBytesFromProc();
		std::cout << "[FLUSH_TRACE]"
		          << " invocation_id=" << trace_invocation_id
		          << " stage=" << stage
		          << " elapsed_ms=" << elapsed_ms
		          << " phase_time_ms=" << phase_time_ms
		          << " rss_bytes=" << rss_bytes
		          << " target_memtable=" << target_memtable_idx
		          << " current_memtable=" << current_memtable_idx_
		          << " l0_tree_num=" << current_version_->GetLevel0TreeNum()
		          << " l1_active_pst_count=" << current_version_->GetLevelSize(1)
		          << "\n";
	};
	emit_flush_trace("entry", 0.0);
    while (!current_version_->CheckSpaceForL0Tree())
    {
		LOG("flush waiting for L0 space, target_memtable=%d", target_memtable_idx);
		MayTriggerCompaction();
		usleep(1000);
	}
	LOG("start flush target_memtable=%d, level0treenum=%d,table=%d", target_memtable_idx, current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
	stopwatch_t sw;
	sw.start();
	// Memtable switch already done at trigger point.
	// Wait for in-flight writers to finish on the target memtable.
	auto phase_begin = std::chrono::steady_clock::now();
	for (int i = 0; i < MAX_USER_THREAD_NUM; ++i)
	{
		while (memtable_states_[target_memtable_idx].thread_write_states[i].load(std::memory_order_acquire))
		{
			std::this_thread::yield();
		}
	}
	auto phase_end = std::chrono::steady_clock::now();
	emit_flush_trace(
		"wait_writers_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	// Persist any client-local buffered log entries that still belong to the flushed memtable.
	phase_begin = std::chrono::steady_clock::now();
	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	{
		auto *client = client_list_[i];
		if (client != nullptr)
		{
			client->Persist_Log(target_memtable_idx);
		}
	}
	phase_end = std::chrono::steady_clock::now();
	emit_flush_trace(
		"persist_logs_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	// Streaming flush (no temporary vector allocation)
	DEBUG("flush step 3");
	FlushJob fj(mem_index_[target_memtable_idx], target_memtable_idx, segment_allocator_, current_version_, manifest_, partition_info_, flush_thread_pool_);
	phase_begin = std::chrono::steady_clock::now();
	auto ret = fj.run();
	phase_end = std::chrono::steady_clock::now();
	emit_flush_trace(
		"flush_job_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	// Clean up memtable
	DEBUG("step 4");
	phase_begin = std::chrono::steady_clock::now();
	memtable_states_[target_memtable_idx].state = MemTableStates::EMPTY;
	DEBUG("before delete memtable");
#ifdef MASSTREE_MEMTABLE
	delete (MasstreeIndex *)mem_index_[target_memtable_idx];
#endif
	DEBUG("after delete memtable");
#ifdef HOT_MEMTABLE
	delete (HOTIndex *)mem_index_[target_memtable_idx];
#endif
	mem_index_[target_memtable_idx] = nullptr;
	ClearMemtableSize(target_memtable_idx);
	phase_end = std::chrono::steady_clock::now();
	emit_flush_trace(
		"cleanup_memtable_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	phase_begin = std::chrono::steady_clock::now();
	segment_allocator_->ClearLogGroup(target_memtable_idx);
	phase_end = std::chrono::steady_clock::now();
	emit_flush_trace(
		"clear_log_group_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);

	flushing_count_.fetch_sub(1);
	// Return freed Masstree pool memory to OS
	phase_begin = std::chrono::steady_clock::now();
	malloc_trim(0);
	phase_end = std::chrono::steady_clock::now();
	emit_flush_trace(
		"malloc_trim_end",
		static_cast<double>(
			std::chrono::duration_cast<std::chrono::microseconds>(phase_end - phase_begin).count()) /
			1000.0);
	auto ms = sw.elapsed<std::chrono::milliseconds>();
	LOG("finish flush target_memtable=%d, level0treenum=%d,table=%d,time=%f ms", target_memtable_idx, current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0), ms);
	INFO("flush end, time=%f ms", ms);
	emit_flush_trace("exit", ms);
	return true;
}

bool MYDB::BGCompaction()
{
	const bool trace_enabled = CompactionTraceEnabled();
	const uint64_t trace_invocation_id =
		g_compaction_trace_invocation_id.fetch_add(1, std::memory_order_relaxed) + 1;
	const auto trace_begin = std::chrono::steady_clock::now();
	const auto emit_compaction_trace = [&](const char* stage,
	                                       double phase_time_ms,
	                                       int picked_inputs,
	                                       int ok_flag) {
		if (!trace_enabled) {
			return;
		}
		const auto now = std::chrono::steady_clock::now();
		const double elapsed_ms =
			static_cast<double>(
				std::chrono::duration_cast<std::chrono::microseconds>(now - trace_begin).count()) /
			1000.0;
		const uint64_t rss_bytes = ReadProcessRSSBytesFromProc();
		std::cout << "[COMPACTION_TRACE]"
		          << " invocation_id=" << trace_invocation_id
		          << " stage=" << stage
		          << " elapsed_ms=" << elapsed_ms
		          << " phase_time_ms=" << phase_time_ms
		          << " rss_bytes=" << rss_bytes
		          << " l0_tree_num=" << current_version_->GetLevel0TreeNum()
		          << " l1_active_pst_count=" << current_version_->GetLevelSize(1);
		if (picked_inputs >= 0) {
			std::cout << " picked_inputs=" << picked_inputs;
		}
		if (ok_flag >= 0) {
			std::cout << " ok=" << ok_flag;
		}
		std::cout << "\n";
	};

	emit_compaction_trace("entry", 0.0, -1, -1);
	CompactionJob *c = new CompactionJob(segment_allocator_, current_version_, manifest_, partition_info_,compaction_thread_pool_);
	// 1 PickCompaction (lock, freeze pst range)
	stopwatch_t sw;
	sw.start();
	emit_compaction_trace("pick_start", 0.0, -1, -1);
	LOG("PickCompaction start");
	auto num = c->PickCompaction();
	auto ms = sw.elapsed<std::chrono::milliseconds>();
	auto total_ms = ms;
	LOG("PickCompaction end, time: %f ms", ms);
	emit_compaction_trace("pick_end", ms, static_cast<int>(num), 1);
	if (num == 0)
	{
		is_l0_compacting_ = false;
		emit_compaction_trace("exit_no_input", total_ms, 0, 0);
		return false;
	}
	// 2 Prepare
	emit_compaction_trace("prepare_start", 0.0, static_cast<int>(num), -1);
	sw.clear();
	sw.start();
	auto ret = c->CheckPmRoomEnough();
	ms = sw.elapsed<std::chrono::milliseconds>();
	total_ms += ms;
	emit_compaction_trace("prepare_end", ms, static_cast<int>(num), ret ? 1 : 0);
	if (!ret)
	{
		is_l0_compacting_ = false;
		emit_compaction_trace("exit_no_room", total_ms, static_cast<int>(num), 0);
		return false;
	}
	// 3 Merge sorting
	LOG("RunCompaction start");
	sw.clear();
	sw.start();
	emit_compaction_trace("run_start", 0.0, static_cast<int>(num), -1);
	const bool use_serial_compaction = c->ShouldUseSerialCompaction();
	ret = use_serial_compaction ? c->RunCompaction() : c->RunSubCompactionParallel();
	ms = sw.elapsed<std::chrono::milliseconds>();
	LOG("RunCompaction end, time: %f ms", ms);
	total_ms += ms;
	emit_compaction_trace("run_end", ms, static_cast<int>(num), ret ? 1 : 0);
	// exit(-1);
	if (!ret)
	{
		emit_compaction_trace("rollback_start", 0.0, static_cast<int>(num), -1);
		sw.clear();
		sw.start();
		c->RollbackCompaction();
		ms = sw.elapsed<std::chrono::milliseconds>();
		total_ms += ms;
		emit_compaction_trace("rollback_end", ms, static_cast<int>(num), 1);
		is_l0_compacting_ = false;
		emit_compaction_trace("exit_run_fail", total_ms, static_cast<int>(num), 0);
		return false;
	}
	// 4 delete obsolute psts and change level0 indexes
	LOG("CleanCompaction start");
	sw.clear();
	sw.start();
	emit_compaction_trace("clean_start", 0.0, static_cast<int>(num), -1);
	if (use_serial_compaction) {
		c->CleanCompaction();
	} else {
		c->CleanCompactionWhenUsingSubCompaction();
	}
	ms = sw.elapsed<std::chrono::milliseconds>();
	LOG("CleanCompaction end, time: %f ms", ms);
	total_ms += ms;
	emit_compaction_trace("clean_end", ms, static_cast<int>(num), 1);
	is_l0_compacting_ = false;
	LOG("before compaction end");
	print_dram_consuption();
	emit_compaction_trace("before_delete_job", 0.0, static_cast<int>(num), -1);
	delete c;
	emit_compaction_trace("after_delete_job", 0.0, static_cast<int>(num), 1);
	print_dram_consuption();
	INFO("compaction end, time=%f ms", total_ms);
	emit_compaction_trace("exit_success", total_ms, static_cast<int>(num), 1);
	MayTriggerFlushOrCompaction();

	return true;
}

void MYDB::WaitForFlushAndCompaction()
{
	EnableReadOptimizedMode();
	EnableReadOnlyMode();
	while (flushing_count_.load() > 0 || is_l0_compacting_.load())
	{
		sleep(1);
	}
	DisableReadOnlyMode();
}

void MYDB::StopBackgroundTriggerForTesting()
{
	stop_bgwork_ = true;
	if (bgwork_trigger_)
	{
		bgwork_trigger_->join();
		delete bgwork_trigger_;
		bgwork_trigger_ = nullptr;
	}
}

void MYDB::PrintSSDUsage()
{

	segment_allocator_->PrintSSDUsage();
}

void MYDB::PrintCacheStats()
{
	uint64_t hits = DataBlockReader::GetGlobalCacheHits();
	uint64_t misses = DataBlockReader::GetGlobalCacheMisses();
	uint64_t queries = DataBlockReader::GetGlobalPointQueries();
	uint64_t total_io = hits + misses;
	double hit_rate = total_io > 0 ? (100.0 * hits / total_io) : 0.0;
	
	uint64_t get_success = global_get_success_.load(std::memory_order_relaxed);
	uint64_t get_failure = global_get_failure_.load(std::memory_order_relaxed);
	uint64_t get_total = get_success + get_failure;
	double success_rate = get_total > 0 ? (100.0 * get_success / get_total) : 0.0;
	
	printf("=== FlowKV Performance Stats ===\n");
	printf("Get Operations:\n");
	printf("  Total:   %10lu\n", get_total);
	printf("  Success: %10lu (%.2f%%)\n", get_success, success_rate);
	printf("  Failure: %10lu (%.2f%%)\n", get_failure, 100.0 - success_rate);
	printf("\nDataBlock I/O:\n");
	printf("  PointQuery calls: %10lu (%.2f%% of Gets)\n", queries, 
	       get_total > 0 ? (100.0 * queries / get_total) : 0.0);
	printf("  Cache Hits:       %10lu\n", hits);
	printf("  Cache Misses:     %10lu\n", misses);
	printf("  Total I/O:        %10lu\n", total_io);
	printf("  Cache Hit Rate:   %.4f%%\n", hit_rate);
	printf("================================\n");
}

void MYDB::PrintL1IndexStats()
{
	if (current_version_) {
		current_version_->PrintL1IndexStats();
	}
}

MYDB::EngineResidentMemoryStats MYDB::DebugEstimateEngineResidentMemory() const
{
    EngineResidentMemoryStats stats;

    if (current_version_ != nullptr) {
        const auto l1_memory = current_version_->DebugEstimateLevel1MemoryUsage();
        const auto l1_resident = current_version_->DebugEstimateLevel1ResidentMemory();
        const auto l0_list_memory = current_version_->DebugEstimateLevel0ListMemory();
        const auto l0_tree_index_memory = current_version_->DebugEstimateLevel0TreeIndexMemory();

        stats.l1_route_index_estimated_bytes = l1_memory.route_index_estimated_bytes;
        stats.l1_route_index_measured_bytes = l1_memory.route_index_measured_bytes;
        stats.l1_route_index_pool_bytes = l1_memory.route_index_pool_bytes;
        stats.l1_route_partition_bytes = l1_memory.route_partition_bytes;
        stats.l1_subtree_published_bytes = l1_memory.subtree_bytes;
        stats.l1_subtree_cache_bytes = l1_memory.subtree_cache_bytes;
        stats.l1_pending_changed_route_keys_bytes = l1_resident.pending_changed_route_keys_bytes;
        stats.l1_pending_delta_estimated_bytes = l1_resident.pending_delta_estimated_bytes;
        stats.l0_table_lists_total_size = l0_list_memory.total_size;
        stats.l0_table_lists_total_capacity = l0_list_memory.total_capacity;
        stats.l0_table_lists_total_capacity_bytes = l0_list_memory.total_capacity_bytes;
        stats.l0_tree_index_count = l0_tree_index_memory.tree_count;
        stats.l0_tree_index_tree_bytes = l0_tree_index_memory.tree_bytes;
        stats.l0_tree_index_pool_bytes = l0_tree_index_memory.tree_pool_bytes;
        stats.l0_tree_index_total_bytes = l0_tree_index_memory.total_bytes;

        const uint64_t route_index_bytes =
            l1_memory.route_index_measured_bytes != 0
                ? static_cast<uint64_t>(l1_memory.route_index_measured_bytes)
                : static_cast<uint64_t>(l1_memory.route_index_estimated_bytes);
        stats.l1_total_estimated_bytes =
            route_index_bytes +
            static_cast<uint64_t>(l1_memory.route_partition_bytes) +
            static_cast<uint64_t>(l1_memory.subtree_bytes) +
            static_cast<uint64_t>(l1_memory.subtree_cache_bytes) +
            static_cast<uint64_t>(l1_resident.pending_changed_route_keys_bytes) +
            static_cast<uint64_t>(l1_resident.pending_delta_estimated_bytes);
    }

    if (segment_allocator_ != nullptr) {
        const auto seg_stats = segment_allocator_->DebugEstimateResidentMemory();
        stats.seg_bitmap_bytes = seg_stats.segment_bitmap_bytes;
        stats.seg_bitmap_history_bytes = seg_stats.segment_bitmap_history_bytes;
        stats.seg_bitmap_freed_bits_capacity_bytes =
            seg_stats.segment_bitmap_freed_bits_capacity_bytes;
        stats.seg_log_bitmap_bytes = seg_stats.log_segment_bitmap_bytes;
        stats.seg_log_bitmap_history_bytes = seg_stats.log_segment_bitmap_history_bytes;
        stats.seg_log_bitmap_freed_bits_capacity_bytes =
            seg_stats.log_segment_bitmap_freed_bits_capacity_bytes;
        stats.seg_cache_count = seg_stats.data_segment_cache_count;
        stats.seg_cache_queue_estimated_bytes = seg_stats.data_segment_cache_queue_estimated_bytes;
        stats.seg_cache_segment_object_bytes =
            seg_stats.data_segment_cache_segment_object_bytes;
        stats.seg_cache_segment_buffer_bytes =
            seg_stats.data_segment_cache_segment_buffer_bytes;
        stats.seg_cache_segment_bitmap_bytes =
            seg_stats.data_segment_cache_segment_bitmap_bytes;
        stats.seg_cache_segment_bitmap_freed_bits_capacity_bytes =
            seg_stats.data_segment_cache_segment_bitmap_freed_bits_capacity_bytes;
        stats.seg_log_group_slot_bytes = seg_stats.log_segment_group_slot_bytes;
        stats.seg_total_estimated_bytes = seg_stats.total_estimated_bytes;
    }

    if (manifest_ != nullptr) {
        const auto manifest_stats = manifest_->DebugEstimateResidentMemory();
        stats.manifest_super_buffer_bytes =
            manifest_stats.aligned_super_page_buffer_bytes;
        stats.manifest_super_meta_bytes = manifest_stats.super_meta_bytes;
        stats.manifest_batch_super_meta_bytes = manifest_stats.batch_super_meta_bytes;
        stats.manifest_l0_freelist_estimated_bytes =
            manifest_stats.l0_freelist_estimated_bytes;
        stats.manifest_batch_pages_count = manifest_stats.batch_pages_count;
        stats.manifest_batch_pages_data_bytes = manifest_stats.batch_pages_data_bytes;
        stats.manifest_batch_pages_map_node_estimated_bytes =
            manifest_stats.batch_pages_map_node_estimated_bytes;
        stats.manifest_batch_pages_map_bucket_bytes =
            manifest_stats.batch_pages_map_bucket_bytes;
        stats.manifest_total_estimated_bytes = manifest_stats.total_estimated_bytes;
    }

#ifdef MASSTREE_MEMTABLE
    for (int i = 0; i < MAX_MEMTABLE_NUM; ++i) {
        auto* masstree_index = dynamic_cast<MasstreeIndex*>(mem_index_[i]);
        if (masstree_index == nullptr) {
            continue;
        }
        ++stats.memtable_masstree_active_count;
        stats.memtable_masstree_tree_bytes +=
            static_cast<uint64_t>(masstree_index->DebugEstimateTreeBytes());
        stats.memtable_masstree_pool_bytes +=
            static_cast<uint64_t>(masstree_index->DebugEstimateThreadPoolBytes());
        stats.memtable_masstree_total_bytes +=
            static_cast<uint64_t>(masstree_index->DebugEstimateTotalBytes());
    }
#endif

    stats.db_core_fixed_estimated_bytes =
        sizeof(MYDB) +
        sizeof(mem_index_) +
        sizeof(memtable_states_) +
        sizeof(temp_memtable_size_) +
        sizeof(partition_info_) +
        sizeof(client_list_);

        stats.total_known_resident_estimated_bytes =
        stats.l1_total_estimated_bytes +
        stats.l0_tree_index_total_bytes +
        stats.seg_total_estimated_bytes +
        stats.manifest_total_estimated_bytes +
        stats.db_core_fixed_estimated_bytes;
    return stats;
}

size_t MYDB::DebugReleaseLevel0TableListCapacityForProbe()
{
    if (current_version_ == nullptr) {
        return 0;
    }
    return current_version_->DebugReleaseLevel0TableListCapacityForProbe();
}

size_t MYDB::DebugReleaseSegmentCacheForProbe()
{
    if (segment_allocator_ == nullptr) {
        return 0;
    }
    return segment_allocator_->DebugReleaseDataSegmentCacheForProbe();
}

size_t MYDB::DebugReleaseAllLevel1ForProbe()
{
    if (current_version_ == nullptr) {
        return 0;
    }
    return current_version_->DebugReleaseAllLevel1ForProbe();
}

size_t MYDB::DebugReleaseManifestStateForProbe()
{
    if (manifest_ == nullptr) {
        return 0;
    }
    return manifest_->DebugReleaseVolatileStateForProbe();
}

size_t MYDB::DebugReleaseActiveMemtableForProbe()
{
    const int active_idx = current_memtable_idx_;
    if (active_idx < 0 || active_idx >= MAX_MEMTABLE_NUM) {
        return 0;
    }
    if (mem_index_[active_idx] == nullptr) {
        return 0;
    }

    size_t released_estimated_bytes = 0;
#ifdef MASSTREE_MEMTABLE
    if (auto* masstree_index = dynamic_cast<MasstreeIndex*>(mem_index_[active_idx]);
        masstree_index != nullptr) {
        released_estimated_bytes = masstree_index->DebugEstimateTotalBytes();
    }
    delete static_cast<MasstreeIndex*>(mem_index_[active_idx]);
    mem_index_[active_idx] = new MasstreeIndex();
#else
#ifdef HOT_MEMTABLE
    delete static_cast<HOTIndex*>(mem_index_[active_idx]);
    mem_index_[active_idx] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
#endif
    ClearMemtableSize(active_idx);
    return released_estimated_bytes;
}

size_t MYDB::DebugReleaseThreadPoolsForProbe()
{
    size_t released_estimated_bytes = 0;

    if (flush_thread_pool_ != nullptr) {
        flush_thread_pool_->JoinAllThreads();
        released_estimated_bytes += sizeof(*flush_thread_pool_);
        delete flush_thread_pool_;
        flush_thread_pool_ = nullptr;
    }
    if (compaction_thread_pool_ != nullptr) {
        compaction_thread_pool_->JoinAllThreads();
        released_estimated_bytes += sizeof(*compaction_thread_pool_);
        delete compaction_thread_pool_;
        compaction_thread_pool_ = nullptr;
    }
    if (thread_pool_ != nullptr) {
        thread_pool_->JoinAllThreads();
        released_estimated_bytes += sizeof(*thread_pool_);
        delete thread_pool_;
        thread_pool_ = nullptr;
    }
    return released_estimated_bytes;
}
