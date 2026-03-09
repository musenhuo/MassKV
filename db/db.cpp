#include "db.h"
#include "config.h"
#include <libpmem.h>
#include "allocator/segment_allocator.h"
#include "compaction/version.h"
#include "compaction/manifest.h"
#include "compaction/flush.h"
#include "compaction/compaction.h"
#include "datablock_reader.h"
#if defined(USE_HMASSTREE)
#include "lib/index_hmasstree.h"
#else
#include "lib/index_masstree.h"
#endif
#include "util/stopwatch.hpp"
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"
#ifdef HOT_MEMTABLE
#include "lib/index_hot.h"
#endif

// Global Get statistics
std::atomic<uint64_t> MYDB::global_get_success_{0};
std::atomic<uint64_t> MYDB::global_get_failure_{0};

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
	#if defined(USE_HMASSTREE)
	mem_index_[current_memtable_idx_] = new HMasstreeIndex();
	#else
	mem_index_[current_memtable_idx_] = new MasstreeIndex();
	#endif
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
	thread_pool_->SetBackgroundThreads(4);
	flush_thread_pool_ = new ThreadPoolImpl();
	flush_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM);
	compaction_thread_pool_ = new ThreadPoolImpl();
	compaction_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM);

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
	#if defined(USE_HMASSTREE)
	mem_index_[current_memtable_idx_] = new HMasstreeIndex();
	#else
	mem_index_[current_memtable_idx_] = new MasstreeIndex();
	#endif
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
	thread_pool_->SetBackgroundThreads(4);
	flush_thread_pool_ = new ThreadPoolImpl();
	flush_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM);
	compaction_thread_pool_ = new ThreadPoolImpl();
	compaction_thread_pool_->SetBackgroundThreads(RANGE_PARTITION_NUM);

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
	thread_pool_->JoinAllThreads();
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
	delete thread_pool_;
	thread_pool_ = nullptr;
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
		// printf("memtablesize[%d]=%lu\n", current_memtable_idx_, memtablesize);
		bool expect = false;
		if (is_flushing_.compare_exchange_weak(expect, true))
		{
			FlushArgs *fa = new FlushArgs(this);
			thread_pool_->Schedule(&MYDB::TriggerBGFlush, fa, fa, nullptr);
			return true;
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
	// printf("trigger flush\n");
	static_cast<MYDB *>(fa.db_)->BGFlush();
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
	if (!current_version_->CheckSpaceForL0Tree())
	{
		LOG("flush stall due to full L0");
		is_flushing_ = false;
		return false;
	}
	LOG("start flush active_memtable = %d, memtablesize=(%lu,%lu), level0treenum=%d,table=%d", current_memtable_idx_.load(), memtable_size_[0].load(), memtable_size_[1].load(), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
	stopwatch_t sw;
	sw.start();
	// 1. freeze memtable and modify the current_memtable_idx of MYDB
	int target_memtable_idx = current_memtable_idx_;
	int next_memtable_idx = (target_memtable_idx + 1) % MAX_MEMTABLE_NUM;
/* if only enabling single-thread flush, this memtable state control is not neccessary
	LOG("start flush,target idx=%d,kv=%lu", target_memtable_idx, memtable_size_[target_memtable_idx].load());
	if (memtable_states_[target_memtable_idx].state != MemTableStates::ACTIVE)
	{
		DEBUG("memtable_states_[%d].state =%d, flush failed", target_memtable_idx, memtable_states_[target_memtable_idx].state);
		return false;
	}
	LOG("target memtable is active: ok!");
	memtable_states_[target_memtable_idx].state = MemTableStates::FREEZE;
	if (memtable_states_[next_memtable_idx].state != MemTableStates::EMPTY)
		return false;
*/
#ifdef MASSTREE_MEMTABLE
	#if defined(USE_HMASSTREE)
	mem_index_[next_memtable_idx] = new HMasstreeIndex();
	#else
	mem_index_[next_memtable_idx] = new MasstreeIndex();
	#endif
#endif
#ifdef HOT_MEMTABLE
	mem_index_[next_memtable_idx] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
	memtable_states_[next_memtable_idx].state = MemTableStates::ACTIVE;
	current_memtable_idx_ = next_memtable_idx; // change active memtable
	LOG("change memtable. active_memtable = %d, memtablesize=(%lu,%lu), l0treenum=%d,table=%d", current_memtable_idx_.load(), memtable_size_[0].load(), memtable_size_[1].load(), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
	// from this time, user thread can write into new memtable and other flush can be triggered on other memtable
	// 2. wait all threads not busy
	DEBUG("step 2");
	// for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	// {
	//     while (memtable_states_[target_memtable_idx].thread_write_states[i] == true)
	//     {
	//         std::this_thread::yield();
	//     }
	// }
	usleep(100); // just wait for all client put over, instead of checking client state with a shared value
	// Persist any client-local buffered log entries that still belong to the flushed memtable.
	for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
	{
		auto *client = client_list_[i];
		if (client != nullptr && client->current_memtable_idx_ == target_memtable_idx)
		{
			client->Persist_Log();
		}
	}
	// 3. core steps
	DEBUG("flush step 3");
	FlushJob fj(mem_index_[target_memtable_idx], target_memtable_idx, segment_allocator_, current_version_, manifest_, partition_info_,flush_thread_pool_);
	auto ret = fj.subrunParallel();
	//auto ret = fj.run();
	// 4. change memtable state to EMPTY
	DEBUG("step 4");
	memtable_states_[target_memtable_idx].state = MemTableStates::EMPTY;
	int expect = 0;
	DEBUG("before delete memtable");
#ifdef MASSTREE_MEMTABLE
	#if defined(USE_HMASSTREE)
	delete (HMasstreeIndex *)mem_index_[target_memtable_idx];
	#else
	delete (MasstreeIndex *)mem_index_[target_memtable_idx];
	#endif
#endif
	DEBUG("after delete memtable");
#ifdef HOT_MEMTABLE
	delete (HOTIndex *)mem_index_[target_memtable_idx];
#endif
	mem_index_[target_memtable_idx] = nullptr;
	ClearMemtableSize(target_memtable_idx);
	segment_allocator_->ClearLogGroup(target_memtable_idx);

	// MayTriggerFlushOrCompaction(); // to trigger cascade compaction
	is_flushing_ = false;
	auto ms = sw.elapsed<std::chrono::milliseconds>();
	LOG("finish flush active_memtable = %d, memtablesize=(%lu,%lu), level0treenum=%d,table=%d,time=%f ms", current_memtable_idx_, GetMemtableSize(0), GetMemtableSize(1), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0), ms);
	INFO("flush end, time=%f ms", ms);
	return true;
}

bool MYDB::BGCompaction()
{
	CompactionJob *c = new CompactionJob(segment_allocator_, current_version_, manifest_, partition_info_,compaction_thread_pool_);
	// 1 PickCompaction (lock, freeze pst range)
	stopwatch_t sw;
	sw.start();
	LOG("PickCompaction start");
	auto num = c->PickCompaction();
	auto ms = sw.elapsed<std::chrono::milliseconds>();
	auto total_ms = ms;
	LOG("PickCompaction end, time: %f ms", ms);
	if (num == 0)
	{
		is_l0_compacting_ = false;
		return false;
	}
	// 2 Prepare
	auto ret = c->CheckPmRoomEnough();
	if (!ret)
	{
		is_l0_compacting_ = false;
		return false;
	}
	// 3 Merge sorting
	LOG("RunCompaction start");
	sw.clear();
	sw.start();
	ret = c->RunSubCompactionParallel();
	ms = sw.elapsed<std::chrono::milliseconds>();
	LOG("RunCompaction end, time: %f ms", ms);
	total_ms += ms;
	// exit(-1);
	if (!ret)
	{
		c->RollbackCompaction();
		is_l0_compacting_ = false;
		return false;
	}
	// 4 delete obsolute psts and change level0 indexes
	LOG("CleanCompaction start");
	sw.clear();
	sw.start();
	// c->CleanCompaction();
	c->CleanCompactionWhenUsingSubCompaction();
	ms = sw.elapsed<std::chrono::milliseconds>();
	LOG("CleanCompaction end, time: %f ms", ms);
	total_ms += ms;
	is_l0_compacting_ = false;
	LOG("before compaction end");
	print_dram_consuption();
	delete c;
	print_dram_consuption();
	INFO("compaction end, time=%f ms", total_ms);
	MayTriggerFlushOrCompaction();

	return true;
}

void MYDB::WaitForFlushAndCompaction()
{
	EnableReadOptimizedMode();
	EnableReadOnlyMode();
	while (is_flushing_.load() || is_l0_compacting_.load())
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
