/**
 * @file db.h
 * @brief 数据库引擎与客户端的公共接口。
 */

#pragma once

#include <cstdint>
#include <atomic>
#include <string>
#include <array>
#include <memory>
#include <thread>
#include <mutex>
#include "slice.h"
#include "db_common.h"
#include "log_format.h"

class MYDBClient;
class SegmentAllocator;
class LogWriter;
class LogReader;
class PSTReader;
class Version;
class Manifest;
class ThreadPoolImpl;

/**
 * @brief 引擎使用的 memtable 状态容器。
 */
struct MemTableStates
{
    enum
    {
        ACTIVE, ///< 活跃可写。
        FREEZE, ///< 冻结（通常不再接收写入，等待 flush）。
        EMPTY   ///< 空/未初始化。
    } state = EMPTY;

    /// 该 memtable 的按用户线程维度写入状态标记。
    std::atomic_bool thread_write_states[MAX_USER_THREAD_NUM];
};

/**
 * @brief 数据库引擎主对象。
 *
 * 负责持有全局资源（分配器、version/manifest、后台工作线程等），并通过 @ref GetClient
 * 为每个线程提供客户端句柄。
 */
class MYDB
{
private:
    friend class MYDBClient;
    // global
    std::string db_path_;

#ifdef INDEX_LOG_MEMTABLE
    static constexpr size_t LSN_MAP_SIZE = 256;
    std::array<std::atomic_uint_fast32_t, LSN_MAP_SIZE> lsn_map_{};
#endif
#ifdef BUFFER_WAL_MEMTABLE
    static constexpr size_t LSN_MAP_SIZE = 256;
    std::array<std::atomic_uint_fast32_t, LSN_MAP_SIZE> lsn_map_{};
    uint32_t lsn_list_[LSN_MAP_SIZE];
    SpinLock wal_lock_[LSN_MAP_SIZE];
#endif
    SegmentAllocator *segment_allocator_;
    SpinLock client_lock_;

    // FastWriteStore
    Index *mem_index_[MAX_MEMTABLE_NUM];
    // std::atomic<uint64_t> memtable_size_[MAX_MEMTABLE_NUM];
    MemTableStates memtable_states_[MAX_MEMTABLE_NUM];
    std::atomic_uint64_t temp_memtable_size_[MAX_MEMTABLE_NUM];
    int current_memtable_idx_;
	
    // control variables
    ThreadPoolImpl *thread_pool_ = nullptr;
    ThreadPoolImpl *flush_thread_pool_ = nullptr;
	ThreadPoolImpl *compaction_thread_pool_ = nullptr;
    std::thread *bgwork_trigger_ = nullptr;
    bool read_optimized_mode_ = false;
    bool read_only_mode_ = false;
    bool compaction_enabled_ = true;
    bool use_direct_io_ = false;      ///< 是否对 PST 读取使用 O_DIRECT
    size_t l0_compaction_tree_num_ = 4;
    size_t l0_write_stall_tree_num_ = MAX_L0_TREE_NUM - 1;

    std::atomic<int> flushing_count_{0};
    std::atomic<bool> is_l0_compacting_ = false;

    int workload_detect_sample_ = 0;

public: // TODO: change to private
    // BufferStore (level 0) + LeveledStore (Level 1 and level 2)
    Version *current_version_;
    Manifest *manifest_;
    bool stop_bgwork_ = false;
	PartitionInfo partition_info_[RANGE_PARTITION_NUM];
    MYDBClient *client_list_[MAX_USER_THREAD_NUM];


public:
    // MYDB(MYDBConfig cfg = MYDBConfig());

    /** @brief 构造数据库实例（打开/初始化行为由实现决定）。 */
    MYDB();

    /**
     * @brief 使用指定配置构造数据库实例。
     * @param cfg 运行时配置（pm pool 路径/大小/是否恢复等）。
     */
    explicit MYDB(const MYDBConfig &cfg);

    /** @brief 析构数据库实例并释放资源（由实现决定）。 */
    ~MYDB();

    /**
     * @brief 获取线程级客户端句柄。
     * @param tid 逻辑线程 id；-1 可能表示自动分配（由实现决定）。
     * @return 绑定到指定线程 id 的独占 client 句柄。
     */
    std::unique_ptr<MYDBClient> GetClient(int tid = -1);
    // static bool initMYDB();
    // static bool openMYDB();
private:
    /**
     * @brief 恢复日志并重建 memtable。
     * @return 成功返回 true。
     */
    bool RecoverLogAndMemtable();
#ifdef INDEX_LOG_MEMTABLE
    /**
     * @brief 获取与 key 关联的 LSN（index-log memtable 模式）。
     * @param i_key 用于 LSN 映射的内部 key。
     */
    LSN GetLSN(const KeyType &i_key);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    /**
     * @brief 为 key 获取 LSN 锁（buffered WAL memtable 模式）。
     * @param i_key 用于 LSN 映射的内部 key。
     */
    LSN LSN_lock(const KeyType &i_key);

    /**
     * @brief 释放之前获取的 LSN 锁。
     * @param epoch 由 @ref LSN_lock 返回的 epoch/token。
     */
    void LSN_unlock(size_t epoch);
#endif

    /**
     * @brief 获取 memtable 的大小估计/计数。
     * @param idx memtable 索引。
     */
    size_t GetMemtableSize(int idx);

    /**
     * @brief 清空 memtable 的大小计数。
     * @param idx memtable 索引。
     */
    void ClearMemtableSize(int idx);

    /**
     * @brief 向临时 memtable 大小计数器累加字节数。
     * @param idx memtable 索引。
     * @param size 需要累加的字节数。
     */
    void AddTempMemtableSize(int idx, size_t size)
    {
        temp_memtable_size_[idx].fetch_add(size);
    }
	

public: // TODO: change to private
    /**
     * @brief 判断是否触发 flush 或 compaction。
     * @return 若触发了后台任务则返回 true。
     */
    bool MayTriggerFlushOrCompaction();

    /**
     * @brief 判断是否触发 compaction。
     * @return 若触发了 compaction 任务则返回 true。
     */
    bool MayTriggerCompaction();
    /** @brief 执行一次后台 flush 周期（指定 memtable）。 */
    bool BGFlush(int target_memtable_idx);
    /** @brief 手动触发 flush：切换 memtable 并 flush 当前的。 */
    bool BGFlush();

    /** @brief 执行一次后台 compaction 周期。 */
    bool BGCompaction();

    /** @brief 阻塞等待正在进行的 flush/compaction 完成。 */
    void WaitForFlushAndCompaction();

    /** @brief 停止后台触发线程，供测试场景使用。 */
    void StopBackgroundTriggerForTesting();

    /** @brief 输出某个 log group 的调试信息。 */
    void PrintLogGroup(int id);

    /** @brief 输出 SSD 使用情况（由实现决定）。 */
	void PrintSSDUsage();
	
	/** @brief 获取 DataBlock 缓存命中率统计 */
	void PrintCacheStats();
	
	/** @brief 打印 L1 索引统计信息 */
	void PrintL1IndexStats();
	
	// Get 操作统计（全局）
	static std::atomic<uint64_t> global_get_success_;
	static std::atomic<uint64_t> global_get_failure_;

    /**
     * @brief 传递给 @ref PrintLogGroupInMYDB 的参数。
     */
    struct TestParams
    {
        MYDB *db_;
        int log_group_id_;
        std::atomic_bool *is_running_;
        TestParams(MYDB *db, int id, std::atomic_bool *run) : db_(db), log_group_id_(id), is_running_(run) {}
    };

    /**
        * @brief 线程/worker 入口：打印 log group 信息。
        * @param arg 指向 @ref TestParams 的指针。
     */
    static void PrintLogGroupInMYDB(void *arg);

    /**
        * @brief 传递给 @ref TriggerBGFlush 的参数。
     */
    struct FlushArgs
    {
        MYDB *db_;
        int target_memtable_idx_;
        FlushArgs(MYDB *db, int idx) : db_(db), target_memtable_idx_(idx) {}
    };

    /**
        * @brief 线程/worker 入口：触发后台 flush。
        * @param arg 指向 @ref FlushArgs 的指针。
     */
    static void TriggerBGFlush(void *arg);

    /**
        * @brief 传递给 @ref TriggerBGCompaction 的参数。
     */
    struct CompactionArgs
    {
        MYDB *db_;
        CompactionArgs(MYDB *db) : db_(db) {}
    };

    /**
        * @brief 线程/worker 入口：触发后台 compaction。
        * @param arg 指向 @ref CompactionArgs 的指针。
     */
    static void TriggerBGCompaction(void *arg);

    /**
        * @brief 启用读优化模式。
     *
        * 旨在通过强制 flush/compaction 来降低 memtable 读延迟。
     */
    void EnableReadOptimizedMode()
    {
        if (!read_optimized_mode_)
        {
            read_optimized_mode_ = true;
            LOG("read_optimized_mode_ = true");
        }
    }

    /** @brief 禁用读优化模式。 */
    void DisableReadOptimizedMode()
    {
        if (read_optimized_mode_)
        {
            read_optimized_mode_ = false;
            LOG("read_optimized_mode_ = false");
        }
    }

    /** @brief 启用只读模式（写操作预计会被实现拒绝）。 */
    void EnableReadOnlyMode()
    {
        if (!read_only_mode_)
        {
            read_only_mode_ = true;
            LOG("read_only_mode_ = true");
        }
    }

    /** @brief 禁用只读模式。 */
    void DisableReadOnlyMode()
    {
        if (read_only_mode_)
        {
            read_only_mode_ = false;
            LOG("read_only_mode_ = false");
        }
    }

    /** @brief 设置 L0 触发 compaction 的 tree 数阈值。 */
    void SetL0CompactionTreeNum(size_t num)
    {
        l0_compaction_tree_num_ = num;
    }

    /** @brief 设置 L0 写阻塞阈值（达到后前台写等待后台 compaction 降低 L0 树数）。 */
    void SetL0WriteStallTreeNum(size_t num)
    {
        if (num == 0)
        {
            l0_write_stall_tree_num_ = 1;
            return;
        }
        const size_t hard_max = MAX_L0_TREE_NUM - 1;
        l0_write_stall_tree_num_ = (num < hard_max) ? num : hard_max;
    }

    /** @brief 启用/禁用后台 compaction。 */
    void SetCompactionEnabled(bool enabled)
    {
        compaction_enabled_ = enabled;
    }
};

/**
 * @brief 面向线程的数据库操作客户端句柄。
 */
class MYDBClient
{
    friend class MYDB;

public:
    /**
     * @brief 构造一个绑定到数据库与逻辑线程 id 的 client。
     * @param db 数据库实例。
     * @param tid 逻辑线程 id。
     */
    MYDBClient(MYDB *db, int tid);

    /** @brief 析构 client 并释放线程本地资源（由实现决定）。 */
    ~MYDBClient();

    /**
     * @brief 插入或更新一条 key/value。
     * @param key key 字节序列。
     * @param value value 字节序列。
     * @param slow 可选慢路径开关（由实现决定）。
     * @return 成功返回 true。
     */
    bool Put(const Slice key, const Slice value, bool slow = false);

    /**
     * @brief 获取 key 对应的 value。
     * @param key key 字节序列。
     * @param value_out 输出 value 视图（所有权/生命周期由实现决定）。
     * @return 找到则返回 true。
     */
    bool Get(const Slice key, Slice &value_out);

    /**
     * @brief 删除一个 key。
     * @param key key 字节序列。
     * @return 成功返回 true。
     */
    bool Delete(const Slice key);

    /**
     * @brief 从起始 key 向前扫描。
     * @param start_key 起始 key。
     * @param scan_sz 最大扫描记录数。
     * @param key_out 输出 key 向量。
     * @return 实际返回的记录数。
     */
    int Scan(const Slice start_key, int scan_sz, std::vector<KeyType> &key_out);

    /**
     * @brief 持久化该 client 的日志状态（由实现决定）。
     */
    void Persist_Log();
    bool Persist_Log(int memtable_idx);

    /// 该 client 绑定的逻辑线程 id。
    const int thread_id_;

private:
    enum class MemtableLookupResult
    {
        kMiss,
        kFound,
        kDeleted,
    };

    MYDB *db_;
    LogWriter *log_writer_;
    LogReader *log_reader_;
    int current_memtable_idx_;
    PSTReader *pst_reader_;
    size_t put_num_in_current_memtable_[MAX_MEMTABLE_NUM];
    std::atomic_uint64_t total_writes_ = 0;
    std::atomic_uint64_t total_reads_ = 0;
    std::mutex log_writer_mu_;

    /**
        * @brief 尝试从 memtable 满足一次读取。
        * @param key key 字节序列。
        * @param value_out 输出 value 视图。
        * @return 若在 memtable 中命中则返回 true。
     */
    MemtableLookupResult GetFromMemtable(const Slice key, Slice &value_out);

    /**
     * @brief Update current_memtable_idx_ by db_->current_memtable_idx_
     *
     * @return true means current_memtable_idx_ is changed
     * @return false
     */
    inline bool StartWrite();

    /**
        * @brief 结束由 @ref StartWrite 开始的写入临界区。
     */
    inline void FinishWrite();

    size_t GetMemtablePutCount(int memtable_id)
    {
        return put_num_in_current_memtable_[memtable_id];
    }
    void ClearMemtablePutCount(int memtable_id)
    {
        put_num_in_current_memtable_[memtable_id] = 0;
    }
    float GetWriteRatioAndClear()
    {
        if (total_reads_ + total_writes_ == 0)
        {
            return -1;
        }
        float rwratio = (float)total_writes_ / (total_reads_ + total_writes_);
        total_writes_ = 0;
        total_reads_ = 0;
        return rwratio;
    }
};
