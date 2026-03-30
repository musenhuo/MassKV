/**
 * @file config.h
 * @brief 编译期配置宏与数据库运行时配置对象。
 */

#pragma once
#include <string>

/// 引擎维护的 memtable 最大数量。
#define MAX_MEMTABLE_NUM 4

/// 单个 memtable 预计可容纳的 entries 上限（策略阈值；具体使用方式由实现决定）。
/// 使用 4000万 entries（memtable_40m）配置。
#define MAX_MEMTABLE_ENTRIES 40000000

/// L0 层 tree 数量上限（仅上限；触发由 db->l0_compaction_tree_num_ 控制）。
#define MAX_L0_TREE_NUM 32 //only a upper limit. trigger is db->l0_compaction_tree_num_

/// 客户端接口支持的用户线程最大数量。
#define MAX_USER_THREAD_NUM 64

/// 引擎使用的范围分区数量。
#define RANGE_PARTITION_NUM 12


/// 选择 Masstree 作为 memtable 索引实现。
#define MASSTREE_MEMTABLE

/// 选择 Masstree 作为 L1 索引实现。
#define MASSTREE_L1

/// H-Masstree（Masstree 的修改版副本）切换开关。
///
/// 构建命令：`cmake -S . -B build && cmake --build build -j`
///


// Now defined in CMakeList.txt
// #define INDEX_LOG_MEMTABLE
// #define BUFFER_WAL_MEMTABLE

#ifdef INDEX_LOG_MEMTABLE
/// 在 index-log memtable 模式下启用 value 分离布局。
#define KV_SEPARATE
#endif

/**
 * @brief 创建/打开 MYDB 实例的运行时配置。
 */
class MYDBConfig
{
public:
    // MYDBConfig(/* args */){};
    /**
     * @brief 使用指定的持久化内存池路径构造配置。
     * @param pm_pool_path PM pool/设备/文件路径。
     */
    MYDBConfig(std::string pm_pool_path)
    {
        this->pm_pool_path = pm_pool_path;
    }

    /** @brief 使用默认值构造配置。 */
    MYDBConfig()
    {
        
    }
    ~MYDBConfig(){};

    // std::string pm_pool_path = "/mnt/nvme0/ycsb";
    std::string pm_pool_path = "/dev/nvme1n1"; ///< 持久化内存池路径。
    size_t pm_pool_size = 500ul << 30;          ///< 持久化内存池大小（字节）。
    bool recover = false;                       ///< 打开时是否执行恢复流程（由实现决定）。
    bool use_direct_io = false;                 ///< 是否使用 O_DIRECT 绕过页缓存。
    size_t bg_trigger_threads = 4;              ///< 后台触发线程池大小。
    size_t flush_threads = RANGE_PARTITION_NUM; ///< flush 并行线程数。
    size_t compaction_threads = 4;              ///< compaction 并行线程数。
};
