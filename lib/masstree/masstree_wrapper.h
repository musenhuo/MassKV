#pragma once

#include <iostream>
#include <random>
#include <vector>
#include <thread>

#include <pthread.h>

#include "config.h"
#include "compiler.hh"

#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_print.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "string.hh"

#include "db_common.h"

#include <unistd.h>
#include <array>
#include <memory>
#include <string>
#include <stdexcept>
#include <regex>
#include <unordered_set>

/**
 * @brief 打印当前进程的 DRAM（RSS）占用。
 *
 * 通过读取 `/proc/<pid>/status` 中的 `VmRSS` 字段估算当前进程常驻内存。
 *
 * @note 仅适用于 Linux。
 * @throw std::runtime_error 当 popen 失败时抛出。
 */
static void print_dram_consuption()
{
    auto pid = getpid();
    std::array<char, 128> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(
        popen(("cat /proc/" + std::to_string(pid) + "/status").c_str(), "r"),
        pclose);
    if (!pipe)
    {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    {
        std::string result = buffer.data();
        if (result.find("VmRSS") != std::string::npos)
        {
            // std::cout << result << std::endl;
            std::string mem_ocp = std::regex_replace(
                result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
            DEBUG("DRAM consumption: %.3f GB.", stof(mem_ocp) / 1024 / 1024);
            break;
        }
    }
}

class key_unparse_unsigned
{
public:
    /**
     * @brief 将 Masstree 的整型 key 反序列化为可打印字符串。
     *
     * 该函数用于 Masstree 的调试/打印路径（key_unparse_type）。
     *
     * @param key Masstree 内部 key（此处以 uint64_t ikey 为主）。
     * @param buf 输出缓冲区。
     * @param buflen 缓冲区大小。
     * @return 实际写入（或需要写入）的字符数，语义同 snprintf。
     */
    static int unparse_key(Masstree::key<uint64_t> key, char *buf, int buflen)
    {
        return snprintf(buf, buflen, "%" PRIu64, key.ikey());
    }
};

/**
 * @brief FlowKV 对 Masstree 的轻量封装。
 *
 * 该封装将 Masstree 的 `basic_table` + `threadinfo` 生命周期管理隐藏起来，
 * 提供面向 FlowKV 的整数 Key API：Get/Put/Delete/Scan。
 *
 * 约定：
 * - Key 使用 uint64_t，并以字典序（big-endian 比较意义）参与 Masstree 的比较。
 * - Value 也是 uint64_t（FlowKV 内部通常用其承载 ValuePtr/地址/元信息）。
 * - 每个工作线程需通过 `thread_init(tid)` 设置线程 id，以获得对应 threadinfo。
 */
class MasstreeWrapper
{
public:
    /**
     * @brief 插入/生成 key 的上界（用于压测或特定 workload）。
     *
     * @note 该常量是否生效取决于调用方使用方式。
     */
    static constexpr uint64_t insert_bound = 0xfffff; // 0xffffff;

    /**
     * @brief Masstree 表参数配置。
     *
     * 指定节点宽度、并发与预取策略，以及本工程使用的 value/threadinfo/key 打印策略。
     */
    struct table_params : public Masstree::nodeparams<15, 15>
    {
        /// Masstree 叶子存储的值类型（FlowKV 用 uint64_t 承载 ValuePtr/地址等）。
        typedef uint64_t value_type;
        /// Masstree 打印 value 的策略。
        typedef Masstree::value_print<value_type> value_print_type;
        /// 线程上下文（内存分配器、RCU、统计计数等）。
        typedef threadinfo threadinfo_type;
        /// key 打印/反序列化策略（用于调试输出）。
        typedef key_unparse_unsigned key_unparse_type;
        static constexpr ssize_t print_max_indent_depth = 12;
    };

    typedef Masstree::Str Str;
    typedef Masstree::basic_table<table_params> table_type;
    typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
    typedef Masstree::tcursor<table_params> cursor_type;
    typedef Masstree::leaf<table_params> leaf_type;
    typedef Masstree::internode<table_params> internode_type;

    typedef typename table_type::node_type node_type;
    typedef typename unlocked_cursor_type::nodeversion_value_type nodeversion_value_type;

    /**
     * @brief Scan 回调：仅收集 value。
     *
     * 语义：从起始 key 开始，按 Masstree 的有序遍历顺序最多收集 cnt 个 value。
     */
    struct Scanner
    {
        /// 需要收集的最大条数。
        const int cnt;
        /// value 输出容器引用。
        std::vector<table_params::value_type> &vec;

        Scanner(int cnt, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v)
        {
            vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        /**
         * @brief 访问每条扫描到的 KV。
         * @param key 当前 key（Masstree::Str 视图）。
         * @param val 当前 value。
         * @param ti 线程上下文。
         * @return 返回 false 表示提前终止扫描；true 表示继续。
         */
        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
            vec.push_back(val);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };

    /**
     * @brief Scan 回调：同时收集 key 与 value。
     *
     * 支持 8B (uint64_t) 和 16B (Key16) key 的条件编译。
     */
    struct Scanner2
    {
        const int cnt;
        std::vector<table_params::value_type> &vec;
        std::vector<KeyType> &k_vec;

        Scanner2(int cnt, std::vector<KeyType> &k, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v), k_vec(k)
        {
            vec.reserve(cnt);
            k_vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        /**
         * @brief 访问每条扫描到的 KV。
         * @param key 当前 key（Masstree Str，8B 或 16B）。
         * @param val 当前 value。
         * @param ti 线程上下文。
         * @return 返回 false 表示提前终止扫描；true 表示继续。
         */
        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
#if defined(FLOWKV_KEY16)
            // 16B key: 从大端序字节串还原 Key16
            Key16 kint = Key16::FromBigEndianBytes((const uint8_t *)key.data());
#else
            // 8B key: 直接读取 uint64_t
            uint64_t kint = *(uint64_t *)key.data();
#endif
            vec.push_back(val);
            k_vec.emplace_back(kint);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };

    /**
     * @brief Range Scan 回调：收集 [start, end] 范围内的 key/value。
     *
     * 实现方式：从 start 开始 scan，当 key 超过 end 时终止。
     * 支持 8B (uint64_t) 和 16B (Key16) key 的条件编译。
     */
    struct Scanner3
    {
        /// 扫描终止 key（开区间判断：key > end 则停止）。
        Str end;
        std::vector<table_params::value_type> &vec;
        std::vector<KeyType> &k_vec;

        Scanner3(Str end_key, std::vector<KeyType> &k, std::vector<table_params::value_type> &v)
            : end(end_key), vec(v), k_vec(k)
        {
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        /**
         * @brief 访问每条扫描到的 KV，并根据 end key 决定是否停止。
         * @param key 当前 key（Masstree Str，8B 或 16B）。
         * @param val 当前 value。
         * @param ti 线程上下文。
         * @return key 超过 end 时返回 false 终止，否则返回 true。
         */
        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
#if defined(FLOWKV_KEY16)
            // 16B key: 从大端序字节串还原 Key16
            Key16 kint = Key16::FromBigEndianBytes((const uint8_t *)key.data());
#else
            // 8B key: 直接读取 uint64_t
            uint64_t kint = *(uint64_t *)key.data();
#endif
            if (key > end)
            {
                return false;
            }
            vec.push_back(val);
            k_vec.emplace_back(kint);
            return true;
        }
    };

#if defined(FLOWKV_KEY16)
    struct Scanner2U64
    {
        const int cnt;
        std::vector<table_params::value_type> &vec;
        std::vector<uint64_t> &k_vec;

        Scanner2U64(int cnt, std::vector<uint64_t> &k, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v), k_vec(k)
        {
            vec.reserve(cnt);
            k_vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            const uint64_t kint = *(const uint64_t *)key.data();
            vec.push_back(val);
            k_vec.emplace_back(kint);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };

    struct Scanner3U64
    {
        Str end;
        std::vector<table_params::value_type> &vec;
        std::vector<uint64_t> &k_vec;

        Scanner3U64(Str end_key, std::vector<uint64_t> &k, std::vector<table_params::value_type> &v)
            : end(end_key), vec(v), k_vec(k)
        {
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            if (key > end)
            {
                return false;
            }
            const uint64_t kint = *(const uint64_t *)key.data();
            vec.push_back(val);
            k_vec.emplace_back(kint);
            return true;
        }
    };
#endif

    /**
     * @brief ForEach Scanner：对每条 entry 调用用户 callback，不分配临时 vector。
     */
    struct ScannerForEach
    {
        std::function<bool(KeyType, table_params::value_type)> &callback;

        ScannerForEach(std::function<bool(KeyType, table_params::value_type)> &cb)
            : callback(cb) {}

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
#if defined(FLOWKV_KEY16)
            KeyType kint = Key16::FromBigEndianBytes((const uint8_t *)key.data());
#else
            KeyType kint = *(KeyType *)key.data();
#endif
            return callback(kint, val);
        }
    };

    /**
     * @brief ForEach Range Scanner：在 [start, end] 范围内对每条 entry 调用 callback。
     */
    struct ScannerForEachRange
    {
        Str end;
        std::function<bool(KeyType, table_params::value_type)> &callback;

        ScannerForEachRange(Str end_key, std::function<bool(KeyType, table_params::value_type)> &cb)
            : end(end_key), callback(cb) {}

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            if (key > end) return false;
#if defined(FLOWKV_KEY16)
            KeyType kint = Key16::FromBigEndianBytes((const uint8_t *)key.data());
#else
            KeyType kint = *(KeyType *)key.data();
#endif
            return callback(kint, val);
        }
    };

    // static thread_local typename table_params::threadinfo_type *ti;

    /**
     * @brief 当前线程的逻辑 id（1..64），用于索引 `tis[]`。
     *
     * 约定：0 号 slot 保留给 TI_MAIN（初始化/管理线程）。
     */
    static thread_local int thread_id;

    /**
     * @brief 线程上下文数组。
     *
     * `tis[tid]` 由 `get_ti()` 懒加载创建；析构时统一释放。
     */
    typename table_params::threadinfo_type *tis[65];

    /**
     * @brief 构造函数：初始化 threadinfo 数组并初始化 Masstree 表。
     */
    MasstreeWrapper()
    {
        for (int i = 0; i < 65; i++)
        {
            tis[i] = nullptr;
        }
        this->table_init();
    }

    /**
     * @brief 析构函数：释放所有已创建的 threadinfo。
     */
    ~MasstreeWrapper()
    {
        // printf("before free ti\n");
        // print_dram_consuption();
        if (tis[0] != nullptr) {
            // Ensure tree nodes (and their ksuffix malloc allocations) enter
            // the reclaim path before threadinfo pools are released.
            table_.destroy(*tis[0]);
        }
        for (int i = 0; i < 65; i++)
        {
            if (tis[i] != nullptr)
            {
                threadinfo::free_ti(tis[i]);
            }
        }
        // print_dram_consuption();
        // printf("after free ti\n");
    }

    void table_init()
    {

        if (tis[0] == nullptr)
        {
            tis[0] = threadinfo::make(threadinfo::TI_MAIN, -1);
        }
        table_params::threadinfo_type *ti = tis[0];
        table_.initialize(*ti);
        key_gen_ = 0;
    }

    /** @brief 重置内部 key 生成器（若上层使用 key_gen_）。 */
    void keygen_reset()
    {
        key_gen_ = 0;
    }

    /**
     * @brief 初始化当前线程的 Masstree 线程 id。
     * @param tid 线程 id，范围 1..64。
     */
    static void thread_init(int tid)
    {
        assert(tid > 0 && tid <= 64);
        thread_id = tid;
    }

    /**
     * @brief 获取当前线程的 threadinfo（懒加载）。
     * @return 当前线程对应的 threadinfo 指针。
     */
    inline table_params::threadinfo_type *get_ti()
    {
        assert(thread_id >= 0 && thread_id <= 64);
        if (unlikely(tis[thread_id] == nullptr))
        {
            tis[thread_id] = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
        }
        return tis[thread_id];
    }

    /**
     * @brief 插入或更新（upsert）一条记录。
     *
     * 若 key 已存在，会通过 `le_helper.old_val` 返回旧值。
     * 新值通过 `le_helper.new_val` 写入。
     *
     * @param int_key 业务侧 KeyType key。
     * @param le_helper 写入辅助信息（新值/旧值等）。
     */
    void insert(KeyType int_key, ValueHelper &le_helper)
    {
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (found)
        {
            le_helper.old_val = lp.value();
        }
        lp.value() = le_helper.new_val;
        fence();
        lp.finish(1, *ti);
    }

#if defined(FLOWKV_KEY16)
    void insert(uint64_t int_key, ValueHelper &le_helper)
    {
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (found)
        {
            le_helper.old_val = lp.value();
        }
        lp.value() = le_helper.new_val;
        fence();
        lp.finish(1, *ti);
    }
#endif

    /**
     * @brief 带校验的插入/更新：按 LSN 单调性决定是否覆盖旧值。
     *
     * 仅当新值的 LSN >= 旧值的 LSN 时，才会覆盖。
     *
     * @param int_key 业务侧 KeyType key。
     * @param le_helper 写入辅助信息（new_val 内含 ValuePtr/lsn 语义）。
     */
    void insert_validate(KeyType int_key, ValueHelper &le_helper)
    {
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (unlikely(found))
        {
            // le_helper.old_val = lp.value();
            uint64_t new_lsn = ((ValuePtr*)(&le_helper.new_val))->detail_.lsn;
            uint64_t old_lsn = ((ValuePtr*)(&lp.value()))->detail_.lsn;
            if (new_lsn >= old_lsn)
                lp.value() = le_helper.new_val;
        }
        else
        {
            lp.value() = le_helper.new_val;
        }
        fence();
        lp.finish(1, *ti);
    }

    /**
     * @brief 查询 key。
     * @param int_key 业务侧 KeyType key。
     * @param value 输出：若 found 则写入对应 value。
     * @return 是否找到。
     */
    bool search(KeyType int_key, uint64_t &value)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        bool found = table_.get(key, value, *ti);
        return found;
    }

#if defined(FLOWKV_KEY16)
    bool search(uint64_t int_key, uint64_t &value)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        return table_.get(key, value, *ti);
    }
#endif

    /**
     * @brief 从 int_key（含）开始扫描，最多返回 cnt 个 value。
     * @param int_key 起始 key。
     * @param cnt 最大返回条数。
     * @param vec 输出 value 列表。
     */
    void scan(KeyType int_key, int cnt, std::vector<uint64_t> &vec)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        Scanner scanner(cnt, vec);
        table_.scan(key, true, scanner, *ti);
    }

    /**
     * @brief 从 int_key（含）开始扫描，最多返回 cnt 个 key/value。
     * @param int_key 起始 KeyType key。
     * @param cnt 最大返回条数。
     * @param kvec 输出 key 列表（KeyType 可能是 uint64_t 或 Key16）。
     * @param vvec 输出 value 列表。
     */
    void scan(KeyType int_key, int cnt, std::vector<KeyType> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        Scanner2 scanner(cnt, kvec, vvec);
        table_.scan(key, true, scanner, *ti);
    }

#if defined(FLOWKV_KEY16)
    void scan(uint64_t int_key, int cnt, std::vector<uint64_t> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        Scanner2U64 scanner(cnt, kvec, vvec);
        table_.scan(key, true, scanner, *ti);
    }
#endif

    /**
     * @brief 扫描范围 [start, end] 内的 key/value（按有序遍历）。
     *
     * 通过从 start 开始 scan，当 key > end 时停止。
     *
     * @param start 起始 KeyType key（包含）。
     * @param end 结束 KeyType key（包含）。
     * @param kvec 输出 key 列表（KeyType 可能是 uint64_t 或 Key16）。
     * @param vvec 输出 value 列表。
     */
    void scan(KeyType start, KeyType end, std::vector<KeyType> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t start_buf[16], end_buf[16];
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
#else
        uint64_t start_buf, end_buf;
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
#endif
        Scanner3 scanner(end_str, kvec, vvec);
        table_.scan(start_str, true, scanner, *ti);
    }

#if defined(FLOWKV_KEY16)
    void scan(uint64_t start, uint64_t end, std::vector<uint64_t> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t start_buf, end_buf;
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
        Scanner3U64 scanner(end_str, kvec, vvec);
        table_.scan(start_str, true, scanner, *ti);
    }
#endif

    /**
     * @brief 从 int_key 开始遍历所有 entry，对每条调用 callback。
     */
    void for_each(KeyType int_key, std::function<bool(KeyType, uint64_t)> callback)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        ScannerForEach scanner(callback);
        table_.scan(key, true, scanner, *ti);
    }

    /**
     * @brief 在 [start, end] 范围内遍历 entry，对每条调用 callback。
     */
    void for_each_range(KeyType start, KeyType end, std::function<bool(KeyType, uint64_t)> callback)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t start_buf[16], end_buf[16];
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
#else
        uint64_t start_buf, end_buf;
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
#endif
        ScannerForEachRange scanner(end_str, callback);
        table_.scan(start_str, true, scanner, *ti);
    }

    /**
     * @brief 删除指定 key。
     *
     * 当前实现会执行一次 `find_locked + finish(-1)`。
     * @param int_key 待删除 KeyType key。
     * @return 是否删除成功。
     * @note 目前无论 key 是否存在都返回 true（如需严格语义，可改为返回 found）。
     */
    bool remove(KeyType int_key)
    {
        table_params::threadinfo_type *ti = get_ti();
#if defined(FLOWKV_KEY16)
        uint8_t key_buf[16];
        Str key = make_key(int_key, key_buf);
#else
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
#endif
        cursor_type lp(table_, key);
        bool found = lp.find_locked(*ti);
        lp.finish(-1, *ti);
        return true;
    }

    /**
     * @brief Estimate actual memory bytes used by this Masstree instance.
     *
     * This traverses the current tree structure and sums:
     * - leaf allocated bytes
     * - internode bytes
     * - external ksuffix bags (if any)
     *
     * @note For route-layer (8B prefix) this reflects real node footprint,
     *       not a per-entry model estimate.
     */
    size_t EstimateMemoryUsageBytes() const
    {
        using node_ptr = const node_type *;
        std::unordered_set<node_ptr> visited;
        std::vector<node_ptr> stack;
        size_t total_bytes = 0;

        node_ptr root = table_.root();
        if (root == nullptr)
        {
            return 0;
        }
        stack.push_back(root);

        while (!stack.empty())
        {
            node_ptr node = stack.back();
            stack.pop_back();
            if (node == nullptr || !visited.insert(node).second)
            {
                continue;
            }

            if (node->isleaf())
            {
                // Skip cold stubs — they are not real leaf nodes
                auto nv = node->stable(relax_fence_function());
                if (nv.cold()) {
                    continue;
                }
                const auto *leaf = static_cast<const leaf_type *>(node);
                total_bytes += leaf->allocated_size();
                if (leaf->ksuf_ != nullptr)
                {
                    total_bytes += leaf->ksuf_->capacity();
                }

                const auto perm = leaf->permutation();
                for (int r = 0; r < perm.size(); ++r)
                {
                    const int p = perm[r];
                    const int keylenx = leaf->keylenx_[p];
                    if (leaf_type::keylenx_is_layer(keylenx))
                    {
                        auto *layer = reinterpret_cast<node_ptr>(leaf->lv_[p].layer());
                        if (layer != nullptr)
                        {
                            stack.push_back(layer);
                        }
                    }
                }
                continue;
            }

            const auto *internode = static_cast<const internode_type *>(node);
            total_bytes += sizeof(*internode);
            const int nkeys = internode->size();
            for (int i = 0; i <= nkeys; ++i)
            {
                auto *child = reinterpret_cast<node_ptr>(internode->child_[i]);
                if (child != nullptr)
                {
                    stack.push_back(child);
                }
            }
        }

        return total_bytes;
    }

    /**
     * @brief Estimate bytes currently reserved by Masstree thread pools.
     *
     * This counts per-thread `threadinfo` pool chunks (typically 2MB each)
     * that back node allocations and may include unused slack.
     */
    size_t EstimateThreadInfoPoolBytes() const
    {
        size_t total_bytes = 0;
        for (int i = 0; i < 65; ++i)
        {
            if (tis[i] != nullptr)
            {
                total_bytes += tis[i]->debug_allocated_pool_bytes();
            }
        }
        return total_bytes;
    }

    /**
     * @brief Cold-aware search: returns the node_base pointer when a cold stub is hit.
     *
     * @param int_key  Key to look up (uint64_t prefix).
     * @param value    Output value if found in a hot leaf.
     * @param cold_node Output: set to the cold stub node_base* if hit, nullptr otherwise.
     * @return true if found in hot leaf; false if not found or cold stub hit.
     *         When false, check cold_node != nullptr to distinguish cold vs absent.
     */
    bool search_cold_aware(uint64_t int_key, uint64_t &value, node_type *&cold_node)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);

        unlocked_cursor_type lp(table_, key);
        bool found = lp.find_unlocked(*ti);
        if (found) {
            value = lp.value();
            cold_node = nullptr;
            return true;
        }
        // find_unlocked returned false — check if we landed on a cold stub
        auto *n = lp.node();
        if (n != nullptr) {
            auto v = n->stable(relax_fence_function());
            if (v.cold()) {
                cold_node = n;
                return false;
            }
        }
        cold_node = nullptr;
        return false;
    }

    /**
     * @brief Walk the leaf linked list, calling fn(leaf*) for each leaf.
     *
     * Traverses from the leftmost leaf via next_ pointers.
     * For cold stubs, the callback receives a node_base* that is NOT a real leaf.
     * Use IsColdLeafStub() to detect.
     *
     * @param fn  Callback: void(node_type* node, bool is_cold)
     */
    template <typename Fn>
    void ForEachLeaf(Fn&& fn) const
    {
        node_type* root = const_cast<node_type*>(table_.root());
        if (!root) return;

        // Descend to leftmost leaf
        node_type* n = root;
        while (true) {
            auto v = n->stable(relax_fence_function());
            if (v.isleaf()) break;
            auto* in = static_cast<const internode_type*>(n);
            n = const_cast<node_type*>(static_cast<const node_type*>(in->child_[0]));
            if (!n) return;
        }

        // Walk the leaf chain
        while (n) {
            auto v = n->stable(relax_fence_function());
            bool is_cold = v.cold();
            fn(n, is_cold);
            if (is_cold) {
                // Cold stubs don't have next_ at the leaf offset.
                // We stop here — caller should use cold_stubs_ list for cold iteration.
                break;
            }
            auto* leaf = static_cast<leaf_type*>(n);
            n = leaf->safe_next();
        }
    }

    /**
     * @brief Get the root node of the underlying Masstree.
     */
    node_type* root() const {
        return const_cast<node_type*>(table_.root());
    }

private:
    /// Masstree 主表对象（包含 root 指针等）。
    table_type table_;

    /// 简单的 key 生成器（部分 workload/测试可能使用）。
    uint64_t key_gen_;

    /// 控制后台线程/打印的全局标志（具体语义取决于实现文件）。
    static bool stopping;

    /// 打印节流或打印次数控制（具体语义取决于实现文件）。
    static uint32_t printing;

    /**
     * @brief 将 uint64 key 编码为 Masstree 使用的 `Str`（8B 版本）。
     *
     * Masstree 的比较是对 `Str` 做字典序（以及内部 ikey 的可比较编码）。
     *
     * @param int_key 输入 key。
     * @param key_buf 输出：保存编码后的字节序列（其地址作为 Str.data()）。
     * @return 指向 key_buf 的 Str 视图。
     */
    static inline Str make_key(uint64_t int_key, uint64_t &key_buf)
    {
		// Becasuse masstree use dictionary order to compare, we don't need to bswap.
        // key_buf = __builtin_bswap64(int_key);
        key_buf = int_key;
        return Str((const char *)&key_buf, sizeof(key_buf));
    }

#if defined(FLOWKV_KEY16)
    /**
     * @brief 将 Key16 编码为 Masstree 使用的 `Str`（16B 版本）。
     *
     * Key16 以大端序编码到 16 字节 buffer，使得 Masstree 的字典序比较等价于数值序。
     *
     * @param int_key 输入 Key16。
     * @param key_buf 输出：16 字节 buffer。
     * @return 指向 key_buf 的 Str 视图。
     */
    static inline Str make_key(const Key16 &int_key, uint8_t *key_buf)
    {
        int_key.ToBigEndianBytes(key_buf);
        return Str((const char *)key_buf, 16);
    }
#endif
};
