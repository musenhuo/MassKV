/**
 * @file db_common.h
 * @brief DB 引擎共享的公共类型与接口定义。
 */

#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <tuple>
#include <vector>
#include "config.h"
#include "slice.h"
#include "../util/debug_helper.h"
#include "../util/util.h"
#include "../util/lock.h"

#ifndef ERROR_CODE
/**
 * @def ERROR_CODE
 * @brief 通用错误哨兵值。
 */
#define ERROR_CODE (0xffffffffffffffffUL)
#endif
#ifndef INVALID_PTR
/**
 * @def INVALID_PTR
 * @brief 通用无效指针/地址哨兵值。
 */
#define INVALID_PTR (0xffffffffffffffffUL)
#endif
#ifndef KV_SEPARATE
/// key/value 未分离时的固定 value 大小（字节）。
#define VALUESIZE 8
#else
/// key/value 分离时的固定 value 大小（字节）。
#define VALUESIZE 24
#endif
/// 日志条目 header 大小（Key16 模式额外 8 字节）
#if defined(FLOWKV_KEY16)
#define LOG_HEADER_SIZE 24
#else
#define LOG_HEADER_SIZE 16
#endif
/// 日志槽位大小（字节）
/// Key16 模式统一使用 64B 固定槽位，保证 ValuePtr 与 recovery 的固定步长编码成立。
/// 8B key 模式维持原有 24B 槽位。
#if defined(FLOWKV_KEY16)
#define LOG_ENTRY_SIZE 64
#else
#define LOG_ENTRY_SIZE 24
#endif
/// 一个 log buffer block 中可容纳的日志条目数量。
#define LogNumperBlock (logbuffersize/LOG_ENTRY_SIZE)

/**
 * @brief 判断主机字节序是否为小端序。
 *
 * 当前我们确认运行环境为小端（x86_64 常见）。
 * 这里保留一个轻量实现，供 key 编码/比较工具使用。
 */
static inline bool HostIsLittleEndian()
{
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    return true;
#elif defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    return false;
#else
    const uint16_t x = 1;
    return *reinterpret_cast<const uint8_t *>(&x) == 1;
#endif
}

/**
 * @brief 16 字节定长 key。
 *
 * 比较规则：先比较 hi（高 64 位），再比较 lo（低 64 位），均按无符号比较。
 * 若需要与 Masstree/`memcmp` 的字典序保持一致，应将 key 以“大端字节序”编码为 16 字节再进行字典序比较。
 */
struct Key16
{
    uint64_t hi = 0; ///< 高 64 位（数值语义的高位）。
    uint64_t lo = 0; ///< 低 64 位（数值语义的低位）。

    Key16() = default;
    constexpr Key16(uint64_t hi_, uint64_t lo_) : hi(hi_), lo(lo_) {}
    
    // Implicit conversion from uint64_t - uses lo only, hi=0
    constexpr Key16(uint64_t val) : hi(0), lo(val) {}

    static inline Key16 FromBigEndianBytes(const void *p)
    {
        uint64_t be_hi;
        uint64_t be_lo;
        std::memcpy(&be_hi, p, sizeof(uint64_t));
        std::memcpy(&be_lo, reinterpret_cast<const uint8_t *>(p) + sizeof(uint64_t), sizeof(uint64_t));
        if (HostIsLittleEndian())
        {
            return Key16(__builtin_bswap64(be_hi), __builtin_bswap64(be_lo));
        }
        return Key16(be_hi, be_lo);
    }

    inline void ToBigEndianBytes(void *out) const
    {
        uint64_t be_hi = hi;
        uint64_t be_lo = lo;
        if (HostIsLittleEndian())
        {
            be_hi = __builtin_bswap64(be_hi);
            be_lo = __builtin_bswap64(be_lo);
        }
        std::memcpy(out, &be_hi, sizeof(uint64_t));
        std::memcpy(reinterpret_cast<uint8_t *>(out) + sizeof(uint64_t), &be_lo, sizeof(uint64_t));
    }
};

static inline bool operator==(const Key16 &a, const Key16 &b)
{
    return a.hi == b.hi && a.lo == b.lo;
}
static inline bool operator!=(const Key16 &a, const Key16 &b) { return !(a == b); }

/**
 * @brief Key16 三路比较。
 * @return a<b 返回 -1；相等返回 0；a>b 返回 +1。
 */
static inline int CompareKey(const Key16 &a, const Key16 &b)
{
    if (a.hi < b.hi)
        return -1;
    if (a.hi > b.hi)
        return +1;
    if (a.lo < b.lo)
        return -1;
    if (a.lo > b.lo)
        return +1;
    return 0;
}

/**
 * @brief 将 Key16 编码为 16 字节大端序字节串（可直接用于字典序比较）。
 */
static inline std::array<uint8_t, 16> EncodeKeyBigEndianBytes(const Key16 &k)
{
    std::array<uint8_t, 16> out{};
    k.ToBigEndianBytes(out.data());
    return out;
}

/**
 * @brief 以“大端字节序字典序”比较两个 Key16。
 *
 * 语义上等价于 CompareKey（数值序），但实现上通过 16 字节 memcmp 体现“字典序”。
 */
static inline int CompareKeyByBigEndianLexicographic(const Key16 &a, const Key16 &b)
{
    const auto abe = EncodeKeyBigEndianBytes(a);
    const auto bbe = EncodeKeyBigEndianBytes(b);
    const int r = std::memcmp(abe.data(), bbe.data(), 16);
    if (r < 0)
        return -1;
    if (r > 0)
        return +1;
    return 0;
}

/// 索引层使用的 key 类型。
///
/// 当前默认仍为 8 字节（uint64_t），以便分阶段迁移。
/// 当后续开始做“全链路 16B key”改造时，可通过编译宏切换到 Key16。
#if defined(FLOWKV_KEY16)
using KeyType = Key16;
#else
using KeyType = uint64_t;
#endif
// Inline implementation of Slice::ToKey16() after Key16 is complete
inline Key16 Slice::ToKey16() const
{
    return Key16::FromBigEndianBytes(data_);
}
/**
 * @brief KeyType 三路比较。
 * @return a<b 返回 -1；相等返回 0；a>b 返回 +1。
 */
static inline int CompareKeyType(const KeyType &a, const KeyType &b)
{
#if defined(FLOWKV_KEY16)
    return CompareKey(a, b);
#else
    const uint64_t aa = __builtin_bswap64(a);
    const uint64_t bb = __builtin_bswap64(b);
    if (aa < bb)
        return -1;
    if (aa > bb)
        return +1;
    return 0;
#endif
}

static inline bool KeyTypeLess(const KeyType &a, const KeyType &b)
{
    return CompareKeyType(a, b) < 0;
}

static inline bool KeyTypeLessEq(const KeyType &a, const KeyType &b)
{
    return CompareKeyType(a, b) <= 0;
}

static inline bool KeyTypeGreater(const KeyType &a, const KeyType &b)
{
    return CompareKeyType(a, b) > 0;
}
/// 索引层使用的 value 类型（通常为编码后的指针/地址）。
using ValueType = uint64_t;
/// 索引查找的无效 value 哨兵。
static constexpr ValueType INVALID_VALUE = 0;
/// 持久化查询路径使用的固定 16B value。
struct FixedValue16
{
    uint64_t lo = 0;
    uint64_t hi = 0;

    const char *data() const { return reinterpret_cast<const char *>(this); }
    char *data() { return reinterpret_cast<char *>(this); }

    static FixedValue16 Tombstone()
    {
        return FixedValue16{INVALID_PTR, INVALID_PTR};
    }

    bool IsTombstone() const
    {
        return lo == INVALID_PTR && hi == INVALID_PTR;
    }
};
static_assert(sizeof(FixedValue16) == 16, "FixedValue16 must be 16 bytes");

static inline FixedValue16 FixedValue16FromSlice(const Slice &value)
{
    FixedValue16 out{};
    const size_t copy_sz = std::min<size_t>(sizeof(FixedValue16), value.size());
    if (copy_sz > 0)
    {
        std::memcpy(&out, value.data(), copy_sz);
    }
    return out;
}

static inline bool IsDeletedFixedValue16Bytes(const char *value_bytes)
{
    if (value_bytes == nullptr)
    {
        return false;
    }
    FixedValue16 value{};
    std::memcpy(&value, value_bytes, sizeof(value));
    return value.IsTombstone();
}
/// 分配器/存储层使用的 segment 大小。
static constexpr uint64_t SEGMENT_SIZE = 4ul << 20;

#ifndef KV_SEPARATE
/**
 * @brief 压缩的 value 指针编码。
 *
 * 将 valid、对齐后的指针/偏移字段以及 LSN 字段压缩到一个 64-bit 字中。
 * 有效地址空间假设 64B 对齐（低 6 位被丢弃）。
 */
union ValuePtr
{
    struct detail
    {
        uint64_t valid : 1; ///< 该编码指针是否有效。
        uint64_t ptr : 33;  ///< 编码后的指针/偏移（64B 对齐；见注释）。
        uint64_t lsn : 30;  ///< 与 value 相关联的 LSN/版本位。
    } detail_;
    uint64_t data_; ///< 原始 64-bit 压缩表示。
};
#else
/**
 * @brief 压缩的 value 指针编码（KV_SEPARATE 变体）。
 *
 * 与 @ref ValuePtr 类似，但采用不同位分配以支持更大地址空间。
 */
union ValuePtr
{
    struct detail
    {
        uint64_t valid : 1; ///< 该编码指针是否有效。
        uint64_t ptr : 34;  ///< 编码后的指针/偏移（64B 对齐；见注释）。
        uint64_t lsn : 29;  ///< 与 value 相关联的 LSN/版本位。
    } detail_;
    uint64_t data_; ///< 原始 64-bit 压缩表示。
};
#endif

/**
 * @brief 索引写入/更新操作的辅助对象。
 *
 * 用于向索引更新传入新 value，并可选返回旧 value。
 */
struct ValueHelper
{
    ValueType new_val = INVALID_VALUE; ///< 待插入/更新的新 value。
    ValueType old_val = INVALID_VALUE; ///< 旧 value（可能由索引实现填充）。
    // char *index_entry = nullptr;
    bool valid = false; ///< 输出字段是否有效（由实现决定）。

    /**
     * @brief 使用新 value 构造辅助对象。
     * @param _new_val 编码后的新 value。
     */
    ValueHelper(ValueType _new_val) : new_val(_new_val) {}
};

/**
 * @brief 引擎使用的抽象索引接口。
 *
 * 具体实现提供点查、更新，以及可选的范围扫描能力。
 */
class Index
{
public:
    virtual ~Index(){};

    /**
        * @brief 初始化该索引的线程本地状态。
        * @param thread_id 逻辑线程 id。
     */
    virtual void ThreadInit(int thread_id) = 0;

    /**
        * @brief 获取 key 对应的编码 value。
        * @param key 待查找的 key。
        * @return 编码 value（未找到时通常返回 @ref INVALID_VALUE；精确约定由实现决定）。
     */
    virtual ValueType Get(const KeyType key) = 0;

    /**
        * @brief 插入或更新一个 key。
        * @param key 待插入/更新的 key。
        * @param le_helper 输入/输出辅助对象，包含 new_val 以及可选输出字段。
     */
    virtual void Put(const KeyType key, ValueHelper &le_helper) = 0;

    /**
        * @brief 带额外校验语义的插入或更新。
        * @param key 待插入/更新的 key。
        * @param le_helper 输入/输出辅助对象，包含 new_val 以及可选输出字段。
     */
    virtual void PutValidate(const KeyType key, ValueHelper &le_helper) = 0;

    /**
        * @brief 删除一个 key。
        * @param key 待删除的 key。
     */
    virtual void Delete(const KeyType key) = 0;

    /**
     * @brief 从起始 key 向前扫描。
     * @param key 起始 key。
     * @param cnt 最大返回数量。
     * @param vec 输出的编码 value 向量。
     *
     * 默认实现会终止进程，因为该基类不支持扫描。
     */
    virtual void Scan(const KeyType key, int cnt, std::vector<ValueType> &vec)
    {
        // 约定：Scan(key, cnt, ...) 的语义应等价于“lower_bound(key) 起扫”，即返回
        // 满足 entry_key >= key 的前 cnt 条记录（按 key 升序）。
        // Version 层会基于该契约做 L0/L1 表定位；若实现不满足该语义，查找结果会错误。
        ERROR_EXIT("not supported in this class");
    }

    /**
     * @brief 从起始 key 向前扫描，同时返回 key 与 value。
     * @param key 起始 key。
     * @param cnt 最大返回数量。
     * @param kvec 输出的 key 向量（KeyType 可能是 uint64_t 或 Key16）。
     * @param vvec 输出的编码 value 向量。
     *
     * 默认实现会终止进程，因为该基类不支持扫描。
     */
    virtual void Scan2(const KeyType key, int cnt, std::vector<KeyType> &kvec, std::vector<ValueType> &vvec)
    {
        // 约定：Scan2(key, cnt, ...) 与 Scan(key, cnt, ...) 的起扫位置与顺序一致（lower_bound 起扫，升序），
        // 但同时返回 key 与 value。
        ERROR_EXIT("not supported in this class");
    }

    /**
     * @brief 按 key 范围扫描。
     * @param start 范围起始 key。
     * @param end 范围结束 key。
     * @param kvec 输出的 key 向量（KeyType 可能是 uint64_t 或 Key16）。
     * @param vvec 输出的编码 value 向量。
     *
     * 默认实现会终止进程，因为该基类不支持扫描。
     */
    virtual void ScanByRange(const KeyType start, const KeyType end, std::vector<KeyType> &kvec, std::vector<ValueType> &vvec)
    {
        // 约定：ScanByRange(start, end, ...) 应返回所有满足 start <= entry_key <= end 的记录（按 key 升序）。
        ERROR_EXIT("not supported in this class");
    }

    /**
     * @brief 按序遍历所有 entry，对每条调用 callback。
     * @param callback 回调函数，参数为 (key, encoded_value)，返回 false 终止遍历。
     *
     * 用于 flush 路径，避免将全部 entry 复制到临时 vector。
     */
    virtual void ForEachEntry(std::function<bool(KeyType, ValueType)> callback)
    {
        ERROR_EXIT("not supported in this class");
    }

    /**
     * @brief 按 key 范围遍历 entry，对每条调用 callback。
     * @param start 范围起始 key。
     * @param end 范围结束 key。
     * @param callback 回调函数，参数为 (key, encoded_value)，返回 false 终止遍历。
     */
    virtual void ForEachEntryInRange(const KeyType start, const KeyType end,
                                     std::function<bool(KeyType, ValueType)> callback)
    {
        ERROR_EXIT("not supported in this class");
    }
};

/**
 * @brief 将文件位置（fd + offset）压缩编码到一个 64-bit 值。
 *
 * 在 @ref data() 中会将最高位设为 1，用于与其他编码类型区分。
 */
struct FilePtr
{
#define mask63 ((1UL << 63) - 1)
#define mask32 ((1UL << 32) - 1)
    int fd;     ///< 文件描述符。
    int offset; ///< 文件内偏移。

    /**
     * @brief 将当前 (fd, offset) 打包为一个 64-bit 值。
     * @return 打包后的表示。
     */
    uint64_t data()
    {
        return (1UL << 63) | ((uint64_t)(fd) << 32) | offset;
    }

    /**
     * @brief 由文件描述符与偏移构造。
     * @param _fd 文件描述符。
     * @param _offset 文件内偏移。
     */
    FilePtr(int _fd, int _offset)
    {
        fd = _fd;
        offset = _fd;
    }

    /**
     * @brief 通过解析打包值构造。
     * @param data 由 @ref data() 产生的打包表示。
     */
    FilePtr(uint64_t data)
    {
        fd = (mask63 & data) >> 32;
        offset = (mask32 & data);
    }

    /**
     * @brief 返回一个无效的文件指针。
     */
    static FilePtr InvalidPtr()
    {
        return FilePtr{-1, -1};
    }

    /**
     * @brief 判断该文件指针是否有效。
     */
    bool Valid()
    {
        return fd >= 0 && offset > 0;
    }

    /**
     * @brief 相等比较。
     */
    bool operator==(FilePtr b)
    {
        return fd == b.fd && offset == b.offset;
    }
};

/**
 * @brief 范围分区边界信息。
 */
struct PartitionInfo
{
#if defined(FLOWKV_KEY16)
    KeyType min_key{MAX_UINT64, MAX_UINT64}; ///< 分区的最小 key 边界。
    KeyType max_key{0, 0};                  ///< 分区的最大 key 边界。
#else
    KeyType min_key = MAX_UINT64; ///< 分区的最小 key 边界。
    KeyType max_key = 0;          ///< 分区的最大 key 边界。
#endif
};
