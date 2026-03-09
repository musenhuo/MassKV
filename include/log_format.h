/**
 * @file log_format.h
 * @brief 公共日志记录与序列号格式定义。
 */

#pragma once
#include <cstdint>

/**
 * @brief 64-bit 日志序列号。
 *
 * 注意：这是位域布局，其内存表示依赖 ABI/端序。
 * 除非实现显式保证稳定性，否则不建议直接对该结构做 memcpy 形式的持久化/传输。
 */
struct LSN
{
    uint64_t epoch : 16;   ///< Epoch 标识。
    uint64_t lsn : 30;     ///< Epoch 内的序列号。
    uint64_t padding : 18; ///< 保留/填充位。
};
/**
 * @brief log entry with fixed key size
 * 32Bytes or 64Bytes
 *
 */
/**
 * @brief 日志记录中存储的逻辑操作码。
 */
enum OpCode : uint16_t
{
    NOP,    ///< 空操作。
    INSERT, ///< 插入/更新。
    DELETE  ///< 删除。
};
/**
 * @brief 描述日志条目布局的记录类型。
 */
enum LogType : uint16_t
{
    ERROR,          ///< 非法/未知类型。
    LogEntryFixK32, ///< 固定 key 布局（名义上的 32 类；精确编码由实现决定）。
    LogEntryFixK64, ///< 固定 key 布局。
    LogEntryFixK128,///< 固定 key 布局。
    LogEntryFixK256,///< 固定 key 布局。
    LogEntryVarK64, ///< 变长 key 布局。
    LogEntryVarK128,///< 变长 key 布局。
    LogEntryVarK256 ///< 变长 key 布局。
};

// TODO: 32Byte entry make 2x space consuption for 16Byte KV. Try to make LogEntry in 20Bytes so each cacheline contain 3 entry, 4/3x space.
// TODO: or make pure log entry only with kv-size(vsize=0 denotes delete) for L1 or L2 with larger capacity.
/**
 * @brief 适用于小 KV 的紧凑日志条目头。
 *
 * 根据 @ref value_sz 以及写入端实现，该条目可能被用作 32B 记录，或扩展为更大的记录。
 * 注意：在 FLOWKV_KEY16 模式下，LogEntry32 实际会变为 40B（增加 8B 用于存储完整 16B key）
 */
struct LogEntry32
{

    /* data */
    uint32_t valid : 1; ///< 该日志条目是否有效。
    uint32_t lsn : 31;  ///< 该条目的序列号（低位）。

    uint16_t key_sz = 0;   ///< key 长度（字节）。
    uint16_t value_sz = 0; ///< value 长度（字节）。
                         // 0~16:32Bytes log entry, 17~48:64Bytes log entry, >48:var entry
#if defined(FLOWKV_KEY16)
    uint64_t key_hi = 0;   ///< key 高 64 位（16B key 模式）。
    uint64_t key_lo = 0;   ///< key 低 64 位（16B key 模式）。
#else
    uint64_t key = 0;      ///< key 负载（8B key 模式）。
#endif
    union
    {
        uint64_t value_addr = 0; ///< value 地址/指针编码（value 外置时）。
        char value[8];           ///< 内联 value 前缀（可能由写入端实现扩展）。
    };
};
// static constexpr size_t size = sizeof(LogEntry32);
/**
 * @brief 固定大小的 64B 日志条目。
 * 注意：在 FLOWKV_KEY16 模式下，value 数组大小会减少 8B 以容纳完整 16B key
 */
struct LogEntry64
{
    /* data */

    uint32_t valid : 1; ///< 该日志条目是否有效。
    uint32_t lsn : 31;  ///< 该条目的序列号（低位）。
    uint16_t key_sz = 0;   ///< key 长度（字节）。
    uint16_t value_sz = 0; ///< value 长度（字节）。
#if defined(FLOWKV_KEY16)
    uint64_t key_hi = 0;   ///< key 高 64 位（16B key 模式）。
    uint64_t key_lo = 0;   ///< key 低 64 位（16B key 模式）。
    char value[40];        ///< 内联 value 负载（16B key 模式下减少 8B）。
#else
    uint64_t key = 0;      ///< key 负载（8B key 模式）。
    char value[48];        ///< 内联 value 负载或 value 指针编码。
#endif
};
// static constexpr size_t size = sizeof(LogEntry64);
/**
 * @brief 带柔性数组尾部的变长日志条目。
 */
struct LogEntryVar64
{
    /* data */

    uint32_t valid : 1; ///< 该日志条目是否有效。
    uint32_t lsn : 31;  ///< 该条目的序列号（低位）。
    uint16_t key_sz;    ///< key 长度（字节）。
    uint16_t value_sz;  ///< value 长度（字节）。
#if defined(FLOWKV_KEY16)
    uint64_t key_hi;    ///< key 高 64 位（16B key 模式）。
    uint64_t key_lo;    ///< key 低 64 位（16B key 模式）。
#else
    uint64_t key;       ///< key 负载头（8B key 模式）。
#endif
    char value[];       ///< 柔性负载尾部（布局/长度由实现决定）。
};

