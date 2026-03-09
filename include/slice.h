// Copyright (c) 2011 The LevelMYDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

// -------------------- 中文译文（保留原文，避免丢失来源信息）--------------------
// Slice 是一个简单的结构，包含指向外部存储的一段指针以及长度。
// Slice 的使用者必须确保：在对应的外部存储被释放之后，不再继续使用该 Slice。
//
// 多个线程可以在不做额外同步的情况下并发调用 Slice 的 const 方法。
// 但如果任意线程可能调用非 const 方法，则所有访问同一个 Slice 对象的线程
// 必须由外部进行同步保护。
// ------------------------------------------------------------------------

/**
 * @file slice.h
 * @brief 非拥有型字节序列视图（Slice）。
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>
#include <iostream>

// Forward declaration for Key16 (defined in db_common.h)
struct Key16;

/**
 * @brief 对连续字节范围的轻量、非拥有型视图。
 *
 * Slice 不拥有底层内存；调用方必须保证被引用的存储在 Slice 生命周期内保持有效。
 */
class Slice
{
public:
    /// 创建空 slice。
    Slice() : data_(""), size_(0) {}

    /**
        * @brief 创建一个引用 d[0,n-1] 的 slice。
        * @param d 首字节指针。
        * @param n 字节数。
     */
    Slice(const char *d, size_t n) : data_(d), size_(n) {}

    /**
        * @brief 创建一个引用 std::string 内容的 slice。
        * @param s 将被引用其缓冲区的字符串（不拷贝）。
     */
    Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

    /**
        * @brief 创建一个引用以 NUL 结尾的 C 字符串的 slice。
        * @param s C 字符串。
     */
    Slice(const char *s) : data_(s), size_(strlen(s)) {}

    /**
        * @brief 创建一个视图为内存中 uint64（8 字节）的 slice。
        * @param n 指向 uint64 存储的指针。
     */
    Slice(const uint64_t *n) : data_((const char *)n), size_(8){}

    /**
        * @brief 原地对 uint64 做字节序交换，并返回引用该 8 字节的 slice。
        * @param n 指向会被修改的 uint64 存储。
        * @return 引用交换后 8 字节的 slice。
     */
    static Slice BswapUint64(uint64_t *n)
    {
        *n = __builtin_bswap64(*n);
        return Slice((const char *)n, 8);
    }

    /// 允许拷贝（浅拷贝：仅复制指针与长度）。
    Slice(const Slice &) = default;
    Slice &operator=(const Slice &) = default;

    /** @brief 返回被引用数据的起始指针。 */
    const char *data() const { return data_; }

    /** @brief 返回被引用数据的长度（字节）。 */
    size_t size() const { return size_; }

    /** @brief 当长度为 0 时返回 true。 */
    bool empty() const { return size_ == 0; }

    /**
        * @brief 返回第 n 个字节。
        * @param n 索引。
        * @return 第 n 个字节。
        * @pre n < size()。
     */
    char operator[](size_t n) const
    {
        assert(n < size());
        return data_[n];
    }

    /** @brief 将该 slice 重置为空。 */
    void clear()
    {
        data_ = "";
        size_ = 0;
    }

    /**
        * @brief 从该 slice 中丢弃前缀 n 个字节。
        * @param n 需要移除的字节数。
        * @pre n <= size()。
     */
    void remove_prefix(size_t n)
    {
        assert(n <= size());
        data_ += n;
        size_ -= n;
    }

    /** @brief 返回被引用数据的 std::string 拷贝。 */
    std::string ToString() const { return std::string(data_, size_); }

    /**
        * @brief 三路字典序比较。
        * @param b 另一个 slice。
        * @return 若 *this < b 返回 <0；相等返回 0；若 *this > b 返回 >0。
     */
    int compare(const Slice &b) const;

    /**
        * @brief 判断 @p x 是否为该 slice 的前缀。
        * @param x 待判断的前缀。
     */
    bool starts_with(const Slice &x) const
    {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }
    /**
        * @brief 将前 8 字节按主机端序解释为 uint64_t。
        * @return 解析得到的整数。
        * @note 调用方必须保证 size() >= 8。
     */
    uint64_t ToUint64() const
    {
        return *reinterpret_cast<const uint64_t *>(data_);
    }
    /**
        * @brief 将前 8 字节解释为 uint64_t 并进行字节序交换。
        * @return 解析并交换后的整数。
        * @note 调用方必须保证 size() >= 8。
     */
    uint64_t ToUint64Bswap() const
    {
        return __builtin_bswap64(*reinterpret_cast<const uint64_t *>(data_));
    }

    /**
     * @brief 将前 16 字节解析为 Key16（从大端字节序）。
     * @return 解析得到的 Key16。
     * @note 调用方必须保证 size() >= 16。
     * @note 假定输入的 16 字节是大端序编码（与现有 8B key 的约定保持一致），
     *       在小端机器上会自动 bswap 转换为主机端序后存入 Key16 结构。
     */
    Key16 ToKey16() const;

private:
    const char *data_; ///< 外部拥有的数据指针。
    size_t size_;      ///< 视图长度（字节）。
};

inline bool operator==(const Slice &x, const Slice &y)
{
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice &x, const Slice &y) { return !(x == y); }

inline int Slice::compare(const Slice &b) const
{
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0)
    {
        if (size_ < b.size_)
            r = -1;
        else if (size_ > b.size_)
            r = +1;
    }
    return r;
}
/// uint64_t 最大值。
static const uint64_t MAX_UINT64 = 0xfffffffffffffffful;
/// 有符号 32-bit int 最大值。
static const int MAX_INT32 = 0x7fffffff;
/// 哨兵 slice 值（引用 MAX_UINT64 的 8 字节）。
static const Slice INVALID_SLICE = Slice((const char *)&MAX_UINT64, 8);