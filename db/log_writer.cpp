#include "log_writer.h"
#include "allocator/segment_allocator.h"
#include <array>
LogWriter::LogWriter(SegmentAllocator *allocator, int log_segment_group_id) : allocator_(allocator), log_segment_group_id_(log_segment_group_id)
{
    current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
}

LogWriter::~LogWriter()
{
    if (current_segment_)
        allocator_->CloseSegment(current_segment_);
}

// return log ptr
uint64_t LogWriter::WriteLogPut(Slice key, Slice value, LSN lsnumber)
{
    uint64_t ret = 0;
    if(key.size()==0)
    {
        INFO("key size is 0");
        INFO("key: %s", key.ToString().c_str());
        int t;
        std::cin>>t;
    }
    // Support both 8B and 16B keys through conditional compilation
#if defined(FLOWKV_KEY16)
    Key16 k16 = Key16::FromBigEndianBytes(key.data());
    LogEntry64 log = {
        .valid = 1,
        .lsn = (uint32_t)lsnumber.lsn,
        .key_sz = (uint16_t)key.size(),
        .value_sz = (uint16_t)value.size(),
        .key_hi = k16.hi,
        .key_lo = k16.lo,
    };
    if (value.size() > sizeof(log.value))
    {
        ERROR_EXIT("FLOWKV_KEY16 WAL value too large for fixed 64B log entry");
    }
    if (value.size() > 0)
    {
        std::memcpy(log.value, value.data(), value.size());
    }
    ret = append_log<LogEntry64>(&log);
    return ret;
#else
    if (value.size() <= 8)
    {
        // 8B key mode: use uint64_t directly
        LogEntry32 log = {
            .valid = 1,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .value_sz = (uint16_t)value.size(),
            .key = *(const uint64_t *)key.data(),
            .value_addr = *(const uint64_t *)value.data()}; // value_addr is the union of value, we use it as a uint64_t
        LOG("put append log value=%lu", log.value_addr);
        ret = append_log<LogEntry32>(&log);
        return ret;
    }
    else
    {
        LogEntryVar64 *ptr = (LogEntryVar64 *)variable_entry_buffer_;
        // 8B key mode
        (*ptr) = {
            .valid = 1,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .value_sz = (uint16_t)value.size(),
            .key = *(const uint64_t *)key.data()};
        memcpy(ptr->value, value.data(), value.size());
        ret = append_log<LogEntryVar64>(ptr, value.size() + 16);
    }
#ifndef KV_SEPARATE
    ERROR_EXIT("KV_SEPARATE is disabled but value > 8byte");
#endif
    return ret;
#endif
}
uint64_t LogWriter::WriteLogDelete(Slice key, LSN lsnumber)
{
    uint64_t ret;
    // Support both 8B and 16B keys through conditional compilation
#if defined(FLOWKV_KEY16)
    Key16 k16 = Key16::FromBigEndianBytes(key.data());
    LogEntry64 log = {
        .valid = 0,
        .lsn = (uint32_t)lsnumber.lsn,
        .key_sz = (uint16_t)key.size(),
        .value_sz = 0,
        .key_hi = k16.hi,
        .key_lo = k16.lo};
    ret = append_log<LogEntry64>(&log);
#else
    // 8B key mode: original logic
    if (key.size() <= 8)
    {
        LogEntry32 log = {
            .valid = 0,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .key = *(const uint64_t *)key.data()};
        ret = append_log<LogEntry32>(&log);
    }
    else
    {
        LogEntryVar64 log = {
            .valid = 0,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .key = *(const uint64_t *)key.data()};
        ret = append_log<LogEntryVar64>(&log);
    }
#endif
    assert(ret);
    return ret;
}

size_t LogWriter::ReadBufferedValue(const Slice &key, ValuePtr ptr, char* output_buffer)
{
    (void)key;
    if (current_segment_ == nullptr || output_buffer == nullptr)
    {
        return 0;
    }

    off_t offset = ptr.detail_.ptr / LogNumperBlock * logbuffersize +
                   ptr.detail_.ptr % LogNumperBlock * LOG_ENTRY_SIZE;
    std::array<char, sizeof(LogEntry64)> scratch{};
    if (!current_segment_->TryReadBuffered(offset, LOG_ENTRY_SIZE, scratch.data()))
    {
        return 0;
    }

#if defined(FLOWKV_KEY16)
    LogEntry64 *record = reinterpret_cast<LogEntry64 *>(scratch.data());
    size_t value_sz = record->value_sz;
    if (value_sz > sizeof(record->value))
    {
        return 0;
    }
    std::memcpy(output_buffer, record->value, value_sz);
    return value_sz;
#else
    LogEntryVar64 *record = reinterpret_cast<LogEntryVar64 *>(scratch.data());
    size_t value_sz = record->value_sz;
    if (value_sz > 64)
    {
        return 0;
    }
    const size_t total_size = value_sz <= 8 ? sizeof(LogEntry32) : (LOG_HEADER_SIZE + value_sz);
    if (total_size > scratch.size()) {
        return 0;
    }
    if (total_size > LOG_ENTRY_SIZE &&
        !current_segment_->TryReadBuffered(offset, total_size, scratch.data()))
    {
        return 0;
    }

    std::memcpy(output_buffer, record->value, value_sz);
    return value_sz;
#endif
}

void LogWriter::SwitchToNewSegment(int id)
{
    if (current_segment_)
        allocator_->CloseSegment(current_segment_);

    log_segment_group_id_ = id;
    current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
}

void LogWriter::PersistNowSegment()
{
    if (current_segment_)
        allocator_->CloseSegment(current_segment_);
    current_segment_ = nullptr;
}

// return the offset from the start of datapool
template <typename T>
uint64_t LogWriter::append_log(T *data)
{
    uint64_t log_offset;
    int segment_offset;
    // init
    if (current_segment_ == nullptr)
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);

    segment_offset = current_segment_->Append((char *)data, sizeof(T));
    if (segment_offset == -1)
    {
        // segment overflow, allocate another for this logging
        allocator_->CloseSegment(current_segment_);
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
        segment_offset = current_segment_->Append((char *)data, sizeof(T));
    }
    assert(segment_offset >= 0);
    return current_segment_->segment_id_ * SEGMENT_SIZE + segment_offset;
}


template <typename T>
uint64_t LogWriter::append_log(T *data, size_t size)
{
    uint64_t log_offset;
    int segment_offset;
    // init
    if (current_segment_ == nullptr)
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);

    segment_offset = current_segment_->Append((char *)data, size);
    if (segment_offset == -1)
    {
        // segment overflow, allocate another for this logging
        allocator_->CloseSegment(current_segment_);
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
        segment_offset = current_segment_->Append((char *)data, size);
    }
    assert(segment_offset >= 0);
    return current_segment_->segment_id_ * SEGMENT_SIZE + segment_offset;
}
