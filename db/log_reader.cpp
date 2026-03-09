#include "log_reader.h"
#include <array>
LogReader::LogReader(SegmentAllocator *allocator) : seg_allocator_(allocator),fd(allocator->Getfd())
{
}

LogReader::~LogReader()
{
}

size_t LogReader::ReadLogForValue(const Slice &key, ValuePtr valueptr, char* output_buffer)
{
    (void)key;
    // Use LOG_ENTRY_SIZE for ptr decoding to match the encoding in Put
    // Current formula assumes continuous 16KB blocks, but each 4MB segment has 16KB header
    off_t offset = valueptr.detail_.ptr/LogNumperBlock*logbuffersize+valueptr.detail_.ptr%LogNumperBlock*LOG_ENTRY_SIZE;
    
    // Safety check
    if (output_buffer == nullptr) {
        std::cout << "ReadLogForValue: null output_buffer" << std::endl;
        return 0;
    }

    std::array<char, sizeof(LogEntry64)> scratch{};
    auto ret = pread(fd, scratch.data(), LOG_ENTRY_SIZE, offset);
    if(ret != LOG_ENTRY_SIZE) {
        std::cout << "read log wrong: ret=" << ret << " expected=" << LOG_ENTRY_SIZE
                  << " offset=" << offset << " ptr=" << valueptr.detail_.ptr << std::endl;
        return 0;
    }

#if defined(FLOWKV_KEY16)
    LogEntry64 *record = reinterpret_cast<LogEntry64 *>(scratch.data());
    size_t value_sz = record->value_sz;
    if (value_sz > sizeof(record->value)) {
        return 0;
    }
    std::memcpy(output_buffer, record->value, value_sz);
    return value_sz;
#else
    LogEntryVar64 *record = reinterpret_cast<LogEntryVar64 *>(scratch.data());
    size_t value_sz = record->value_sz;
    if (value_sz > 64) {
        return 0;
    }
    const size_t total_size = value_sz <= 8 ? sizeof(LogEntry32) : (LOG_HEADER_SIZE + value_sz);
    if (total_size > scratch.size()) {
        return 0;
    }
    if (total_size > LOG_ENTRY_SIZE) {
        ret = pread(fd, scratch.data(), total_size, offset);
        if (ret != static_cast<ssize_t>(total_size)) {
            std::cout << "read log wrong: ret=" << ret << " expected=" << total_size
                      << " offset=" << offset << " ptr=" << valueptr.detail_.ptr << std::endl;
            return 0;
        }
    }
    std::memcpy(output_buffer, record->value, value_sz);
    return value_sz;
#endif
}

int LogReader::ReadLogFromSegment(int segment_id, char *output)
{
    auto seg = seg_allocator_->GetLogSegment(segment_id);
    if (seg == nullptr)
        return 0;
    auto header = seg->GetHeader();
    
    // The first logbuffersize (16KB) is reserved for the header/buffer area
    // Actual log entries start at offset logbuffersize within the segment
    // objects_tail_offset is the total offset from segment start, including header
    if (header.objects_tail_offset <= logbuffersize) {
        // No log entries written yet (only header)
        delete seg;
        return 0;
    }
    
    size_t data_size = header.objects_tail_offset - logbuffersize;
    off_t data_offset = seg->Getoff() + logbuffersize;
    
    auto ret = pread(fd, output, data_size, data_offset);
    delete seg;
    
    if(ret != (ssize_t)data_size) {
        std::cout << "read log wrong: expected " << data_size << " got " << ret << std::endl;
        return 0;
    }
    return data_size / LOG_ENTRY_SIZE;
}
