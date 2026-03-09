#pragma once

#include "log_format.h"
#include "db_common.h"
#include "allocator/segment_allocator.h"

class SegmentAllocator;
class LogReader
{
private:
    SegmentAllocator* seg_allocator_;
    int fd;

public:
    LogReader(SegmentAllocator *allocator);
    ~LogReader();
    /**
     * @brief 读取log中的value到调用方提供的缓冲区
     * @param key 键
     * @param ptr value指针
     * @param output_buffer 输出缓冲区，必须足够大(至少LOG_ENTRY_SIZE字节)
     * @return 读取的value大小
     */
    size_t ReadLogForValue(const Slice &key, ValuePtr ptr, char* output_buffer);
    int ReadLogFromSegment(int segment_id, char* output);
};
