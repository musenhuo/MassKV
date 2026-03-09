/**
 * @file pst_builder.h
 * @author your name (you@domain.com)
 * @brief Persistent Sorted Table(PST) is minimum write granularity for persistently sorted data and consists of an IndexBlock and several Datablocks
 * @version 0.1
 * @date 2022-08-29
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "datablock_writer.h"
#include "table.h"

class PSTBuilder
{
private:
    DataBlockWriter* data_writer_;
    PSTMeta meta_;

public:
    PSTBuilder(SegmentAllocator *segment_allocator);
    ~PSTBuilder();

    bool AddEntry(Slice key, Slice value);
    PSTMeta Flush();
    void Clear();
    void PersistCheckpoint();
};
