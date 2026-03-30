#include "datablock_writer.h"
#include <sys/mman.h>
#include <cstring>

DataBlockWriter::DataBlockWriter(SegmentAllocator *allocator) : seg_allocator_(allocator), current_segment_(nullptr), fd(allocator->Getfd())
{
    LOG("DataBlockWriter init");
}

DataBlockWriter::~DataBlockWriter()
{
    if (current_segment_)
    {
        seg_allocator_->CloseSegment(current_segment_);
    }

    PersistCheckpoint();
}

bool DataBlockWriter::AddEntry(Slice key, Slice value)
{
#if defined(FLOWKV_KEY16)
    if (key.size() == 16)
    {
        if (value.size() <= sizeof(FixedValue16))
        {
            auto current_block = &blocks_buf_;
            if (!current_block->valid())
            {
                allocate_block();
            }
            if (current_block->is_full())
            {
                return false;
            }

            // Parse big-endian encoded key bytes to Key16 (host order)
            Key16 k16 = Key16::FromBigEndianBytes(key.data());
            FixedValue16 stored_value = FixedValue16FromSlice(value);
            current_block->add_entry(k16.hi, k16.lo, stored_value);
            LOG("add entry in datablock: size=%d", current_block->size);
			num++;
            return true;
        }
    }
    ERROR_EXIT("not 16+16 byte KV");
#else
    if (key.size() <= 8)
    {
        if (value.size() <= 8)
        {
            auto current_block = &blocks_buf_;
            if (!current_block->valid())
            {
                allocate_block();
            }
            if (current_block->is_full())
            {
                return false;
            }

            current_block->add_entry(*reinterpret_cast<const uint64_t *>(key.data()), *reinterpret_cast<const uint64_t *>(value.data()));
            LOG("add entry in datablock: size=%d,%lu:%lu", current_block->size, key.ToUint64Bswap(), *reinterpret_cast<const uint64_t *>(value.data()));
			num++;
            return true;
        }
    }
    ERROR_EXIT("not 8+8 byte KV");
#endif
    return false;
}


KeyType DataBlockWriter::GetCurrentMinKey()
{
#if defined(FLOWKV_KEY16)
    return Key16{blocks_buf_.data_buf.entries[0].key_hi, blocks_buf_.data_buf.entries[0].key_lo};
#else
    return blocks_buf_.data_buf.entries[0].key;
#endif
}
KeyType DataBlockWriter::GetCurrentMaxKey()
{
#if defined(FLOWKV_KEY16)
    return Key16{blocks_buf_.data_buf.entries[blocks_buf_.size].key_hi, blocks_buf_.data_buf.entries[blocks_buf_.size].key_lo};
#else
    return blocks_buf_.data_buf.entries[blocks_buf_.size].key;
#endif
}
KeyType DataBlockWriter::GetKey(size_t idx)
{
#if defined(FLOWKV_KEY16)
    return Key16{blocks_buf_.data_buf.entries[idx].key_hi, blocks_buf_.data_buf.entries[idx].key_lo};
#else
    return blocks_buf_.data_buf.entries[idx].key;
#endif
}


uint64_t DataBlockWriter::Flush()
{

    if (blocks_buf_.page_addr != -1)
    {
        LOG("flush datablock:size=%d", blocks_buf_.size);
        uint64_t block_addr = blocks_buf_.page_addr;
        if (blocks_buf_.size != 0)
        {
            while (!blocks_buf_.is_full())
            {
                #if defined(FLOWKV_KEY16)
                blocks_buf_.add_entry(INVALID_PTR, INVALID_PTR, FixedValue16::Tombstone());
                #else
                blocks_buf_.add_entry(INVALID_PTR, INVALID_PTR);
                #endif
            }
            LOG("Flush SSD datablock to %lu,offset=%lu,%lu", (uint64_t)blocks_buf_.page_addr, block_addr, sizeof(PDataBlock));
            QueueCurrentBlockWrite(block_addr);
        }
        blocks_buf_.clear();
		num=0;
        return block_addr;
    }
    return INVALID_PTR;
}

void DataBlockWriter::allocate_block()
{
    assert(blocks_buf_.page_addr == -1);
    if (current_segment_ == nullptr)
    {
        current_segment_ = seg_allocator_->AllocSortedSegment(sizeof(PDataBlock));
    }
    off_t addr = current_segment_->AllocatePage();
    LOG("datablock writer alloc page=%lu from segment %lu", addr, current_segment_->segment_id_);
    if (addr == -1) // if current segment is full, alloc new segment
    {
        used_segments_.push_back(current_segment_);
        // seg_allocator_->CloseSegment(current_segment_);
        current_segment_ = seg_allocator_->AllocSortedSegment(sizeof(PDataBlock));
        LOG("retry:datablock writer alloc segment");
        addr = current_segment_->AllocatePage();
        if (addr == -1)
        {
            fflush(stdout);
            assert(addr != -1);
        }
        LOG("retry:datablock writer alloc page=%lu from segment %lu", addr, current_segment_->segment_id_);
    }
    blocks_buf_.clear();
    blocks_buf_.set_page(addr);
}

int DataBlockWriter::PersistCheckpoint()
{
    FlushPendingWrites(true);

    int size = used_segments_.size();
    if (current_segment_)
    {
        seg_allocator_->CloseSegment(current_segment_);
        current_segment_ = nullptr;
        size++;
    }
    for (auto &seg : used_segments_)
    {
        seg_allocator_->CloseSegment(seg);
    }
    used_segments_.clear();
    return size;
}
int DataBlockWriter::Empty() { return num==0; }

void DataBlockWriter::QueueCurrentBlockWrite(off_t block_addr)
{
    PendingBlockWrite pending;
    pending.page_addr = block_addr;
    std::memcpy(&pending.block, &blocks_buf_.data_buf, kBlockBytes);
    pending_writes_.push_back(std::move(pending));
    pending_bytes_ += kBlockBytes;
    if (pending_bytes_ >= kWriteBatchBytes) {
        FlushPendingWrites(false);
    }
}

bool DataBlockWriter::FlushPendingWrites(bool force)
{
    if (pending_writes_.empty()) {
        return true;
    }
    if (!force && pending_bytes_ < kWriteBatchBytes) {
        return true;
    }

    size_t run_start_idx = 0;
    while (run_start_idx < pending_writes_.size()) {
        size_t run_end_idx = run_start_idx + 1;
        while (run_end_idx < pending_writes_.size()) {
            const off_t prev_addr = pending_writes_[run_end_idx - 1].page_addr;
            const off_t expect_next = prev_addr + static_cast<off_t>(kBlockBytes);
            if (pending_writes_[run_end_idx].page_addr != expect_next) {
                break;
            }
            ++run_end_idx;
        }

        const size_t run_blocks = run_end_idx - run_start_idx;
        const size_t run_bytes = run_blocks * kBlockBytes;
        std::vector<char> write_buf(run_bytes);
        for (size_t i = 0; i < run_blocks; ++i) {
            std::memcpy(write_buf.data() + i * kBlockBytes,
                        &pending_writes_[run_start_idx + i].block,
                        kBlockBytes);
        }

        const off_t run_addr = pending_writes_[run_start_idx].page_addr;
        const auto ret = pwrite(fd, write_buf.data(), run_bytes, run_addr);
        if (ret != static_cast<ssize_t>(run_bytes)) {
            std::cout << "write wrong, ret=" << ret << ", expected=" << run_bytes << std::endl;
            return false;
        }

        run_start_idx = run_end_idx;
    }

    pending_writes_.clear();
    pending_bytes_ = 0;
    return true;
}
