#include "datablock_writer.h"
#include <sys/mman.h>

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
            auto ret = pwrite(fd, &blocks_buf_.data_buf, sizeof(PDataBlock),block_addr);
            if(ret!=sizeof(PDataBlock))
                std::cout<<"write wrong"<<std::endl; 
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
