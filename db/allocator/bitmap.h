#include "util/util.h"
#include "db_common.h"
#include <sys/param.h>
#include <cstring>
#include <mutex>

// TODO: use atmomic bitmap and optimistic lock
// TODO: persist and recover bitmap
class BitMap
{
public:
    size_t total_bit_num_;
    std::atomic_uint64_t tail_bit_;
    char *bitmap_=nullptr;
    char *history_bitmap_=nullptr;
    int fd;
    off_t off;
    std::vector<size_t> freed_bits_;
    SpinLock lock_;
    bool use_free_list_ = false;
    bool debug_ = false;

public:
    BitMap(size_t total_bits, bool debug = false) : debug_(debug)
    {
        total_bit_num_ = total_bits;
        bitmap_ = static_cast<char*>(aligned_alloc(4096, SizeInByte()));
        memset(bitmap_, 0, SizeInByte());
        history_bitmap_ = static_cast<char*>(aligned_alloc(4096, SizeInByte()));
        memset(history_bitmap_, 0, SizeInByte());
        tail_bit_ = 0;
        freed_bits_.reserve(total_bits);
    }
    ~BitMap()
    {
        // std::vector<uint64_t>().swap(freed_bits_);
        // free(bitmap_);
        // free(history_bitmap_);
    }
    void Setoff(off_t off_) { 
        off = off_; 
    }
    void Recover()
    {
        freed_bits_.clear();
        ssize_t bytes_read = pread(fd, bitmap_, SizeInByte(), off);
        if(bytes_read!=SizeInByte())
            std::cout<<"recover wrong"<<std::endl; 
        memcpy(history_bitmap_, bitmap_, SizeInByte());
        size_t tail = MAX_UINT64;
        bool tail_flag = true;
        for (int i = roundup(total_bit_num_, 8) / 8 - 1; i >= 0; i--)
        {
            if (bitmap_[i] != 0)
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    uint8_t mask = 1 << pos;
                    if ((!(mask & bitmap_[i])) && (i * 8 + pos < total_bit_num_))
                    {
                        freed_bits_.push_back(i * 8 + pos);
                    }
                }

                tail_flag = false;
            }
            else if (tail_flag)
            {
                tail = i;
            }
            else
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    freed_bits_.push_back(i * 8 + pos);
                }
            }
        }
        tail_bit_ = tail * 8;
    }
    void RecoverFrom(off_t off_)
    {
        off_t temp = off;
        off = off_;
        Recover();
        off = temp;
    }
    inline size_t SizeInByte()
    {
        return roundup(total_bit_num_, 4 * 1024 * 8) / 8;
    }
    void PersistToSSD()
    {
        ssize_t bytes_write = pwrite(fd, bitmap_, SizeInByte(), off);
        if(bytes_write!=SizeInByte())
            std::cout<<"PersistToSSD wrong"<<std::endl; 

    }
    void PersistToSSDOnlyAlloc()
    {
        char *temp_arr = static_cast<char*>(aligned_alloc(4096, SizeInByte()));
        ssize_t bytes_read = pread(fd, temp_arr, SizeInByte(), off);
        if(bytes_read!=SizeInByte())
            std::cout<<"PersistToSSDOnlyAlloc wrong"<<std::endl; 
        for (size_t i = 0; i < SizeInByte(); i++)
        {
            temp_arr[i] = (bitmap_[i] ^ history_bitmap_[i]) | temp_arr[i];
        }
        memcpy(bitmap_, temp_arr, SizeInByte());
        ssize_t bytes_write = pwrite(fd, temp_arr, SizeInByte(), off);
        if(bytes_write!=SizeInByte())
            std::cout<<"PersistToSSDOnlyAlloc wrong"<<std::endl; 
        free(temp_arr);
    }
    void PersistToSSDOnlyFree()
    {
        char *temp_arr = static_cast<char*>(aligned_alloc(4096, SizeInByte()));
        ssize_t bytes_read = pread(fd, temp_arr, SizeInByte(), off);
        if(bytes_read!=SizeInByte())
            std::cout<<"PersistToSSDOnlyFree wrong"<<std::endl; 
        for (size_t i = 0; i < SizeInByte(); i++)
        {
            temp_arr[i] = ~(bitmap_[i] ^ history_bitmap_[i]) & temp_arr[i];
        }
        ssize_t bytes_write = pwrite(fd, temp_arr, SizeInByte(), off);
        if(bytes_write!=SizeInByte())
            std::cout<<"PersistToSSDOnlyFree wrong"<<std::endl; 
        free(temp_arr);
    }
    void CopyTo(char *dst)
    {
        memcpy(dst, bitmap_, SizeInByte());
    }
    bool IsFull()
    {
        if (freed_bits_.size())
        {
            return false;
        }

        if (tail_bit_ < total_bit_num_)
        {
            return false;
        }

        return true;
    }
    size_t AllocateOne()
    {
        size_t position;
        std::lock_guard<SpinLock> lock(lock_);
        while (1)
        {
            // bool find_free_bit;
            if (freed_bits_.size())
            {
                position = freed_bits_.back();
                freed_bits_.pop_back();
                // find_free_bit = true;
                if (position >= total_bit_num_)
                {
                    ERROR_EXIT("freebits inlegal %lu", position);
                }
            }
            else
            {
                position = tail_bit_.fetch_add(1);
                // find_free_bit = false;
            }
            if (position >= total_bit_num_)
            {
                return ERROR_CODE;
            }
            if (allocate(position))
            {
                return position;
            }
        }
    }
    bool AllocatePos(size_t pos)
    {
        std::lock_guard<SpinLock> lock(lock_);
        if (allocate(pos))
        {
            if(tail_bit_<pos){
                tail_bit_.store(pos);
            }
            return true;
        }
        return false;
    }
    size_t AllocateMany(size_t num)
    {
        size_t position;
        std::lock_guard<SpinLock> lock(lock_);
        while (1)
        {

            position = tail_bit_.fetch_add(num);

            if (position >= total_bit_num_)
            {
                return ERROR_CODE;
            }
            if (allocate(position))
            {
                return position;
            }
        }
    }
    bool Free(size_t position)
    {
        assert(position < total_bit_num_);
        std::lock_guard<SpinLock> lock(lock_);
        uint8_t byte = get_byte(position / 8);
        uint8_t bit_mask = 0x1 << (position % 8);
        if (!(byte & bit_mask))
            return false;
        set_byte(position / 8, byte & (~bit_mask));
        freed_bits_.push_back(position);
        return true;
    }
    bool Exist(size_t position)
    {
        std::lock_guard<SpinLock> lock(lock_);
        uint8_t byte = get_byte(position / 8);
        uint8_t bit_mask = 0x1 << (position % 8);
        if (byte & bit_mask)
            return true;
        return false;
    }
    /*not thread safe*/
    bool GetUsedBits(std::vector<uint64_t> &list)
    {
        if (!list.empty())
            return false;
        for (int i = 0; i < roundup(tail_bit_,8)/8; i++)
        {
            if (bitmap_[i] != 0)
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    uint8_t mask = 1 << pos;
                    if ((mask & bitmap_[i]) && (i * 8 + pos < total_bit_num_))
                    {
                        list.push_back(i * 8 + pos);
                    }
                }
            }
        }
        return true;
    }
    size_t GetUsedBitsNum()
    {
        size_t count=0;
        for (int i = 0; i < roundup(tail_bit_,8)/8; i++)
        {
            if (bitmap_[i] != 0)
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    uint8_t mask = 1 << pos;
                    if ((mask & bitmap_[i]) && (i * 8 + pos < total_bit_num_))
                    {
                        count++;
                    }
                }
            }
        }
        return count;
    }

private:
    inline size_t allocate(size_t position)
    {
        assert(position < total_bit_num_);
        uint8_t byte = get_byte(position / 8);
        uint8_t bit_mask = 0x1 << (position % 8);
        if (byte & bit_mask)
            return false;
        set_byte(position / 8, byte | bit_mask);
        return true;
    }
    inline uint8_t get_byte(size_t i)
    {
        assert(i < roundup(total_bit_num_, 8) / 8);
        return bitmap_[i];
    }
    inline void set_byte(size_t i, uint8_t byte)
    {
        assert(i < roundup(total_bit_num_, 8) / 8);
        bitmap_[i] = byte;
    }

	inline size_t count_used_bits(){
		size_t count=0;
		for(size_t i=0;i+8<roundup(tail_bit_,8);i+=8){
			size_t n = *(bitmap_+i);
			count+=__builtin_popcountll(n);
		}
		return count;
	}

    DISALLOW_COPY_AND_ASSIGN(BitMap);
};