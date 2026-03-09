#include <cstdio>
#include <iostream>
#include <unistd.h>
#include <string>
#include <sstream>
#include <array>
#include <regex>
#include <filesystem>
#include <gflags/gflags.h>

#include "db.h"
#include "db/compaction/version.h"
#include "util/stopwatch.hpp"
#include "util/kgen.h"

DEFINE_uint64(num, 100000000, "Total number of data");
DEFINE_uint64(num_ops, 10000000, "Number of operations for each benchmark");
DEFINE_string(benchmarks, "read", "write: random update, read: random get");
DEFINE_uint64(threads, 16, "Number of user threads during loading and benchmarking");
DEFINE_string(pool_path, "/dev/nvme1n1", "Directory of target pmem");
DEFINE_uint64(pool_size_GB, 500, "Total size of pmem pool");
DEFINE_bool(recover, false, "Recover an existing db instead of recreating a new one");
DEFINE_bool(skip_load, false, "Not load data");
DEFINE_string(load_type, "C", "Choose load type from A to F");
DEFINE_bool(use_direct_io, false, "Use O_DIRECT to bypass page cache");
DEFINE_uint64(duration, 0, "Test duration in seconds (0 = use num_ops instead)");
#define scansize 100

// Global stop flag for timed benchmarks
std::atomic<bool> g_stop_flag{false};
std::atomic<uint64_t> g_total_ops{0};

void print_dram_consuption()
{
    auto pid = getpid();
    std::array<char, 128> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(
        popen(("cat /proc/" + std::to_string(pid) + "/status").c_str(), "r"),
        pclose);
    if (!pipe)
    {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    {
        std::string result = buffer.data();
        if (result.find("VmRSS") != std::string::npos)
        {
            // std::cout << result << std::endl;
            std::string mem_ocp = std::regex_replace(
                result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
            printf("DRAM consumption: %.3f GB.\n", stof(mem_ocp) / 1024 / 1024);
            break;
        }
    }
}

char value[1024]={};

#if defined(FLOWKV_KEY16)
static inline Slice MakeKeySlice(const Key16 &key, uint8_t *buf)
{
    key.ToBigEndianBytes(buf);
    return Slice(reinterpret_cast<const char *>(buf), 16);
}
static inline Key16 MakeKeyFromHash(uint64_t h)
{
    return Key16{0, h};
}
#else
static inline Slice MakeKeySlice(uint64_t key, uint64_t *buf)
{
    *buf = __builtin_bswap64(key);
    return Slice(reinterpret_cast<const char *>(buf), 8);
}
static inline uint64_t MakeKeyFromHash(uint64_t h)
{
    return h;
}
#endif
void put_thread(MYDB *db, size_t start, size_t count)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient>
        c = db->GetClient();
    for (size_t i = start; i < start + count; i++)
    {
        size_t key = utils::multiplicative_hash<uint64_t>(i + 1);
        char value[1024];
        //memcpy(value, &key, 8);
        Slice v(value, VALUESIZE);
#if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
#else
        Slice k = MakeKeySlice(key, &keybuf);
#endif
        auto success = c->Put(k, v);
        if (!success)
        {
            ERROR_EXIT("insert after flush error");
        }
        if ((i != start) && ((i - start) % 5000000 == 0))
        {
            printf("thread %d, %lu operations finished\n", c->thread_id_, i - start);
        }
    }
    c.reset();
}
void get_thread_random(MYDB *db, size_t start, size_t count)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    c->Persist_Log();
    char vbuf[1024];
    Slice valueout(vbuf);
    
    // Seed random generator with thread-specific value
    srand(time(NULL) + start);
    
    for (size_t i = start; i < start + count; i++)
    {
        // Random key selection within the valid range
        int t = rand() % count + 1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
    #if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
    #else
        Slice k = MakeKeySlice(key, &keybuf);
    #endif
        auto success = c->Get(k, valueout);
    }
    c.reset();
}

// Timed random read: runs until g_stop_flag is set, samples from [1, total_keys]
void get_thread_random_timed(MYDB *db, size_t total_keys, int thread_id)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    c->Persist_Log();
    char vbuf[1024];
    Slice valueout(vbuf);
    
    // Thread-local random seed
    unsigned int seed = time(NULL) + thread_id * 1000;
    uint64_t local_ops = 0;
    
    while (!g_stop_flag.load(std::memory_order_relaxed))
    {
        // Random key selection from [1, total_keys]
        size_t t = (rand_r(&seed) % total_keys) + 1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
    #if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
    #else
        Slice k = MakeKeySlice(key, &keybuf);
    #endif
        c->Get(k, valueout);
        local_ops++;
    }
    
    g_total_ops.fetch_add(local_ops, std::memory_order_relaxed);
    c.reset();
}

void get_thread(MYDB *db, size_t start, size_t count)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    c->Persist_Log();
    char vbuf[1024];
    Slice valueout(vbuf);
    for (size_t i = start; i < start + count; i++)
    {
        //随机在start+1到start+count之间取一个数
        //int t=rand()%(count)+start+1;
        int t=i+1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
        //size_t key = i+1;
    #if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
    #else
        Slice k = MakeKeySlice(key, &keybuf);
    #endif
        auto success = c->Get(k, valueout);
        // if(!success)
        // {
        //     std::cout<<"get error"<<std::endl;
        // }
        // else if(memcmp(valueout.data(),(char *)&key,8))
        // {
        //     std::cout<<"get wrong"<<std::endl<<std::endl;
        //     std::cout<<"key: "<<key<<std::endl;
        //     std::cout<<"value: "<<*(size_t *)(valueout.data())<<std::endl;
        //     //std::cout<<"valuesize: "<<valueout.size()<<std::endl;
        // }
    }
    c.reset();
}

void put_get_thread(MYDB *db, size_t start, size_t count, double put_ratio)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    char vbuf[1024];
    Slice valueout(vbuf);
    for (size_t i = start; i < start + count; i++)
    {
        //int t=rand()%(count)+start+1;
        int t=i+1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
    #if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
    #else
        Slice k = MakeKeySlice(key, &keybuf);
    #endif
        //用生成随机数方式满足比例
        if (rand() % 100 < put_ratio * 100)
        {
            char value[1024];
            Slice v(value, VALUESIZE);
            auto success = c->Put(k, v);
            if (!success)
            {
                ERROR_EXIT("insert after flush error");
            }
        }
        else
        {
            auto success = c->Get(k, valueout);
        }
        if ((i!= start) && ((i - start) % 5000000 == 0))
        {
            printf("thread %d, %lu operations finished\n", c->thread_id_, i - start);
        }
    }
    c.reset();
}

void put_get_thread1(MYDB *db, size_t start, size_t count, double put_ratio)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    char vbuf[1024];
    Slice valueout(vbuf);
    for (size_t i = start; i < start + count; i++)
    {
        //int t=rand()%(count)+start+1;
        int t=i+1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
    #if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
    #else
        Slice k = MakeKeySlice(key, &keybuf);
    #endif
        //用生成随机数方式满足比例
        if (rand() % 100 < put_ratio * 100)
        {
            char value[1024];
            Slice v(value, VALUESIZE);
            auto success = c->Put(k, v);
            if (!success)
            {
                ERROR_EXIT("insert after flush error");
            }
        }
        else
        {
            auto success = c->Get(k, valueout);
        }
        if ((i!= start) && ((i - start) % 5000000 == 0))
        {
            printf("thread %d, %lu operations finished\n", c->thread_id_, i - start);
        }
    }
    c.reset();
}

void scan_and_put_thread(MYDB *db, size_t start, size_t count)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    char vbuf[1024];
    Slice valueout(vbuf);
    for (size_t i = start; i < start + count; i++)
    {
        int t=i+1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
#if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
#else
        Slice k = MakeKeySlice(key, &keybuf);
#endif
        if (rand() % 100 < 95)
        {
            std::vector<KeyType> keyout;
            auto success = c->Scan(k, rand() % 100 + 1, keyout);
        }
        else
        {
            char value[1024];
            Slice v(value, VALUESIZE);
            auto success = c->Put(k, v);
        }
    }
    c.reset();
}
void rmw_and_get_thread(MYDB *db, size_t start, size_t count)
{
#if defined(FLOWKV_KEY16)
    uint8_t keybuf[16];
#else
    uint64_t keybuf = 0;
#endif
    std::unique_ptr<MYDBClient> c = db->GetClient();
    char vbuf[1024];
    Slice valueout(vbuf);
    for (size_t i = start; i < start + count; i++)
    {
        //int t=rand()%(count)+start+1;
        int t=i+1;
        size_t key = utils::multiplicative_hash<uint64_t>(t);
    #if defined(FLOWKV_KEY16)
        Key16 int_key = MakeKeyFromHash(key);
        Slice k = MakeKeySlice(int_key, keybuf);
    #else
        Slice k = MakeKeySlice(key, &keybuf);
    #endif
        if (rand() % 100 < 50)
        {
            auto success = c->Get(k, valueout);
            if (!success)
            {
                ERROR_EXIT("insert after flush error");
            }
            success = c->Put(k, valueout);
            if (!success)
            {
                ERROR_EXIT("insert after flush error");
            }
        }
        else
        {
            auto success = c->Get(k, valueout);
            if (!success)
            {
                ERROR_EXIT("insert after flush error");
            }
        }
    }
    c.reset();
}


int main(int argc, char **argv)
{
    google::SetUsageMessage("FlowKV benchmarks");
    google::ParseCommandLineFlags(&argc, &argv, true);
#ifdef BUFFER_WAL_MEMTABLE
    std::cout << "KV separation is disabled" << std::endl;
#else
#ifdef KV_SEPARATE
    std::cout << "KV separation is enabled" << std::endl;
#endif
#endif
std::cout << " number of thread: " << FLAGS_threads << 
				"\n data volume: "<< FLAGS_num <<
				"\n operation: "  << FLAGS_num_ops <<
				"\n benchmark: " << FLAGS_load_type <<  std::endl;
    if (FLAGS_num == 0)
    {
        std::fprintf(stderr, "Invalid flag 'num=%lu'\n", FLAGS_num);
        std::exit(1);
    }
    // Skip num_ops check when using duration-based testing
    if (FLAGS_duration == 0 && (FLAGS_num_ops == 0 || FLAGS_num_ops > FLAGS_num))
    {
        std::fprintf(stderr, "Invalid flag 'num_ops=%lu' (must be > 0 and <= num=%lu)\n", FLAGS_num_ops, FLAGS_num);
        std::exit(1);
    }

    if (!FLAGS_recover && std::filesystem::exists(FLAGS_pool_path))
    {
        fprintf(stderr, "[Warning] Pool path '%s' already exists but recover=false. It will be overwritten.\n", FLAGS_pool_path.c_str());
    }
    if (FLAGS_recover && !std::filesystem::exists(FLAGS_pool_path))
    {
        fprintf(stderr, "pool path '%s' not exists but recover=true\n", FLAGS_pool_path.c_str());
        std::exit(1);
    }
    if (FLAGS_recover && !FLAGS_skip_load)
    {
        printf("[Warning] Recover is enabled but skip_load is disabled. This will result in full dataset updating\n");
    }
    if (!FLAGS_recover && FLAGS_skip_load)
    {
        printf("[Warning] Recover is disabled but skip_load is enabled. This will result in all reads failing\n");
    }
    // init
    stopwatch_t sw;
    MYDBConfig cfg;
    cfg.pm_pool_path = FLAGS_pool_path;
    cfg.pm_pool_size = FLAGS_pool_size_GB << 30ul;
    cfg.recover = FLAGS_recover;
    cfg.use_direct_io = FLAGS_use_direct_io;

    sw.start();
    MYDB *db = new MYDB(cfg);
    // Enable background compaction to test its correctness
    db->SetCompactionEnabled(true);
    auto elapsed = sw.elapsed<std::chrono::milliseconds>();
    std::cout << "initialize or recover time:" << elapsed << "ms" << std::endl;
#if defined(FLOWKV_KEY16)
    uniform_key_generator_t keygen(FLAGS_num, 16);
#else
    uniform_key_generator_t keygen(FLAGS_num, 8);
#endif

    std::vector<std::thread> tlist;

    // load
    if (!FLAGS_skip_load)
    {
        sw.start();
        for (int i = 0; i < FLAGS_threads; i++)
        {
            tlist.emplace_back(std::thread(put_thread, db, FLAGS_num / FLAGS_threads * i, FLAGS_num / FLAGS_threads));
        }
        for (auto &th : tlist)
        {
            th.join();
        }
        auto us = sw.elapsed<std::chrono::microseconds>();
        std::cout << "***************\ncount=" << FLAGS_num << "thpt=" << FLAGS_num / us << "MOPS, total time:" << us / 1000000 << "s\n*****************" << std::endl;
        tlist.clear();
    }
    print_dram_consuption();
    printf("wait for unfinished flush and compaction....\n");
    db->WaitForFlushAndCompaction();
    // run benckmark
    auto us = sw.elapsed<std::chrono::microseconds>();
    switch (FLAGS_load_type[0])
    {
        case 'A':
            std::cout << "run benchmark A" << std::endl;
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();   
            print_dram_consuption();     
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(put_get_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads, 0.5));
            }
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
            tlist.clear();
            break;
        case 'B':
            std::cout << "run benchmark B" << std::endl;
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
            print_dram_consuption();
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(put_get_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads, 0.05));
            }  
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
            tlist.clear();
            break;
        case 'C':
            std::cout << "run benchmark C" << std::endl;
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
            print_dram_consuption();
            sleep(5);
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(get_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads));
            }
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
            db->PrintCacheStats();
            db->PrintL1IndexStats();
            PrintL1DebugStats();
            tlist.clear();
            break;
        case 'D':
            std::cout << "run benchmark D" << std::endl;
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(put_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads));
            }
            for (auto &th : tlist)
            {
                th.join();
            }
            tlist.clear();
            print_dram_consuption();
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(put_get_thread1, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads, 0.05));
            }  
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
            tlist.clear();
            break;            
        case 'E':
        db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
                std::cout << "run benchmark E" << std::endl;
                print_dram_consuption();
                sw.start();
                for (int i = 0; i <  FLAGS_threads ; i++)
                {
                    tlist.emplace_back(std::thread(scan_and_put_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads ));
                }
                for (auto &th : tlist)
                {
                    th.join();
                }
                us = sw.elapsed<std::chrono::microseconds>();
                std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
                tlist.clear();

            break;
        case 'F':
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
            std::cout << "run benchmark F" << std::endl;
            print_dram_consuption();    
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(rmw_and_get_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads));
            }
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
            tlist.clear();
            break;
        case 'G':
            std::cout << "run benchmark G" << std::endl;
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
            print_dram_consuption();
            sleep(5);
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(get_thread_random, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads));
            }
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
            tlist.clear();
            break;
        case 'H':
            // Timed random read benchmark
            std::cout << "run benchmark H (timed random read)" << std::endl;
            if (FLAGS_duration == 0) {
                std::cerr << "[ERROR] Benchmark H requires --duration=<seconds> to be set" << std::endl;
                delete db;
                return 1;
            }
            std::cout << "  Total keys: " << FLAGS_num << std::endl;
            std::cout << "  Duration: " << FLAGS_duration << " seconds" << std::endl;
            std::cout << "  Threads: " << FLAGS_threads << std::endl;
            
            db->EnableReadOnlyMode();
            db->EnableReadOptimizedMode();
            db->WaitForFlushAndCompaction();
            print_dram_consuption();
            sleep(2);
            
            // Reset counters
            g_stop_flag.store(false);
            g_total_ops.store(0);
            
            sw.start();
            for (int i = 0; i < FLAGS_threads; i++)
            {
                tlist.emplace_back(std::thread(get_thread_random_timed, db, FLAGS_num, i));
            }
            
            // Wait for duration
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_duration));
            g_stop_flag.store(true);
            
            for (auto &th : tlist)
            {
                th.join();
            }
            us = sw.elapsed<std::chrono::microseconds>();
            {
                uint64_t total_ops = g_total_ops.load();
                double seconds = us / 1000000.0;
                double mops = total_ops / us;
                std::cout << "********************" << std::endl;
                std::cout << "Timed Random Read Results:" << std::endl;
                std::cout << "  Total operations: " << total_ops << std::endl;
                std::cout << "  Elapsed time: " << seconds << " s" << std::endl;
                std::cout << "  Throughput: " << mops << " MOPS" << std::endl;
                std::cout << "  Avg latency: " << (us / total_ops) << " us/op" << std::endl;
                db->PrintCacheStats();
                std::cout << "********************" << std::endl;
            }
            tlist.clear();
            break;
        default:
            std::cout << "111" << std::endl;
    }
    delete db;
    return 0;
}