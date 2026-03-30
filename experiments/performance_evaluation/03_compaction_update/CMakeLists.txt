add_executable(write_online_benchmark
    write_online_benchmark.cpp
)
target_link_libraries(write_online_benchmark PRIVATE flowkv)

add_executable(reopen_steady_rss_benchmark
    reopen_steady_rss_benchmark.cpp
)
target_link_libraries(reopen_steady_rss_benchmark PRIVATE flowkv)
