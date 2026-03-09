add_executable(point_lookup_benchmark
    point_lookup_benchmark.cpp
    ../../common/fast_bulk_l1_builder.cpp
)
target_link_libraries(point_lookup_benchmark PRIVATE flowkv)
