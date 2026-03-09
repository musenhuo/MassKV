find_package(gflags REQUIRED)

add_executable(benchmark ${PROJECT_SOURCE_DIR}/benchmarks/simple_benchmark.cpp)
target_link_libraries(benchmark flowkv gflags)