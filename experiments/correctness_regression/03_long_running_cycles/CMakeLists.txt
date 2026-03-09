add_executable(l1_long_running_cycle_regression
    ${PROJECT_SOURCE_DIR}/experiments/correctness_regression/03_long_running_cycles/l1_long_running_cycle_regression.cpp)

target_link_libraries(l1_long_running_cycle_regression flowkv)

add_test(NAME l1_long_running_cycle_regression
    COMMAND /bin/bash -lc "./l1_long_running_cycle_regression")
set_tests_properties(l1_long_running_cycle_regression PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/experiments/correctness_regression/03_long_running_cycles)
