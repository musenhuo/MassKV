add_executable(l1_differential_regression
    ${PROJECT_SOURCE_DIR}/experiments/correctness_regression/04_differential_regression/l1_differential_regression.cpp)

target_link_libraries(l1_differential_regression flowkv)

add_test(NAME l1_differential_regression
    COMMAND /bin/bash -lc "./l1_differential_regression")
set_tests_properties(l1_differential_regression PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/experiments/correctness_regression/04_differential_regression)
