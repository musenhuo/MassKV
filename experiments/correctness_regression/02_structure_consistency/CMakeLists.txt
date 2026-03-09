add_executable(l1_structure_consistency_regression
    ${PROJECT_SOURCE_DIR}/experiments/correctness_regression/02_structure_consistency/l1_structure_consistency_regression.cpp)

target_link_libraries(l1_structure_consistency_regression flowkv)

add_test(NAME l1_structure_consistency_regression
    COMMAND /bin/bash -lc "./l1_structure_consistency_regression")
set_tests_properties(l1_structure_consistency_regression PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/experiments/correctness_regression/02_structure_consistency)
