add_executable(hybrid_l1_test
    ${PROJECT_SOURCE_DIR}/tests/hybrid_l1_test.cpp)

target_link_libraries(hybrid_l1_test hybrid_l1)

add_test(NAME hybrid_l1_test COMMAND hybrid_l1_test)

add_executable(version_l1_selection_test
    ${PROJECT_SOURCE_DIR}/tests/version_l1_selection_test.cpp)

target_link_libraries(version_l1_selection_test flowkv)

add_test(NAME version_l1_selection_test COMMAND version_l1_selection_test)

add_executable(db_l1_route_smoke_test
    ${PROJECT_SOURCE_DIR}/tests/db_l1_route_smoke_test.cpp)

target_link_libraries(db_l1_route_smoke_test flowkv)

add_executable(db_l1_recovery_smoke_test
    ${PROJECT_SOURCE_DIR}/tests/db_l1_recovery_smoke_test.cpp)

target_link_libraries(db_l1_recovery_smoke_test flowkv)

add_test(NAME db_l1_recovery_smoke_test
    COMMAND /bin/bash -lc "./db_l1_recovery_smoke_test")
set_tests_properties(db_l1_recovery_smoke_test PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/tests)

add_executable(db_delete_correctness_smoke_test
    ${PROJECT_SOURCE_DIR}/tests/db_delete_correctness_smoke_test.cpp)

target_link_libraries(db_delete_correctness_smoke_test flowkv)

add_test(NAME db_delete_correctness_smoke_test
    COMMAND /bin/bash -lc "./db_delete_correctness_smoke_test")
set_tests_properties(db_delete_correctness_smoke_test PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/tests)

add_executable(manifest_l1_snapshot_test
    ${PROJECT_SOURCE_DIR}/tests/manifest_l1_snapshot_test.cpp)

target_link_libraries(manifest_l1_snapshot_test flowkv)

add_test(NAME manifest_l1_snapshot_test
    COMMAND /bin/bash -lc "./manifest_l1_snapshot_test")
set_tests_properties(manifest_l1_snapshot_test PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/tests)

add_executable(manifest_batch_txn_replay_test
    ${PROJECT_SOURCE_DIR}/tests/manifest_batch_txn_replay_test.cpp)

target_link_libraries(manifest_batch_txn_replay_test flowkv)

add_test(NAME manifest_batch_txn_replay_test
    COMMAND /bin/bash -lc "./manifest_batch_txn_replay_test")
set_tests_properties(manifest_batch_txn_replay_test PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/tests)

add_executable(manifest_durable_crash_recovery_smoke_test
    ${PROJECT_SOURCE_DIR}/tests/manifest_durable_crash_recovery_smoke_test.cpp)

target_link_libraries(manifest_durable_crash_recovery_smoke_test flowkv)

add_test(NAME manifest_durable_crash_recovery_smoke_test
    COMMAND /bin/bash -lc "./manifest_durable_crash_recovery_smoke_test")
set_tests_properties(manifest_durable_crash_recovery_smoke_test PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/tests)

add_executable(manifest_durable_crash_fuzz_test
    ${PROJECT_SOURCE_DIR}/tests/manifest_durable_crash_fuzz_test.cpp)

target_link_libraries(manifest_durable_crash_fuzz_test flowkv)

add_test(NAME manifest_durable_crash_fuzz_test
    COMMAND /bin/bash -lc "./manifest_durable_crash_fuzz_test")
set_tests_properties(manifest_durable_crash_fuzz_test PROPERTIES
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/tests)
