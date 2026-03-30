---
name: 代码改动后必做回归测试
description: 每次改动代码后必须运行的测试，确保改动正确性与架构稳定性
type: feedback
---

每次改动代码后，必须在完成改动后立即运行回归测试，不需要用户提醒。

**Why:** 代码改动可能破坏现有功能，必须通过测试验证正确性与架构稳定性。

**How to apply:** 每次结构性代码改动后，按以下顺序执行：

1. 编译相关 target（确保无编译错误）
2. 运行 `hybrid_l1_test`（核心索引逻辑）
3. 运行 `manifest_batch_txn_replay_test`（manifest 持久化）
4. 运行 `manifest_durable_crash_recovery_smoke_test`（崩溃恢复）

```bash
cmake --build build_hybrid_check --target hybrid_l1_test manifest_batch_txn_replay_test manifest_durable_crash_recovery_smoke_test -- -j$(nproc)
./build_hybrid_check/tests/hybrid_l1_test
./build_hybrid_check/tests/manifest_batch_txn_replay_test
./build_hybrid_check/tests/manifest_durable_crash_recovery_smoke_test
```

如果有新增测试 target，也一并加入。测试全部通过后才算改动完成。
