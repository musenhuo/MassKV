# L1 Hybrid B+Tree 设计方案 V7（补丁粒度收紧 + TinyDirect/NormalPack）

## 1. 版本定位

V7 在 V6 的“增量补丁驱动索引更新”基础上，聚焦修复高 prefix 比例下的写放大与 compaction 退化问题：

- 收紧 `DeltaBatch` 粒度（从 prefix 区间展开改为真实触达窗口）
- 引入 `descriptor` 双模式（`TinyDirect` / `NormalPack`）
- 保持 CoW 判定逻辑暂不调整（本版不动 CoW 门槛）
- 将索引写路径从“prefix 级散写”收敛为“页级聚合写”

## 2. 问题复盘（V6 实测退化根因）

高 prefix 比例（如 `0.1N`）下，V6 出现 compaction 变慢、吞吐下降，核心原因：

1. `DeltaBatch` 构建阶段按 `min_prefix~max_prefix` 连续展开，放大了受影响 prefix 集合。
2. prefix 更新粒度过细，导致大量小子树独立写入，形成写放大。
3. 小前缀场景中，大量子树实际上只有极少记录，却仍按 normal 子树流程更新。

V7 目标是优先降低“无效更新面”和“重复小写入”。

## 3. 不变约束

1. layer0 仍使用 Masstree 作为内存路由层。
2. layer1 仍是 SSD 驻留索引结构。
3. 叶子 payload 仍使用 `kv_block_ptr + offset + count` 语义。
4. manifest + L1 状态发布仍保持批次原子提交与可恢复语义。

## 4. 主线一：补丁粒度收紧

### 4.1 当前问题

当前补丁生成按表级 key 范围推导 prefix 区间，导致“未真实触达的 prefix”也被纳入更新。

### 4.2 V7 方案

`DeltaBatch` 改为按真实触达窗口构建：

1. 以 `KV block` 为单位，扫描 `entry_num` 有效项。
2. 构建 `prefix -> (offset,count,suffix_begin,suffix_end)` 窗口。
3. 仅为真实出现的 prefix 生成 `add/delete/replace` op。
4. 同一批次内做净化（net delta）：
   - 相邻窗口合并
   - 可抵消的 `add+delete` 消除
   - 可转 `replace` 的直接转化

### 4.3 目标

1. 显著降低 `delta_prefix_count`。
2. 显著降低 `delta_ops_count`。
3. 减少后续索引更新无效工作量。

## 5. 主线二：Descriptor 双模式

### 5.1 描述符定义

layer0 value 统一为 64-bit descriptor，最高 1 bit 作为类型：

1. `type=0`：`TinyDirect`
2. `type=1`：`NormalPack`

### 5.2 TinyDirect（不走 layer1）

适用条件：一个 prefix 当前仅对应一个 `KV block` 窗口。

descriptor 直接编码：

1. `kv_block_ptr`
2. `offset`
3. `count`

读路径：

1. layer0 命中 prefix
2. 若 `type=0`，直接在对应窗口执行点查（不进入 layer1）

该模式用于“prefix 极小且窗口单一”的高频场景。

### 5.3 NormalPack（共享打包）

用于不满足 TinyDirect 的 prefix。多个 prefix 共享 pack 页，降低小更新写放大。

## 6. 主线三：NormalPack 结构参数

### 6.1 Pack 页参数（首版）

1. `PACK_PAGE_SIZE = 16KB`
2. `PACK_SLOT_BYTES = 16B`
3. `PACK_MAX_SLOTS = 256`（目录区约 4KB）
4. `PACK_ENTRY_BYTES = 24B`

### 6.2 Entry 语义（与叶子语义对齐）

每条 entry：

1. `suffix_min`
2. `suffix_max`
3. `leaf_value`（`kv_block_ptr + offset + count`）

说明：

1. `prefix` 放在 slot 头，不在 entry 重复存储。
2. `suffix_min/suffix_max` 必须同时保留，用于窗口边界、删除匹配和范围查询正确性。

## 7. 主线四：Compaction 更新路径改造

### 7.1 执行方式

索引更新从“每 prefix 即时散写”改为“按脏 PackPage 聚合写”：

1. 先构建净化后的 prefix 增量。
2. 按 descriptor 类型分流：
   - TinyDirect：更新 descriptor 即可
   - NormalPack：更新内存中的 pack 页镜像
3. 对同一脏页只写一次，进行页级批量持久化。

### 7.2 提交语义

1. 索引页写入仍处于 manifest batch 生命周期内。
2. 与版本推进、snapshot 写入一起统一 `CommitBatchUpdate()`。

## 8. 升降级阈值（V7 重定义）

采用滞回阈值，避免模式抖动：

1. Tiny 进入条件（`enter_tiny`）：
   - `block_count == 1`
   - `window_count == 1`
   - `entry_count <= 48`
2. Tiny 退出条件（`exit_tiny`）：
   - `block_count > 1` 或 `window_count > 1` 或 `entry_count >= 96`
3. Normal -> Tiny 回切要求：
   - 连续 2 个 compaction 批次满足 `enter_tiny`

备注：本版不调整 CoW 判定阈值，仅控制 descriptor 模式切换。

## 9. 可观测性与验收指标（新增）

V7 需要新增并在实验报告输出：

1. `effective_delta_prefix_count`
2. `effective_delta_ops_count`
3. `tiny_descriptor_count`
4. `normal_pack_count`
5. `tiny_hit_ratio`
6. `dirty_pack_pages`
7. `pack_write_bytes`
8. `index_update_total_ms`
9. `compaction_total_time_ms`

验收目标：

1. 在 `10M, prefix_ratio=0.1` 场景下，`compaction_total_time_ms` 明显低于 V6 当前退化结果。
2. `effective_delta_*` 相比 V6 显著下降。
3. 写吞吐恢复到不低于 V6 退化前水平。

## 10. 分阶段落地计划

### 阶段 A：补丁粒度改造

1. 真实触达窗口构建
2. 批内净化（merge/cancel/replace）
3. 指标打点

### 阶段 B：Descriptor 双模式

1. 引入 `type` 位和两种编码
2. TinyDirect 查询路径接通
3. 与现有路由兼容

### 阶段 C：NormalPack 与聚合写

1. Pack 页结构落地
2. 索引更新由 prefix 散写改为页级聚合写
3. 保持批次提交语义

### 阶段 D：实验回归

1. 重跑 `03` 写性能实验（同配置 A/B）
2. 对比 V6 结果并更新 progress 文档

## 11. Route Swap（内存可控扩展）

为对齐“内存可控与性能同等重要”的目标，V7 增加 route swap 扩展：

1. layer0 保持“内部路由常驻内存”，仅 route 叶子条目可下沉；
2. `hot_index_max_bytes` 超阈值时触发叶子下沉：把 `prefix -> descriptor/root` 叶子条目按页写入 SSD；
3. 内存侧仅保留目录级索引（用于定位叶子页），不再保留全量叶子副本；
4. 点查：`prefix -> (hot/目录) -> 叶子页 -> descriptor/root`；
5. 范围查：从目录定位起始页后按页扫描目标 prefix 范围；
6. 默认策略采用 `swap_all_on_overflow=true`，确保 route 内存上限可强约束。

对应配置：

1. `FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES`
2. `FLOWKV_L1_ROUTE_SWAP_ALL_ON_OVERFLOW`
