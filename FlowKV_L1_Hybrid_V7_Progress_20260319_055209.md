# FlowKV L1 Hybrid V7 进度（更新于 2026-03-21 10:35:00 UTC）

## 已实现结果（当前有效）

### 1) V7 设计文档已建立

- 新增 V7 设计主文档：
  - `l1_hybrid_bptree_design_v7.md`
- 已明确本阶段优化主线：
  - 补丁粒度收紧
  - descriptor 双模式（TinyDirect / NormalPack）
  - compaction 页级聚合写
  - 升降级阈值（滞回）

### 2) 补丁粒度收紧（阶段 A）已开始落地

- 已在 compaction 侧改造 `DeltaBatch` 构建逻辑：
  - 从“`min_prefix~max_prefix` 区间展开”切到“按 `KV block` 有效 entry 扫描真实 prefix 窗口”。
  - 窗口结构：`prefix -> (offset,count,suffix_begin,suffix_end)`。
- 代码位置：
  - `db/compaction/compaction.cpp`

### 3) 批内净化（net delta）初版已接入

- 已加入两类净化：
  - `add/delete` 精确同窗口抵消
  - 同窗口 `delete + add` 可转 `replace`（携带新 payload）
- 空补丁前缀已在 batch 内裁剪，减少后续无效 rebuild 触发。

### 4) 阶段 A 可运行闭环（本轮新增）

- 定向构建已通过（主线相关 target）：
  - `flowkv`
  - `hybrid_l1_test`
  - `db_l1_route_smoke_test`
  - `version_l1_selection_test`
- 回归通过：
  - `hybrid_l1_test`
  - `version_l1_selection_test`
  - `db_l1_route_smoke_test`（可执行程序直跑通过）
- 在线写路径最小冒烟通过（`write_online_benchmark`，`100000` 写入，触发 flush+compaction，流程正常结束）。
- 备注：全量工程构建仍有历史问题（`hmasstree` 重定义与 `manifest_l1_snapshot_test` 的 `OpLogSize`），与本轮 V7 改动无直接关联。

### 5) `replace` 删除匹配语义已对齐

- 修复点：
  - `delete + add -> replace` 后，删除旧窗口不再要求 `kv_block_ptr` 一致；
  - 对 `replace` 采用“同 prefix + 同 suffix 精确窗口”匹配旧记录后再写入新 payload。
- 代码位置：
  - `lib/hybrid_l1/l1_hybrid_rebuilder.cc`（`MatchesDeleteOp`）
- 定向回归：
  - `hybrid_l1_test` 通过
  - `version_l1_selection_test` 通过
  - `db_l1_route_smoke_test` 通过

### 6) 阶段 B（descriptor 双模式）最小闭环已落地（本轮新增）

- 新增 descriptor 基础结构与编码：
  - `lib/hybrid_l1/route_descriptor.h`
  - 支持 `TinyDirect`（`type=0`）与 `NormalSubtree`（`type=1`）判定/解码。
- `RoutePartition` 新增 descriptor 状态字段：
  - `descriptor_mode`
  - `tiny_leaf_value`
- rebuilder 已接入 tiny 判定与写回：
  - 在分区构建阶段，当分区满足“单窗口小前缀”条件（当前实现：`records.size()==1` 且 `window.count<=48`）时，标记为 `TinyDirect`；
  - 否则保持 `NormalSubtree`。
- 路由层改造为双索引：
  - 保留原 `prefix -> root_page_ptr` 路由索引（范围查/候选查不回退）；
  - 新增 `prefix -> descriptor` 索引用于点查快速分流。
- 点查路径已接通 TinyDirect：
  - `LookupCandidate` 优先走 descriptor；
  - `TinyDirect` 命中时直接返回 leaf window（不走 layer1 B+Tree 页遍历）；
  - `Normal` 仍走原 root->leaf SSD 路径。
- 兼容性策略：
  - 范围查与候选查继续使用原 B+Tree 路径（确保行为一致）；
  - persisted snapshot 仍沿用原字段，descriptor 状态当前为“在线构建态”，未单独序列化。

### 7) 阶段 B 定向构建/回归结果（本轮新增）

- 构建通过：
  - `flowkv`
  - `hybrid_l1_test`
  - `db_l1_route_smoke_test`
  - `version_l1_selection_test`
- 测试通过：
  - `hybrid_l1_test`
  - `version_l1_selection_test`
  - `db_l1_route_smoke_test` 直跑通过

### 8) 阶段 B 收敛补齐（descriptor 持久化 + 滞回）已完成（本轮新增）

- descriptor 状态持久化已接通：
  - L1 snapshot 协议升级到 `format=4`；
  - 新增序列化字段：
    - `descriptor_mode`
    - `tiny_enter_streak`
    - `tiny_leaf_value`
  - 导入兼容旧 `format=3`（默认回落 `NormalSubtree`）。
- Tiny/Normal 滞回逻辑已接入 rebuilder：
  - `enter_tiny`：`block_count==1 && window_count==1 && entry_count<=48`
  - `exit_tiny`：`block_count>1 || window_count>1 || entry_count>=96`
  - `Normal -> Tiny`：连续 2 批满足 `enter_tiny` 才切换（初始全量 build 允许首批进入 tiny）。
- 路由快照结构同步：
  - published snapshot / debug partition 已包含 `tiny_enter_streak`。
- 定向回归（改动后）：
  - `flowkv` 构建通过
  - `hybrid_l1_test` 通过
  - `version_l1_selection_test` 通过
  - `db_l1_route_smoke_test` 通过

### 9) 阶段 C 首步：页级聚合写已接入（本轮新增）

- `SubtreePageStore` 写路径已从“逐页 `pwrite`”升级为“连续页批量写（默认 128KB 聚合）”：
  - 新增批量写 helper：连续物理页合并后一次 `pwrite`；
  - 非连续页自动退化为小批/单页写，保证正确性。
- 已接入两条关键持久化路径：
  - `Persist`
  - `PersistCow`
- 代码位置：
  - `lib/hybrid_l1/subtree_page_store.cc`
- 定向回归：
  - `flowkv` 构建通过
  - `hybrid_l1_test` 通过
  - `version_l1_selection_test` 通过
  - `db_l1_route_smoke_test` 通过

### 10) 阶段 C 第二步：NormalPack 基础结构与读路径解析已接入（本轮新增）

- 新增 `NormalPack` 页结构与编解码：
  - `lib/hybrid_l1/normal_pack.h`
  - `lib/hybrid_l1/normal_pack.cc`
  - 支持 `slot(prefix, entry_begin, entry_count)` 与 `entry(suffix_min, suffix_max, leaf_value)`。
- descriptor 编码已扩展：
  - `type=1` 下细分为 `NormalSubtree` / `NormalPack`；
  - 支持 `pack_page_ptr + slot_id` 编码与解码。
- 路由元数据已扩展：
  - `RoutePartition` / published snapshot 增加 `pack_page_ptr` 与 `pack_slot_id` 字段。
- 点查路径已接入 NormalPack 解析分支：
  - 若路由模式为 `NormalPack`，可按 descriptor 读取 pack 页并按 `slot+suffix` 定位 `leaf_value`。
- snapshot 协议版本升级：
  - `kL1HybridStateFormat = 5`；
  - 增加 pack 相关字段持久化，并兼容旧格式读取。
- 说明：
  - 当前仅完成 NormalPack 结构与读路径能力；
  - 仍未把 compaction 的索引更新路径切到“真实 pack 页聚合更新产出”。

- 定向回归：
  - `flowkv` 构建通过
  - `hybrid_l1_test` 通过
  - `version_l1_selection_test` 通过
  - `db_l1_route_smoke_test` 通过

### 11) 阶段 C 第三步：NormalPack 持久化与增量回收链路已接入（本轮新增）

- `SubtreePageStore` 新增 opaque 页 API：
  - `PersistOpaquePages(page_size, pages)`：批量分配并批量写入 pack 页，返回物理页指针。
  - `DestroyOpaquePages(page_size, page_ptrs)`：按页回收 pack 页。
- rebuilder 已接通“changed prefix -> pack slot 编排”流程：
  - 对变更前缀中的非 tiny 分区，生成 `NormalPackPage`（`slot + entries`）。
  - 统一编码后批量持久化，回填 `pack_page_ptr + pack_slot_id`。
  - descriptor 自动切换到 `NormalPack`（tiny 仍保持 `TinyDirect`）。
- old pack 页回收已接入：
  - 对本轮 changed prefix 的旧 `NormalPack` 页面，按“新快照仍引用则保留，否则回收”处理。
- Reset 路径也已补齐 pack 页回收：
  - `ResetPartitions(...)` 不再只回收 subtree 页，同时会回收 normal-pack 页。
- 代码位置：
  - `lib/hybrid_l1/subtree_page_store.h`
  - `lib/hybrid_l1/subtree_page_store.cc`
  - `lib/hybrid_l1/l1_hybrid_rebuilder.cc`
- 定向回归：
  - `flowkv` 构建通过
  - `hybrid_l1_test` 通过
  - `version_l1_selection_test` 通过
  - `db_l1_route_smoke_test` 通过

### 12) 阶段 C 第四步：V7 指标观测链路已接通（本轮新增）

- 在 rebuilder 统计中新增并可累计输出：
  - `effective_delta_prefix_count`
  - `effective_delta_ops_count`
  - `tiny_descriptor_count`
  - `normal_pack_count`
  - `tiny_hit_ratio`
  - `dirty_pack_pages`
  - `pack_write_bytes`
- 在 `03` 写实验链路已接入对应字段：
  - benchmark stdout
  - `run_write_online_batch.py` 解析白名单
  - `results.csv`
  - `RESULTS.md`（V7 Index-Update Detail Table）
- 代码位置：
  - `lib/hybrid_l1/l1_hybrid_rebuilder.h`
  - `lib/hybrid_l1/l1_hybrid_rebuilder.cc`
  - `experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp`
  - `experiments/performance_evaluation/03_compaction_update/run_write_online_batch.py`
  - `experiments/performance_evaluation/03_compaction_update/generate_write_online_report.py`
  - `experiments/performance_evaluation/03_compaction_update/README.md`
- 定向回归：
  - `flowkv` 构建通过
  - `write_online_benchmark` 构建通过
  - `hybrid_l1_test` 通过
  - `version_l1_selection_test` 通过
  - `db_l1_route_smoke_test` 通过

### 13) 阶段 D 首轮实验：03 写性能（uniform, 10M）已完成（本轮新增）

- 运行目录：
  - `experiments/performance_evaluation/03_compaction_update/results/20260319_063858`
- 配置：
  - `write_ops=10,000,000`
  - `threads=1`
  - `distribution=uniform`
  - `prefix_ratio=0.1/0.05/0.01`
- 已产出完整实验物：
  - `raw/*.txt`
  - `results.csv`
  - `RESULTS.md`
  - `plots/*.png`
- 关键结果（实测）：
  - `0.1N`: ingest `13,120 ops/s`, `compaction_total_time_ms=745,392`, `index_update_total_ms=681,593`, `dirty_pack_pages=40,808`, `pack_write_bytes=668,598,272`
  - `0.05N`: ingest `29,953 ops/s`, `compaction_total_time_ms=322,566`, `index_update_total_ms=293,206`, `dirty_pack_pages=32,103`, `pack_write_bytes=525,975,552`
  - `0.01N`: ingest `127,269 ops/s`, `compaction_total_time_ms=66,523`, `index_update_total_ms=37,336`, `dirty_pack_pages=4,503`, `pack_write_bytes=73,777,152`
- 初步结论：
  - compaction 与 index_update 开销随 prefix 比例显著上升，且 `0.1N` 场景代价最高；
  - V7 新增指标已可直接支撑后续定位（dirty pack 页规模、pack 写字节、tiny/pack 比例）。

### 14) 阶段 D 第二轮实验：03 写性能（prefix-skew, 10M）已完成（本轮新增）

- 运行目录：
  - `experiments/performance_evaluation/03_compaction_update/results/20260319_065947`
- 配置：
  - `write_ops=10,000,000`
  - `threads=1`
  - `distribution=prefix-skew`
  - `prefix_ratio=0.1/0.05/0.01`
- 已产出完整实验物：
  - `raw/*.txt`
  - `results.csv`
  - `RESULTS.md`
  - `plots/*.png`
- 关键结果（实测）：
  - `0.1N`: ingest `49,004 ops/s`, `compaction_total_time_ms=185,256`, `index_update_total_ms=164,620`, `dirty_pack_pages=11,732`, `pack_write_bytes=192,217,088`
  - `0.05N`: ingest `117,183 ops/s`, `compaction_total_time_ms=73,077`, `index_update_total_ms=54,626`, `dirty_pack_pages=4,545`, `pack_write_bytes=74,465,280`
  - `0.01N`: ingest `255,952 ops/s`, `compaction_total_time_ms=25,790`, `index_update_total_ms=17,847`, `dirty_pack_pages=1,687`, `pack_write_bytes=27,639,808`
- 与 uniform 对照结论（同 10M）：
  - 在三档 prefix 比例下，`prefix-skew` 的 ingest 吞吐都显著高于 uniform；
  - 对应 `index_update_total_ms`、`dirty_pack_pages`、`pack_write_bytes` 都明显更低；
- 说明当前瓶颈主要与“受影响前缀规模 + 脏 pack 页规模”强相关，V7 指标可以直接解释性能差异。

### 15) 论文写作总述文档已生成（本轮新增）

- 新增汇总文档：
  - `FlowKV_HybridL1_Innovation_Summary_20260319_095233.md`
- 文档范围：
  - 按 `design v5/v6/v7` 与 `V7 progress` 映射整理创新点、架构演进与优化手段；
  - 明确基线口径：架构对比 FlowKV、写性能对比 RocksDB；
  - 汇总读/写关键结果与可解释性指标链（`effective_delta_*`, `dirty_pack_pages`, `pack_write_bytes` 等）。

### 16) 0.01N 写吞吐冲线实验已完成（本轮新增）

- 目标：
  - 10M 写入、`0.01N` 下把 ingest 吞吐从 ~`255k ops/s` 提升到 `>=300k ops/s`。
- 改动（按“前两项”执行）：
  1. 放宽 TinyDirect 阈值：
     - `enter <= 128`（原 `<=48`）
     - `exit >= 192`（原 `>=96`）
  2. 降低 compaction 触发频率（本轮实验参数）：
     - `flush_batch=500000`（原 `250000`）
     - `l0_compaction_trigger=8`（原 `4`）
- 代码位置：
  - `lib/hybrid_l1/l1_hybrid_rebuilder.cc`
- 结果目录：
  - `experiments/performance_evaluation/03_compaction_update/results/20260320_025101`
- 关键结果（`uniform`, `10M`, `0.01N`）：
  - `ingest_throughput_ops = 320,977`（已超过 `300,000` 目标）
  - `compaction_total_time_ms = 17,526.1`（对比上轮 `66,523.5`，显著下降）
  - `index_update_total_ms = 6,891.01`（对比上轮 `37,336`，显著下降）
  - `flush_count = 20`（原 `40`）
  - `compaction_count = 3`（原 `10`）

### 17) 0.01N 多线程写实验（16线程）已完成（本轮新增）

- 实验配置：
  - `uniform`, `10M`, `0.01N`
  - 继承冲线参数：`flush_batch=500000`, `l0_compaction_trigger=8`
  - 前台写线程：`16`
- 结果目录：
  - `experiments/performance_evaluation/03_compaction_update/results/20260320_025359`
- 关键结果：
  - `ingest_throughput_ops = 313,353`（仍高于 `300,000` 目标）
  - `avg_put_latency_ns = 900.48`
  - `p99_put_latency_ns = 2,849`
  - `compaction_total_time_ms = 18,915.2`
  - `index_update_total_ms = 8,397.14`
- 与本轮单线程冲线结果（`20260320_025101`）对照：
  - 吞吐：`320,977 -> 313,353`（略降）
  - put 路径吞吐：`3.70M -> 1.11M`（明显下降，提示多线程共享路径竞争仍存在）
  - compaction/index-update 时延略增
  - 结论：当前 `0.01N` 下多线程并未带来进一步吞吐增益，后续需重点排查前台写路径共享资源争用。

### 18) 0.01N 单线程/16线程复测对比已完成（本轮新增）

- 复测目的：
  - 在当前稳定写路径（`maintenance_mode=manual`）下，重新验证 `10M, 0.01N` 的线程扩展性。
- 结果目录：
  - 单线程：`experiments/performance_evaluation/03_compaction_update/results/20260320_031353`
  - 16线程：`experiments/performance_evaluation/03_compaction_update/results/20260320_031435`
- 核心结果：
  - ingest 吞吐：`293,215 -> 339,267 ops/s`（+15.7%）
  - put_path 吞吐：`3.67M -> 1.30M ops/s`（显著下降）
  - avg put latency：`272ns -> 769ns`，p99：`540ns -> 2400ns`（显著上升）
  - compaction / index-update 代价变化有限（同量级）
- 结论：
  - 总吞吐有提升，但远低于理想线性扩展；
  - 前台并发路径仍存在显著共享资源争用，是当前多线程扩展的主瓶颈。

### 19) benchmark 并发路径改造（overlap 模式）已接入（本轮新增）

- 目标：
  - 将 `03` 写实验从“批次后串行维护”扩展为“前台写与维护并发重叠”的可控实验模式。
- 改动：
  - `write_online_benchmark` 新增 `maintenance_mode=overlap`；
  - overlap 模式下由 benchmark 内部维护线程并发执行 `BGFlush/BGCompaction`；
  - 关闭 DB 自带触发线程，避免双触发导致干扰；
  - 保留 `manual/background` 两种原有模式，兼容历史实验脚本。
- 代码位置：
  - `experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp`
  - `experiments/performance_evaluation/03_compaction_update/README.md`
- 验证：
  - `write_online_benchmark` 目标构建通过；
  - 小规模冒烟（`100k`, `4 threads`, `maintenance_mode=overlap`）运行通过并输出完整指标。

### 20) 写 benchmark 已对齐 FlowKV 原生并发口径（本轮新增）

- 参考来源：
  - `benchmarks/simple_benchmark.cpp`（FlowKV 原生）
- 核心对齐点：
  - 原生口径为“前台多线程持续 `Put` + 后台触发线程维护 flush/compaction + 末尾统一 `WaitForFlushAndCompaction`”；
  - 不在写循环中手工调用 `BGFlush/BGCompaction`。
- 改动落地：
  - `write_online_benchmark` 中 `background/overlap` 模式改为原生口径：
    - 前台一次性并发写入整个工作集；
    - 写结束后统一 barrier（`WaitForFlushAndCompaction`）；
    - 去除该路径中的手工 `BGFlush/BGCompaction` 循环，避免线程池生命周期断言。
  - `manual` 模式保持“显式 flush/compaction”用于可控对照。
- 代码位置：
  - `experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp`
  - `experiments/performance_evaluation/03_compaction_update/README.md`
- 验证：
  - 目标构建通过；
  - `maintenance_mode=background` 小规模冒烟运行通过。

### 21) 并发写崩溃定位与修复（本轮新增）

- 问题：
  - `background + 16 threads` 大规模写入在 benchmark 中出现崩溃/异常退出。
- 定位：
  - 通过 `gdb --batch` 抓栈，崩溃点在：
    - `Masstree::tcursor<...>::make_split`
    - 调用链：`MasstreeIndex::PutValidate -> MYDBClient::Put`
  - 根因是 benchmark 里把 `MYDBClient` 在主线程创建、在 worker 线程使用，破坏了 Masstree 线程本地初始化语义。
- 修复：
  - 改为“每个 worker 在线程内创建自己的 client”，但 client 生命周期延长到 `WaitForFlushAndCompaction()` 之后再释放；
  - 这样既满足 Masstree 的线程语义，也避免后台 flush 访问 `client_list_` 时碰到已析构 client。
- 代码位置：
  - `experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp`
- 结果（`uniform`, `10M`, `0.01N`, `flush_batch=500000`, `trigger=8`）：
  - `background, 1 thread`：`ingest=1,046,930 ops/s`（目录 `20260320_035705`）
  - `background, 16 threads`：`ingest=820,302 ops/s`（目录 `20260320_035645`，已稳定跑通）
  - `manual, 1 thread`：`ingest=311,162 ops/s`（目录 `20260320_034451`）
  - `manual, 16 threads`：`ingest=323,171 ops/s`（目录 `20260320_035734`）

### 22) `background` 模式线程扫描（1/2/4/8）已完成（本轮新增）

- 目的：
  - 验证“多线程是否普遍不如单线程”，还是仅在 `16` 线程出现退化。
- 统一配置：
  - `uniform`, `10M`, `0.01N`
  - `maintenance_mode=background`
  - `flush_batch=500000`, `l0_compaction_trigger=8`, `use_direct_io=1`
- 结果目录：
  - `1 thread`：`experiments/performance_evaluation/03_compaction_update/results/20260320_040209`
  - `2 threads`：`experiments/performance_evaluation/03_compaction_update/results/20260320_040227`
  - `4 threads`：`experiments/performance_evaluation/03_compaction_update/results/20260320_040300`
  - `8 threads`：`experiments/performance_evaluation/03_compaction_update/results/20260320_040321`
- 核心结果（ingest）：
  - `1 thread`：`1,049,470 ops/s`
  - `2 threads`：`434,359 ops/s`
  - `4 threads`：`864,425 ops/s`
  - `8 threads`：`338,364 ops/s`
  - 参考 `16 threads`（上轮稳定跑通）：`820,302 ops/s`（`20260320_035645`）
- 结论：
  - 不是“只在 16 线程才退化”；
  - 在当前 `background` 配置下，`2/4/8/16` 线程均未超过单线程吞吐；
  - 退化与波动（特别是 `2/8`）说明并发路径仍存在明显共享资源争用与调度不稳问题，后续需要继续收敛。

### 23) 写 benchmark 统计口径修正 + 新一轮 1/2/4/8/16 已完成（本轮新增）

- 口径修正（仅 benchmark 层）：
  - `put_path_throughput_ops` 改为“前台 Put 阶段墙钟吞吐”（不再用 per-op 延迟求和推导）。
  - 新增输出：
    - `foreground_put_throughput_ops`
    - `foreground_put_phase_time_ms`
    - `drain_wait_time_ms`
    - `end_to_end_throughput_ops`
  - `background` 模式中不再将 `WaitForFlushAndCompaction()` 的等待时间伪装为 `compaction_total_time_ms`。
- 报告链路已对齐：
  - `run_write_online_batch.py` 字段白名单更新
  - `generate_write_online_report.py` 主表改为前台吞吐优先
  - `plot_write_online_results.py` 新增前台吞吐/端到端吞吐/drain 等图
  - `03_compaction_update/README.md` 指标口径更新
- 新实验（`background`, `uniform`, `10M`, `0.01N`, `flush_batch=500000`, `trigger=8`）：
  - `1线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_064943`
    - `foreground_put_throughput_ops=3,092,410`
    - `end_to_end_throughput_ops=1,082,890`
  - `2线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_065006`
    - `foreground_put_throughput_ops=3,746,560`
    - `end_to_end_throughput_ops=937,189`
  - `4线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_065028`
    - `foreground_put_throughput_ops=4,008,750`
    - `end_to_end_throughput_ops=444,505`
  - `8线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_065101`
    - `foreground_put_throughput_ops=9,936,070`
    - `end_to_end_throughput_ops=666,285`
  - `16线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_065128`
    - `foreground_put_throughput_ops=8,046,190`
    - `end_to_end_throughput_ops=755,038`
- 阶段性结论：
  - 仅看前台写吞吐，线程扩展已表现出提升（到 `8` 线程达到峰值）；
  - 端到端吞吐仍受后台清空阶段影响明显（`drain_wait_time_ms` 高），系统层并发治理仍是后续重点。

### 24) 写实验维护模式口径收敛（FlowKV 风格）已完成（本轮新增）

- 背景：
  - 为避免“误把诊断模式当正式吞吐口径”，将 03 写实验默认维护模式收敛到 FlowKV 原生风格（前台连续写 + 后台自动维护）。
- 已完成改动：
  - `run_write_online_batch.py` 默认 `--maintenance-mode` 从 `manual` 改为 `background`。
  - `write_online_benchmark.cpp` 中 `maintenance_mode=overlap` 已兼容回退到 `background`（并输出 deprecate 提示），不再作为独立语义模式。
  - `03_compaction_update/README.md` 已同步更新为：
    - `background` 作为默认正式口径
    - `manual` 仅用于调试拆分 flush/compaction
    - `overlap` 仅保留兼容，不再独立使用
- 定向构建：
  - `write_online_benchmark` 重新构建通过。

### 25) 引擎层双阈值治理（自动 compaction + L0 满写阻塞）已接入并完成多线程实测（本轮新增）

- 引擎改动（系统层）：
  - 新增 L0 写阻塞阈值参数：
    - `l0_write_stall_tree_num_`（默认 `MAX_L0_TREE_NUM-1`，即 L0 满）
  - 新增判定逻辑：
    - `NeedWriteStallOnL0()`
    - 当达到写阻塞阈值时，前台 `Put` 线程在 `StartWrite()` 中等待，并主动调用 `MayTriggerCompaction()` 促发后台 compaction。
  - 语义对齐：
    - 软阈值：`l0_compaction_tree_num_`（触发 compaction）
    - 硬阈值：`l0_write_stall_tree_num_`（前台写阻塞，直到低于阈值）
- benchmark 配置链路改动：
  - `write_online_benchmark.cpp` 新增参数 `--l0-write-stall-threshold`
  - `run_write_online_batch.py` 增加同名参数并默认传递（默认 `31`）
  - `generate_write_online_report.py` / `README.md` 已同步记录该阈值

- 代码位置：
  - `include/db.h`
  - `db/db.cpp`
  - `db/db_client.cpp`
  - `experiments/performance_evaluation/03_compaction_update/write_online_benchmark.cpp`
  - `experiments/performance_evaluation/03_compaction_update/run_write_online_batch.py`
  - `experiments/performance_evaluation/03_compaction_update/generate_write_online_report.py`
  - `experiments/performance_evaluation/03_compaction_update/README.md`

- 定向构建：
  - `flowkv` 通过
  - `write_online_benchmark` 通过

- 多线程实测（`uniform`, `10M`, `0.01N`, `background`, `flush_batch=500000`, `l0_compaction_trigger=8`, `l0_write_stall_threshold=31`）：
  - `1线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_0730_t1`
    - `foreground_put_throughput_ops=2,854,950`
    - `end_to_end_throughput_ops=1,176,010`
  - `2线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_0730_t2`
    - `foreground_put_throughput_ops=4,225,920`
    - `end_to_end_throughput_ops=879,703`
  - `4线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_0730_t4`
    - `foreground_put_throughput_ops=5,087,840`
    - `end_to_end_throughput_ops=715,952`
  - `8线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_0730_t8_retry`
    - `foreground_put_throughput_ops=6,389,550`
    - `end_to_end_throughput_ops=737,104`
  - `16线程`：`experiments/performance_evaluation/03_compaction_update/results/20260320_0730_t16`
    - `foreground_put_throughput_ops=11,755,600`
    - `end_to_end_throughput_ops=778,051`

- 阶段结论：
  - 前台纯写吞吐随线程数增长明显提升（1 -> 16 约 4.1x）；
  - 端到端吞吐仍显著受后台排空影响（`drain_wait_time_ms` 高位），说明系统层后台维护调度仍是瓶颈主线。

### 26) 200M/0.01N 快速方案复测（本轮新增）

- 快速方案改动确认：
  - `L0MetaSize` 提升到 `204,800,000`（4x）
  - `ManifestTxnLogSize` 提升到 `256MB`
- 先验失败定位：
  - 目录：`experiments/performance_evaluation/03_compaction_update/results/20260320_200m_0p01N_t1_quickfix`
  - 失败原因：并非 manifest overflow，而是默认 `pool_size=64GiB` 触发 segment allocator 空间不足。
- 修正后正式复测：
  - 目录：`experiments/performance_evaluation/03_compaction_update/results/20260320_200m_0p01N_t1_quickfix_pool256g`
  - 配置：
    - `write_ops=200,000,000`
    - `prefix_ratio=0.01N`
    - `threads=1`
    - `compaction_threads=16`
    - `flush_batch=500,000`
    - `l0_compaction_trigger=4`
    - `l0_write_stall_threshold=8`
    - `pool_size_bytes=274,877,906,944`
  - 关键结果（实测）：
    - `foreground_put_throughput_ops=2,391,870`
    - `end_to_end_throughput_ops=462,253`
    - `foreground_put_phase_time_ms=83,616`
    - `drain_wait_time_ms=349,046`
    - `index_update_total_ms=277,682`
  - 结论：
    - 快速方案已有效消除“manifest txn payload overflow”这一致命中断点；
    - 200M 场景可稳定跑通，当前端到端瓶颈仍主要在后台排空阶段（drain）。

### 27) Layer0 Route Swap（hot Masstree + cold snapshot）机制已落地（本轮新增）

- 目标：
  - 在不改 layer1 子树语义前提下，实现 layer0 路由内存可控；
  - 超阈值时将 route 叶子语义降级到 SSD 可恢复的冷快照，避免 route 内存无限增长。

- 实现位置：
  - `lib/hybrid_l1/route_layout.h`
  - `lib/hybrid_l1/route_layout.cc`
  - `lib/hybrid_l1/l1_hybrid_index.h`
  - `lib/hybrid_l1/l1_hybrid_index.cc`
  - `db/compaction/version.cpp`
  - `tests/hybrid_l1_test.cpp`

- 机制细节：
  1. 新增 `RouteSwapOptions` 与 `RouteSnapshotEntry`；
  2. `RefreshPartitions()` 期间始终构建全量 `cold_snapshot_`（按 prefix 排序）；
  3. hot 路由 Masstree 受 `hot_index_max_bytes` 约束；
  4. 默认策略 `swap_all_on_overflow=true`：若估算 hot 路由内存超阈值，hot Masstree 全量降级为 cold（内存仅保留空索引壳）；
  5. 查找路径改造：
     - `FindDescriptorByKey`：先 hot，miss 回落 cold snapshot；
     - `FindRootByKey`：先 hot，miss 回落 cold snapshot；
     - `CollectRootsForRange`：直接按 cold snapshot 范围扫描（保证即使全量 swap 也可范围检索）。

- 配置入口（环境变量）：
  - `FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES`
    - `0` 表示禁用 swap（全部 route 常驻 hot Masstree）；
    - `>0` 表示启用内存上限治理。
  - `FLOWKV_L1_ROUTE_SWAP_ALL_ON_OVERFLOW`
    - `true/1`：超阈值时全量 route 降级到 cold；
    - `false/0`：保留部分 hot（按上限容量截断）。

- 回归验证：
  - 构建通过：
    - `flowkv`
    - `hybrid_l1_test`
    - `version_l1_selection_test`
    - `db_l1_route_smoke_test`
  - 测试通过：
    - `hybrid_l1_test`（新增 `TestHybridIndexRouteSwapColdSnapshotFallback`）
    - `version_l1_selection_test`
    - `db_l1_route_smoke_test`

- 阶段结论：
  - Route swap 最小闭环已完成：内存上限治理可启用，查找/范围查在全量 swap 场景仍正确；
  - 当前实现不依赖 layer0 持久化，路由语义由 cold snapshot 提供兜底查询能力。

### 28) Route Swap 语义修正：改为“仅叶子下沉 SSD，内部路由常驻内存”（本轮新增）

- 修正背景：
  - 之前实现是 `hot Masstree + cold snapshot`，超阈值后几乎全量降温，和“只下沉叶子”目标不一致。

- 本轮最终语义（已落地）：
  1. 未超阈值：layer0 路由仍走全热 Masstree（叶子不下沉）。
  2. 超阈值：仅 route 叶子条目页下沉到 SSD（按页组织），内存仅保留目录级路由索引。
  3. 点查流程：`prefix -> (hot 索引或目录定位页) -> SSD 叶子页二分 -> descriptor/root`。
  4. 范围流程：目录定位到起始页后，按页扫描目标 prefix 范围。

- 本轮代码位置：
  - `lib/hybrid_l1/route_layout.cc`（重建并实现页级叶子下沉）
  - `lib/hybrid_l1/route_layout.h`（swap 选项与冷页元数据）
  - `lib/hybrid_l1/l1_hybrid_index.cc`（RouteSwapOptions 传参与冷页 SSD 统计接入）
  - `lib/hybrid_l1/l1_hybrid_index.h`（内存统计字段语义补齐）

- 指标口径修正：
  - 新增并输出：`l1_route_cold_ssd_bytes`（叶子下沉到 SSD 的实际字节）
  - `l1_route_hot_*_measured_bytes` 仅统计内存热路由部分

- 最小验证（实测）：
  - 构建：`point_lookup_benchmark` 通过
  - 回归：`hybrid_l1_test` 通过
- 小规模点查验证：
    - `FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES=0`：`l1_route_cold_ssd_bytes=0`（不下沉）
    - `FLOWKV_L1_ROUTE_HOT_INDEX_MAX_BYTES=65536`：`l1_route_cold_ssd_bytes=122,880`（触发叶子下沉）

### 29) Cold Leaf Bin-Pack（>=16KB 聚合页）已落地（本轮新增）

- 目标：
  - 避免“每个 cold leaf 独占 4KB 页”导致的 SSD 空间放大；
  - 将多个 cold leaf 按 slot 打包到同一逻辑页，并通过底层批写实现 SSD 侧 `>=16KB` 物理聚合（best-effort）。

- 本轮改动：
  1. cold 页格式升级到 `version=2`，引入 `slot directory`：
     - 每个 slot 存 `begin_entry + entry_count`（4B）；
     - entry 仍保持 `prefix(8B) + descriptor(8B)`，不改业务语义。
  2. `ColdLeafStub` 新增 `slot_id`，用于定位共享页中的具体 slot。
  3. swap 持久化改为：
     - 先按页大小做 `bin-pack`；
     - 再批量 `PersistOpaquePages` 落盘。
  4. 点查/范围查改为按 `ssd_page_ptr + slot_id` 查找；
  5. `leaf_page_size` 默认保持 `4KB`（逻辑页），构造处仅要求 `>=4KB 且 4KB 对齐`；
     物理写路径由 page store 批写器负责按 `>=16KB` 目标做连续聚合。

- 代码位置：
  - `lib/hybrid_l1/route_cold_leaf.h`
  - `lib/hybrid_l1/route_layout.cc`
  - `lib/hybrid_l1/route_layout.h`
  - `lib/hybrid_l1/l1_hybrid_index.cc`

- 定向构建/运行验证：
  - 构建：`point_lookup_benchmark` 通过
  - 运行：小规模强制 swap sanity 通过
  - 结果：`cold_stub_count=2114` 时，`l1_route_cold_ssd_bytes=311,296`（显著低于“每 stub 独占页”的空间成本）

## 尚未完成（下一步）

1. 阶段 C 继续收敛：
   - 当前 pack 已在 rebuilder 端落地，但 subtree rebuild 路径仍保留；
   - 下一步需要把“写放大优化”继续压到更新主路径，避免重复索引写工作。
2. 继续阶段 D：
   - 基于 `uniform vs prefix-skew` 已测数据，开始定量归因（以 `effective_delta_*`、`dirty_pack_pages`、`pack_write_bytes` 为主）。
   - 制定并落地下一轮“减少脏 pack 页规模”的优化改动，再做 A/B 回归。

## 当前总进度（V7）

- 设计与实现进度：约 **93%**
- 当前重点：route swap 实验归因 + 后台 drain 收敛
