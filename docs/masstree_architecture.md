# Masstree 子模块架构与交互（FlowKV 内置版）

本文档基于 `lib/masstree/` 目录的源码（以及 FlowKV 对其封装 `lib/masstree/masstree_wrapper.h`、适配层 `lib/index_masstree.h`），总结 Masstree 的模块构成、关键数据结构、以及 Get/Insert/Remove/Scan 的交互链路。

> 说明
> - 这里的 Masstree 是“分层 B+Tree / Trie 混合”的实现：每层是一个按 fixed-size **ikey**（如 8 字节）组织的 B+Tree；当 key 超过 ikey 容量时，叶子里会存储一个“下一层树”的指针（layer），继续在后续 suffix 上查找。
> - FlowKV 当前使用的 key/value 类型与线程上下文由 `MasstreeWrapper::table_params` 指定。

## 1. 目录与模块划分

### 1.1 对外入口与最小 API

- 表对象：`Masstree::basic_table<P>`（声明见 [lib/masstree/masstree.hh](../lib/masstree/masstree.hh#L58)）
  - `get(key, value, ti)`：只读查询（内部用 unlocked cursor）。
  - `scan/rscan(firstkey, matchfirst, scanner, ti)`：正向/反向扫描。

- 游标（cursor）：
  - `Masstree::unlocked_tcursor<P>`（声明见 [lib/masstree/masstree_tcursor.hh](../lib/masstree/masstree_tcursor.hh#L29)）
    - 读路径：`find_unlocked()`（实现见 [lib/masstree/masstree_get.hh](../lib/masstree/masstree_get.hh#L21)）。
  - `Masstree::tcursor<P>`（声明见 [lib/masstree/masstree_tcursor.hh](../lib/masstree/masstree_tcursor.hh#L94)）
    - 写路径：`find_locked()` / `find_insert()` / `finish()`（实现分布：
      - `find_locked()`：[lib/masstree/masstree_get.hh](../lib/masstree/masstree_get.hh#L71)
      - `find_insert()`：[lib/masstree/masstree_insert.hh](../lib/masstree/masstree_insert.hh#L21)
      - `finish()`：[lib/masstree/masstree_insert.hh](../lib/masstree/masstree_insert.hh#L178)
    )
    - 删除与 GC：`finish_remove()/remove_leaf()/gc_layer()`（实现见 [lib/masstree/masstree_remove.hh](../lib/masstree/masstree_remove.hh#L24)）。

### 1.2 核心数据结构（树节点）

核心节点类型定义在 [lib/masstree/masstree_struct.hh](../lib/masstree/masstree_struct.hh#L41)：

- `node_base<P>`：所有节点的基类，继承了 `nodeversion`（并发版本控制）。提供：
  - `parent()/set_parent()/make_layer_root()`
  - `reach_leaf(k, version, ti)`：自顶向下找到目标叶子（声明见 [lib/masstree/masstree_struct.hh](../lib/masstree/masstree_struct.hh#L87)）。

- `internode<P>`：B+Tree 内部节点（非叶），保存分隔 ikey 与 child 指针。
  - 布局要点：`ikey0_[width]` + `child_[width+1]` + `height_`。
  - 分裂逻辑：`internode::split_into()`（实现见 [lib/masstree/masstree_split.hh](../lib/masstree/masstree_split.hh#L96)）。

- `leaf<P>`：B+Tree 叶子节点，保存 key/value（或 layer 指针）。
  - 布局要点（见 [lib/masstree/masstree_struct.hh](../lib/masstree/masstree_struct.hh#L248) 起）：
    - `ikey0_[width]`：每个槽位的 ikey。
    - `keylenx_[width]`：每个槽位的 key 长度/类型编码。
    - `lv_[width]`：对应的值（`leafvalue`，可为真实 value 或 layer 指针）。
    - `permutation_`：kpermuter 编码的“逻辑顺序”，用于保持有序视图并降低搬移成本。
    - `prev_` / `next_`：叶子链表，用于 scan/rscan。
  - 叶子分裂：`leaf::split_into()`（实现见 [lib/masstree/masstree_split.hh](../lib/masstree/masstree_split.hh#L37)）。

- `leafvalue<P>`：叶子槽位的联合值（value 或 layer 指针）
  - `value()`：真实 value。
  - `layer()`：指向下一层树（在 leaf 里用 `keylenx_` 区分）。
  - 见 [lib/masstree/masstree_struct.hh](../lib/masstree/masstree_struct.hh#L195)。

### 1.3 Key 表示与比较

- `Masstree::key<I>`：把字符串 key 分解为 `ikey + suffix`，并支持“shift/unshift”表示下降到下一层（见 [lib/masstree/masstree_key.hh](../lib/masstree/masstree_key.hh#L35)）。
  - `ikey` 是经过 byte-swap 处理的整数，便于比较。
  - `shift()` 等价于对 suffix 重新构造 key，但更快，并可 `unshift_all()` 回退。

- 有序查找（lower/upper bound）：`ksearch.hh` 提供通用的二分/线性 bound（见 [lib/masstree/ksearch.hh](../lib/masstree/ksearch.hh#L39)）。

- `kpermuter<W>`：把槽位排列编码在一个整型里，维护“已用元素的有序序列 + 空闲槽位池”（见 [lib/masstree/kpermuter.hh](../lib/masstree/kpermuter.hh#L44)）。
  - 插入常用：`insert_from_back(i)`（从空闲池取一个槽位，插入逻辑序列第 i 位）。
  - 删除常用：`remove(i)` / `remove_to_back(i)`。

### 1.4 并发控制与内存回收

- `nodeversion`：每个节点的版本字，提供：
  - `stable()`：自旋等待 dirty 位清除，获取稳定视图。
  - `lock()/try_lock()/unlock()`：写路径锁。
  - `mark_insert()/mark_split()/mark_deleted()`：协助读者检测变化并重试。
  - 定义见 [lib/masstree/nodeversion.hh](../lib/masstree/nodeversion.hh#L21)。

- 叶子链表的并发正确性：`btree_leaflink` 用 CAS “标记 next 指针”实现 split/unlink（见 [lib/masstree/btree_leaflink.hh](../lib/masstree/btree_leaflink.hh#L23)）。

- 线程上下文与 RCU：`threadinfo` 提供 allocator、计数器、以及 epoch-based 回收（limbo list）。
  - 定义见 [lib/masstree/kvthread.hh](../lib/masstree/kvthread.hh#L100)。

## 2. 关键操作链路（交互/时序）

下面的“链路”是代码级的主路径抽象，忽略了部分边界条件（例如并发插入导致的重试、层 GC 的额外分支等）。

### 2.1 Get（只读查询）

入口：`basic_table::get()`（实现见 [lib/masstree/masstree_get.hh](../lib/masstree/masstree_get.hh#L55)）

主要链路：

1. 构造 `unlocked_tcursor(table, key)`。
2. `find_unlocked(ti)`（[lib/masstree/masstree_get.hh](../lib/masstree/masstree_get.hh#L21)）：
   - `root->reach_leaf(ka, v, ti)`：定位目标叶。
   - `perm = leaf.permutation()` + `lower_bound(ka)` 找槽位。
   - `ksuf_matches()` 检查 suffix：
     - 若命中且是 layer（返回 < 0）：`ka.shift_by(-match)` 并把 `root = lv.layer()`，回到 retry（进入下一层）。
     - 若命中且非 layer：返回 found。
   - 若 `has_changed(v)`：通过 `advance_to_key()` 追到更新后的叶并重试。

特性：
- 读路径无锁（unlocked），依赖 `nodeversion` + 重试保证一致性。

### 2.2 Insert/Upsert（写入）

入口（FlowKV 封装）：`MasstreeWrapper::insert()`（见 [lib/masstree/masstree_wrapper.h](../lib/masstree/masstree_wrapper.h#L232)）

主要链路：

1. `cursor_type lp(table_, key)`。
2. `lp.find_insert(ti)`（[lib/masstree/masstree_insert.hh](../lib/masstree/masstree_insert.hh#L21)）：
   - 先 `find_locked(ti)`（[lib/masstree/masstree_get.hh](../lib/masstree/masstree_get.hh#L71)）：
     - `reach_leaf()` + `lower_bound()` 定位。
     - 给叶子加锁 `leaf.lock(v)`；若版本/排列变化则 unlock + retry。
     - 若命中 layer：把该槽位 `lv_[p]` 改成“可达 root”（`maybe_parent()`），然后 retry（这是处理并发层根变化的关键点）。
   - 若 key 已存在（`state_ != 0`）返回 found。
   - 若槽位指向 layer：`make_new_layer()`（[lib/masstree/masstree_insert.hh](../lib/masstree/masstree_insert.hh#L107)）创建“twig”并把原值挂到下一层。
   - 否则尝试在当前 leaf 插入：
     - leaf 未满：`assign(p, ka)` 写入 key/value 槽位。
     - leaf 满：`make_split()`（[lib/masstree/masstree_split.hh](../lib/masstree/masstree_split.hh#L179)）执行 leaf split，并可能级联 split internode。
3. 调用者写值：`lp.value() = new_val`。
4. `lp.finish(+1, ti)`：把新槽位纳入 permutation（[lib/masstree/masstree_insert.hh](../lib/masstree/masstree_insert.hh#L178)）。

关键点：
- “有序视图”由 `permutation_` 决定，插入实际是：先找一个空槽位写数据，再更新 permutation 把该槽位放到逻辑序列正确位置。
- split 时会维护叶子链表（`btree_leaflink::link_split`）。

### 2.3 Remove（删除）

入口：`MasstreeWrapper::remove()`（封装文件同上；删除核心在 `tcursor`）

关键链路：

1. `find_locked()` 找到并锁住目标 leaf。
2. `finish(-1, ti)` 分支会走 `finish_remove()`（[lib/masstree/masstree_remove.hh](../lib/masstree/masstree_remove.hh#L162)）：
   - 从 `permutation_` 移除该 key 的逻辑位置。
   - 若 leaf 变空：调用 `remove_leaf()`（[lib/masstree/masstree_remove.hh](../lib/masstree/masstree_remove.hh#L179)）
     - 标记 leaf deleted + RCU free。
     - `btree_leaflink::unlink()` 从叶子链表移除。
     - 自底向上把该 leaf 从父 internode 中断开，必要时做 `redirect()` 修正边界 ikey，并可能回收空 internode。

### 2.4 Scan / RScan（范围扫描）

入口：`basic_table::scan/rscan`（实现见 [lib/masstree/masstree_scan2.hh](../lib/masstree/masstree_scan2.hh#L392) 与 [lib/masstree/masstree_scan2.hh](../lib/masstree/masstree_scan2.hh#L400)）。

实现框架：
- `scanstackelt` 持有当前叶节点、版本、permutation、以及一个 `node_stack_`（用于跨层下降/回退）。
- 扫描状态机：`scan_emit / scan_find_next / scan_down / scan_up / scan_retry`（定义见 [lib/masstree/masstree_scan2.hh](../lib/masstree/masstree_scan2.hh#L57)）。
- `forward_scan_helper` / `reverse_scan_helper` 通过“如何 lower_bound / 如何前进 leaf / 如何去重”来抽象正向与反向。

在 FlowKV 的封装里：
- `MasstreeWrapper::scan(start, cnt, vec)` 以“从 start 开始，返回最多 cnt 个 value”为主语义（见 [lib/masstree/masstree_wrapper.h](../lib/masstree/masstree_wrapper.h#L280)）。
- 扫描回调 `Scanner::visit_value(key, val, ti)` 决定何时提前停止。

## 3. FlowKV 接入点

- `MasstreeWrapper`：把 Masstree table + threadinfo 生命周期管理封装成简单接口（见 [lib/masstree/masstree_wrapper.h](../lib/masstree/masstree_wrapper.h#L65)）。
  - `table_params` 指定：`value_type=uint64_t`、`threadinfo_type=threadinfo`、以及 key 反序列化方式。
  - `get_ti()` 为每个线程 lazily 创建 `threadinfo`。

- `MasstreeIndex`：实现 FlowKV 的 `Index` 接口适配（见 [lib/index_masstree.h](../lib/index_masstree.h#L6)）。
  - `Get/Put/Delete/Scan*` 直接调用 wrapper。

## 4. 读写一致性与潜在注意点（面向接入/改造）

- **读路径无锁 + 重试**：`unlocked_tcursor::find_unlocked` 依赖 `nodeversion::stable/has_changed`，读到变化会追到新叶并重试。
- **写路径锁粒度在 leaf/internode**：`find_locked` 对 leaf 加锁；split 会“hand-over-hand locking”向上处理父 internode。
- **多层（layer）是 Masstree 的核心**：
  - 当 key 超过 ikey，leaf 槽位会保存下一层 root 指针（`leafvalue::layer()`），读写都会 `shift()` 到 suffix 继续。
  - 删除时可能触发层 GC（`gc_layer()`）。
- **扫描实现是状态机 + 层栈**：既能跨叶子链表前进，也能在遇到 layer 时下钻/回退。

---

### 参考源码入口（快速跳转）

- 表接口：`basic_table` [lib/masstree/masstree.hh](../lib/masstree/masstree.hh#L58)
- 读游标：`unlocked_tcursor::find_unlocked` [lib/masstree/masstree_get.hh](../lib/masstree/masstree_get.hh#L21)
- 写游标：`tcursor::find_insert/finish` [lib/masstree/masstree_insert.hh](../lib/masstree/masstree_insert.hh#L21)
- 分裂：`tcursor::make_split` [lib/masstree/masstree_split.hh](../lib/masstree/masstree_split.hh#L179)
- 删除：`tcursor::remove_leaf` [lib/masstree/masstree_remove.hh](../lib/masstree/masstree_remove.hh#L179)
- 扫描：`basic_table::scan/rscan` [lib/masstree/masstree_scan2.hh](../lib/masstree/masstree_scan2.hh#L392)
- 版本/锁：`nodeversion` [lib/masstree/nodeversion.hh](../lib/masstree/nodeversion.hh#L21)
- 线程/RCU：`threadinfo` [lib/masstree/kvthread.hh](../lib/masstree/kvthread.hh#L100)
