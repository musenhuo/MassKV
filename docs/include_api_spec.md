# FlowKV `include/` 接口规范说明

> 范围：仅覆盖 `FlowKV/include` 下公开头文件中的“接口定义”（宏/常量、类型别名、枚举、结构体/类、成员变量、成员函数声明）。
> 
> 说明风格：尽量基于源码可见信息进行“语义约束化”描述；对实现细节、持久化格式等无法从头文件确定的部分，会明确标注为“实现决定”。

---

## include/config.h

### 编译期常量/宏

- `MAX_MEMTABLE_NUM`
  - **含义**：内存表（memtable）最大数量。
  - **用途**：用于静态数组维度、状态数组维度。
  - **约束**：为编译期常量；修改会影响内存占用与并发逻辑。

- `MAX_MEMTABLE_ENTRIES`
  - **含义**：memtable 容量上限（以 entries 计）。
  - **用途**：实现侧用于触发 flush/compaction 的阈值或容量估算（具体策略由实现决定）。

- `MAX_L0_TREE_NUM`
  - **含义**：L0 层 tree 数量上限。
  - **备注**：注释指出“仅上限”，实际触发由 `db->l0_compaction_tree_num_` 控制。

- `MAX_USER_THREAD_NUM`
  - **含义**：用户线程最大数量。
  - **用途**：线程状态数组、client 列表等固定维度。

- `RANGE_PARTITION_NUM`
  - **含义**：范围分区数量。
  - **用途**：分区信息数组维度。

### 特性开关宏

- `MASSTREE_MEMTABLE` / `MASSTREE_L1`
  - **含义**：启用 Masstree 作为 memtable / L1 相关实现的编译期选择。
  - **影响面**：影响 `Index` 的具体实现类选择（实现文件中决定）。

- `INDEX_LOG_MEMTABLE` / `BUFFER_WAL_MEMTABLE`
  - **含义**：注释表明“由 CMakeLists.txt 定义”；用于选择不同的 memtable 日志策略。

- `KV_SEPARATE`
  - **含义**：当定义 `INDEX_LOG_MEMTABLE` 时自动开启；表示 value 可能采用“分离存储/指针化”的布局（具体格式由实现决定）。

### 类 `MYDBConfig`

#### 角色

- **配置对象**：承载数据库实例初始化时所需的存储路径、容量、恢复开关等参数。

#### 构造/析构

- `MYDBConfig(std::string pm_pool_path)`
  - **功能**：用指定的持久化内存池路径初始化配置。
  - **参数**：
    - `pm_pool_path`：持久化存储池路径。
  - **副作用**：覆盖默认的 `pm_pool_path` 成员。

- `MYDBConfig()`
  - **功能**：默认构造；使用类内默认值。

- `~MYDBConfig()`
  - **功能**：默认析构；无资源释放语义。

#### 成员变量

- `std::string pm_pool_path`
  - **含义**：持久化存储池路径。
  - **默认值**：`"/dev/nvme1n1"`（实现/部署相关，可能需要按环境调整）。

- `size_t pm_pool_size`
  - **含义**：持久化存储池大小。
  - **默认值**：`500ul << 30`（约 500 GiB）。

- `bool recover`
  - **含义**：是否进行恢复流程（例如基于日志/WAL/manifest 重建状态，具体由实现决定）。
  - **默认值**：`false`。

---

## include/slice.h

### 类 `Slice`

#### 角色

- **轻量视图类型**：`Slice` 表示对外部连续字节序列的一段只读引用（指针 + 长度）。
- **所有权**：不拥有底层内存；调用方必须保证底层存储在 `Slice` 生命周期内有效。

#### 线程安全

- 头文件注释声明：多个线程可并发调用 `const` 方法；若存在任意线程调用非 `const` 方法（如 `clear/remove_prefix`），则访问同一对象需外部同步。

#### 构造/赋值

- `Slice()`
  - **功能**：构造空 slice（长度 0，指向空字符串常量）。

- `Slice(const char* d, size_t n)`
  - **功能**：引用区间 `[d, d+n)`。
  - **前置条件**：`d` 指向至少 `n` 字节可读内存。

- `Slice(const std::string& s)`
  - **功能**：引用 `s.data()` 的内容。
  - **注意**：`Slice` 不拷贝数据；若 `s` 被销毁或修改可能导致悬垂/内容变化。

- `Slice(const char* s)`
  - **功能**：引用 C 字符串 `s`，长度为 `strlen(s)`。

- `Slice(const uint64_t* n)`
  - **功能**：将 `n` 指向的 8 字节视作 slice。
  - **用途**：常用于将整数“按字节序列”作为 key/value 传递。

- `static Slice BswapUint64(uint64_t* n)`
  - **功能**：对 `*n` 做 64 位字节序交换后，返回引用该 8 字节的 slice。
  - **副作用**：会原地修改 `*n`。

- 拷贝/赋值：`Slice` 可拷贝，默认语义为浅拷贝（仅复制指针与长度）。

#### 访问与工具函数

- `const char* data() const`
  - **功能**：返回数据指针。

- `size_t size() const`
  - **功能**：返回长度（字节数）。

- `bool empty() const`
  - **功能**：长度是否为 0。

- `char operator[](size_t n) const`
  - **功能**：返回第 `n` 个字节。
  - **前置条件**：`n < size()`，内部 `assert` 检查。

- `void clear()`
  - **功能**：重置为 empty slice。

- `void remove_prefix(size_t n)`
  - **功能**：丢弃前缀 `n` 字节。
  - **前置条件**：`n <= size()`，内部 `assert`。
  - **后置条件**：`data()` 前移 `n`，`size()` 减少 `n`。

- `std::string ToString() const`
  - **功能**：拷贝并返回当前 slice 的内容。

- `int compare(const Slice& b) const`
  - **功能**：三路比较（按字典序），返回 `<0/==0/>0`。

- `bool starts_with(const Slice& x) const`
  - **功能**：判断 `x` 是否为当前 slice 的前缀。

- `uint64_t ToUint64() const`
  - **功能**：将 `data()` 处的前 8 字节按主机字节序解释为 `uint64_t`。
  - **前置条件**：`size() >= 8` 且 `data()` 满足对齐/可读要求（否则行为由平台决定）。

- `uint64_t ToUint64Bswap() const`
  - **功能**：读取 8 字节为 `uint64_t` 后做字节序交换。

#### 非成员运算符

- `operator==(const Slice&, const Slice&)` / `operator!=`
  - **功能**：长度与字节内容相等性比较。

#### 全局常量

- `MAX_UINT64`
  - **含义**：`uint64_t` 最大值常量。

- `MAX_INT32`
  - **含义**：`int32_t` 最大值常量。

- `INVALID_SLICE`
  - **含义**：用 `MAX_UINT64` 的 8 字节表示的 slice。
  - **用途**：作为哨兵值（具体语义由上层约定）。

---

## include/log_format.h

### 结构体 `LSN`

#### 角色

- **只读序列号**：用于标识日志顺序（注释："64bit only-read log sequence number"）。

#### 字段（位域）

- `uint64_t epoch : 16`
  - **含义**：epoch 编号（可能用于分代/周期切换；具体策略由实现决定）。

- `uint64_t lsn : 30`
  - **含义**：epoch 内的序列号。

- `uint64_t padding : 18`
  - **含义**：填充位；用于对齐或预留。

> 备注：位域布局与端序/ABI 相关；持久化/网络传输时不应直接按内存布局序列化，除非实现明确保证。

### 枚举 `OpCode : uint16_t`

- `NOP`：无操作（占位）。
- `INSERT`：插入/写入语义。
- `DELETE`：删除语义。

### 枚举 `LogType : uint16_t`

用于区分日志 entry 物理格式或 key/value 编码方式：

- `ERROR`：错误/非法类型。
- `LogEntryFixK32/64/128/256`：固定 key 尺寸的日志格式（尺寸指 key 的最大/固定字节数，具体由实现决定）。
- `LogEntryVarK64/128/256`：变长 key 的日志格式。

### 结构体 `LogEntry32`

#### 角色

- **固定大小日志 entry（32B 或 64B）**：注释表明该结构可表示 32B 或 64B 两种形态；具体取决于 `value_sz` 以及实现如何扩展存储。

#### 字段

- `uint32_t valid : 1`
  - **含义**：entry 是否有效。

- `uint32_t lsn : 31`
  - **含义**：与该 entry 关联的序列号（31 位）。

- `uint16_t key_sz`
  - **含义**：key 字节长度。

- `uint16_t value_sz`
  - **含义**：value 字节长度。
  - **约束注释**：
    - `0~16`：32B log entry
    - `17~48`：64B log entry
    - `>48`：变长 entry

- `uint64_t key`
  - **含义**：key 内容（当前定义为 64 位承载；当 `key_sz > 8` 时如何编码由实现决定）。

- `union { uint64_t value_addr; char value[8]; }`
  - **含义**：
    - `value_addr`：value 的地址/指针编码（当 value 分离存储或采用指针化编码时）。
    - `value[8]`：内嵌 value 的前 8 字节；注释指出可依 `value_sz` 扩展更多后缀。

### 结构体 `LogEntry64`

- **角色**：固定大小 64B 日志 entry。
- **字段**：
  - `valid(1bit)`、`lsn(31bit)`、`key_sz`、`value_sz`、`key` 与 `LogEntry32` 含义一致。
  - `char value[48]`：内嵌 value 或 value_ptr（注释：最多 32 字节 value 或 value 指针）。

### 结构体 `LogEntryVar64`

- **角色**：变长日志 entry（尾部 `value[]` 为柔性数组）。
- **字段**：
  - 头部字段与 `LogEntry64` 一致。
  - `char value[]`：变长 payload，长度由 `value_sz`（以及可能的 key/value 编码方案）决定。
- **约束**：该结构必须按“实际分配大小”使用，不能对其做 `sizeof(LogEntryVar64)` 假设来访问尾部。

---

## include/db_common.h

### 宏/常量

- `ERROR_CODE`
  - **默认值**：`0xffffffffffffffffUL`。
  - **用途**：通用错误返回值哨兵（具体由实现/调用方约定）。

- `INVALID_PTR`
  - **默认值**：`0xffffffffffffffffUL`。
  - **用途**：无效指针/地址编码哨兵。

- `VALUESIZE`
  - **含义**：value 的固定大小（字节）。
  - **取值**：
    - 未定义 `KV_SEPARATE`：`8`
    - 定义 `KV_SEPARATE`：`24`
  - **用途**：日志/缓冲区布局与计算。

- `LogNumperBlock`
  - **含义**：每个 log buffer block 中可容纳的日志条目数量。
  - **定义**：`logbuffersize/(VALUESIZE+16)`。
  - **注意**：依赖外部符号 `logbuffersize`（应在其他头文件或编译单元中定义）。

### 类型别名与常量

- `using KeyType = uint64_t;`
  - **含义**：索引层 key 的类型。

- `using ValueType = uint64_t;`
  - **含义**：索引层 value 的类型（通常为地址/指针编码或内联值）。

- `static constexpr ValueType INVALID_VALUE = 0;`
  - **含义**：无效 value 哨兵。

- `static constexpr uint64_t SEGMENT_SIZE = 4ul << 20;`
  - **含义**：segment 大小（4 MiB）。

### 联合体 `ValuePtr`

#### 角色

- **压缩指针编码**：将 `valid`、`ptr`、`lsn` 打包进一个 `uint64_t`。

#### 字段（`detail_` 位域）

- `valid : 1`
  - **含义**：该指针编码是否有效。

- `ptr : 33`（或 `34`，取决于 `KV_SEPARATE`）
  - **含义**：指针/偏移字段。
  - **注释含义**：假设 64B 对齐（丢弃低 6 位），剩余位数用于索引较大地址空间（注释给出 512GB 或 1TB 的估算）。

- `lsn : 30`（或 `29`）
  - **含义**：与该 value 对应的序列号/版本号片段。

- `uint64_t data_`
  - **含义**：整体 64 位原始编码。

> 约束：位域布局、对齐假设（64B）是编码成立的关键前提；改变对齐/地址空间策略会影响可寻址范围与解析规则。

### 结构体 `ValueHelper`

#### 角色

- **Put/Update 辅助对象**：在 `Index::Put/PutValidate` 等 API 中承载新旧 value、有效标记等信息。

#### 字段

- `ValueType new_val`
  - **含义**：新写入的 value 编码。

- `ValueType old_val`
  - **含义**：旧 value 编码。
  - **注释**：`gc put` 作为入参/出参；`db put` 作为出参（具体逻辑由实现决定）。

- `bool valid`
  - **含义**：辅助对象是否携带有效信息（例如 put 是否成功、old_val 是否可用等；实现决定）。

#### 构造

- `ValueHelper(ValueType _new_val)`
  - **功能**：用新 value 初始化 helper。

### 抽象类 `Index`

#### 角色

- **索引接口**：为不同索引结构（如 Masstree、CCEH、HOT 等）提供统一访问入口。

#### 生命周期

- `virtual ~Index()`：虚析构，允许通过基类指针释放派生类。

#### 线程相关

- `virtual void ThreadInit(int thread_id) = 0;`
  - **功能**：线程级初始化（如线程本地缓存、epoch 注册等）。
  - **调用时机**：通常应在该线程首次调用 `Get/Put/Delete/Scan` 前调用。

#### 点查/更新

- `virtual ValueType Get(const KeyType key) = 0;`
  - **功能**：根据 key 获取对应的 value 编码。
  - **返回**：若不存在通常返回 `INVALID_VALUE`（具体约定由实现决定）。

- `virtual void Put(const KeyType key, ValueHelper &le_helper) = 0;`
  - **功能**：写入或更新 key。
  - **入/出参**：`le_helper` 作为输入携带 `new_val`，并可能输出 `old_val/valid` 等。

- `virtual void PutValidate(const KeyType key, ValueHelper &le_helper) = 0;`
  - **功能**：带校验语义的写入（例如 CAS/验证旧值/验证版本等；具体由实现决定）。

- `virtual void Delete(const KeyType key) = 0;`
  - **功能**：删除 key。

#### 范围查询（默认不支持）

- `virtual void Scan(const KeyType key, int cnt, std::vector<ValueType> &vec)`
- `virtual void Scan2(const KeyType key, int cnt, std::vector<uint64_t> &kvec, std::vector<ValueType> &vvec)`
- `virtual void ScanByRange(const KeyType start, const KeyType end, std::vector<uint64_t> &kvec, std::vector<ValueType> &vvec)`

默认实现会调用 `ERROR_EXIT("not supported in this class")` 直接终止（行为由 `util/debug_helper.h` 定义）。派生类若支持范围扫描，需要覆盖这些方法。

### 结构体 `FilePtr`

#### 角色

- **文件位置编码**：将 `(fd, offset)` 打包为一个 `uint64_t` 值，最高位设置为 1，用于区分其他编码类型（例如内存地址/PM 地址等）。

#### 字段

- `int fd`：文件描述符。
- `int offset`：文件内偏移（单位由实现决定，通常为字节）。

#### 成员函数

- `uint64_t data()`
  - **功能**：将 `fd/offset` 打包为 `uint64_t`。
  - **编码**：`(1ULL<<63) | ((uint64_t)fd<<32) | offset`。

- `FilePtr(int _fd, int _offset)`
  - **功能**：用 `fd/offset` 构造。
  - **注意**：当前实现中 `offset = _fd;`（而非 `_offset`）。这看起来像实现错误；若该类型被实际使用，建议在实现侧确认并修正。

- `FilePtr(uint64_t data)`
  - **功能**：从打包值解析 `fd/offset`。

- `static FilePtr InvalidPtr()`
  - **功能**：返回无效位置（`{-1,-1}`）。

- `bool Valid()`
  - **功能**：判断是否有效。
  - **条件**：`fd >= 0 && offset > 0`。

- `bool operator==(FilePtr b)`
  - **功能**：比较 fd 与 offset 是否相等。

### 结构体 `PartitionInfo`

- `size_t min_key`：分区最小 key。
- `size_t max_key`：分区最大 key。

> 语义：用于描述一个范围分区的 key 边界（闭区间/开区间由使用方约定）。

---

## include/db.h

### 前置声明

- `MYDBClient`, `SegmentAllocator`, `LogWriter`, `LogReader`, `PSTReader`, `Version`, `Manifest`, `ThreadPoolImpl`
  - **含义**：在该头文件中仅声明类型名，用于指针成员与方法声明；其定义位于实现侧或其他头文件中。

### 结构体 `MemTableStates`

#### 角色

- **memtable 状态 + 线程写入状态**：用于描述某个 memtable 当前阶段，以及用户线程是否正在对其进行写入。

#### 枚举 `state`

- `ACTIVE`：可写状态。
- `FREEZE`：冻结状态（通常意味着不再接收新写入，等待 flush；具体由实现决定）。
- `EMPTY`：空/不可用状态。

#### 字段

- `state`：默认 `EMPTY`。
- `bool thread_write_states[MAX_USER_THREAD_NUM]`
  - **含义**：按线程标记写入状态。
  - **约束**：数组大小受 `MAX_USER_THREAD_NUM` 限制。

### 类 `MYDB`

#### 角色

- **数据库引擎主对象**：管理 memtable、日志、版本/manifest、后台 flush/compaction 线程池与运行模式。

#### 关键设计点（从头文件可见信息推断）

- 使用多个 memtable（数组 `mem_index_[MAX_MEMTABLE_NUM]`）。
- 使用原子变量记录临时大小、flush/compaction 进行状态。
- 通过线程池和一个触发线程进行后台工作。

#### 构造/析构

- `MYDB()` / `~MYDB()`
  - **功能**：创建/销毁数据库对象。
  - **资源管理**：涉及线程池、日志/分配器等指针成员；具体释放策略在实现中定义。

#### 客户端获取

- `std::unique_ptr<MYDBClient> GetClient(int tid = -1)`
  - **功能**：为指定线程 id 获取一个 `MYDBClient`（客户端句柄）。
  - **参数**：
    - `tid`：线程 id；`-1` 的含义由实现决定（可能为自动分配）。
  - **返回**：独占所有权的 client。
  - **并发**：内部存在 `client_lock_`，表明可能在这里做注册/分配并发保护。

#### 恢复与 LSN（私有）

- `bool RecoverLogAndMemtable()`
  - **功能**：恢复日志与 memtable 状态（实现决定具体范围）。

- `LSN GetLSN(uint64_t i_key)`（仅 `INDEX_LOG_MEMTABLE`）
  - **功能**：获取与 `i_key` 映射的 LSN（可能通过 `lsn_map_` 分桶）。

- `LSN LSN_lock(uint64_t i_key)` / `void LSN_unlock(size_t epoch)`（仅 `BUFFER_WAL_MEMTABLE`）
  - **功能**：与 WAL/LSN 相关的锁定与解锁（从命名推断为分桶自旋锁）。

#### memtable 大小管理（私有）

- `size_t GetMemtableSize(int idx)` / `void ClearMemtableSize(int idx)`
  - **功能**：读取/清空某 memtable 的大小统计。

- `void AddTempMemtableSize(int idx, size_t size)`
  - **功能**：对临时大小计数器执行原子累加。

#### 后台工作（当前在 public 区域，注释标为“TODO: change to private”）

- `bool MayTriggerFlushOrCompaction()` / `bool MayTriggerCompaction()`
  - **功能**：检查并决定是否触发 flush/compaction。

- `bool BGFlush()` / `bool BGCompaction()`
  - **功能**：执行一次后台 flush/compaction。

- `void WaitForFlushAndCompaction()`
  - **功能**：阻塞等待 flush/compaction 完成。

- `void PrintLogGroup(int id)` / `void PrintSSDUsage()`
  - **功能**：调试/监控输出（具体输出内容由实现决定）。

- `static void PrintLogGroupInMYDB(void* arg)`
- `static void TriggerBGFlush(void* arg)`
- `static void TriggerBGCompaction(void* arg)`
  - **功能**：线程池/线程入口函数封装。
  - **参数约定**：分别通过 `TestParams/FlushArgs/CompactionArgs` 结构传参。

#### 运行模式控制

- `EnableReadOptimizedMode()` / `DisableReadOptimizedMode()`
  - **功能**：切换“读优化模式”。
  - **可见副作用**：设置 `read_optimized_mode_` 并输出日志（`LOG(...)`）。

- `EnableReadOnlyMode()` / `DisableReadOnlyMode()`
  - **功能**：切换“只读模式”。
  - **可见副作用**：设置 `read_only_mode_` 并输出日志。

#### 重要成员变量（接口视角）

- `std::string db_path_`：数据库路径。
- `SegmentAllocator* segment_allocator_`：段分配器。
- `Index* mem_index_[MAX_MEMTABLE_NUM]`：memtable 索引实例数组。
- `MemTableStates memtable_states_[MAX_MEMTABLE_NUM]`：memtable 状态数组。
- `std::atomic_uint64_t temp_memtable_size_[MAX_MEMTABLE_NUM]`：临时大小统计。
- `int current_memtable_idx_`：当前 active memtable 索引。
- `ThreadPoolImpl* thread_pool_ / flush_thread_pool_ / compaction_thread_pool_`：后台线程池。
- `std::thread* bgwork_trigger_`：后台工作触发线程。
- `Version* current_version_` / `Manifest* manifest_`：版本与元数据管理。
- `PartitionInfo partition_info_[RANGE_PARTITION_NUM]`：范围分区信息。
- `MYDBClient* client_list_[MAX_USER_THREAD_NUM]`：client 列表。

> 备注：大量成员当前暴露在 `public`（标注 TODO），从接口稳定性角度建议未来收敛到 `private/protected` 并提供受控访问器。

### 类 `MYDBClient`

#### 角色

- **线程绑定客户端**：对外提供 Put/Get/Delete/Scan 等操作入口；绑定 `thread_id_`，并持有线程相关的日志读写器、memtable 视图等。

#### 构造/析构

- `MYDBClient(MYDB* db, int tid)`
  - **功能**：为 `tid` 线程创建 client，并绑定到 `db`。

- `~MYDBClient()`
  - **功能**：销毁 client，释放其持有的日志/reader 等资源（具体由实现决定）。

#### 读写接口

- `bool Put(const Slice key, const Slice value, bool slow = false)`
  - **功能**：写入/更新 key-value。
  - **参数**：
    - `key/value`：以 `Slice` 传入的字节序列。
    - `slow`：慢路径开关（具体语义由实现决定，例如强制持久化/同步/绕过 cache 等）。
  - **返回**：写入是否成功。

- `bool Get(const Slice key, Slice& value_out)`
  - **功能**：读取 value。
  - **输出**：`value_out` 用于承载读取结果（具体内存归属与生命周期由实现决定；调用方需确认是否需要拷贝）。
  - **返回**：是否找到/成功读取。

- `bool Delete(const Slice key)`
  - **功能**：删除 key。
  - **返回**：删除是否成功（或是否存在并被删除；实现决定）。

- `int Scan(const Slice start_key, int scan_sz, std::vector<uint64_t>& key_out)`
  - **功能**：从 `start_key` 开始扫描 `scan_sz` 条目。
  - **输出**：将 key 填入 `key_out`。
  - **返回**：实际返回条目数（从函数签名推断；精确定义以实现为准）。

- `void Persist_Log()`
  - **功能**：触发日志持久化（例如刷盘/刷 PM；具体由实现决定）。

- `const int thread_id_`
  - **含义**：该 client 绑定的线程 id。

#### 内部辅助（私有）

- `bool GetFromMemtable(const Slice key, Slice& value_out)`
  - **功能**：从 memtable 命中读取。

- `inline bool StartWrite()` / `inline void FinishWrite()`
  - **功能**：写入开始/结束的协作逻辑。
  - **注释**：`StartWrite` 会根据 `db_->current_memtable_idx_` 更新 `current_memtable_idx_`，并返回是否发生变化。

- 写入统计：
  - `size_t put_num_in_current_memtable_[MAX_MEMTABLE_NUM]`
  - `GetMemtablePutCount/ClearMemtablePutCount`
  - `total_writes_/total_reads_` 与 `GetWriteRatioAndClear()`

用于工作负载监测与触发策略（如 flush/compaction/模式切换；实现决定）。

---

## 备注：可从头文件直接看出的潜在风险点（不改变代码，仅提示）

- `FilePtr(int _fd, int _offset)` 构造函数中 `offset = _fd;` 可能是笔误；若该类型参与持久化/编码解析，可能导致严重定位错误。
- 位域（`LSN`、`ValuePtr`、log entry 中 `valid/lsn`）在跨编译器/ABI/端序场景下不可移植；如需持久化格式稳定性，应在实现侧明确采用按字节序列化与解码。
