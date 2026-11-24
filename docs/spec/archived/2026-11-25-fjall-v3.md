# Fjall 3 存储格式实现规格

Feature: `docs/feature/fjall-v3.md` @ `23aa3f1d7af9033894116935d045d64d7703f74a`

开始或恢复实现前，核对 `git hash-object docs/feature/fjall-v3.md`。每步一个可构建、通过测试的提交，末行使用 `Spec-Step: fjall-v3#<n>`。实现期间保持此规格固定。最后一步将 feature 状态改为 `shipped`，用 `git mv` 将本文件移入 `docs/spec/archived/<YYYY-MM-DD>-fjall-v3.md`。

## 基线与目标

远端 `main@62867fe7` 使用 Rust 1.97.1，工作区声明 `fjall = "2.4"`，锁定 2.11.2。目标是 crates.io 的稳定版 **3.1.13**（MSRV 1.90），工作区声明 `fjall = "3.1.13"` 并提交锁文件，保留默认 LZ4。在同一基线上，Dependabot 头提交 `934fa903` 只改了两个 Cargo 文件。对该提交独立执行 `cargo check --workspace --all-targets --locked` 失败，首批错误包括存储和 blob 引用已移除的 `PartitionHandle`、内核引用 `fjall::Batch`。实施从 main 取代码，只更新 Fjall 和所需的传递依赖。[Issue #8](https://github.com/meritozh/mmdb/issues/8) · [PR #6](https://github.com/meritozh/mmdb/pull/6) · [发布包](https://docs.rs/crate/fjall/3.1.13)

### 接入面

| 目录 / 入口 | 创建和持有数据库的模块 | 需要适配的调用 |
| --- | --- | --- |
| `Store::{create,open}` 的受管根 | `mmdb-kernel::Kernel` | 内核元数据、按 era 命名的分区、跨模块批次、租约及重置恢复；原生记忆、context、session 继续走内核 |
| `Database::{open,open_with,builder}` 的多模型根 | `mmdb-storage::Storage` | 同一引擎句柄交给节点、图、向量以及 facade 的 access、audit、lexical、runtime、state、index_repair；检查 query/catalog 重建等间接扫描方 |
| 多模型根下的 `blob-meta/`，也可由 `BlobStore` 独立打开 | `mmdb-blob::MetaStore` | 元数据 keyspace `m`；`blobs/` 文件、引用计数、普通打开与显式 repair 的先后顺序 |
| `StateStore::open_path` 的独立根 | `mmdb::state` | 调用方格式 ID、租约、CAS、分页、稀疏队列和持久化确认；`Database::state()` 复用多模型句柄 |

六个直接依赖者是 `mmdb-kernel`、`mmdb-storage`、`mmdb-graph`、`mmdb-vector`、`mmdb-blob`、`mmdb`。同步更新直接构造引擎的测试和 `recall_bench` 示例。保持现有 [存储内核分层测试](../../crates/mmdb/tests/layering.rs) 通过。统一运行状态与记忆数据模型仍由[存储内核目标](../polaris/storage-kernel.md)分期交付。

## 打开、拒绝和重置

**受管 Store。** 将应用格式 ID 从 `mmdb-store-v1` 改为 `mmdb-store-v2`，标记的是 MMDB 的第二代磁盘契约。持有原有 `StoreLease` 后先校验清单，再打开引擎。旧 ID 返回现有 `FormatMismatch`，包含预期与实际 ID；清单缺失、损坏、路径被占用沿用各自错误。受管根只有新清单、引擎尚未创建时，允许完成初始化，这也覆盖整库重置刚发布替换目录的状态。

**其余入口。** 独立 StateStore 继续接受调用方的 `StoreFormatDescriptor`。即使该 ID 相同，也要识别旧引擎。多模型库没有这份应用清单，而 `blob-meta/` 是第二个物理数据库。由 `mmdb-kernel` 提供一处只读引擎格式检查，`Storage`、独立状态与 blob 元数据打开方复用。只探测目标引擎目录的 `version` 文件头，2.11.2、3.1.13 分别为 `FJL\x02`、`FJL\x03`；已识别的版本交给对应策略，未知、截断或读取失败带路径返回。检查格式和新建资格是两件事，后者由知道目录用途的入口判断。[3.1.13 打开与恢复源码](https://docs.rs/crate/fjall/3.1.13/source/src/db.rs) · [格式头](https://docs.rs/crate/fjall/3.1.13/source/src/version.rs)

| 发现的状态 | 打开行为 |
| --- | --- |
| 路径不存在、允许初始化的空目录，或合法的新受管清单 | 按原入口的 create/open 契约初始化。普通 `Store::open` 仍要求已有正确清单 |
| 有 v2 格式头 | 报告实际路径、版本与另建空库的办法。受管根还可走显式重置 |
| 标记损坏、已有引擎文件却缺标记，或仅剩没有元数据的 blob 数据 | 拒绝采用，保留现场。完整 v3 引擎在自身恢复阶段报出的损坏继续向上传递 |
| 新版多模型根配旧版 `blob-meta/`，或反过来 | 在打开任一引擎、建布局、索引重建或孤儿清理**之前**检查两处。直接调用 `BlobStore::open` 或 `open_with_repair` 时，检查元数据目录，以及父根中已存在的引擎标记；独立 blob 根无需创建主引擎 |

整库重置使用已有 `ResetPlan::build`、完整 digest 与 `commit_reset`。源 ID 取旧清单，替换 ID 取新描述，旧目录整体进入 receipt 指向的隔离位置。独立状态可继续用调用方 ID，通过新目录的引擎区分。无 MMDB 清单的多模型库和独立 blob 库使用另一个空根，原根留给原版程序读取。全程无 v2→v3 转换、双读或真实用户数据操作。v3 的**模块**重置仍在一次同步批次内切换 era、写退休标记和业务行，随后删除旧 keyspace；它与整库隔离有不同的确认和恢复契约。

## v3 接口与采用位置

| v2 调用或假设 | 3.1.13 实现 | 必须保住的行为 |
| --- | --- | --- |
| `Config::new(...).open()`、根 `Keyspace`、`PartitionHandle` | `fjall::Database::builder(root).open()?`、`fjall::Database`、`fjall::Keyspace`；`db.keyspace(name, KeyspaceCreateOptions::default)?` 的第二参是创建闭包 | 一个物理根只打开一次，再分发克隆。区分 `mmdb::Database` 和引擎句柄；业务分区名及编码继续由原模块拥有 |
| `list_partitions`、`partition_exists`、`delete_partition` | `list_keyspace_names`、`keyspace_exists`、`delete_keyspace(handle)` | 按逻辑名验证 schema 和 era；删除后句柄可能延迟物理回收。旧句柄在 MMDB 层立即失效，清理中断可重开续做 |
| 内核 `fjall::Batch`；若干写入先 commit 再 persist | 需要写出类型名时用 `fjall::OwnedWriteBatch`；需要同步确认的批次用 `db.batch().durability(Some(PersistMode::SyncAll))` | 保留操作级锁、修订比较、回执与 `CommitUnknown`。显式 `confirm_durable` 保留数据库级屏障。不同数据库、blob 文件、HNSW 内存状态各自保留恢复协议 |
| 两个 `snapshot_at` 手工对齐 seqno | 一次 `db.snapshot()`，通过 `fjall::Readable` 在同一快照对指定 keyspace 做 range/get | `scan_by_time`、`nodes_by_metadata`、`neighbours_in` 的一次索引连接只能见到一个提交版本。元数据哈希冲突仍须读值验证；标签过滤亦须验证原文 |
| 迭代项 `Result<KvPair>` | 迭代项为 `Guard`，在 `key()?`、`value()?` 或 `into_inner()?` 时传播 I/O 错误 | 时间索引、图反向索引/标签字典、向量墓碑只取键；audit 与 lexical 的纯值扫描只取值。需要同时校验键和值的路径保留两者；任何方向的迭代错误均不得当成结束 |
| 从 `PartitionHandle::path()` 上溯两层定位 HNSW | 由创建多模型库的一端把 MMDB 根或明确的快照目录传给 `VectorStore`；同步调整独立构造器、调用方与示例 | 默认目录为 `<root>/vector_hnsw_snapshots`，显式覆盖仍有效。有效快照重载，损坏时从向量行重建；Fjall 分配的内部 keyspace ID 不参与路径计算 |

节点时间查询的引擎侧调用如下，`lo..hi`、tenant 和条数上限仍使用现有规则。示例只列出连接两侧的读取。

```rust
use fjall::Readable;

let view = db.snapshot();
for row in view.range(&nodes_by_time, lo..hi) {
    let key = row.key()?;
    if let Some(id) = keys::id_from_time_key(&key) {
        let bytes = view.get(&nodes, keys::node_key(tenant, id))?;
        // 交给现有解码和 limit 逻辑。
    }
}
```

**按键回收大载荷。** 在现有 `mmdb_kernel::Partition` 增加一个有实际调用方的窄接口。实现复用 `PartitionScan`，向前或向后取下一项、读取 guard，都在本次 era 检查保护内；返回后释放读锁。首次 stale、poison 或 I/O 错误返回一次，再终止。上层收集删除键时无需持有引擎 guard，也无需分配正文。

```rust
pub fn prefix_keys(&self, prefix: impl AsRef<[u8]>)
    -> impl DoubleEndedIterator<Item = KernelResult<Bytes>> + '_;
```

`records::payload::recover_payloads` 改用 `table.payloads.prefix_keys(prefix)`，保留块数、上传数和 `more` 的判断；用于判断还有下一块的探测同样处理读取错误。读取正文仍走 `get` 并验证摘要。写入、键序、反向扫描及会话可见性通过现有公共接口验证。[迭代器 Guard 源码](https://docs.rs/crate/fjall/3.1.13/source/src/guard.rs)

**按数据用途配置值分离。** 两个存储模块的 `records_payloads_v1` 都存放带摘要的 32 KiB 正文块。分区声明携带存储用途，内核在首次创建及准备新 era 时将其翻译为创建选项。普通分区使用默认选项。模块声明复用 `records::scoped` 的载荷名；内核只认识布局，不能按 `"session"`、`"memory"` 或名称后缀猜测。

```rust
let options = || KeyspaceCreateOptions::default().with_kv_separation(Some(
    KvSeparationOptions::default().separation_threshold(8 * 1024),
));
let payloads = db.keyspace(&qualified_name, options)?;
```

8 KiB 是这次选定的**编码值**阈值，尾块按实际长度处理。v3 将分离值的空间回收并入 compaction。逻辑删除立即生效，物理空间的回收允许滞后。创建选项只在首次建 keyspace 时使用，因此随新格式第一次发布，在全新目录和新 era 上验收。小索引、回执、17 字节 blob 元数据使用普通布局。[v3 设计与值回收](https://fjall-rs.github.io/post/fjall-3/) · [创建选项](https://docs.rs/crate/fjall/3.1.13/source/src/keyspace/options.rs)

保留默认分层压缩、过滤策略和启用 LZ4 时的大值 journal 压缩。记录载荷可直接利用 journal 压缩，`SyncAll` 仍决定何时向调用方确认成功。Fjall 的文件锁保护单个引擎根，MMDB 的上层租约还覆盖目录替换和未完成重置的恢复。批量导入无法在当前写入中一起发布回执与索引，`clear()` 会绕过 era 切换，全局事务也无法覆盖文件系统副作用，因而这些 API 保持在本次实现范围之外。[数据库默认配置](https://docs.rs/crate/fjall/3.1.13/source/src/db_config.rs)

## 验收

| 行为 | 通过条件 |
| --- | --- |
| 真实 v2 目录可恢复（issue #8） | 用基线 MMDB 和锁定的 Fjall 2.11.2 在临时目录生成两个库：① `Store` 写入会话来源及引用它的记忆，记录 pin、owner、来源和操作 ID；② 独立 `StateStore` 写入会话 / Work 回执并记录 key、值、revision。关闭句柄后记录文件清单及内容摘要。v3 两次打开都明确拒绝，清单及摘要相同。显式重置后 v3 库为空，隔离目录可用**原版**读回上述身份、来源、回执。对无清单多模型根、独立及混合版本 blob 元数据另测拒绝且无副作用 |
| 跨 keyspace 读取 | 保留 `metadata_query_matches_one_prefix_of_ordered_commits`；新增可区分前后版本的时间索引与入边测试，控制在索引选中后提交替换/删除，检查整次读取只返回旧版完整结果，下一次读取见新版。覆盖正反向扫描、空区间和 limit=0 |
| durability 与恢复 | 运行 `actual_process_interruption_leaves_no_partial_visible_history`、`interrupted_multi_index_write_recovers_all_models` 和操作重试测试。确认态可在子进程退出后重开读回，丢失确认按原回执消歧；状态冲突整批无效、稀疏队列分页预算和 `confirm_durable` 契约成立 |
| 内核生命周期 | 通过 `mmdb-kernel` 的首次初始化中断、未知 reset 结果、旧 era 与延迟清理测试。在只读键迭代器前后两端各保留一个游标跨越 reset，下一次读取报 stale 后终止；记忆重置不影响原会话日志。用有效及损坏快照跑向量重开测试 |
| 大载荷及资源收益 | 在两个全新的 v3 根以相同数据分布对比普通布局和分离布局，固定机器、缓存、随机种子、记录数和 32 KiB 块。写入、覆盖、删除多轮并 flush/compact 后重开，比较目录字节数、写入与分页 P95、回收相同数目块的耗时；注明冷热状态及重复次数。功能门槛是跨阈值内容逐字节一致、键扫描不访问分离值、预算一致。在测试构建中计数值访问或使用受控的分离值读取故障来区分按键扫描与整行读取；耗时本身不能证明少读正文 |
| 依赖与外部契约 | `cargo tree --locked -i fjall` 只解析一个 3.1.13；`cargo check --workspace --all-targets --locked`、`cargo test --workspace --locked` 通过。确认独立 harness 重开可共享类型、历史与关系；v2 验证程序独立构建，不进入正式依赖图 |

## 实现步骤

每步提交前运行工作区测试，以下列出该步额外需要的证据。

1. **固定 v2 验收数据和读回程序。** After: none. 使用上面的两个入口编写临时目录 fixture 生成和核对工具，产出逻辑身份与摘要。导出 `62867fe7` 到临时目录，以该基线的锁文件构建旧版写入与读回程序，同一二进制用于重置前后核对。Check: 两库分别关闭、用旧版重开，通过逻辑断言。
2. **解除向量快照对引擎目录层级的依赖。** After: 1. 在 v2 上完成构造器及调用方调整。Check: `cargo test --locked -p mmdb-vector`，默认和显式目录各验证一次重载。
3. **一次切换磁盘格式和六个 crate 的绑定。** After: 2. 同一提交交付拒绝/重置策略、版本锁、创建布局、批次、Guard 基础适配及节点索引的单快照连接；每个引擎根首次创建即使用最终布局。保持现有分区接口及迭代错误语义。更新受影响的格式说明、README 和旧版 keyspace 术语。Check: 执行 v2 fixture 拒绝、隔离读回、混合目录及 v3 重开验收，并编译全部 target。
4. **用同一快照读取入边及其正文。** After: 3. 将 `neighbours_in` 的反向范围扫描与正向行读取置于同一 read view；一次 BFS 的多个调用各自释放快照。同步更新图查询说明。Check: `cargo test --locked -p mmdb-graph` 与并发索引连接测试。
5. **让按键回收和扫描只读取所需字段。** After: 4. 实现并使用内核 `prefix_keys`，按上表核对图、向量及 facade 中每个只消耗键或值的扫描。`StateStore::scan_page_after` 可用 `Guard::into_inner_if` 先检查 namespace 前缀及排他游标，再为合格键取值，保留删除标记消耗 `max_examined` 的计数规则。Check: 在 flush 后的分离值上校验读取路径，覆盖跨 era、正反向和预算耗尽，记录普通与分离布局对照结果。
6. **完成跨入口验收并归档。** After: 5. 重跑整个验收表，包括进程中断和独立 harness 的精确测试；记录准确的构建版本、命令和性能结果。将 feature 标为 `shipped` 并按文件头归档本 spec。
