# 运行状态与独立存储目录

Status: shipped

工作流、游标和待处理队列可以按 tenant、namespace 和二进制 key 读写 JSON 值。每项包含修订和更新时间。状态记录存入 `app_state_v1`，不会作为记忆节点参与语义召回。

`Database::state()` 复用多模型库的 keyspace。对于必须在记忆库重置后继续使用的运行状态，使用 `StateStore::open_path` 打开独立目录，传入 tenant 和存储格式描述。这个入口持有自己的租约。两个目录各自提交，跨库恢复由调用方协调。

`apply` 和 `apply_namespaced` 在一次批次中检查所有预期修订并执行写入或删除。这样一个轮次的终止状态和待投递的记忆事件可以一起落盘。任一条件冲突，整批都不会生效。I/O 错误后的可见值仍需要通过 `confirm_durable` 确认持久化。

`scan_page_after` 同时限制返回的存活条目数和检查的物理记录数。删除标记消耗扫描预算，空页也可能带有下一页游标。按返回的游标继续，直到游标为空。稀疏队列可选用 `sparse_queue_head`，以有界扫描增量建立存活 key 目录。

[状态存储与测试](../../crates/mmdb/src/state.rs) · [格式、租约与显式重置](../../crates/mmdb-kernel/src/store_format.rs)
