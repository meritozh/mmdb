# 会话和运行检查点

Status: shipped

harness 在切换窗口或恢复中断任务时，用检查点定位同一 session 中已处理的会话日志位置，再读取其后的事件。检查点载荷不可变，固定 session/run、父版本、日志位置和来源。Session 与各 Run 分别比较父版本，子运行只能更新自己的 Run 检查点。

`save_checkpoint` 比较预期父版本和生成开始前捕获的可用性 epoch，拒绝未提交的位置、并发覆盖或失效来源。`latest_checkpoint` 和 `latest_run_checkpoint` 返回用于定位的记录头。恢复前用 `validate_current` 复查传递依赖；单条记录的 `current_state` 只描述自身。

| 事件 | 当前恢复与日志读取 |
| --- | --- |
| 来源普通修订 | 可用性 epoch 推进，旧检查点不能用于当前恢复。旧 pin 的载荷仍可用于核对已经发生的操作。 |
| 记录清除或日志事件撤回 | 脱敏 epoch 同批次推进，较早检查点的日志载荷也不可再读，覆盖未逐项列出的摘要输入。 |
| 旧 writer 缺少匹配的脱敏 marker | 按当前可用性 epoch 保守处理，避免重新暴露旧摘要。 |

[检查点实现](../../crates/mmdb/src/context/checkpoints.rs) · [会话与子运行重开测试](../../crates/mmdb/tests/context_contract.rs)
