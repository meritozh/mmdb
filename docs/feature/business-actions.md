# 业务动作定义与执行记录

Status: shipped

应用可以给「里程碑」类型定义更新动作，列出输入属性、来源及前置条件。agent 按对象发现适用动作；harness 绑定实际工具、判断执行权限，并把执行结果写回 MMDB。一个模型生成的定义本身没有调度权限。

| 操作 | 契约 |
| --- | --- |
| `define_action`、`revise_action`、`find_action` | 保存或读取有版本的动作定义。`Exists` 检查属性是否存在，`Equals` 的值必须符合属性类型。 |
| `actions_for` | 按目标对象、范围和游标分页返回动作与前置条件满足情况。 |
| `prepare_action_once` | 检查动作及目标的当前版本、时间、范围、传递来源、参数和前置条件。相同操作重试返回原回执；只有首次准备的调用方取得调度权。 |
| `finish_action` | 可信 System 或 Operator 用预期版本保存结果与来源。终态区分成功、派发前拒绝、确定无效果的失败及效果未知。 |

准备记录先以 `Prepared` 持久化。重开后见到 `Prepared` 或 `EffectUnknown` 时，harness 要核对外部效果；仅凭缺少成功记录就重发可能重复执行。MMDB 保存发生过什么，外部工具的副作用及其恢复属于 harness。

[动作实现](../../crates/mmdb/src/context/actions.rs) · [并发准备与权限测试](../../crates/mmdb/tests/context_contract.rs)
