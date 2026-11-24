# 有来源的领域对象、关系与召回

Status: decided

research harness 可以先保存一段观察，再定义「决策」对象和「支持」关系。trading harness 用同一 owner 重开存储，就能找到决策、沿关系读取观察，并辨认哪些内容来自模型推断。日常召回使用整理过的记录，原始材料可以按引用展开。[可运行示例](../../crates/mmdb/examples/context_harness.rs)

## 打开与共享

用 `Store::create(path)` 创建新存储，用 `Store::open(path)` 重开，再以 `store.context(ContextAccess::new(owner, actor))` 取得句柄。同一存储一次由一个实例持有租约，同进程可共享实例。普通打开会拒绝错误格式。领域上下文属于记忆模块：重置记忆时它换新 era，旧引用读作已重置，会话日志不受影响，见[连续会话日志与原文搜索](session-log.md)。整库重置隔离旧目录，所有模块换新 era。

可信 harness 提供 owner、actor、允许的 scope 及来源 agent、session、run。默认范围是该 owner 的 `Personal`。不同 agent 和 session 可以共享这个范围内的资料；这两个身份用于追溯来源。MMDB 在读取、扩展关系和写入时检查权限范围。harness 负责鉴权、规划、工具执行与模型窗口选择。

## 类型、写入与修订

| 操作 | 可观察行为 |
| --- | --- |
| `find_type`、`list_types`、`define_type` | 应用或 agent 可在运行时发现和复用类型。同名同定义返回已有版本，同名不同定义报告冲突。可选的 `mmdb.note` 也要通过普通接口注册。 |
| `revise_type` | 带预期版本新增类型修订。已有实例继续引用创建或修订时选定的 schema。 |
| `save_object`、`revise_object` | 校验必填与未知属性、类型、引用目标、来源及预期版本。属性支持文本、布尔、整数、有限数值、时间、记录引用和列表。保存时保留观察、推断或指示的身份。 |
| `save_relation`、`revise_relation` | 关系有自己的类型版本、两端、有效时间与依据。写入校验端点，更新邻接和词项索引。 |
| `head`、`inspect_header`、`read` | 稳定引用定位记录，固定版本用于审计。修订不会覆盖旧修订；清除来源后，派生载荷不可展开，记录头仍可解释原因。 |
| `retract`、`purge` | 撤回改变当前可用性，清除传播到依赖载荷。旧版本更新及把自身直接或间接作为来源会被拒绝。 |

普通对象和关系写入需要 1 至 64 个可用来源。类型定义用于校验 schema，不能充当观察证据。只有 User 或 Operator 可以把内容标记为指示。观察和推断经校验即可参与召回；原生记忆的提案另有显式裁决流程。

普通写入在同一持久批次提交记录、索引和操作回执。提交结果不确定时，调用方查询 `operation_receipt` 或以原操作 ID、输入和来源重试。同一 ID 绑定不同内容会发生冲突。回执证明操作已提交，再次读取载荷时仍检查其可用性。

## 按相关性读取

`ContextStore::recall` 接受文本、种子引用、可选有效时间与预算，使用词项和显式关系路径，返回固定版本、来源、命中路径及截断状态。中文使用单字和相邻双字词项。`relations` 支持按方向、关系类型与范围分页。循环由访问集合和扩展预算限制。默认预算为 4,096 个候选、512 条边、深度 2、最多 12 项结果。

默认召回检查记录及其来源的当前可用性。来源修订后旧的派生内容退出日常召回，历史读取仍可按 pin 核对可读版本。`validate_current` 用于计划或调度前重验 head、有效时间和传递依赖。结构化记录有 128 KiB 上限，来源检查限制深度 32、总访问量 4,096。harness 从候选中选择进入模型窗口的材料。

[请求与响应类型](../../crates/mmdb/src/context/types.rs) · [对象和关系](../../crates/mmdb/src/context/records.rs) · [召回](../../crates/mmdb/src/context/search.rs) · [跨 harness 契约测试](../../crates/mmdb/tests/context_contract.rs)
