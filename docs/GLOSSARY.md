# MMDB 术语表

| 术语 | 在本项目中的含义 | 易混用的说法 |
| --- | --- | --- |
| 运行状态（operational state） | harness 推进执行所需的会话、工作流、游标或队列数据，由 `StateStore` 按命名空间管理。 | 语义记忆；模型窗口 |
| 记忆节点（`MemoryNode`） | 多模型 `Database` 中由 tenant 和 ULID 定位的记录，种类为 Episode、Fact、Entity 或 Artifact。 | 把状态项或每一种 context 记录都称作节点 |
| 领域上下文（context ontology） | harness 通过 `ContextStore` 管理的有类型对象、关系、来源与关联历史。 | 模型本轮看到的全部文本 |
| harness | 提供可信身份、管理窗口、规划并调度工具的宿主。多个 harness 可先后打开同一上下文库。 | MMDB 的执行引擎 |
| owner / scope | owner 标识归属用户，scope 限定可访问的资料范围。agent、session 和 run 记录来源身份。 | 用 session 充当用户隔离边界；将 owner 当成多模型库的 tenant 编号 |
| 记录引用（`ContextRef`） | 以 era、owner、记录族及 ID 定位一条上下文记录。 | 指向某次修订的 pin |
| 固定版本（`RecordPin`） | 记录引用加具体 revision，可定位写入时使用的证据或 schema。 | 当前 head；摘要文本 |
| 存储代际（era） | 标识一个受管存储实例的生命周期。显式重置产生新 era，格式 ID 描述可读写的磁盘契约。 | 格式版本；记录修订 |
| 来源（sources / provenance） | sources 固定所依据的记录版本；provenance 标明操作的 actor 及可选 agent、session、run。 | 把向量相似度当作关系的依据 |
| 操作回执（operation receipt） | 将操作 ID 和请求绑定到已提交的结果，供重试或确认提交状态。 | 外部工具已经产生效果的证明 |
| 会话历史（session history） | 同一 session 按逻辑序号排列的原始事件，可按 pin 展开载荷。 | 召回候选；当前可用窗口 |
| 检查点（checkpoint） | 用于恢复 session 或 run 的不可变记录，固定历史位置、父版本和依赖。 | 命名快照；一次整库备份 |
| 命名快照（`NamedSnapshot`） | 多模型 catalog 在内存中登记的名称、序号及创建时间。 | 可以直接恢复数据的分支 |
| 投影（embedding projection） | 节点内容在指定模型配置下生成的向量及其工作状态。 | 原始记忆；显式图关系 |
