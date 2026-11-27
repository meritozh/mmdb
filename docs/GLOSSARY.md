# MMDB 术语表

| 术语 | 在本项目中的含义 | 易混用的说法 |
| --- | --- | --- |
| 存储内核（storage kernel） | MMDB 中唯一直接使用存储引擎的一层，提供目录租约、格式清单、存储模块的 era、写锁与批次提交，不认识任何一类 agent 数据。 | 原生事务内核；把 `MemoryDatabase` 称作内核 |
| 存储（store） | 由存储内核管理的一个目录，包含若干存储模块，一次由一个进程持有租约。 | 数据库；上下文库；指整个目录时说记忆存储 |
| 存储模块（store module） | 存储中负责一类 agent 数据的部分，有自己的 era，可以单独重置。会话模块存放会话日志与检查点，记忆模块存放原生记忆与领域上下文。 | crate；分区；子库 |
| 运行状态（operational state） | harness 推进执行所需的工作流、游标或队列数据，由 `StateStore` 按命名空间管理。 | 语义记忆；模型窗口；对话 |
| 记忆节点（`MemoryNode`） | 多模型 `Database` 中由 tenant 和 ULID 定位的记录，种类为 Episode、Fact、Entity 或 Artifact。 | 把状态项或每一种 context 记录都称作节点 |
| 领域上下文（context ontology） | harness 通过 `ContextStore` 管理的有类型对象、关系及其来源，来源可以是会话日志事件。 | 模型本轮看到的全部文本 |
| 审计记录（audit journal） | MMDB 对自身操作的追加式记录：操作、输入、结果与修复。 | log；日志；会话日志 |
| harness | 提供可信身份、管理窗口、规划并调度工具的宿主。多个 harness 可先后打开同一存储。 | MMDB 的执行引擎 |
| owner / scope | owner 标识归属用户，scope 限定可访问的资料范围。agent、session 和 run 记录来源身份。 | 用 session 充当用户隔离边界；将 owner 当成多模型库的 tenant 编号 |
| 记录引用（`ContextRef`） | 以记录所在存储模块的 era、owner、记录族及 ID 定位一条记录。 | 指向某次修订的 pin |
| 固定版本（`RecordPin`） | 记录引用加具体 revision，可定位写入时使用的证据或 schema。 | 当前 head；摘要文本 |
| 存储代际（era） | 标识一个存储模块的生命周期。重置这个模块或整个存储都产生新 era；格式 ID 描述可读写的磁盘契约。 | 格式版本；记录修订；清单里标识目录这一代的 `StoreEraId` |
| 来源（sources / provenance） | sources 固定所依据的记录版本；provenance 标明操作的 actor 及可选 agent、session、run。 | 把向量相似度当作关系的依据 |
| 操作回执（operation receipt） | 将操作 ID 和请求绑定到已提交的结果，供重试或确认提交状态。 | 外部工具已经产生效果的证明 |
| 会话（session） | 一次 agent 交互，由会话头和它的会话日志组成，可以从另一会话的日志前缀 fork 出来。 | 对话；conversation |
| 会话头（session header） | 每个会话的一条记录，记下会话 ID、类型、scope、父会话与 fork 来源，以及 harness 定义的元数据。 | 单说「头记录」；会话目录项 |
| 子会话（child session） | 由另一会话中的某个 run 发起的会话，会话头记下父会话和父 run。 | 把 fork 出的会话称作子会话 |
| fork | 从一个会话日志的某个位置创建新会话，新会话的日志以这段前缀开头，与来源共享其中的事件。 | 复制会话；分支 |
| 会话变更号（session change number） | 同一 owner 下每次写入会话时单调递增的计数，会话列表按它从新到旧排列。 | 修订号；revision；记录修订 |
| 会话日志（session log） | 同一会话按逻辑序号追加的类型化原始事件，是该会话交互的唯一来源，可按 pin 展开载荷。 | 会话历史；history；单说「日志」或 log；召回候选；当前可用窗口 |
| 轮次（turn） | 会话日志中由一个 run 开启、从领取一条输入开始到不再欠下工作为止的一段事件，包含零个或多个步骤，以及不经模型请求直接发起的工具调用。 | 单条消息；一次模型请求 |
| 步骤（step） | 轮次中的一次模型请求，以及它要求的工具调用和结果。 | 调用；call；一次工具调用 |
| 对话（conversation） | 从会话日志派生出的模型可见消息序列，任何时候都能由日志重建。 | 会话；把对话轮次另存为运行状态 |
| 对话缓存（conversation cache） | 绑定到会话日志某个位置的已派生对话，可以丢弃后从日志重建。 | 投影检查点；对话快照；检查点 |
| 检查点（checkpoint） | 用于恢复 session 或 run 的不可变记录，固定会话日志位置、父版本和依赖。 | 命名快照；一次整库备份；落盘确认（flush） |
| 命名快照（`NamedSnapshot`） | 多模型 catalog 在内存中登记的名称、序号及创建时间。 | 可以直接恢复数据的分支 |
| 投影（embedding projection） | 节点内容在指定模型配置下生成的向量及其工作状态。 | 原始记忆；显式图关系；从会话日志得到对话（称为派生） |
