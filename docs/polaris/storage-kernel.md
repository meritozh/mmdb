# 所有 agent 数据共用一个存储内核

agent 需要持久化的每一类数据，包括会话、会话日志、对话、运行状态和记忆，都是同一个存储内核上的模块。只有存储内核调用 fjall，第三方依赖停在 fjall 这一层，每个 crate 都有一项说得出的职责。

Status: active

- Signal: `rg 'fjall::'` 只命中存储内核所在的 crate。依赖白名单测试对照 `cargo tree`，例外清单为空；白名单只收嵌入式存储引擎，以及自身不读写磁盘的数据结构与算法库。所有模块共用一套租约、格式清单、批次提交与崩溃恢复，每个模块可以单独重置。[让不同 harness 复用有来源的上下文](agent-context.md) 的契约测试在新结构上通过。
- Horizon: 一个季度

## Route

先从现有原生库中抽出存储内核，让会话与会话日志成为它的第一个使用者，再分别迁入运行状态和记忆。每一部分删掉它取代的 crate 和依赖，依赖白名单从第一部分起守住依赖边界。项目处于 startup，可以深改设计并重置数据而不迁移；这条路线让每一部分交付后才开始下一部分，功能负载最可控。

- Rejected: 记忆先行，即先把三套记忆收成一套，再按它的需要确定存储内核；当记忆的设计能在几周内收敛，或存储内核的通用部分主要由记忆决定时，它更好。另起新内核并逐类重写，会丢掉已加固的恢复逻辑，新旧并行期间负载翻倍。
- Wrong-call signal: 第 1 部分确定存储内核接口时，必须先回答记忆的问题（例如清除怎样跨模块传播）才能继续；或者某一部分落地时，接入方 harness 的改动大于 MMDB 本身的改动。

## Parts

### 1. 会话与会话日志建在存储内核上

存储内核上线，成为唯一调用 fjall 的一层。会话有头记录、fork 和带修订号的列表。会话日志支持追加、按序号续读，冷恢复时关闭未结束的轮次。对话缓存绑定到会话日志的位置，对话的派生规则由 harness 定义；MMDB 继续校验工具结果与调用的配对，以及片段与消息的归属。检查点随会话迁入。会话日志不再随记忆重置。依赖白名单测试上线，现有越线依赖列入只减不增的例外清单，WebAssembly UDF 移出 MMDB。

- After: none
- Builds on: [连续会话日志与原文搜索](../feature/session-log.md), [会话和运行检查点](../feature/context-checkpoints.md), [WebAssembly user-defined functions](../feature/wasm-udfs.md)
- Done when: 独立 harness 关闭并重开目录后，能列出会话、续写会话日志、从日志前缀 fork 子会话，并用对话缓存加日志尾部还原对话。重置记忆后会话完好。会话模块不直接使用 fjall。

### 2. 运行状态建在存储内核上

运行状态成为同一存储中的模块。状态变更与会话日志追加在同一批次提交，运行状态不再需要独立目录来躲过记忆重置。

- After: 1
- Builds on: [运行状态与独立存储目录](../feature/operational-state.md)
- Done when: 轮次的终止状态与它的 `turn/end` 事件要么都已落盘，要么都没有，并有崩溃测试覆盖。运行状态不再需要单独的存储根目录。

### 3. 记忆建在存储内核上

多模型 `Database`、原生 `MemoryDatabase` 与 `ContextStore` 三套记忆模型收成一套，审计记录只留一套，向量索引不再在存储内核之外写文件。记忆的内部模型由这一部分的 feature 决定。

- After: 1
- Builds on: [Embedded multi-model memory](../feature/multi-model-memory.md), [Memory nodes and indexed reads](../feature/memory-nodes.md), [Model-specific vector and hybrid search](../feature/vector-search.md), [Directed edges and graph traversal](../feature/graph-traversal.md), [Content-addressed blob storage](../feature/blob-storage.md), [Embedding projections](../feature/embedding-projections.md), [Evidence recall and revision-checked proposals](../feature/evidence-recall.md), [Reversible dream maintenance](../feature/dream-maintenance.md), [Memory operation audit](../feature/memory-audit.md), [Model registry, statistics and named snapshots](../feature/memory-catalog.md), [Query planning and execution](../feature/query-execution.md), [MMQL recall queries](../feature/mmql.md), [有来源的领域对象、关系与召回](../feature/context-ontology.md), [业务动作定义与执行记录](../feature/business-actions.md)
- Done when: 只剩一套记忆模型，且它不直接使用 fjall。agent-context 的契约测试在其上通过，依赖白名单的例外清单为空。

## Features

1. [连续会话日志与原文搜索](../feature/session-log.md)：会话日志和检查点从记忆存储迁到存储内核上的会话模块，从随记忆一起重置变为重置记忆后仍完整可读。Part: 1. After: none
2. [连续会话日志与原文搜索](../feature/session-log.md)：会话日志从只记录消息和工具事件，变为也记录轮次的开始与结束，并在重开时把未结束的轮次关闭并标为中断。Part: 1. After: 1
3. harness 可以创建带头记录的会话、按修订号列出会话，并从一段日志前缀 fork 出子会话。New. Part: 1. After: 1, 2
4. harness 可以把派生出的对话存成绑定到日志位置的对话缓存，之后只读缓存之后的日志尾部就能还原对话，并在日志事件被撤回或清除后认出缓存已过期。New. Part: 1. After: 1
5. WebAssembly UDF 从随 MMDB 提供变为移出 MMDB。Iterate [WebAssembly user-defined functions](../feature/wasm-udfs.md). Part: 1. After: none
6. 运行状态从独立目录或共用多模型库 keyspace，变为同一存储中的模块，状态变更与会话日志追加同批提交。Iterate [运行状态与独立存储目录](../feature/operational-state.md). Part: 2. After: 1, 2

## Not this

- 不引入更高层的组件（如 SQLite、tantivy、LanceDB）来组装存储。
- 不迁移数据：每一部分落地时，旧格式的存储显式重置。
- 不保留旧 API 兼容，接入方随每一部分调整。
- 本目标不决定记忆的内部模型。
- 不改变 harness 与 MMDB 的分工：窗口选择、调度和对话的派生规则属于 harness。
- 不改变 agent-context 要求 harness 能做到的事。
