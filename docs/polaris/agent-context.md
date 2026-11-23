# 让不同 harness 复用有来源的上下文

Status: active

同一用户的 research agent 在一个 session 里保存观察和决策，trading agent 在另一个 session 里应当能按项目或相关对象找回决策，说明它与当前问题的关系，并沿引用核对原文。原文被清除时，读取应返回明确的不可用状态。会话切换模型窗口后仍沿原会话日志追加，运行计划、工具权限与窗口预算由 harness 管理。

## 如何判断目标达成

独立 Rust harness 可以关闭并重开同一存储，复用类型，沿显式关系找到另一场会话的决定并展开来源。真实任务还要检验三件事：关系能否找到单靠文本漏掉的材料，来源是否足以核对判断，以及在给定候选数、字节数和窗口预算下能否取到任务需要的信息。首轮真实样本和确定性契约测试分别覆盖应用接入与接口边界，跨领域的召回质量、开销和 token 节省仍缺少代表性样本。[独立 harness 示例](../../crates/mmdb/examples/context_harness.rs) · [首轮 MiuMiu 任务验收](https://github.com/meritozh/miumiu/blob/aaf280a3cb4a973ea57f90c545344ab7fb87eda1/docs/CONTEXT-ACCEPTANCE.md)

## 路线与取舍

1. 以完整的会话日志作为来源，允许 agent 在运行时复用或定义类型，并保存带版本和依据的对象与关系。借鉴 Ontology 的对象、属性、关系和动作，领域词表由应用定义。[领域上下文](../feature/context-ontology.md) · [会话日志](../feature/session-log.md) · [Ontology 概念来源](https://www.palantir.com/docs/foundry/ontology/core-concepts)
2. 从词项命中和显式关系扩展候选，返回固定版本与来源。多模型数据库已经提供向量检索。若引入向量或重排，需要用实际任务比较新增命中、图噪声及资源开销。相似度本身不能证明事实关系。[多模型证据召回](../feature/evidence-recall.md)
3. 在独立 harness 和 MiuMiu 中使用相同的读写契约。检查点定位恢复位置，业务动作关联工具操作的实际结果。MiuMiu 可以按新证据重规划，分别管理模板版本和运行计划。MMDB 提供持久数据，harness 作出调度决定。[检查点](../feature/context-checkpoints.md) · [业务动作](../feature/business-actions.md) · [运行状态](../feature/operational-state.md)

直接把原生库与多模型库并排交给每个 harness 会产生两套记录身份和双写恢复责任。选定的路径让上下文接口复用 `MemoryDatabase` 的事务与批次提交，索引引用同一条记录；模型或应用可以变化，来源仍指向原来的版本。

## Features

| 产品能力 | 可检查的结果 | 功能契约 |
| --- | --- | --- |
| O-01 保存有来源的上下文，O-06 修订与撤回 | 新写入的推断能被召回并展开来源。过期修订被拒绝，撤回或清除后按引用可辨认状态。 | [对象写入与修订](../feature/context-ontology.md#类型写入与修订) |
| O-02 对象与关系，O-07 运行时领域建模 | 另一 harness 可以发现已定义的类型、保存实例、查询关系端点及依据。修改类型后，旧实例保留原 schema 版本。 | [领域类型与关系](../feature/context-ontology.md) |
| O-03 按需召回，O-04 有界关系扩展 | 从直接命中的对象沿显式关系找到间接相关记录，结果说明路径、来源和预算是否用尽。 | [按相关性读取](../feature/context-ontology.md#按相关性读取) |
| O-05 跨 agent 和 session 共享，O-08 独立 harness 接入 | 两个 harness 顺序打开同一目录，同一 owner 在允许范围内复用记录；其他 owner 的内容不能经关系扩展泄露。 | [打开与共享](../feature/context-ontology.md#打开与共享) |
| O-09 连续会话日志 | 未经整理的工具结果仍能按引用完整读取。切换窗口后从检查点恢复，新消息沿原链追加。 | [会话日志](../feature/session-log.md) · [检查点](../feature/context-checkpoints.md) |
| O-10 业务动作 | 按对象发现动作、输入及前置条件，执行后能查到可信 harness 写回的结果与来源。 | [业务动作](../feature/business-actions.md) |
