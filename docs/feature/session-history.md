# 连续会话历史与原文搜索

Status: shipped

同一 session 的用户消息、assistant 消息、工具调用与结果、通知共享一条持久历史链。切换模型窗口仍沿用原 session。MMDB 在写入时分配单调递增的序号，时间戳描述事件时间。即使 agent 当时没有整理某段对话，后续仍可搜索原文、固定其版本，并作为新对象的来源。

## 追加、读取与恢复

- `append_history` 接受稳定事件 ID、可选预期位置、输入流及来源。同一事件同内容重试会取回原序号，即使后面已经追加其他事件。同一身份提交不同内容会冲突。
- assistant 的开始、有序片段和终止事件共用消息 ID。一个片段不可用时，聚合消息及其余片段也不能用来重现原文。工具结果必须关联同一 session 的唯一调用。来源的普通修订保留历史可读性，清除会影响调用及结果的依赖链。
- 历史快照可引用可读的旧修订。复制时登记 `HistoryInput.sources`，使后续清除也作用于副本。独立观察可不带外部来源。
- 载荷按 32 KiB 分块并校验摘要。完整写入后，事件头、序号、索引和回执一起发布。中断不会推进历史位置；`recover_payloads` 有界清理未完成载荷。此版本读取输入流期间持有写锁，长时间不结束的流会阻塞同库其他写入。

## 翻页与搜索

`history_position` 与 `history` 按序号定位和翻页。`read_payload` 一次最多读取 1 MiB。`history_sessions` 与 `tool_history` 为可信 harness 恢复提供 session 目录和调用配对。恢复时读已有历史，只追加新产生的交互。

`search_history` 返回匹配事件的固定引用、首个匹配的字节位置和续查游标。跨 session 按键序、同 session 按逻辑序号遍历。子串可以跨存储块和查询页。ASCII 匹配忽略大小写，中文按 UTF-8 字节匹配。单次请求最多扫描 4,096 个事件或 16 MiB；未命中且仍有游标时可继续扫描。

[历史读写](../../crates/mmdb/src/context/history.rs) · [搜索](../../crates/mmdb/src/context/search.rs) · [大载荷、中断与重开测试](../../crates/mmdb/tests/context_contract.rs)
