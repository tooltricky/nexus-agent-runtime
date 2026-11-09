# HTTP 与领域事件合约

`openapi.yaml` 使用 OpenAPI 3.1（Schema Object 为 JSON Schema 2020-12）。当前 HTTP 合约版本为
`0.4.0`；`openapi-v0.1.yaml`、`openapi-v0.2.yaml`、`openapi-v0.3.yaml` 和对应事件 Schema 保留旧版定义。

创建 Run 的 `session_id` / `input` 必须是非空白字符串；空白按当前 Bean Validation 的
`String.trim()` 语义判断（U+0000–U+0020），内容不做 trim。`Idempotency-Key` 必须提供，
同样非空白，最多 255 个字符。HTTP 传输层可能移除头部两端空格，幂等比较使用应用实际收到的值。
未知 JSON 字段会被忽略，不参与幂等 hash；合法字符串请求保持兼容。缺字段、null、空白、
错误字段类型（包括数字/布尔值，不自动转换为字符串）、无法反序列化的 JSON 和非法幂等键返回 `400 {code: invalid_request, message: ...}`。
错误消息不是稳定的机器判断依据，客户端应使用 code。Session 创建不消费请求体，两个查询接口
不声明输入校验型 400；资源不存在返回 404，幂等冲突返回 409。
取消接口无需请求体或幂等键，请求体忽略；成功或重复取消返回 200 和当前 CANCELLED Run，
已成功 / 失败返回 `409 {code: cancellation_conflict, message: ...}`，不存在返回 404。

Run / Task 的 `output` 在成功时提供，其他状态省略；租约 owner / 到期时间仅在 LEASED /
EXECUTING 提供，其他状态省略。以上字段不返回 JSON null。`attempt`、`max_attempts`、
`next_attempt_at` 始终提供；后者在 RETRYING 时表示允许下次领取的时间。RUN 事件通常省略
`task_id`，读取旧 envelope 时也允许 null；TASK 事件必须有非 null 的 task_id。

事件 Schema 校验所有现有 event_type、聚合类别及各 payload 的必填字段、类型、状态迁移、
原因枚举和时间格式。TaskRetried 必须包含 attempt / max_attempts、reason、from / to、
retry_delay_ms 和 next_attempt_at；TaskFailed 与 RunFailed 分别描述耗尽的 Task 和关联 Run。
`error` 为可选诊断上下文，租约过期时通常省略。跨字段关系（如 attempt 不超过 max_attempts、
aggregate_id 的关联、退避计算与数据库状态一致）仍由领域/集成测试验证，JSON Schema 不替代状态机。

演进规则：

- 响应、事件 envelope 和 payload 允许新增可选字段；消费者必须忽略未知字段。请求未知字段
  仍被忽略。新增字段不得改变既有字段语义，新增必填字段不属于兼容变更。
- 删除/重命名字段、收紧类型或范围、改变 null/省略约定、增加状态或 event_type 枚举均按
  破坏性变更处理：保留旧版本文件/样本，发布新的 Schema `$id` 和明确的 API/事件版本边界，
  定义双读及迁移规则后再切换。`info.version` 只是文档版本，不能充当事件运行时版本。
- 历史事件没有 schema_version，不能回填版本或改写历史事件。B04 新增可空数据库列及查询支持；
  现有事件写入继续省略版本，B05 的取消事件必须显式使用字符串 `0.2`。未知类型或版本应保留并报告，不猜测处理。
- 每次演进同时更新实现、合约、正反例与必要的历史版本样本。兼容字段应能被当前校验器读取；
  破坏性变更不得以放宽旧版本枚举掩盖。

自动检查位于 `ContractValidationTest`、`CancellationContractTest`、`RuntimeHttpIntegrationTest`、
`TaskRetryIntegrationTest` 和 `RunCancellationIntegrationTest`。前两者无需数据库；后三者使用
独立 PostgreSQL，覆盖真实 HTTP 成功/错误/中间状态/重试终态/取消、成功/失败/租约过期/取消事件，
以及对真实事件删除必填字段或替换错误类型的反例。
校验显式启用 date-time format assertion，所有 `$ref` 从本地合约资源解析，不依赖在线 Schema 服务。
运行方式及数据库安全要求见根目录 [README](../../README.md#测试)。

## B04 / B05：取消与版本边界

B04 准备取消规则、迁移、读取支持和数据合约；B05 提供 `POST /v1/runs/{run_id}/cancel`，
在同一事务中取消 Run / Task、清除租约并写入 TaskCancelled → RunCancelled，失败一起回滚。
重复取消无状态变化或重复事件，与成功 / 最终失败竞争时先有效提交的终态事务获胜。具体规则见
[ADR 0005](../../docs/adr/0005-cancellation-foundation.md)。Run 的 CREATED / QUEUED / RUNNING、
Task 的 PENDING / LEASED / EXECUTING / RETRYING 可以进入 CANCELLED；终态不可再转换。
取消事件使用 schema_version `0.2`，旧事件仍使用无版本的冻结 Schema。成功 GET Run / timeline
和 POST cancel 带 `Nexus-API-Contract: 0.2`，不提供旧版响应协商。样本位于 `examples/`；
取消成功响应要求 Run 与唯一 Task 均为 CANCELLED；取消冲突与幂等冲突使用不同的错误 Schema。
取消可能等待正在执行的事务，不保证立即中断外部调用或撤销外部副作用。B07 已补充内部心跳续租（无公共续租 API）。

## B08：截止时间、TIMEOUT 与 0.3 版本

B08 当时成功 Run / timeline GET 及 cancel POST 响应带 `Nexus-API-Contract: 0.3`。
上述 B04 / B05 段落描述其历史 0.2 边界；冻结文件保留原样，不协商旧响应版本。

创建请求新增可选 `deadline_at`：带时区偏移、包含秒且最多九位小数秒的时间字符串，支持 `Z` 及 `-23:59` 至 `+23:59` 的 RFC 3339 偏移（分钟 00–59）。省略或 null 表示无限期；查询在
无期限时省略字段。规范化 UTC 并截断为微秒后参与幂等比较；相同绝对时刻不同偏移相等。
0001–9999 限制输入的本地年份，允许偏移换算后跨入 UTC 年份 0000 / 10000；这类边界的响应及事件期限以 `+23:59` / `-23:59` 表示，保持四位本地年份，其余仍输出 UTC。
首次创建拒绝已到期输入，重放同键同请求即使到期也返回原 Run，不重新计算期限。
TIMEOUT 的 Run / Task 必须提供 deadline_at，且不提供 output、lease_owner 或 lease_expires_at。
取消超时 Run 返回 409 cancellation_conflict；取消入口首次发现到期时先提交超时再返回 409。

无 schema_version 使用冻结 0.1，字符串 0.2 使用冻结取消 Schema，字符串 0.3 使用
`events/domain-event-v0.3.schema.json`。新版本只用于 TaskTimedOut、RunTimedOut 和
TaskRetrySkipped；未知、null、数字版本均拒绝，历史事件不改写。前两者原因是
DEADLINE_EXCEEDED，带截止时间及原 / 目标状态，Task 事件还带 attempt / max_attempts，
Run 事件带关联 task_id。TaskRetrySkipped 原因是 DEADLINE_BEFORE_RETRY，表示释放租约后
在 RETRYING 等到期限，不是发起重试，next_attempt_at 等于 deadline_at。

样本为 `examples/run-timeout-v0.3.json`、`examples/events-timeout-v0.3.json`，由
DeadlineContractTest 和真实 HTTP / PostgreSQL 测试校验，并保留旧样本回归。
校验器保留 date-time format assertion，单独校验完整 RFC 3339 偏移并用原库校验日历与本地时间，避免测试库内部 `OffsetDateTime` 的 ±18 小时限制缩窄公开范围；覆盖 `±19:00`、`±23:59` 正例及小时 24 / 分钟 60 反例。
事务优先级、审批等待计时及中断边界见 [ADR 0007](../../docs/adr/0007-execution-deadlines.md)。

## B09：PAUSED、checkpoint 与 0.4 版本

当前 Run / timeline GET、cancel / pause / resume POST 成功响应带 `Nexus-API-Contract: 0.4`。
`openapi-v0.3.yaml` 冻结 B08 的定义；不协商旧版响应。新增 `POST /v1/runs/{run_id}/pause`
和 `/resume`，无需请求体或 Idempotency-Key，忽略请求体；200 返回当前 Run，404 表示不存在，
409 `pause_conflict` 表示终态、到期或恢复时暂停编号不匹配。首次恢复时若已耗尽尝试次数，
200 返回 FAILED；不增加一次执行。重复暂停（已 PAUSED）或恢复（仍活动）不追加事件。
恢复的可选 `Pause-Checkpoint` 头必须与最新 checkpoint_id 相等；带头重试能防止误恢复新的
暂停周期。不带头始终针对当前周期，跨周期迟到请求无去重保证。暂停在中间恢复后再次到达
也属于新操作。新发现到期的调用先提交 TIMEOUT 再返回 409，暂停不停止截止时间计时。

PAUSED 为非终态，Run 与 Task 一致；Task 必须有非空 `checkpoint`，不得有租约或 output。
checkpoint 版本为整数 1，记录暂停周期 UUID、Run / Task ID、暂停前状态和版本、attempt、
next_attempt_at、paused_at。被恢复 / 次数耗尽 / 取消 / 超时消费后，提供 consumed_at 与
RESUMED / ATTEMPTS_EXHAUSTED / CANCELLED / TIMEOUT disposition；两字段同时出现。
只有最新 checkpoint 保留为普通数据，旧周期见不可变时间线。其余状态允许省略 checkpoint；
存在时必须已消费。字段关系与数据库一致性由集成测试补充验证。

`events/domain-event-v0.4.schema.json` 只接受显式字符串 0.4：TaskPaused、RunPaused、
TaskResumed、RunResumed、从 PAUSED 耗尽的 TaskFailed / RunFailed，以及从 PAUSED 取消 /
超时的既有事件类型。新暂停 / 恢复与次数耗尽事件带 checkpoint_id、from / to；Task 事件
有 attempt / max_attempts，Run 事件有关联 task_id。次数耗尽原因固定 PAUSED_ATTEMPTS_EXHAUSTED。
从非 PAUSED 来源的取消 / 超时继续使用 0.2 / 0.3，旧事件和冻结 Schema 均不改写。
无版本、0.2、0.3、0.4 混合读取；未知版本拒绝。新版示例位于
`examples/run-paused-v0.4.json`、`examples/events-pause-v0.4.json`，由 PauseContractTest
和 RunPauseIntegrationTest 验证正反例、真实 HTTP 和真实持久化事件。

恢复只在未完成 Task 边界进行，不是线程或模型调用快照；尝试次数不退还，旧结果和旧停止
通知不能影响恢复后的执行。事务与竞争规则见 [ADR 0008](../../docs/adr/0008-pause-and-resume.md)。
