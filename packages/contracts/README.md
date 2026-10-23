# HTTP 与领域事件合约

`openapi.yaml` 使用 OpenAPI 3.1（Schema Object 为 JSON Schema 2020-12），时间线直接引用
`events/domain-event.schema.json`，不维护第二份事件定义。当前合约版本为 `0.1.0`。

创建 Run 的 `session_id` / `input` 必须是非空白字符串；空白按当前 Bean Validation 的
`String.trim()` 语义判断（U+0000–U+0020），内容不做 trim。`Idempotency-Key` 必须提供，
同样非空白，最多 255 个字符。HTTP 传输层可能移除头部两端空格，幂等比较使用应用实际收到的值。
未知 JSON 字段会被忽略，不参与幂等 hash；合法字符串请求保持兼容。缺字段、null、空白、
错误字段类型（包括数字/布尔值，不自动转换为字符串）、无法反序列化的 JSON 和非法幂等键返回 `400 {code: invalid_request, message: ...}`。
错误消息不是稳定的机器判断依据，客户端应使用 code。Session 创建不消费请求体，两个查询接口
不声明输入校验型 400；资源不存在返回 404，幂等冲突返回 409。

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
- 当前事件没有 schema_version，不能无声更改当前 `$id` 的语义或改写历史事件。未来事件版本
  分发/未知版本处理需要明确协议；目前未知 event_type 校验失败，消费者应保留并报告，不猜测处理。
- 每次演进同时更新实现、合约、正反例与必要的历史版本样本。兼容字段应能被当前校验器读取；
  破坏性变更不得以放宽旧版本枚举掩盖。

自动检查位于 `ContractValidationTest`、`RuntimeHttpIntegrationTest` 和 `TaskRetryIntegrationTest`。
前者无需数据库；后两者使用独立 PostgreSQL，并覆盖真实 HTTP 成功/错误/中间状态/重试终态、
成功/失败/租约过期事件，以及对真实事件删除必填字段或替换错误类型的反例。
校验显式启用 date-time format assertion，所有 `$ref` 从本地合约资源解析，不依赖在线 Schema 服务。
运行方式及数据库安全要求见根目录 [README](../../README.md#测试)。
