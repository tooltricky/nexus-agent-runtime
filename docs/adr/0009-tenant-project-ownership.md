# ADR 0009：租户 / 项目归属与租户范围幂等

- 状态：已接受
- 日期：2026-11-12
- 范围：B10；真实 OAuth2 / OIDC 身份校验留待 B11，RBAC / ABAC 留待 B12。

## 问题与规则

原有 Session、Run、Task 和事件没有归属，Run 的幂等键全局唯一。现在租户表示公司，项目表示公司中的工作范围。项目主键为 `(tenant_id, id)`；不同租户可使用相同项目编号。Session、Run、Task 必须具有归属，Run 继承 Session、Task 继承 Run。复合外键限制关联必须属于同一租户和项目，触发器禁止修改已有资源归属或替换 Run 的 Session、Task 的 Run。checkpoint 继续通过 V6 的 `(task_id, run_id)` 外键继承归属。

幂等唯一范围为 `(tenant_id, idempotency_key)`，不包含项目。同租户同键同请求返回原 Run；不同请求或项目返回不包含原 Run 内容的 409；不同租户同键独立处理。保留原请求 hash 算法、期限规范化及过期重放规则，避免旧键升级后失效。数据库唯一约束负责并发仲裁。

## 身份来源与资源入口

`TenantContextSource` 提供服务端上下文，`TenantContext` 校验标识。默认来源拒绝访问，资源请求返回 `403 identity_unavailable`。仅显式启用 `nexus.identity.development-enabled` 时，才使用服务端固定配置的 tenant-id / project-id，并检查对应项目存在。缺少或非法配置使启动失败；项目不存在时请求被拒绝。请求头、未知 JSON 身份字段均不用于选择身份。

开发模式每个实例只有一个固定身份，不认证真实用户，不作为正式多租户入口。演示 Compose 显式启用 `legacy/legacy`；普通应用默认关闭，不能自动回退。测试资源显式配置开发身份；专项测试的身份切换来自受控服务端 Bean，客户端不能通过头部切换。正式 Token 认证、issuer / audience / 签名校验及真实用户成员关系必须由 B11 接入该接口后验收。

创建 Session 从上下文取归属，创建 Run 先检查 Session 作用域。查询、时间线、取消、暂停、恢复在服务入口检查 Run 作用域；不匹配返回 404。归属不可变，因此状态操作在 Task → Run 加锁前的归属检查不会被普通业务写入改变。已有内部 RuntimeStore 全局读取仅供受控后台服务使用，不是对外 API。

后台 worker、心跳、watchdog 按原资源运行，不读取 HTTP 上下文，也不使用默认开发租户。状态机、短事务、锁顺序、执行资格和截止时间规则不变。当前调度仍为全局队列，租户配额和公平调度不在本任务范围。

## V7 与不可变事件

V7 新建最小 tenants / projects 表，将旧 Session、Run、Task 显式归入 `legacy/legacy`。它代表历史归档范围，不推断原用户属于哪家公司。保留全部原 ID、请求 hash、内容、结果、状态、版本、租约、期限、checkpoint。已有 V1–V6 不修改。历史访问需要显式配置匹配上下文。

原 `domain_events` 行完全不修改；payload、sequence、occurred_at、schema_version 和禁止 UPDATE / DELETE 的触发器保持不变。新增不可变 `event_ownership`，关联 event_id、Run、可选 Task、租户和项目。迁移通过原 Run 回填关联。复合外键校验归属，插入触发器还校验关联 Run / Task 与原事件一致；原事件新增 `(task_id, run_id)` 外键，防止 Task 接错 Run。

数据库 AFTER INSERT 触发器为每条新事件自动插入归属关联，归属来自持久化 Run。这比让每个事件调用方另写一次关联更易保证完整；原计划中“同事务写入”的要求由数据库实现。归属关联失败时事件、业务状态及 checkpoint 一起回滚；后台和直接 SQL 插入均执行该规则。读取使用 LEFT JOIN 并显式检查归属，缺失关联报错，不能因 INNER JOIN 而丢失历史记录。关联表同样禁止 UPDATE / DELETE；测试清理必须同时 TRUNCATE 相关业务表。

应用数据库账户是受信任的内部边界，超级用户可改表或关触发器，不在客户端身份隔离的保证内。升级发现既有错误 Task / Run 关联时会失败，须先调查数据；不会自动重写事件修复。

## HTTP 0.5 与历史兼容

冻结 `openapi-v0.4.yaml`，当前 HTTP 合约为 0.5。Session、Run、Task 响应新增必填 tenant_id / project_id；时间线事件增加来自关联表的必填归属覆盖层 `OwnedDomainEvent`。Run / timeline 查询和 cancel / pause / resume 成功响应带 `Nexus-API-Contract: 0.5`。

事件原 schema_version 继续按无版本 0.1、0.2、0.3、0.4 校验 payload，既不回填也不伪造 0.5 事件版本。`owned-domain-event-v0.5.schema.json` 只定义 HTTP 归属覆盖层，与原事件 Schema 组合使用；原始历史样本继续按冻结版本读取。完整因果标识和 Trace / Agent / Tool envelope 演进留待 B35。

## 验证

使用独立 PostgreSQL 验证 V6 → V7 的旧行 / checkpoint / 事件保留、旧键重放、数据库错误关联拒绝、并发租户幂等、跨项目冲突、HTTP 作用域与伪造字段、默认拒绝、关联写入故障回滚、后台归属及完整应用重启。合约包含必填 / 类型 / 空白反例与历史样本。全量执行、报告防跳过检查及打包结果记录在 backlog；不把受控身份切换测试当作真实 Token 验收。
