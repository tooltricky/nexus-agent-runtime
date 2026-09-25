# Agent Harness 企业级实战项目设计说明书

## 1. 项目概述

### 1.1 项目名称

**Nexus Agent Runtime：面向企业研发场景的多 Agent 执行与治理平台**

### 1.2 项目背景

企业希望把代码分析、故障排查、发布检查等工作交给 AI Agent，但直接调用大模型存在会话无法恢复、工具权限过大、任务执行不可控、成本不可追踪和故障难以复盘等问题。本项目建设一套企业级 Agent Harness/Runtime，把 Agent 运行所需的调度、工具调用、状态、权限、观测和弹性基础设施平台化。

### 1.3 目标

- 提供统一的 Agent Session、任务、工具和事件模型，支持长任务、暂停/恢复、重试和幂等执行。
- 通过 Serverless Worker 按需启动隔离的 Agent Sandbox，按并发弹性扩缩容并回收空闲实例。
- 打通 Trace、Log、Metric、Event 四类观测数据，支持按 Run 一键回放。
- 以企业 IAM 为边界实施最小权限、临时凭证和全量审计，防止权限穿透。
- 支持 Planner、Coder、Reviewer、Security 等多 Agent 协作，由 SuperAgent 进行路由编排。
- 以 MCP 兼容协议接入 Git、制品库、工单、数据库等企业工具，并保留替换模型/框架的能力。

### 1.4 典型演示场景

用户提交“分析订单服务最近一次发布导致的错误率上升，给出修复建议并创建工单”。SuperAgent 拆解任务：Observability Agent 查询指标和日志，Coder Agent 检查代码变更，Security Agent 校验敏感操作，Reviewer Agent 汇总证据；所有调用均在受限 Sandbox 中执行，结果可追踪、可审批、可回放。

## 2. 范围与非目标

### 2.1 本期范围（MVP）

- OpenAI/兼容模型适配器、Tool Calling、MCP Client。
- Session/Run/Task/Tool Invocation 生命周期管理。
- 基于 Kubernetes Job/Deployment 的 Sandbox Worker，支持队列消费、超时和重试。
- OTel Trace/Metric、结构化日志、领域 Event 总线和 Run Replay。
- OAuth2/OIDC 登录、RBAC + ABAC 策略、短期工具凭证、审批流和审计报表。
- SuperAgent 路由、DAG 任务编排、Agent 间消息协议。

### 2.2 非目标

- 自研基础大模型和通用 Kubernetes 调度器。
- 对生产数据库执行不可逆变更（仅提供只读示范和审批后的模拟操作）。
- 一次性覆盖所有企业系统；通过 MCP Adapter 保持可扩展。

## 3. 总体架构

```text
                    ┌──────────────────────────────┐
                    │ Web Console / CLI / OpenAPI  │
                    └──────────────┬───────────────┘
                                   │
                 ┌─────────────────▼─────────────────┐
                 │ API Gateway + OIDC + Rate Limit   │
                 └──────────┬──────────────┬──────────┘
                            │              │
             ┌──────────────▼───┐  ┌─────▼──────────────┐
             │ Control Plane     │  │ Policy/IAM Service  │
             │ Session/Run/Task   │  │ RBAC/ABAC/Approval  │
             └──────┬────────────┘  └──────────┬─────────┘
                    │                         │
          ┌─────────▼─────────┐     ┌─────────▼──────────┐
          │ Orchestrator       │◄───►│ Event Bus           │
          │ DAG/SuperAgent     │     │ Kafka/Redis Stream  │
          └─────────┬─────────┘     └─────────┬──────────┘
                    │                         │
          ┌─────────▼─────────────────────────▼─────────┐
          │ Runtime Gateway / Worker Scheduler           │
          │ queue、lease、retry、quota、idempotency       │
          └─────────┬─────────────────────────┬─────────┘
                    │                         │
       ┌────────────▼───────────┐   ┌─────────▼───────────┐
       │ Agent Sandbox Workers   │   │ MCP Tool Gateway     │
       │ container + seccomp     │   │ Git/CI/DB/Ticket     │
       └────────────┬───────────┘   └─────────┬───────────┘
                    │                         │
             ┌──────▼──────┐           ┌──────▼────────────┐
             │ Model Router │           │ Audit/Observability│
             │ OpenAI/Claude│           │ OTel + ClickHouse  │
             └─────────────┘           └───────────────────┘
```

### 3.1 组件职责

| 组件 | 主要职责 | 建议实现 |
|---|---|---|
| API Gateway | 鉴权、限流、幂等键、流式 SSE/WebSocket | TypeScript/NestJS 或 Go |
| Control Plane | Session、Run、Task、配置和配额 CRUD | Java/Spring Boot |
| Orchestrator | 状态机、DAG、SuperAgent 路由、人工审批节点 | Python + LangGraph |
| Runtime Gateway | 任务投递、租约、重试、取消、背压 | Go |
| Worker | Prompt 执行、模型调用、工具调用、上下文压缩 | Python |
| MCP Tool Gateway | MCP 协议适配、参数校验、凭证注入、审计 | TypeScript |
| Policy/IAM | RBAC/ABAC、策略评估、审批和临时 Token | Java |
| Event/Telemetry | 事件总线、Trace/Log/Metric 汇聚和查询 | Kafka + OTel + ClickHouse |
| Console | Run 时间线、Trace、审批、回放和成本看板 | React/TypeScript |

## 4. 核心领域模型与状态机

### 4.1 核心实体

- **Session**：用户与 Agent 的长期会话，包含租户、项目、默认策略和上下文摘要。
- **Run**：一次可观测、可回放的执行实例；拥有 `run_id`、`trace_id`、预算和超时。
- **Task**：Run 内的原子或 DAG 节点，记录输入、输出、依赖和重试次数。
- **ToolInvocation**：一次工具调用，保存 schema 版本、脱敏参数、策略决策和结果摘要。
- **AgentMessage**：Agent 间消息，包含 `message_id`、`correlation_id`、能力标签和 TTL。
- **Approval**：高风险动作的人工审批请求，绑定策略快照和过期时间。

### 4.2 状态

```text
Run:     CREATED -> QUEUED -> RUNNING -> (WAITING_APPROVAL | PAUSED)
                                      -> SUCCEEDED / FAILED / CANCELLED / TIMEOUT
Task:    PENDING -> LEASED -> EXECUTING -> RETRYING -> SUCCEEDED/FAILED/SKIPPED
Worker:  STARTING -> READY -> BUSY -> DRAINING -> TERMINATED
```

状态迁移使用数据库乐观锁和唯一事件键；Worker 通过 lease 心跳续租，租约过期后由调度器安全接管，避免重复执行。

### 4.3 数据存储

- PostgreSQL：实体、状态机、策略版本、审批和审计索引。
- Redis：短期 Session 缓存、分布式锁、队列去重和 Worker lease。
- Kafka：领域事件、Agent 消息和异步遥测管道。
- S3/MinIO：长文本、代码补丁、原始工具输出和回放快照。
- ClickHouse：Trace Span、成本、事件和运营分析。

## 5. 关键流程

### 5.1 创建并执行 Run

1. 客户端携带 OIDC Token、`Idempotency-Key` 创建 Run。
2. API Gateway 校验租户配额，Control Plane 写入 Run 和初始 Task。
3. Orchestrator 生成 DAG；Runtime Gateway 将可运行 Task 投递到优先级队列。
4. Scheduler 获取 Worker lease；若无空闲 Worker，则调用 Kubernetes API 按镜像、资源和租户配额启动 Sandbox。
5. Worker 拉取 Task，建立 OTel Span，执行模型/工具调用，并以事件方式上报增量结果。
6. Orchestrator 根据结果解锁后继 Task；遇到高风险工具则挂起 Run，等待 Approval。
7. 所有节点完成后生成摘要、成本明细和 Replay Snapshot，Run 进入终态。

### 5.2 工具调用与 MCP

```json
{
  "name": "git.diff",
  "arguments": {"repo": "order-service", "base": "prod", "head": "release-2026.07"},
  "context": {"tenant_id": "acme", "run_id": "run_123", "purpose": "diagnosis"}
}
```

MCP Tool Gateway 按工具注册表校验 JSON Schema，执行顺序为：身份确认 → 策略评估 → 参数脱敏/注入 → 下游调用 → 输出过滤 → 审计落库。写操作默认 `deny`，需具备 `ticket.write` 权限且获得一次性 Approval Token。

### 5.3 Multi-Agent 协作

- **SuperAgent**：识别意图、选择能力、生成 DAG，不直接持有业务系统凭证。
- **Specialist Agent**：通过 capability registry 注册能力和输入输出 schema。
- **Message Bus**：使用 `request/reply`、`publish/subscribe` 两种模式；消息带 correlation/causation ID、优先级、TTL 和最大跳数。
- **共享上下文**：短上下文放 Redis，版本化事实和证据放 PostgreSQL/S3；禁止 Agent 直接修改其他 Agent 的状态。
- **冲突处理**：Reviewer Agent 对关键结论进行独立验证；超过置信度阈值才允许自动关闭任务。

## 6. Serverless Agent 基础设施

### 6.1 Worker 生命周期

1. Scheduler 根据队列深度、任务资源画像和冷启动预算计算期望副本数。
2. 通过 KEDA/自定义 Controller 扩容 Worker Deployment；高隔离任务使用 Kubernetes Job + gVisor。
3. Worker 启动时仅获取短期、任务范围内的凭证，注册能力和心跳。
4. 任务完成或超时后进入 `DRAINING`，清理临时目录、撤销凭证并回收 Pod。

### 6.2 弹性与可靠性策略

- 目标：p95 冷启动 < 8 秒，队列等待 p95 < 3 秒。
- 每租户并发、CPU、Token 和费用配额；队列按租户加权公平调度。
- 指数退避重试（最多 3 次），对非幂等工具仅允许带幂等键重试。
- Worker 失联由 lease watchdog 接管；Run 通过 checkpoint 从最近一个成功 Task 恢复。
- 熔断模型供应商和下游工具，支持降级模型及人工接管。

## 7. 身份、权限与安全边界

- 登录：OIDC/OAuth2；服务间调用使用 mTLS + SPIFFE/SPIRE 工作负载身份。
- 授权：RBAC（角色）叠加 ABAC（租户、项目、环境、资源标签、时间窗）。策略以 Cedar/OPA 表达并版本化。
- 最小权限：Agent 不继承用户全量权限；Policy Service 根据 `subject + agent + tool + resource + purpose` 生成一次性 Token。
- 权限穿透防护：Tool Gateway 是唯一出口，Worker 无法直连内网；NetworkPolicy、egress allowlist、seccomp、只读根文件系统和资源限额共同隔离。
- 数据安全：Prompt/工具参数按字段脱敏，密钥进入 Vault；日志默认不记录原文，支持租户级保留周期和删除请求。
- 审批：`prod.deploy`、`ticket.write`、`db.mutate` 等动作触发审批，Approval Token 绑定 `run_id`、参数哈希和 10 分钟过期时间。

## 8. 可观测性与调试回放

### 8.1 统一关联字段

所有请求和事件携带 `tenant_id`、`session_id`、`run_id`、`task_id`、`agent_id`、`tool_call_id`、`trace_id`，通过 W3C Trace Context 跨 API、队列、Worker 和 MCP 传播。

### 8.2 四类信号

- **Trace**：模型调用、Prompt 组装、队列等待、工具调用、审批和 Agent 消息均为 Span。
- **Log**：JSON 结构化日志，包含 severity、decision、latency、error_type 和脱敏后的摘要。
- **Metric**：成功率、端到端延迟、Token/费用、队列深度、冷启动、工具错误率、策略拒绝数。
- **Event**：`RunCreated`、`TaskRetried`、`ToolDenied`、`ApprovalGranted` 等不可变领域事件。

### 8.3 Replay

每个 Task 保存模型版本、system prompt hash、工具 schema 版本、输入快照和随机种子。回放模式默认使用录制的工具响应，避免再次修改生产数据；对需要重跑的节点可替换模型并标记 `replay=true`，结果与原 Run 做 diff。

## 9. API 草案

```http
POST /v1/sessions
POST /v1/runs
GET  /v1/runs/{run_id}
POST /v1/runs/{run_id}/cancel
POST /v1/runs/{run_id}/approvals/{approval_id}
GET  /v1/runs/{run_id}/timeline
POST /v1/runs/{run_id}/replay
GET  /v1/tools?capability=code.read
```

`POST /v1/runs` 请求示例：

```json
{
  "session_id": "ses_abc",
  "agent": "superagent",
  "input": "分析订单服务发布后的错误率异常并创建修复工单",
  "budget": {"max_tokens": 12000, "max_cost_cents": 80},
  "execution": {"timeout_seconds": 900, "approval_mode": "required_for_write"}
}
```

返回 `202 Accepted` 和 `run_id`；客户端通过 SSE `/v1/runs/{run_id}/events` 获取增量事件。

## 10. 技术栈与工程规范

- TypeScript：NestJS API Gateway、MCP Gateway、React Console。
- Python：Worker、LangGraph Orchestrator、模型适配器和评测脚本。
- Go：Runtime Gateway、Scheduler、lease watchdog（高并发、低延迟）。
- Java：Policy/IAM、审计服务（Spring Boot、PostgreSQL）。
- 基础设施：Docker、Kubernetes、Helm、KEDA、Kafka、Redis、PostgreSQL、ClickHouse、MinIO、Vault、OpenTelemetry、Prometheus、Grafana。
- 工程质量：OpenAPI/JSON Schema 合约测试，golangci-lint/ruff/ESLint/Checkstyle，Trivy 镜像扫描，GitHub Actions CI，SLO 驱动告警。

## 11. 里程碑（8 周）

| 周期 | 交付内容 | 验收标准 |
|---|---|---|
| 第 1 周 | 领域模型、OpenAPI、单体骨架、开发环境 | 可创建 Session/Run，事件可查询 |
| 第 2 周 | Worker、队列、lease、重试和取消 | 任务成功率 ≥ 99%，可恢复中断任务 |
| 第 3 周 | MCP Gateway 与 Git/日志两个工具 | Schema 校验、超时、幂等、审计可见 |
| 第 4 周 | LangGraph DAG、SuperAgent、Agent 消息 | 三个 Specialist Agent 可协作完成演示场景 |
| 第 5 周 | Kubernetes Serverless、KEDA、Sandbox 隔离 | 并发 50 Run，冷启动 p95 < 8 秒 |
| 第 6 周 | OTel、ClickHouse、Grafana、Replay | 一键定位失败节点并完成离线回放 |
| 第 7 周 | OIDC、RBAC/ABAC、OPA、审批和 Vault | 越权调用 100% 拦截，凭证自动过期 |
| 第 8 周 | 压测、故障演练、文档和演示 | SLO、Runbook、架构答辩材料齐备 |

## 12. 测试与验收

### 12.1 自动化测试

- 单元测试：状态机、策略决策、重试/幂等、消息路由（覆盖率 ≥ 80%）。
- 合约测试：MCP Tool schema、模型适配器、OpenAPI 和事件 schema 向后兼容。
- 集成测试：Kafka 重复消息、Redis 故障、Worker 崩溃、数据库锁冲突。
- 安全测试：越权、Prompt Injection、SSRF、工具参数污染、敏感信息泄漏。
- 评测测试：固定 30 条故障诊断数据集，比较任务成功率、证据完整度、Token 成本和人工介入率。

### 12.2 故障演练

1. 杀掉执行中的 Worker：Run 在 30 秒内被重新租赁，已完成 Task 不重复执行。
2. 模型供应商超时：触发熔断并切换备用模型，用户收到可解释事件。
3. 撤销用户角色：尚未执行的写操作立即拒绝，已发放 Token 失效。
4. Kafka 延迟：背压指标告警，队列不丢消息，恢复后按优先级消费。

## 13. 演示脚本与产出物

### 13.1 10 分钟演示

1. CLI 创建 Run，展示 SuperAgent 生成 DAG。
2. Console 查看实时 Trace、队列等待和 Token 成本。
3. Tool Gateway 拦截 `ticket.write`，展示审批卡片和策略解释。
4. 批准后完成工单创建，展示审计记录和短期 Token 已撤销。
5. 人为杀掉 Coder Worker，展示自动恢复和幂等结果。
6. 点击 Replay，使用录制工具响应重跑 Reviewer，并对比差异。

### 13.2 代码仓库结构

```text
nexus-agent-runtime/
├── apps/console/              # React Console
├── services/api-gateway/      # TypeScript
├── services/policy/           # Java/Spring Boot
├── services/runtime-gateway/  # Go
├── services/orchestrator/     # Python/LangGraph
├── services/worker/           # Python Sandbox Worker
├── services/mcp-gateway/      # MCP adapters
├── packages/contracts/        # OpenAPI/Event/Tool schema
├── deploy/helm/               # K8s、KEDA、NetworkPolicy
├── evals/                     # 评测集和回放脚本
└── docs/                      # ADR、Runbook、威胁模型
```

## 14. 面试展示重点与可量化结果

- 用一张架构图说明 Control Plane 与 Data Plane 的边界，以及为什么 Tool Gateway 是安全边界。
- 现场演示 Worker 崩溃恢复、审批拒绝和 Replay，不只展示“模型回答”。
- 用压测数据说明：50 并发 Run、p95 端到端延迟、冷启动、成功率、Token 成本和租户公平性。
- 说明一次关键取舍：MVP 使用 LangGraph 编排，执行和治理能力放在自研 Runtime，后续可替换编排框架而不影响平台。

简历可描述为：

> 设计并实现企业级 Agent Runtime，基于 Go/Python/TypeScript/Java 构建任务调度、Kubernetes Serverless Worker、MCP Tool Gateway、OPA 权限治理和 OTel 全链路观测；支持多 Agent DAG 协作、故障恢复与可回放调试，在 50 并发压测下实现 p95 冷启动 < 8 秒、越权调用 100% 拦截。
