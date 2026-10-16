# Nexus Agent Runtime

Nexus Agent Runtime 的可运行纵向切片：创建 Session 和幂等 Run，由进程内 fake worker 通过真实 Task Lease 执行固定 Task，支持租约过期回收、失败重试和重试耗尽，并从 PostgreSQL 查询完整事件时间线。

## 范围

- Spring Boot 3 + Java 21 模块化单体
- PostgreSQL 持久化和 Flyway 迁移
- Run 与 Task 显式状态机
- 基于真实 Task Lease 的数据库任务领取和可重启 fake worker
- 持久化 `RETRYING`、指数退避和 Task/Run 原子失败
- append-only `domain_events` 时间线
- OpenAPI 3.1 和 JSON Schema 合约
- Docker Compose 一键启动

当前唯一任务的固定输出为：

```json
{
  "summary": "发现错误率上升，需要进一步检查最近发布"
}
```

## 启动

需要 Docker 与 Docker Compose。项目无需本机安装 Java 或 PostgreSQL。

```bash
cd /home/dev/nexus-agent-runtime
docker compose -f deploy/docker-compose.yml up --build -d
docker compose -f deploy/docker-compose.yml ps
curl --fail http://localhost:8080/actuator/health
```

首次构建会下载 Maven 依赖并执行单元测试。PostgreSQL 使用命名卷 `nexus-postgres-data`，普通的 `down`、容器重建或服务重启不会删除数据。只有显式执行 `docker compose down -v` 才会删除数据卷。

## 演示闭环

创建 Session：

```bash
curl -sS -X POST http://localhost:8080/v1/sessions
```

使用返回的 `session_id` 创建 Run：

```bash
curl -i -X POST http://localhost:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-order-errors-001' \
  -d '{
    "session_id": "<session_id>",
    "input": "分析订单服务错误率"
  }'
```

接口返回 `202 Accepted` 和 `run_id`。再次发送完全相同的请求会得到同一个 `run_id`，不会创建第二个 Run；同一幂等键配合不同请求会返回 `409 Conflict`。

查询最终状态和有序时间线：

```bash
curl -sS http://localhost:8080/v1/runs/<run_id>
curl -sS http://localhost:8080/v1/runs/<run_id>/timeline
```

正常时间线包含以下事件：

```text
RunCreated -> TaskCreated -> RunQueued -> TaskLeased
           -> RunStarted -> TaskExecuting -> TaskSucceeded -> RunSucceeded
```

任务领取时会原子写入 `lease_owner`、`lease_expires_at` 和 `attempt`。只有租约持有者
且租约未过期时，worker 才能推进任务到 `EXECUTING` 或 `SUCCEEDED`。watchdog 会在轮询时
将过期的 `LEASED`/`EXECUTING` 任务清除租约，记录 `TaskLeaseExpired`，再按同一失败策略
安排重试或标记失败。租约时长可通过 `FAKE_WORKER_LEASE_DURATION_MS` 配置，默认 30 秒。

执行异常和租约过期都消耗当前尝试。默认 `max_attempts=3` 表示含首次在内最多执行 3 次，
每次领取才递增 `attempt`。尚有额度时，Task 进入 `RETRYING` 并写入 `TaskRetried`；
达到额度时，Task 与 Run 在同一事务内进入 `FAILED`，写入 `TaskFailed -> RunFailed`。
重试期间 Run 保持 `RUNNING`，不会重复写入 `RunStarted`。

退避为 `min(initial_delay × 2^(attempt-1), max_delay)`，配置如下：

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `FAKE_WORKER_RETRY_INITIAL_DELAY_MS` | `1000` | 首次失败后的等待时间 |
| `FAKE_WORKER_RETRY_MAX_DELAY_MS` | `30000` | 等待时间上限 |

等待时间以数据库时钟写入 `next_attempt_at`；到期前任何 worker 都不能领取此任务。
迟到、重复或来自其他 worker 的失败报告不会改变状态或追加事件。
默认 fake executor 仍返回固定成功结果；测试通过替换 `TaskExecutor` 注入执行故障。

验证重启持久化：

```bash
docker compose -f deploy/docker-compose.yml restart control-plane
curl -sS http://localhost:8080/v1/runs/<run_id>
curl -sS http://localhost:8080/v1/runs/<run_id>/timeline
```

## 测试

镜像构建默认执行测试。也可以单独在 Maven 容器中运行：

```bash
docker run --rm \
  -v "$PWD/services/control-plane:/workspace" \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 mvn -B test
```

状态机和重试策略测试无需数据库。PostgreSQL 集成测试需要显式提供 `TEST_DB_URL`
（可选 `TEST_DB_USERNAME` / `TEST_DB_PASSWORD`，均默认为 `nexus`）；未提供时会跳过。
集成测试会清空该数据库的业务表，必须使用独立测试库。下面的命令会创建临时 PostgreSQL，
执行全部测试并保留 Maven 缓存到已忽略的 `target/` 目录：

```bash
docker compose -f deploy/docker-compose.test.yml up --abort-on-container-exit --exit-code-from tests
docker compose -f deploy/docker-compose.test.yml down
```

测试覆盖退避到期前不可领取、重试成功、耗尽失败、`LEASED`/`EXECUTING` 租约过期、
重复与迟到报告、多个 watchdog 竞争、成功与失败竞争、事务回滚和成功任务不再执行。

## 目录

```text
nexus-agent-runtime/
├── services/control-plane/       # Spring Boot API、状态机、fake worker
├── packages/contracts/
│   ├── openapi.yaml              # HTTP API 合约
│   └── events/                   # 领域事件 JSON Schema
├── deploy/docker-compose.yml     # 服务与 PostgreSQL
├── docs/adr/0001-mvp-vertical-slice.md
└── README.md
```

架构取舍见 [ADR 0001](docs/adr/0001-mvp-vertical-slice.md)、
[ADR 0002](docs/adr/0002-task-lease.md) 和 [ADR 0003](docs/adr/0003-task-failure-and-retry.md)。
