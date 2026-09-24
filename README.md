# Nexus Agent Runtime

Nexus Agent Runtime 的第一个可运行纵向切片：创建 Session 和幂等 Run，由进程内 fake worker 执行固定 Task，并从 PostgreSQL 查询完整事件时间线。

## 范围

- Spring Boot 3 + Java 21 模块化单体
- PostgreSQL 持久化和 Flyway 迁移
- Run 与 Task 显式状态机
- 数据库任务领取和可重启的 fake worker
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

状态机测试覆盖允许的完整路径，以及跳级、终态回退等非法迁移。

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

架构取舍见 [ADR 0001](docs/adr/0001-mvp-vertical-slice.md)。
