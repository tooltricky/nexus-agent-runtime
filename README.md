# Nexus Agent Runtime

Nexus Agent Runtime 的可运行纵向切片：创建 Session 和幂等 Run，由进程内 fake worker 通过真实 Task Lease 执行固定 Task，支持租约过期回收、失败重试和重试耗尽，并从 PostgreSQL 查询完整事件时间线。

## 范围

- Spring Boot 3 + Java 21 模块化单体
- PostgreSQL 持久化和 Flyway 迁移
- Run 与 Task 显式状态机及原子取消接口、重复取消与迟到结果防护
- 基于真实 Task Lease 的数据库任务领取和可重启 fake worker
- 持久化 `RETRYING`、指数退避和 Task/Run 原子失败
- append-only `domain_events` 时间线
- OpenAPI 3.1 / JSON Schema 2020-12 合约及真实 HTTP / 事件自动验证
- GitHub Actions CI：独立 PostgreSQL 测试、打包与测试报告防跳过检查
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

## 取消 Run（B05）

```bash
curl -i -X POST http://localhost:8080/v1/runs/<run_id>/cancel
```

无需请求体或 `Idempotency-Key`，请求体不参与处理。排队、已领取、执行中和等待重试的工作
均可取消。成功返回 `200` 和当前 Run，Run 与 Task 都为 `CANCELLED`；租约清空，输出省略，
尝试次数、重试时间与历史记录保留。已取消时重复请求同样返回 `200`，不改变版本、时间或追加事件。
不存在的 Run 返回 `404 {code: not_found, ...}`；已成功 / 失败的 Run 返回
`409 {code: cancellation_conflict, ...}`，保留原状态和结果。

取消在同一数据库事务中保存状态并依次追加 `TaskCancelled -> RunCancelled`，事件写入失败则
全部回滚。取消与成功、最终失败竞争时，先有效提交的终态事务获胜；已提交的重试仍可取消。
取消后 worker 不能再领取或回收该 Task，旧 owner、旧 attempt 或旧版本提交的迟到结果均无效。
成功、失败、取消都是不可逆终态，规则见 [ADR 0005](docs/adr/0005-cancellation-foundation.md)。

当前执行器仍在持有 Task 行锁的事务内调用，取消可能等待正在执行的事务结束；若该事务先成功
提交，取消会返回 `409`。取消不撤销已发生的外部操作，也不保证立即中断外部调用。长事务拆分
属于 B06，心跳续租属于 B07，本任务尚未提供这些能力。fake worker 通常很快完成，演示中取消
已完成的 Run 得到 `409` 属于正常结果；自动测试通过受控执行器和数据库同步覆盖各取消阶段。

B05 复用 B04 的 V4 迁移，无新增迁移，V1–V4 未改动；历史事件不回填 schema_version。
Run / timeline 成功查询及取消成功响应带 `Nexus-API-Contract: 0.2`。旧 HTTP 定义保存在
`packages/contracts/openapi-v0.1.yaml`，实际取消事件带 `schema_version: "0.2"`，其他既有事件
仍省略版本。启用取消前，客户端须先支持 CANCELLED 和混合版本事件。

## 测试

镜像构建默认执行测试。也可以单独在 Maven 容器中运行：

```bash
docker run --rm \
  -v "$PWD/services/control-plane:/workspace" \
  -v "$PWD/packages/contracts:/packages/contracts:ro" \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 mvn -B test
```

状态机和重试策略测试无需数据库。PostgreSQL 集成测试需要显式提供 `TEST_DB_URL`
（可选 `TEST_DB_USERNAME` / `TEST_DB_PASSWORD`，均默认为 `nexus`）；未提供时会跳过。
集成测试会清空该数据库的业务表，必须使用独立测试库。下面的命令会创建临时 PostgreSQL，
执行 `mvn verify`（测试并打包），并保留 Maven 缓存到已忽略的 `target/` 目录：

```bash
docker compose -f deploy/docker-compose.test.yml up --abort-on-container-exit --exit-code-from tests
docker compose -f deploy/docker-compose.test.yml down
```

测试覆盖如下场景：

- 真实 HTTP 服务的五个接口、固定成功输出、完整有序时间线、请求校验和 400 / 404 / 409 错误码。
- 顺序和并发幂等创建；同键不同 input 或有效 Session 的竞争只保留一个 Run / Task 和一组创建事件。
- 多个独立 worker 通过独立事务竞争单个或多个 Task，每个 Task 只领取一次，完成后不再执行。
- 关闭并重建整个 Spring 应用上下文及连接池后，成功结果和事件保持不变，定时 worker 继续处理排队任务；
  `LEASED` / `EXECUTING` 的有效旧租约受到保护，过期后先回收并退避，再由新 owner 接管，拒绝旧 owner 的迟到报告。
- 数据库拒绝事件 UPDATE / DELETE，原时间线的数量和内容不变。
- 退避到期前不可领取、重试成功、耗尽失败、租约过期、重复与迟到报告、多个 watchdog 竞争、
  成功与失败竞争、事务回滚和成功任务不再执行。
- 四个阶段取消、并发重复取消、终态冲突、真实版本化事件及合约反例；取消与领取、成功、失败、
  重试、watchdog 按两种先后顺序使用独立事务竞争；首个事务持锁时观察第二个事务确已等待或
  通过 SKIP LOCKED 跳过，再放行提交。数据库触发器注入第一 / 第二条取消事件写入失败，
  验证状态、租约和事件一起回滚；已回滚的取消不算竞争获胜。

HTTP 回归测试为避免后台调度干扰并发创建断言，禁用定时 worker，通过独立事务主动推进真实 fake worker；
重启测试另行启用真实定时 worker 验证自动执行。租约和退避测试直接调整独立测试库的时间字段，避免长时间等待。
共享清表和有超时的并发同步辅助代码位于 `src/test/java/io/nexus/controlplane/support/RuntimeTestSupport.java`。
测试类共享同一测试库，不能并行运行这些集成测试类。

合约测试随 `mvn test` 执行，通过 networknt JSON Schema 校验器（2020-12，显式校验时间格式）
读取仓库的 OpenAPI 响应定义及外部事件 Schema。无需数据库的反例测试可单独运行：

```bash
docker run --rm \
  -v "$PWD/services/control-plane:/workspace" \
  -v "$PWD/packages/contracts:/packages/contracts:ro" \
  -w /workspace \
  maven:3.9.11-eclipse-temurin-21 mvn -B -Dtest=ContractValidationTest test
```

完整验收仍使用上面的独立 PostgreSQL Compose，检查 `services/control-plane/target/surefire-reports/`
报告中的实际执行数与跳过数。历史验证结果见 [backlog 完成记录](docs/backlog.md#完成记录)。
没有设置 `TEST_DB_URL` 的单元测试运行不能代替独立数据库验证。合约字段与演进规则见
[合约说明](packages/contracts/README.md)。测试读取 `packages/contracts`，因此 Maven 从完整仓库运行；
Docker 构建上下文也使用仓库根目录。

## 持续集成（CI）

`.github/workflows/ci.yml` 在 GitHub 收到 `push`、`pull_request` 或手动触发时运行。
工作流使用标准 Ubuntu runner，在 `maven:3.9.11-eclipse-temurin-21` 容器内编译 Java 21、
执行现有单元 / 数据库集成 / 合约测试并打包。数据库来自同一份测试 Compose，
使用临时 PostgreSQL，不连接演示或业务数据库；不需要配置开发者密钥或真实模型凭证。
集成测试仍串行运行，避免共享清表互相干扰。

每次 CI 使用新 runner 的工作区，不复用旧测试报告。Maven 返回成功后仍要检查 Surefire XML：
所有 11 个现有测试类必须有报告并实际执行，任何报告包含失败、错误、跳过、零项测试、
不一致的计数或损坏内容都会使 CI 失败。新增或重命名测试类时，同步维护
`scripts/check-test-reports.py` 的 `REQUIRED_SUITES`；发现的其他测试类也接受同样检查。
所以即使漏配 `TEST_DB_URL` 导致 Maven 跳过集成测试，整次 CI 也不会显示成功。
报告检查不替代 Maven 的退出码：编译、测试、打包或 Compose 失败都会保留失败结果。

本地运行报告检查器和它的回归测试需要 Python 3.10+，无需安装第三方 Python 包：

```bash
python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
python3 scripts/check-test-reports.py
```

运行第二条命令前，先使用测试 Compose 完成一次全量验证。检查器只检查现有报告，
不会启动测试，也不判断文件是否属于本次运行；重复本地验收时先清理
`services/control-plane/target/surefire-reports/` 中的旧报告，不能拿历史报告证明本次通过。
不要删除整个 `target/` 来清报告，否则会一并删除其中的 Maven 缓存。

在 GitHub 的 **Actions → CI → 对应运行** 查看各步骤结果。无论测试成功或失败，
后续步骤都会尝试收集容器日志、清理测试容器和网络，并上传 `ci-reports-<运行编号>-<重跑次数>`
附件（Compose 输出、容器日志和 Surefire XML / 文本报告），保留 7 天。
报告检查失败的具体原因也会显示在该步骤日志中；若下载镜像或编译前就失败，
可能没有 Surefire 报告，CI 会保留当时可获取的日志。

本地验证不等同于 GitHub 平台验收；首次推送后还需确认一次真实 Actions 运行完成、
附件可下载且清理步骤执行。B03 的实际验证与未完成项见 [完成记录](docs/backlog.md#完成记录)。

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
[ADR 0002](docs/adr/0002-task-lease.md)、[ADR 0003](docs/adr/0003-task-failure-and-retry.md)
、[ADR 0004](docs/adr/0004-contract-validation.md)
和 [ADR 0005](docs/adr/0005-cancellation-foundation.md)。
