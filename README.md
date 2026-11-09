# Nexus Agent Runtime

Nexus Agent Runtime 的可运行纵向切片：创建 Session 和幂等 Run，由进程内 fake worker 通过真实 Task Lease 执行固定 Task，支持租约过期回收、失败重试和重试耗尽，并从 PostgreSQL 查询完整事件时间线。

## 范围

- Spring Boot 3 + Java 21 模块化单体
- PostgreSQL 持久化和 Flyway 迁移
- Run 与 Task 显式状态机及原子取消接口、重复取消与迟到结果防护
- 基于真实 Task Lease 的数据库任务领取和可重启 fake worker
- 独立心跳续租与租约 / 心跳配置的启动校验
- 持久化截止时间、原子 TIMEOUT、独立超时扫描与协作式执行中断
- 持久化 PAUSED、暂停 / 恢复接口与 Task 边界 checkpoint
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
不存在的 Run 返回 `404 {code: not_found, ...}`；已成功 / 失败 / 超时的 Run 返回
`409 {code: cancellation_conflict, ...}`，保留原状态和结果。

取消在同一数据库事务中保存状态并依次追加 `TaskCancelled -> RunCancelled`，事件写入失败则
全部回滚。取消与成功、最终失败竞争时，先有效提交的终态事务获胜；已提交的重试仍可取消。
取消后 worker 不能再领取或回收该 Task，旧 owner、旧 attempt 或旧版本提交的迟到结果均无效。
成功、失败、取消、超时都是不可逆终态，规则见 [ADR 0005](docs/adr/0005-cancellation-foundation.md)。

执行器调用不持有 Task 行锁的数据库事务：领取、进入执行和提交结果分别使用短事务，执行器
运行期间其他 Run 和 watchdog 仍可推进。提交结果会校验 owner、attempt、version 和有效租约，
因此租约过期后旧执行者的迟到结果会被拒绝。取消不撤销已发生的外部操作，也不保证立即中断
外部调用。B07 已增加独立心跳续租，规则见下文。fake worker 通常很快完成，演示中取消已
完成的 Run 得到 `409` 属于正常结果；自动测试通过受控执行器和数据库同步覆盖各取消阶段。

worker 和 lease watchdog 使用两个独立的定时线程；worker 领取后把执行工作提交到独立的
执行线程池，因此同一个进程中一个 Run 的执行器阻塞时，其他 Run 仍能继续领取和推进。
worker 会先预留执行名额再领取任务，执行线程池不保留等待队列；没有空闲名额时，任务继续
保持 `PENDING`，不会提前开始租约或消耗 attempt。并发名额由
`FAKE_WORKER_EXECUTION_CONCURRENCY` 配置，默认 16。
如果执行线程池暂时没有容量，派发会被短暂退避，不会阻塞调度线程或反复选择同一个旧任务；
退避时间由 `FAKE_WORKER_DISPATCH_RETRY_DELAY_MS` 配置，默认 200 毫秒，到期后再尝试派发。
执行线程在真正调用执行器前会再次检查 owner、attempt、version 和有效租约。watchdog 仍能
独立回收过期租约，即使 `FAKE_WORKER_ENABLED=false` 停止 fake worker 领取和执行也会继续工作；
可通过 `LEASE_WATCHDOG_ENABLED=false` 显式关闭 watchdog，默认保持开启。
每轮最多处理 `LEASE_WATCHDOG_BATCH_SIZE` 个任务，默认 50，轮询间隔由
`LEASE_WATCHDOG_POLL_DELAY_MS` 配置，默认 1 秒。已过期的 `LEASED` / `EXECUTING` 任务不会
再次进入执行器，必须先由 watchdog 回收。

B05 复用 B04 的 V4 迁移，无新增迁移，V1–V4 未改动；历史事件不回填 schema_version。
Run / timeline 成功查询及取消成功响应在 B09 后带 `Nexus-API-Contract: 0.4`。旧 HTTP 定义保存在
`packages/contracts/openapi-v0.1.yaml`，实际取消事件带 `schema_version: "0.2"`，其他既有事件
仍省略版本。启用取消前，客户端须先支持 CANCELLED 和混合版本事件。

## 心跳续租（B07）

worker 从领取任务后开始跟踪租约，进入执行时换用最新的状态版本，直到结果提交结束。
独立心跳线程定期延长有效租约，所以执行器工作超过最初的 30 秒时仍有机会正常提交结果。
心跳不占用执行线程池，也不使用 worker 或 watchdog 的定时线程；任务完成后清理注册，
取消或失去租约后下一次心跳被拒绝并清理，应用关闭时清理注册和心跳调度资源。

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `FAKE_WORKER_LEASE_DURATION_MS` | `30000` | 领取及续租时使用的租约时长，单位毫秒 |
| `FAKE_WORKER_HEARTBEAT_INTERVAL_MS` | `10000` | 心跳轮询间隔，单位毫秒 |

对应 Spring 配置为 `nexus.worker.lease-duration-ms` 和 `nexus.worker.heartbeat-interval-ms`。
两个值必须为正，心跳间隔必须小于租约时长，否则应用启动失败并指出原因；关闭 worker 时
仍执行配置校验。修改时应为调度和数据库延迟留出余量，例如 30 秒租约配合 10 秒心跳。
环境变量须传入 control-plane 容器；在宿主机设置变量并不会自动注入现有 Compose 服务。

续租在独立短事务中用 `FOR UPDATE NOWAIT` 尝试锁定 Task，再按数据库当前时间检查 owner、
attempt、version、活动状态及未过期租约。遇到行锁立即跳过本轮，保留注册下轮重试，使其他任务
仍能续租；锁释放后如果已经过期，也不能续回来。心跳只延长租约并更新 `updated_at`，
不增加 attempt 或状态版本，不追加领域事件；原 `TaskLeased` 事件保留领取时的到期时间，
查询 Task 的 `lease_expires_at` 可看到当前到期时间。结果提交仍使用原有版本和有效租约检查。

心跳遇到数据库故障时记录任务标识并留待下一轮尝试，不将其记成模型失败；下一轮如果已经
过期，续租仍会被拒绝。停止心跳后，watchdog 按原有退避和次数限制回收任务；新 attempt
接管后，旧心跳和迟到的成功 / 失败报告均无效。取消、完成与续租竞争也不会让终态任务复活。
心跳代表 worker 仍持有任务，不代表下游调用有进展；执行器一直卡住但心跳正常时会持续续租。
B08 已增加截止时间 / TIMEOUT，规则见下文。心跳实现规则见 [ADR 0006](docs/adr/0006-lease-heartbeats.md)。

## 截止时间与超时（B08）

创建 Run 时可选传 `deadline_at`，例如：

```json
{
  "session_id": "<session_id>",
  "input": "分析订单服务错误率",
  "deadline_at": "2026-12-01T12:00:00+08:00"
}
```

请替换成实际的未来时间。必须是带时区偏移、包含秒的时间字符串，最多九位小数秒；支持 `Z` 及 RFC 3339 的 `-23:59` 至 `+23:59` 偏移（分钟须为 00–59）。数字时间戳、不带时区、格式错误
或首次创建时已经到期会返回 `400 invalid_request`。省略或传 `null` 表示不设期限，旧数据
也保持无期限；查询响应在无期限时省略此字段。UTC 与其他偏移表示的同一时刻视为相同输入，
精度统一截断到微秒。当前 Run 与唯一 Task 共用同一个截止时间，查询两者均可看到该值。
年份 0001–9999 限制针对输入的本地日期；偏移换算后的 UTC 年份可以跨入 0000 或 10000。
数据库仍保存同一绝对时刻，这两种边界的查询和事件期限使用 `+23:59` / `-23:59` 表示，保持 RFC 3339 的四位年份。

排队、执行和等待重试都计入期限，重启不会重新计时。同键、同输入及同一规范化截止时间的
重放仍返回原 `run_id`，即使期限已经过去；同键修改期限返回 `409 idempotency_conflict`。
有剩余尝试但退避将跨过截止时间时，Task 进入不可领取的 RETRYING 等待状态，
`next_attempt_at` 等于截止时间，记录 `TaskRetrySkipped`，到期才标记超时。
未来审批等待也将继续计时；审批本身尚未实现。

到期后，任务不能再领取、进入执行、续租、重试或提交结果，心跳也不能把租约延长到期限之后。
独立扫描器将未完成 Run / Task 一起改为 `TIMEOUT`，清除租约和输出，并原子记录
`TaskTimedOut → RunTimedOut`；重复扫描不重复记录。扫描可能因轮询或行锁稍晚更新可见状态，
但执行入口始终按数据库当前时间拒绝过期操作。扫描不占用执行线程池，worker 关闭时仍可运行。

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `DEADLINE_WATCHDOG_ENABLED` | `true` | 是否运行超时扫描；关闭后入口仍拒绝过期操作，但无人扫描时可见状态可能停留在活动状态 |
| `DEADLINE_WATCHDOG_POLL_DELAY_MS` | `1000` | 超时写入扫描与本机执行停止检查的间隔，毫秒；对应 `nexus.timeout.poll-delay-ms`，必须为正 |
| `DEADLINE_WATCHDOG_BATCH_SIZE` | `50` | 每轮超时写入上限及每批本机执行停止查询上限；对应 `nexus.timeout.batch-size`，必须为正 |

容器环境变量需实际传给 control-plane；宿主机设置不会自动注入已运行容器。
超时与取消、成功、失败按锁定后的有效检查决定：截止前已作出的有效终态决定可以提交，
之后超时不覆盖它；截止后仍未结束的工作必须超时。取消若发现到期，会先保存 TIMEOUT，
再返回 `409 cancellation_conflict`；已有 TIMEOUT 同样返回 409。已取消 Run 的重复取消仍是 200。

超时事务提交成功后，系统向本进程同一 Task 的当前及所有仍在运行的旧 attempt 发送停止信号、线程中断并清理心跳。
因此，租约过期并被新 attempt 接管后，旧执行器也会收到超时通知；迟到的旧 attempt 通知不会影响更新的 attempt 或其他 Task。
每个实例还会分批检查本机仍在执行的 Task 是否已在数据库中提交为 TIMEOUT，所以其他实例处理超时后，执行实例也会补发停止通知。
此检查不依赖心跳注册，即使本机关闭超时写入扫描仍会运行；不会因尚未提交或已回滚的超时而中断执行，数据库暂不可用时保留注册、下轮再查。
`TaskExecutor` 可在步骤之间检查信号；响应中断的执行器退出后释放执行名额。不响应中断的
执行器可能继续占用线程，其迟到结果仍被拒绝。不能保证立即停止已发出的外部请求或撤销
已发生的副作用。具体事务判定点、中断和重试规则见 [ADR 0007](docs/adr/0007-execution-deadlines.md)。

B08 使用 V5 升级，V1–V4 不变，历史事件不回填。B08 当时的 HTTP Run / timeline / cancel 成功响应使用
`Nexus-API-Contract: 0.3`，冻结的 0.2 定义保存在 `packages/contracts/openapi-v0.2.yaml`；
新超时和跳过重试事件使用 `schema_version: "0.3"`，取消事件仍为 0.2，既有事件仍无版本。
B09 的当前版本与兼容要求见下节。

## 暂停 / 恢复（B09）

```bash
curl -i -X POST http://localhost:8080/v1/runs/<run_id>/pause
curl -sS http://localhost:8080/v1/runs/<run_id>
curl -i -X POST http://localhost:8080/v1/runs/<run_id>/resume \
  -H 'Pause-Checkpoint: <tasks[0].checkpoint.checkpoint_id>'
```

请替换 Run ID 和查询返回的 checkpoint_id。两个接口都不要求请求体或 Idempotency-Key，
请求体忽略。排队、已领取、执行中、等待重试都能暂停；成功返回 200，Run / Task 都为
PAUSED，租约清空。查询 Task 的 checkpoint 可看到暂停周期、暂停前状态、已耗 attempt、
状态版本、原重试时间和暂停时间。输入沿用原 Task，未完成输出不保存，成功 Task 不重跑。
同一 Task 只保留最新一次 checkpoint，历史暂停周期及决定保存在不可变事件中。

恢复后只调度尚未完成的 Task：原排队任务重新排队，原 RETRYING 保留绝对 next_attempt_at，
到点才可领取；原 LEASED / EXECUTING 从整个 Task 开头重做，下一次领取消耗新 attempt。
暂停 / 恢复本身不增加 attempt，也不退还已耗次数。被中断 attempt 已耗尽 max_attempts 时，
恢复返回 200 和 FAILED，不发起额外执行；checkpoint 的 disposition 为 ATTEMPTS_EXHAUSTED。
其他正常恢复标记 RESUMED 和 consumed_at。没有步骤级恢复或外部操作撤销能力。

已经 PAUSED 时重复暂停、活动状态下重复恢复，返回 200 且不改变版本或追加事件。
不存在返回 404；已成功 / 失败 / 取消 / 超时返回 409 pause_conflict。可选的 Pause-Checkpoint
只允许恢复匹配的最新暂停周期，不匹配返回 409；它也适用于该周期已恢复但仍活动时的重试。
不带此头的恢复总是针对当前周期，因此上一轮迟到请求可能恢复新一轮暂停；需要跨周期重试
隔离的客户端应保存并发送该编号。暂停请求在中间发生过恢复后再到达，视为新的暂停请求。
该编号用于匹配进度，不是身份凭证。fake executor 通常很快完成，暂停已成功 Run 得到 409
属于正常情况，可使用关闭 worker 的环境先创建并暂停排队任务。

暂停不延长 deadline_at，暂停期间可以取消或到期为 TIMEOUT。暂停 / 恢复首次发现到期时，
先提交超时状态和事件再返回 409；恢复不能复活已终止的工作。状态、checkpoint、租约、
版本及 TaskPaused → RunPaused / TaskResumed → RunResumed 在同一短事务内保存，失败全部
回滚；次数耗尽恢复直接记录 TaskFailed → RunFailed，不记录虚假的恢复事件。

暂停提交后按旧 attempt / version 发送协作式停止，清理旧心跳；各实例还会按
DEADLINE_WATCHDOG_POLL_DELAY_MS / BATCH_SIZE 核对本机正在执行 Task 的持久化暂停记录，
即使另一实例已快速暂停再恢复、心跳已移除或本机关闭超时写入扫描，也能补发旧执行的停止。
这项读取不等待任务行锁，数据库读取失败保留注册下轮再查。旧通知不能误停新 attempt；
忽略中断的执行器会继续占用名额直到退出，迟到结果被拒绝，不能撤销已发生的外部副作用。
重启后仍为 PAUSED，不会自动执行；完整规则见 [ADR 0008](docs/adr/0008-pause-and-resume.md)。

V6 增加 checkpoint 与 PAUSED 数据约束，并扩展暂停任务的超时扫描索引，V1–V5 不改动。
当前 Run / timeline 查询及 cancel / pause / resume 成功响应带 Nexus-API-Contract: 0.4，
冻结的 0.3 定义为 packages/contracts/openapi-v0.3.yaml。暂停 / 恢复、次数耗尽恢复和从
PAUSED 取消 / 超时的事件使用 0.4；其他来源继续原来的事件版本，历史事件保持不变。
部署前客户端须支持 PAUSED 和混合 0.1 / 0.2 / 0.3 / 0.4 事件。

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

- 真实 HTTP 服务的七个接口、固定成功输出、完整有序时间线、请求校验和 400 / 404 / 409 错误码。
- 顺序和并发幂等创建；同键不同 input 或有效 Session 的竞争只保留一个 Run / Task 和一组创建事件。
- 多个独立 worker 通过独立事务竞争单个或多个 Task，每个 Task 只领取一次，完成后不再执行。
- 关闭并重建整个 Spring 应用上下文及连接池后，成功结果和事件保持不变，定时 worker 继续处理排队任务；
  `LEASED` / `EXECUTING` 的有效旧租约受到保护，过期后先回收并退避，再由新 owner 接管，拒绝旧 owner 的迟到报告。
- 数据库拒绝事件 UPDATE / DELETE，原时间线的数量和内容不变。
- 退避到期前不可领取、重试成功、耗尽失败、租约过期、重复与迟到报告、多个 watchdog 竞争、
  成功与失败竞争、事务回滚、执行池拒绝后的派发退避和成功任务不再执行。
- 四个阶段取消、并发重复取消、终态冲突、真实版本化事件及合约反例；取消与领取、成功、失败、
  重试、watchdog 按两种先后顺序使用独立事务竞争；首个事务持锁时观察第二个事务确已等待或
  通过 SKIP LOCKED 跳过，再放行提交。数据库触发器注入第一 / 第二条取消事件写入失败，
  验证状态、租约和事件一起回滚；已回滚的取消不算竞争获胜。
- 真实独立心跳调度支持超过初始租约的执行；停止心跳后接管、阻塞执行中取消、续租资格与版本
  校验、续租与完成 / 失败 / 取消 / watchdog 的竞争、单个任务持锁时其他任务持续续租、
  行锁释放后过期任务不能补续、数据库故障回滚、
  注册替换与清理，以及非法租约 / 心跳配置启动失败。

B08 另覆盖四个阶段超时、独立扫描、退避跨截止时间、数据库入口拒绝过期操作、续租上限、
超时与终态竞争、事件写入故障回滚、V4 → V5 升级和旧事件保留、期限输入 / 幂等重放、
完整应用重启、执行池满时排队超时，以及响应 / 忽略中断的受控执行器。审查回归还覆盖租约接管后同时中断新旧 attempt、释放两个执行名额，以及完整 RFC 3339 偏移范围的 HTTP 规范化与幂等重放。
另用两个独立应用上下文共享测试数据库，验证其他实例提交超时后的本机停止、移除心跳后仍能停止、回滚不误中断及执行名额复用。
HTTP 偏移正例按数据库当前时间生成未来期限；年份边界另验证 UTC 跨界、持久化和查询表示。

B09 另覆盖四个阶段暂停 / 恢复、重复和并发请求、暂停周期匹配、原退避和尝试上限、暂停中
取消 / 超时、checkpoint 和两条事件故障回滚、与各状态写入者两种顺序的竞争及锁等待跨期限。
使用真实应用重启、定时 worker、可控阻塞执行器及两个共享独立 PostgreSQL 的应用实例，
验证快速恢复后旧通知不误停新 attempt、无心跳时停止、回滚不误中断、不响应中断的迟到结果，
以及 V5 → V6 升级、数据库约束和 0.4 合约正反例。

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
所有 24 个现有测试类必须有报告并实际执行，任何报告包含失败、错误、跳过、零项测试、
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
、[ADR 0005](docs/adr/0005-cancellation-foundation.md)
、[ADR 0006](docs/adr/0006-lease-heartbeats.md)
、[ADR 0007](docs/adr/0007-execution-deadlines.md)
和 [ADR 0008](docs/adr/0008-pause-and-resume.md)。
