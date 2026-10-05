# DAG Workflow Orchestrator (本地进程内版本)

一个纯进程内的工作流编排引擎：按 DAG 依赖推进任务、并行执行无依赖分支、
模拟成功/失败/耗时、失败重试（退避 + 单次超时）、运行取消、超时阈值告警，
以及可回放的有序事件流。**不依赖任何外部服务或数据库**（无 MQ、无 DB、无 Redis），
所有运行状态保存在 JVM 内存中。

## 技术栈

- Java 21、Spring Boot 3.4（仅 `spring-boot-starter-web`）
- JUnit 5 + Awaitility（随 `spring-boot-starter-test` 提供）

## 1. 工作流定义格式

`POST /api/workflows/runs`

```json
{
  "submissionId": "wf-demo-001",
  "tasks": [
    {
      "id": "fetch",
      "dependsOn": [],
      "behavior": { "outcome": "SUCCESS", "delayMs": 50, "message": null },
      "retries": 2,
      "backoffMs": 100,
      "timeoutMs": null,
      "alertAfterMs": null
    },
    {
      "id": "transform",
      "dependsOn": ["fetch"],
      "behavior": { "outcome": "FAILURE", "delayMs": 30, "message": "simulated boom" },
      "retries": 1,
      "backoffMs": 200,
      "timeoutMs": 500,
      "alertAfterMs": 100
    }
  ]
}
```

字段说明：

| 字段 | 含义 |
| --- | --- |
| `submissionId` | 提交标识（幂等键）。同一标识重复提交返回同一个运行，不重建、不重置状态。 |
| `tasks[].id` | 工作流内唯一的任务标识。 |
| `tasks[].dependsOn` | 依赖的任务 id 列表；所有依赖 **SUCCEEDED** 后任务才可执行。 |
| `tasks[].behavior.outcome` | `SUCCESS` 或 `FAILURE`，每次尝试行为一致，便于稳定复现。 |
| `tasks[].behavior.delayMs` | 模拟执行耗时；想复现超时就让它大于 `timeoutMs`。 |
| `tasks[].behavior.message` | 失败时记录的错误信息。 |
| `retries` | 首次尝试失败后的最大重试次数（`0` = 不重试，共执行 `1+retries` 次）。 |
| `backoffMs` | 两次尝试之间的固定退避时间。退避中的任务不会阻塞无关分支。 |
| `timeoutMs` | 单次尝试超时阈值；超时按失败处理并参与重试。空/0 表示不超时。 |
| `alertAfterMs` | 单次尝试运行超过该阈值未完成则产生一次 `TASK_ALERT`；每个任务最多一次。 |

提交前的图校验（返回 HTTP 400，`reason` 可区分）：

| reason | 触发条件 |
| --- | --- |
| `MISSING_SUBMISSION_ID` | `submissionId` 为空 |
| `EMPTY_GRAPH` | 没有任何任务 |
| `MISSING_TASK_ID` | 任务缺少 id |
| `DUPLICATE_TASK_ID` | 任务 id 重复 |
| `UNKNOWN_DEPENDENCY` | 依赖指向不存在的任务 |
| `SELF_DEPENDENCY` | 任务依赖自身 |
| `CYCLIC_DEPENDENCY` | 依赖图存在环（DFS 染色检测） |

## 2. 状态机

运行（Run）：

```
PENDING -> RUNNING -> SUCCEEDED            （全部任务终态且无 FAILED）
                   \-> FAILED               （存在任务 FAILED）
RUNNING/PENDING -> CANCEL_REQUESTED -> CANCELLED
```

任务（Task）：

```
PENDING -> RUNNING -> SUCCEEDED
                   \-> FAILED                    （重试耗尽/超时耗尽）
PENDING  -> SKIPPED                             （上游终态未成功）
PENDING/RUNNING -> CANCELLED                    （运行被取消）
```

规则：

- 所有终态（`SUCCEEDED/FAILED/SKIPPED/CANCELLED` 与运行的三个终态）**不可回退**。
- 上游任务 FAILED/CANCELLED/SKIPPED 后，其后继标记为 **SKIPPED**，绝不执行、也不挂起。
- 同一任务同一次运行内不会被并发重复执行：所有状态迁移在该运行的监视器内完成，
  每次尝试的执行区间互不重叠。
- 无依赖关系的分支并行执行（缓存线程池），某任务在退避等待不影响其它分支。

## 3. 重试与取消语义

重试：

- 失败（含超时）后若 `attempt <= retries`，记录 `TASK_RETRY_SCHEDULED`，
  任务短暂回到 `PENDING` 等待退避，之后产生 `TASK_RETRY_STARTED` 发起下一次尝试。
- 每次尝试都保留一条 `AttemptResult`（尝试序号、结果、是否超时、起止时间、错误信息）。
- 重试耗尽：任务 `FAILED`，后继 `SKIPPED`；运行最终 `FAILED`。

取消：

- `POST /api/workflows/runs/{runId}/cancel`。
- 取消后运行进入 `CANCEL_REQUESTED`：尚未开始（含退避等待中）的任务立即 `CANCELLED`；
  **不再调度任何重试**；后继任务不再执行。
- 取消时已在运行的尝试允许跑完，但结果被丢弃（`TASK_ATTEMPT_DISCARDED`），
  任务最终 `CANCELLED`。
- 对已终态的运行执行取消返回 **409 `ILLEGAL_RUN_STATE`**，信息中包含当前终态；
  查询不存在的运行返回 **404 `RUN_NOT_FOUND`**。

告警：

- 某次尝试运行超过 `alertAfterMs` 仍未完成时发出一次 `TASK_ALERT`。
- 每个任务最多告警一次；任务结束或运行取消后挂起的告警定时器被取消，不再补发。

## 4. 事件流

`GET /api/workflows/runs/{runId}/events` 返回按 `seq` 严格递增的追加式事件流，
事件类型包括：`RUN_CREATED`、`RUN_STATUS_CHANGED`、`TASK_STARTED`、
`TASK_RETRY_STARTED`、`TASK_ATTEMPT_SUCCEEDED/FAILED/DISCARDED`、
`TASK_RETRY_SCHEDULED`、`TASK_STATE_CHANGED`、`TASK_ALERT`。

`GET /api/workflows/runs/{runId}/replay` 演示仅凭事件流即可还原运行状态、
每个任务的最终状态、尝试次数与告警事实（终态在回放时同样不会被覆盖）。

## 5. HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/workflows/runs` | 提交（校验 + 幂等创建） |
| GET  | `/api/workflows/runs` | 列出全部运行快照 |
| GET  | `/api/workflows/runs/{runId}` | 查询运行与任务状态 |
| POST | `/api/workflows/runs/{runId}/cancel` | 取消运行 |
| GET  | `/api/workflows/runs/{runId}/events` | 有序事件流 |
| GET  | `/api/workflows/runs/{runId}/replay` | 由事件流回放最终状态 |

## 6. 本地验证

运行全部单测/集成测试（日志中 `BUSINESS-COVERAGE` 行会打印业务分组、
每个用例名称及通过/失败结果）：

```bash
mvn test
```

测试覆盖的业务分组：

- **任务图校验** `GraphValidationTest`：空图、重复 id、未知/自依赖、成环、合法 DAG。
- **失败重试与超时** `RetryAndTimeoutTest`：重试保留每次尝试、超时即失败、
  后继 SKIPPED、退避不阻塞无关分支、并行分支耗时验证。
- **运行取消** `CancellationTest`：退避中取消、运行中取消丢弃结果、
  对终态运行取消返回 409。
- **并发竞争** `ConcurrencyTest`：并发幂等提交、同任务尝试区间不重叠、
  取消与尝试完成竞争、告警仅一次且结束后不补发。
- **事件流与状态还原** `EventStreamReplayTest`：失败/跳过/取消场景回放一致。
- **HTTP 接口** `WorkflowApiTests`：全链路、可区分校验错误、409/404。

手动联表（启动后端口为 8080）：

```bash
mvn spring-boot:run

# 提交一个“先成功后失败（重试 1 次）”的工作流
curl -s -X POST localhost:8080/api/workflows/runs -H 'Content-Type: application/json' -d '{
  "submissionId": "demo-1",
  "tasks": [
    {"id":"a","dependsOn":[],"behavior":{"outcome":"SUCCESS","delayMs":50},"retries":0,"backoffMs":0},
    {"id":"b","dependsOn":["a"],"behavior":{"outcome":"FAILURE","delayMs":10,"message":"boom"},"retries":1,"backoffMs":100,"timeoutMs":null,"alertAfterMs":200}
  ]}'

# 用返回的 runId 查询状态 / 事件流 / 回放结果
curl -s localhost:8080/api/workflows/runs/<runId>
curl -s localhost:8080/api/workflows/runs/<runId>/events
curl -s localhost:8080/api/workflows/runs/<runId>/replay
```
