# DAG Workflow Orchestrator (Java, in-process)

本地运行的工作流编排引擎：全部运行状态保存在进程内存中，不依赖任何外部服务或数据库。
支持按 DAG 依赖推进任务、分支并行、失败重试与退避、超时判定与一次性告警、
运行取消、合法的生命周期状态机，以及一条可完整还原最终状态的有序事件流。

技术栈：Spring Boot 3.4 / Java 21 / JUnit 5。

## 1. 快速开始

```bash
# 启动服务（默认随机端口，也可在 src/main/resources/application.properties 固定 server.port）
mvn spring-boot:run

# 运行全部单测（控制台会打印“业务-用例-结果”覆盖报告）
mvn test
```

## 2. HTTP 接口

所有接口前缀：`/api/runs`

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/runs` | 校验并提交一个工作流运行；`clientId` 为幂等键 |
| `GET /api/runs` | 列出全部运行 |
| `GET /api/runs/{runId}` | 查询运行与每个任务的实时状态、尝试明细 |
| `POST /api/runs/{runId}/cancel` | 请求取消；对终态运行返回 `409 RUN_ALREADY_TERMINAL` |
| `GET /api/runs/{runId}/events` | 有序事件流 + 仅靠事件流还原出的最终状态 |

错误响应统一形如：

```json
{ "timestamp": "...", "errorCode": "CYCLIC_DEPENDENCY", "message": "Dependency cycle ..." }
```

### 提交示例

```bash
curl -s -X POST http://localhost:$PORT/api/runs -H 'Content-Type: application/json' -d '{
  "clientId": "demo-001",
  "workflow": {
    "name": "demo",
    "tasks": [
      { "id": "fetch",   "dependencies": [],
        "behavior": { "outcomes": ["SUCCESS"], "durationMillis": 100 } },
      { "id": "transform-a", "dependencies": ["fetch"],
        "behavior": { "outcomes": ["FAIL", "SUCCESS"], "maxRetries": 1, "backoffMillis": 200 } },
      { "id": "transform-b", "dependencies": ["fetch"],
        "behavior": { "outcomes": ["SUCCESS"], "durationMillis": 300,
                      "timeoutMillis": 1000 } },
      { "id": "report", "dependencies": ["transform-a", "transform-b"],
        "behavior": { "outcomes": ["SUCCESS"] } }
    ]
  }
}'
```

## 3. 工作流定义格式

```jsonc
{
  "clientId": "可选，幂等键；相同值重复提交不会创建第二个运行，也不会重置状态",
  "workflow": {
    "name": "工作流名称",
    "tasks": [
      {
        "id": "任务唯一标识",
        "dependencies": ["必须先成功的任务 id"],
        "behavior": {
          "outcomes": ["SUCCESS | FAIL | TIMEOUT，按尝试序号取值，越界复用最后一个"],
          "durationMillis": 0,          // 每次尝试的模拟耗时
          "durationsMillis": [300, 0],  // 可选：按尝试序号覆盖 durationMillis
          "timeoutMillis": 0,           // 0/null 表示不判定超时
          "maxRetries": 0,              // 首次尝试之后允许的重试次数
          "backoffMillis": 0            // 每次重试前的固定退避
        }
      }
    ]
  }
}
```

行为说明：
- `outcomes` 决定每次尝试的模拟结果。`FAIL`/`TIMEOUT` 消耗一次尝试并按重试策略处理；
  实际墙钟耗时超过 `timeoutMillis` 时，即使计划结果是 `SUCCESS` 也判定为超时。
- 该结构让成功、失败、重试、退避、超时全部可稳定复现，无需真实外部依赖。

### 提交前校验（可区分的错误码）

| 错误码 | 触发条件 |
| --- | --- |
| `EMPTY_GRAPH` | `tasks` 为空或缺失 |
| `INVALID_TASK` | 任务 id 为空 |
| `DUPLICATE_TASK_ID` | 同一工作流内任务 id 重复 |
| `INVALID_DEPENDENCY` | 依赖名为空 |
| `UNKNOWN_DEPENDENCY` | 依赖指向不存在的任务 |
| `CYCLIC_DEPENDENCY` | 依赖成环（含自依赖） |
| `INVALID_BEHAVIOR` | outcome 非法或重试/超时/退避参数为负 |

## 4. 状态机

运行状态（终态不可回退）：

```
RUNNING ──所有任务 SUCCEEDED──────────► SUCCEEDED
RUNNING ──存在 FAILED/BLOCKED/SKIPPED─► FAILED
RUNNING ──cancel──────────────────────► CANCELLED
```

任务状态：

```
PENDING ──依赖全部 SUCCEEDED──► SCHEDULED ──► RUNNING ──成功──► SUCCEEDED (终态)
   ▲                              │            │
   │                              │            ├──失败/超时且仍有重试额度──► RETRYING
   │                              │            │                                 │
   │                              │            │                            退避结束
   │                              │            │                                 ▼
   │                              │            └──重试耗尽──► FAILED (终态)  重新 SCHEDULED
   │                              │
   └──上游 FAILED（直接后继）──────► BLOCKED (终态)
   └──上游 BLOCKED/SKIPPED────────► SKIPPED (终态)
```

补充语义：
- 只有上游**全部成功**才会执行；一个任务在 `RETRYING` 等待退避时，
  与之无依赖关系的分支照常并行推进。
- 任务重试耗尽进入 `FAILED` 后，直接后继标记为 `BLOCKED`，
  再下一跳标记为 `SKIPPED`；后继既不会被执行，也不会无限挂起。
- 同一任务在同一次运行内任意时刻最多只有一个执行线程
  （`activeThreads` 守卫 + 尝试序号校验，过期的回调结果会被丢弃）。

## 5. 重试、超时与告警语义

- **重试**：失败或超时消耗一次尝试；只要已用尝试数 `<= maxRetries`，
  任务进入 `RETRYING`，等待 `backoffMillis` 后重新调度。每次尝试的
  outcome、起止时间、耗时、超时标记、错误信息都会保留在任务的 `attemptDetails` 中。
- **超时**：`timeoutMillis > 0` 时为每次尝试启动看门狗定时器。
  - 尝试实际耗时超过阈值，或计划 outcome 为 `TIMEOUT`，该尝试记为超时。
  - 到达阈值的那一刻产生一次 `TASK_ALERT_RAISED` 告警；
    **同一任务在一次运行内最多告警一次**（跨重试不重复），运行结束后也不会补告警。

## 6. 取消语义

- 仅 `RUNNING` 运行可取消；对 `SUCCEEDED`/`FAILED`/`CANCELLED` 运行取消，
  返回 `409 RUN_ALREADY_TERMINAL` 且状态不变。
- 取消会：中断所有执行中的任务线程、取消看门狗与待触发的重试定时器，
  并把所有非终态任务置为 `SKIPPED`，运行进入 `CANCELLED`。
- 取消后在途尝试的迟到结果被直接丢弃：不再产生重试、后继调度或补告警。

## 7. 事件流与状态还原

`GET /api/runs/{runId}/events` 返回按发生顺序排列的事件（`seq` 单调递增），
并附带 `replayedFromEvents`：仅重放事件即可得到运行终态、每个任务的终态、
最终尝试次数和是否告警，无需读取任何实时内存状态。

事件类型：`RUN_SUBMITTED`、`TASK_SCHEDULED`、`TASK_STARTED`、`ATTEMPT_FAILED`、
`TASK_RETRY_SCHEDULED`、`TASK_TIMED_OUT`、`TASK_ALERT_RAISED`、
`TASK_SUCCEEDED`、`TASK_FAILED`（重试耗尽）、`TASK_BLOCKED`、`TASK_SKIPPED`、
`RUN_CANCEL_REQUESTED`、`RUN_CANCELLED`、`RUN_SUCCEEDED`、`RUN_FAILED`。

## 8. 本地验证

```bash
mvn test
```

测试日志按业务模块组织（`BUSINESS_COVERAGE` logger），每个用例都会打印
`[业务用例开始] / [业务用例结果] -> PASS|FAIL`，并在结尾汇总：

| 测试类 | 覆盖业务 |
| --- | --- |
| `GraphValidationTest` | 空图、重复 id、未知依赖、成环（自依赖/2 节点/3 节点）、非法行为参数 |
| `RetryBackoffTest` | 失败后重试成功、重试耗尽后继 BLOCKED/SKIPPED、TIMEOUT 重试、退避期间不发起新尝试、尝试明细保留 |
| `TimeoutAlertTest` | 超阈值看门狗告警、每任务仅告警一次、快速完成不告警 |
| `ParallelExecutionTest` | 无依赖分支并行、重试等待不阻塞无关分支、同任务无并发重复执行 |
| `CancellationTest` | 运行中/退避中取消、取消后无新事件与重试、终态取消 409、未知运行 404 |
| `IdempotencyTest` | clientId 重复提交幂等、8 线程并发同 clientId 只建一个运行 |
| `EventReplayTest` | 成功链与失败传播链的事件流状态还原 |
| `HttpApiIntegrationTest` | 提交/查询/取消/事件流 HTTP 端到端、错误码、409、幂等 |
