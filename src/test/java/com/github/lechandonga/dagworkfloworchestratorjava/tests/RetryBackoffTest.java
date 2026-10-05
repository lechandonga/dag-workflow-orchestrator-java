package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import java.util.List;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowValidator;
import com.github.lechandonga.dagworkfloworchestratorjava.model.EventType;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.Await;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：失败重试与退避。
 * 覆盖用例：失败后重试成功、达到重试上限后任务失败且后继 BLOCKED、隔级后继 SKIPPED、
 * TIMEOUT outcome 也走重试、每次尝试结果均被保留。
 */
@DisplayName("业务-失败重试与退避")
class RetryBackoffTest {

    private WorkflowEngine newEngine() {
        return new WorkflowEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("用例: 失败两次后第三次成功 -> SUCCEEDED, attempts=3, 保留全部尝试结果")
    void retriesThenSucceeds() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("retry-ok",
                WorkflowFixtures.task("a", WorkflowFixtures.failsThenSucceeds(2, 0, 30)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        TaskState task = run.getTasks().get("a");
        assertEquals(TaskStatus.SUCCEEDED, task.getStatus());
        assertEquals(3, task.getAttempts().size());
        assertEquals(TaskBehavior.FAIL, task.getAttempts().get(0).outcome());
        assertEquals(TaskBehavior.FAIL, task.getAttempts().get(1).outcome());
        assertEquals(TaskBehavior.SUCCESS, task.getAttempts().get(2).outcome());

        long retryEvents = run.getEvents().stream()
                .filter(event -> event.type() == EventType.TASK_RETRY_SCHEDULED).count();
        assertEquals(2, retryEvents);
    }

    @Test
    @DisplayName("用例: 一直失败 -> 任务 FAILED, 直接后继 BLOCKED, 隔级后继 SKIPPED, 运行 FAILED")
    void retriesExhaustedBlocksSuccessors() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("retry-exhausted",
                WorkflowFixtures.task("a", WorkflowFixtures.alwaysFails(2, 10)),
                WorkflowFixtures.task("b", WorkflowFixtures.success(0), "a"),
                WorkflowFixtures.task("c", WorkflowFixtures.success(0), "b"));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        assertEquals(RunStatus.FAILED, run.getStatus());
        assertEquals(TaskStatus.FAILED, run.getTasks().get("a").getStatus());
        assertEquals(TaskStatus.BLOCKED, run.getTasks().get("b").getStatus());
        assertEquals(TaskStatus.SKIPPED, run.getTasks().get("c").getStatus());
        assertEquals(3, run.getTasks().get("a").getAttempts().size(),
                "1 initial attempt + 2 retries must all be recorded");
        assertNotNull(run.getFinishedAt());
    }

    @Test
    @DisplayName("用例: TIMEOUT 行为重试后成功 -> SUCCEEDED, 首次尝试 timedOut=true")
    void simulatedTimeoutRetried() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("timeout-retry",
                WorkflowFixtures.task("a",
                        WorkflowFixtures.timeoutThenSucceeds(0, 1000, 10)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        TaskState task = run.getTasks().get("a");
        assertEquals(TaskStatus.SUCCEEDED, task.getStatus());
        assertEquals(2, task.getAttempts().size());
        assertTrue(task.getAttempts().get(0).timedOut());
        assertTrue(task.isAlertRaised(), "timeout must raise exactly one alert");
        long alerts = run.getEvents().stream()
                .filter(event -> event.type() == EventType.TASK_ALERT_RAISED).count();
        assertEquals(1, alerts);
    }

    @Test
    @DisplayName("用例: 退避期间任务保持 RETRYING, 不会立刻发起第二次尝试")
    void backoffDelaysNextAttempt() throws InterruptedException {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("backoff",
                WorkflowFixtures.task("a",
                        WorkflowFixtures.custom(List.of(TaskBehavior.FAIL, TaskBehavior.SUCCESS),
                                0L, null, 1, 400L)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getTasks().get("a").getStatus() == TaskStatus.RETRYING,
                "task enters RETRYING");
        Thread.sleep(150);
        assertEquals(TaskStatus.RETRYING, run.getTasks().get("a").getStatus(),
                "task must still be waiting out its backoff");
        assertEquals(1, run.getTasks().get("a").getAttempts().size(),
                "no new attempt during backoff");

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
    }
}
