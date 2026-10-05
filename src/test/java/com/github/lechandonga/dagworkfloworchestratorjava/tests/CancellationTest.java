package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import java.util.List;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowValidator;
import com.github.lechandonga.dagworkfloworchestratorjava.error.ApiException;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：取消语义。
 * 覆盖用例：运行中取消会中断在执行任务、跳过未开始任务，取消后不产生新重试，
 * 对已结束运行取消返回 409/RUN_ALREADY_TERMINAL。
 */
@DisplayName("业务-取消语义")
class CancellationTest {

    private WorkflowEngine newEngine() {
        return new WorkflowEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("用例: 长任务运行中取消 -> CANCELLED, 后继 SKIPPED, 无新事件, 无新重试")
    void cancelMidRun() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("cancel-mid",
                WorkflowFixtures.task("slow",
                        WorkflowFixtures.custom(List.of(TaskBehavior.SUCCESS),
                                10_000L, null, 2, 10L)),
                WorkflowFixtures.task("down", WorkflowFixtures.success(0), "slow"));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(
                () -> run.getTasks().get("slow").getStatus() == TaskStatus.RUNNING,
                "slow task running");

        engine.cancel(run.getRunId());

        assertEquals(RunStatus.CANCELLED, run.getStatus());
        assertEquals(TaskStatus.SKIPPED, run.getTasks().get("slow").getStatus());
        assertEquals(TaskStatus.SKIPPED, run.getTasks().get("down").getStatus());

        int eventCountAfterCancel = run.getEvents().size();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(eventCountAfterCancel, run.getEvents().size(),
                "no new events (no retry, no late success) after cancellation");
        long retryEvents = run.getEvents().stream()
                .filter(event -> event.type() == EventType.TASK_RETRY_SCHEDULED).count();
        assertEquals(0, retryEvents);
    }

    @Test
    @DisplayName("用例: 等待退避重试时取消 -> 不再触发后续尝试")
    void cancelDuringBackoff() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("cancel-backoff",
                WorkflowFixtures.task("a",
                        WorkflowFixtures.custom(List.of(TaskBehavior.FAIL, TaskBehavior.SUCCESS),
                                0L, null, 1, 5_000L)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(
                () -> run.getTasks().get("a").getStatus() == TaskStatus.RETRYING,
                "task waiting in backoff");
        engine.cancel(run.getRunId());
        assertEquals(RunStatus.CANCELLED, run.getStatus());

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TaskState task = run.getTasks().get("a");
        assertEquals(TaskStatus.SKIPPED, task.getStatus());
        assertEquals(1, task.getAttempts().size(), "the scheduled retry must never execute");
    }

    @Test
    @DisplayName("用例: 对已成功的运行再次取消 -> 409 RUN_ALREADY_TERMINAL, 状态不回退")
    void cancelFinishedRunRejected() {
        WorkflowEngine engine = newEngine();
        RunState run = engine.submit(null, WorkflowFixtures.workflow("done",
                WorkflowFixtures.task("a", WorkflowFixtures.success(0))));
        Await.awaitUntil(() -> run.getStatus() == RunStatus.SUCCEEDED, "run succeeds");

        ApiException error = assertThrows(ApiException.class,
                () -> engine.cancel(run.getRunId()));
        assertEquals(409, error.getStatus().value());
        assertEquals("RUN_ALREADY_TERMINAL", error.getErrorCode());
        assertEquals(RunStatus.SUCCEEDED, run.getStatus(), "terminal state must not regress");
    }

    @Test
    @DisplayName("用例: 对已取消的运行再次取消 -> 409 RUN_ALREADY_TERMINAL")
    void doubleCancelRejected() {
        WorkflowEngine engine = newEngine();
        RunState run = engine.submit(null, WorkflowFixtures.workflow("dc",
                WorkflowFixtures.task("a", WorkflowFixtures.success(5_000L))));
        Await.awaitUntil(
                () -> run.getTasks().get("a").getStatus() == TaskStatus.RUNNING,
                "task running");
        engine.cancel(run.getRunId());
        ApiException error = assertThrows(ApiException.class,
                () -> engine.cancel(run.getRunId()));
        assertEquals("RUN_ALREADY_TERMINAL", error.getErrorCode());
        assertFalse(run.getStatus() == RunStatus.RUNNING);
        assertTrue(run.getStatus().isTerminal());
    }

    @Test
    @DisplayName("用例: 取消未知运行 -> RUN_NOT_FOUND")
    void cancelUnknownRun() {
        WorkflowEngine engine = newEngine();
        ApiException error = assertThrows(ApiException.class, () -> engine.cancel("no-such-run"));
        assertEquals("RUN_NOT_FOUND", error.getErrorCode());
    }
}
