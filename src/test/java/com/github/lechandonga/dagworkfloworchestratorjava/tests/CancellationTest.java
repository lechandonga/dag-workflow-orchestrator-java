package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.RunViews;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowRun;
import com.github.lechandonga.dagworkfloworchestratorjava.exception.IllegalRunStateException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowEvent;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.BusinessLogging;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import com.github.lechandonga.dagworkfloworchestratorjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static com.github.lechandonga.dagworkfloworchestratorjava.tests.support.TestAwait.awaitFinished;
import static com.github.lechandonga.dagworkfloworchestratorjava.tests.support.TestAwait.awaitStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：取消语义（取消后无新重试/后继执行、终态拒绝取消、终态不可回退）。
 */
@ExtendWith(BusinessLogging.class)
@DisplayName("运行取消")
class CancellationTest {

    private final WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator());

    @Test
    @DisplayName("退避期间取消：不再产生重试，待执行后继被取消")
    void cancelDuringBackoffStopsRetry() {
        TaskDef retrying = WorkflowFixtures.task("retrying", List.of(),
                TaskBehavior.fail(0, "boom"), 1, 2_000L, null, null);
        TaskDef child = WorkflowFixtures.task("child", List.of("retrying"),
                TaskBehavior.success(0), 0, 0L, null, null);
        WorkflowRun run = engine.submit(new WorkflowDef("cancel-1", List.of(retrying, child)));

        // 第一次尝试失败后进入 2s 退避，此时取消。
        awaitStatus(run, RunStatus.RUNNING);
        engine.cancel(run.getRunId());
        awaitFinished(run);

        assertEquals(RunStatus.CANCELLED, run.getStatus());
        assertEquals(1, RunViews.snapshot(run).tasks().get(0).attempts().size(),
                "取消后不得再产生重试尝试");
        assertEquals(TaskState.CANCELLED,
                RunViews.snapshot(run).tasks().get(1).state());

        List<WorkflowEvent> events = run.snapshotEvents();
        assertTrue(events.stream().noneMatch(e -> "TASK_RETRY_STARTED".equals(e.type())),
                "取消后不得出现重试开始事件");
        assertTrue(events.stream().noneMatch(e -> "TASK_STARTED".equals(e.type())
                && "child".equals(e.taskId())),
                "后继任务不得执行");
    }

    @Test
    @DisplayName("运行中任务遇取消：结果被丢弃，任务最终 CANCELLED，不触发重试")
    void cancelWhileRunningDiscardsResult() {
        TaskDef running = WorkflowFixtures.task("running", List.of(),
                TaskBehavior.success(300), 2, 10L, null, null);
        WorkflowRun run = engine.submit(new WorkflowDef("cancel-2", List.of(running)));

        engine.cancel(run.getRunId());
        awaitFinished(run);

        assertEquals(TaskState.CANCELLED,
                RunViews.snapshot(run).tasks().get(0).state());
        long successEvents = run.snapshotEvents().stream()
                .filter(e -> "TASK_ATTEMPT_SUCCEEDED".equals(e.type())).count();
        assertEquals(0, successEvents, "取消后完成的尝试结果必须丢弃");
    }

    @Test
    @DisplayName("对已结束运行执行取消被拒绝并说明原因，终态不回退")
    void cancelFinishedRunRejected() {
        TaskDef ok = WorkflowFixtures.task("ok", TaskBehavior.success(0));
        WorkflowRun run = engine.submit(new WorkflowDef("cancel-3", List.of(ok)));
        awaitFinished(run);
        assertEquals(RunStatus.SUCCEEDED, run.getStatus());

        IllegalRunStateException ex = assertThrows(IllegalRunStateException.class,
                () -> engine.cancel(run.getRunId()));
        assertTrue(ex.getMessage().contains("already ended with status SUCCEEDED"),
                "错误信息必须说明当前终态: " + ex.getMessage());
        assertEquals(RunStatus.SUCCEEDED, run.getStatus(), "终态不可回退");

        // 重复取消一个 CANCELLED 的运行同样被拒绝。
        TaskDef slow = WorkflowFixtures.task("slow", TaskBehavior.success(200));
        WorkflowRun run2 = engine.submit(new WorkflowDef("cancel-4", List.of(slow)));
        engine.cancel(run2.getRunId());
        awaitFinished(run2);
        assertThrows(IllegalRunStateException.class, () -> engine.cancel(run2.getRunId()));
        assertEquals(RunStatus.CANCELLED, run2.getStatus());
    }
}
