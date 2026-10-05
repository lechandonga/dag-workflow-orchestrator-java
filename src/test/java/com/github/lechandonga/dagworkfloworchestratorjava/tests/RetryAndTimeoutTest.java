package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.RunViews;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowRun;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskView;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.BusinessLogging;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import com.github.lechandonga.dagworkfloworchestratorjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static com.github.lechandonga.dagworkfloworchestratorjava.tests.support.TestAwait.awaitFinished;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：失败重试、退避、超时、后继跳过，以及无关分支不被等待中的重试卡住。
 */
@ExtendWith(BusinessLogging.class)
@DisplayName("失败重试与超时")
class RetryAndTimeoutTest {

    private final WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator());

    private TaskView task(WorkflowRun run, String id) {
        return RunViews.snapshot(run).tasks().stream()
                .filter(t -> t.id().equals(id)).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("失败任务按 retries 重试，保留每次尝试，最终 FAILED")
    void retriesExhausted() {
        TaskDef flaky = WorkflowFixtures.task("flaky", List.of(),
                TaskBehavior.fail(0, "boom"), 2, 20L, null, null);
        WorkflowRun run = engine.submit(new WorkflowDef("retry-1", List.of(flaky)));

        awaitFinished(run);

        assertEquals(RunStatus.FAILED, run.getStatus());
        TaskView view = task(run, "flaky");
        assertEquals(TaskState.FAILED, view.state());
        assertEquals(3, view.attempts().size(), "首次尝试 + 2 次重试都应保留");
        assertEquals(3, view.currentAttempt());
        assertTrue(view.attempts().stream().allMatch(a -> a.outcome().name().equals("FAILURE")));
    }

    @Test
    @DisplayName("超过 timeoutMs 的尝试记为超时失败并触发重试")
    void timeoutCountsAsFailure() {
        TaskDef slow = WorkflowFixtures.task("slow", List.of(),
                TaskBehavior.success(300), 1, 10L, 80L, null);
        WorkflowRun run = engine.submit(new WorkflowDef("retry-2", List.of(slow)));

        awaitFinished(run);

        TaskView view = task(run, "slow");
        assertEquals(TaskState.FAILED, view.state());
        assertEquals(2, view.attempts().size());
        assertTrue(view.attempts().get(0).timedOut(), "第一次尝试应当超时");
        assertEquals("attempt timed out after 80ms", view.attempts().get(0).error());
    }

    @Test
    @DisplayName("上游失败后后继任务标记为 SKIPPED，而不是执行或挂起")
    void successorsAreSkipped() {
        TaskDef bad = WorkflowFixtures.task("bad", List.of(),
                TaskBehavior.fail(0, "boom"), 0, 0L, null, null);
        TaskDef child = WorkflowFixtures.task("child", List.of("bad"),
                TaskBehavior.success(0), 0, 0L, null, null);
        TaskDef grand = WorkflowFixtures.task("grand", List.of("child"),
                TaskBehavior.success(0), 0, 0L, null, null);
        WorkflowRun run = engine.submit(new WorkflowDef("retry-3", List.of(bad, child, grand)));

        awaitFinished(run);

        assertEquals(RunStatus.FAILED, run.getStatus());
        assertEquals(TaskState.FAILED, task(run, "bad").state());
        assertEquals(TaskState.SKIPPED, task(run, "child").state());
        assertEquals(TaskState.SKIPPED, task(run, "grand").state());
        assertEquals(0, task(run, "child").attempts().size(), "被跳过的任务不得执行");
    }

    @Test
    @DisplayName("等待重试退避的任务不会卡住无关分支的并行执行")
    void unrelatedBranchProceedsDuringBackoff() {
        TaskDef retrying = WorkflowFixtures.task("retrying", List.of(),
                TaskBehavior.fail(0, "boom"), 1, 500L, null, null);
        TaskDef independent = WorkflowFixtures.task("independent", List.of(),
                TaskBehavior.success(0), 0, 0L, null, null);
        WorkflowRun run = engine.submit(
                new WorkflowDef("retry-4", List.of(retrying, independent)));

        com.github.lechandonga.dagworkfloworchestratorjava.tests.support.TestAwait
                .awaitState(run, "independent", TaskState.SUCCEEDED);

        assertEquals(TaskState.SUCCEEDED, task(run, "independent").state());
        assertFalse(task(run, "retrying").state().isTerminal(),
                "退避期间 retrying 仍未结束，但无关分支已经完成");
        awaitFinished(run);
    }

    @Test
    @DisplayName("无依赖的两个任务并行执行（完成耗时接近单任务耗时而非之和）")
    void parallelBranchesRunConcurrently() {
        TaskDef a = WorkflowFixtures.task("pa", List.of(),
                TaskBehavior.success(200), 0, 0L, null, null);
        TaskDef b = WorkflowFixtures.task("pb", List.of(),
                TaskBehavior.success(200), 0, 0L, null, null);
        long start = System.currentTimeMillis();
        WorkflowRun run = engine.submit(new WorkflowDef("retry-5", List.of(a, b)));
        awaitFinished(run);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertTrue(elapsed < 350,
                "串行需要 ~400ms，并行应明显更短，实际 " + elapsed + "ms");
    }
}
