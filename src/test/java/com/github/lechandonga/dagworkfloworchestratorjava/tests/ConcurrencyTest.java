package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowRun;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.BusinessLogging;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import com.github.lechandonga.dagworkfloworchestratorjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.lechandonga.dagworkfloworchestratorjava.tests.support.TestAwait.awaitFinished;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：并发竞争 —— 幂等提交、同任务不并发执行、取消竞争、告警去重。
 */
@ExtendWith(BusinessLogging.class)
@DisplayName("并发竞争")
class ConcurrencyTest {

    private final WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator());

    @Test
    @DisplayName("同一 submissionId 并发提交只创建一个运行，且不重置状态")
    void concurrentDuplicateSubmission() throws Exception {
        TaskDef task = WorkflowFixtures.task("ok", TaskBehavior.success(300));
        Set<String> runIds = ConcurrentHashMap.newKeySet();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    WorkflowRun run = engine.submit(new WorkflowDef("idem-1", List.of(task)));
                    runIds.add(run.getRunId());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(1, runIds.size(), "相同提交标识必须映射到同一个运行");
        assertEquals(1, engine.allRuns().size());

        WorkflowRun run = engine.allRuns().get(0);
        awaitFinished(run);
        assertEquals(RunStatus.SUCCEEDED, run.getStatus());

        // 终态后再次提交同一标识：返回同一运行，状态不被重置。
        WorkflowRun again = engine.submit(new WorkflowDef("idem-1", List.of(task)));
        assertSame(run, again);
        assertEquals(RunStatus.SUCCEEDED, again.getStatus());
        assertEquals(1, again.getTasks().get("ok").getAttempts().size(),
                "重复提交不得重新执行或重置已有运行");
    }

    @Test
    @DisplayName("同一任务在同一次运行里绝不可能被并发重复执行")
    void sameTaskNeverRunsConcurrently() {
        TaskDef a = WorkflowFixtures.task("a", List.of(),
                TaskBehavior.fail(80, "boom"), 2, 10L, null, null);
        WorkflowRun run = engine.submit(new WorkflowDef("conc-1", List.of(a)));
        awaitFinished(run);

        // 每次尝试都记录 startedAt/finishedAt；同一任务的尝试区间不得重叠。
        var attempts = run.getTasks().get("a").getAttempts();
        assertEquals(3, attempts.size());
        for (int i = 1; i < attempts.size(); i++) {
            assertTrue(attempts.get(i).startedAt() >= attempts.get(i - 1).finishedAt(),
                    "同一任务的尝试区间发生重叠，存在并发重复执行");
        }
        long startedEvents = run.snapshotEvents().stream()
                .filter(e -> ("TASK_STARTED".equals(e.type()) || "TASK_RETRY_STARTED".equals(e.type()))
                        && "a".equals(e.taskId()))
                .count();
        assertEquals(3, startedEvents, "每次尝试只启动一次");
        assertEquals(TaskState.FAILED, run.getTasks().get("a").getState());
    }

    @Test
    @DisplayName("取消与尝试完成同时竞争时终态仍然合法（取消或失败，不回退）")
    void cancelRacingWithAttemptCompletion() throws Exception {
        for (int i = 0; i < 20; i++) {
            TaskDef t = WorkflowFixtures.task("t" + i, List.of(),
                    TaskBehavior.fail(5, "boom"), 3, 5L, null, null);
            WorkflowRun run = engine.submit(new WorkflowDef("race-" + i, List.of(t)));
            engine.cancel(run.getRunId());
            awaitFinished(run);
            RunStatus status = run.getStatus();
            assertTrue(status == RunStatus.CANCELLED || status == RunStatus.FAILED,
                    "竞争结果必须是合法终态, 实际: " + status);
            TaskState state = run.getTasks().get("t" + i).getState();
            assertTrue(state == TaskState.CANCELLED || state == TaskState.FAILED);
        }
    }

    @Test
    @DisplayName("超过 alertAfterMs 只产生一次 TASK_ALERT，运行结束后不补告警")
    void alertFiresExactlyOnce() {
        TaskDef slow = WorkflowFixtures.task("slow", List.of(),
                TaskBehavior.fail(250, "boom"), 1, 30L, null, 50L);
        WorkflowRun run = engine.submit(new WorkflowDef("alert-1", List.of(slow)));
        awaitFinished(run);

        long alerts = run.snapshotEvents().stream()
                .filter(e -> "TASK_ALERT".equals(e.type())).count();
        assertEquals(1, alerts, "同一任务不得重复告警");

        // 运行已结束，再等待一段时间，确认不会补发告警。
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(1, run.snapshotEvents().stream()
                .filter(e -> "TASK_ALERT".equals(e.type())).count());
    }
}
