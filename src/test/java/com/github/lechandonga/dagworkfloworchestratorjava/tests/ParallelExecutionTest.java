package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowValidator;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：依赖推进与分支并行。
 * 覆盖用例：无依赖分支并行执行；某分支任务处于 RETRYING 时不阻塞无关分支；
 * 同一任务在一次运行中不会被并发重复执行。
 */
@DisplayName("业务-依赖推进与分支并行")
class ParallelExecutionTest {

    private WorkflowEngine newEngine() {
        return new WorkflowEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("用例: 两个根任务并行, 总耗时接近最长任务而非两者之和")
    void independentBranchesRunInParallel() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("parallel",
                WorkflowFixtures.task("a", WorkflowFixtures.success(300)),
                WorkflowFixtures.task("b", WorkflowFixtures.success(300)));
        Instant start = Instant.now();
        RunState run = engine.submit(null, def);
        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        long elapsed = java.time.Duration.between(start, Instant.now()).toMillis();

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertTrue(elapsed < 500,
                "parallel branches should finish in ~300ms, took " + elapsed + "ms");
    }

    @Test
    @DisplayName("用例: 分支A在退避重试, 无关分支B照常完成")
    void retryingBranchDoesNotBlockOthers() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("independent-retry",
                WorkflowFixtures.task("a1", WorkflowFixtures.failsThenSucceeds(1, 0, 800)),
                WorkflowFixtures.task("a2", WorkflowFixtures.success(0), "a1"),
                WorkflowFixtures.task("b1", WorkflowFixtures.success(50)),
                WorkflowFixtures.task("b2", WorkflowFixtures.success(50), "b1"));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(
                () -> run.getTasks().get("b2").getStatus() == TaskStatus.SUCCEEDED,
                "branch B fully succeeds while A retries");
        assertEquals(TaskStatus.RETRYING, run.getTasks().get("a1").getStatus(),
                "branch A is still waiting for its retry");
        assertEquals(TaskStatus.PENDING, run.getTasks().get("a2").getStatus());

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
    }

    @Test
    @DisplayName("用例: 同一任务任意时刻只有一个执行线程(无并发重复执行)")
    void sameTaskNeverRunsConcurrently() throws Exception {
        WorkflowEngine engine = newEngine();
        List<String> violations = new CopyOnWriteArrayList<>();

        Thread submitter = new Thread(() -> {
            WorkflowDef def = WorkflowFixtures.workflow("single-task",
                    WorkflowFixtures.task("a", WorkflowFixtures.success(200)));
            RunState run = engine.submit("client-single", def);

            Thread[] pollers = new Thread[8];
            for (int i = 0; i < pollers.length; i++) {
                pollers[i] = new Thread(() -> {
                    for (int j = 0; j < 50; j++) {
                        engine.getRun(run.getRunId());
                    }
                });
            }
            for (Thread poller : pollers) {
                poller.start();
            }
            for (Thread poller : pollers) {
                try {
                    poller.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
            TaskState task = run.getTasks().get("a");
            if (task.getAttempts().size() != 1) {
                violations.add("expected exactly 1 attempt, got " + task.getAttempts().size());
            }
            List<Long> threads = run.getEvents().stream()
                    .filter(event -> event.type() == com.github.lechandonga.dagworkfloworchestratorjava.model.EventType.TASK_STARTED)
                    .map(event -> event.seq())
                    .toList();
            if (threads.size() != 1) {
                violations.add("expected exactly 1 TASK_STARTED event, got " + threads.size());
            }
        });
        submitter.start();
        submitter.join(10_000);
        assertTrue(violations.isEmpty(), "concurrency violations: " + violations);
    }
}
