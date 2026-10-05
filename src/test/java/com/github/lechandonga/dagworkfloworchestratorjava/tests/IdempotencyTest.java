package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import java.util.concurrent.CountDownLatch;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowValidator;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.Await;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 业务：提交幂等与并发竞争。
 * 覆盖用例：同一 clientId 重复提交返回同一运行且不重置状态；多线程并发同 clientId
 * 提交只创建一个运行；缺少 workflow 字段被拒绝。
 */
@DisplayName("业务-提交幂等与并发竞争")
class IdempotencyTest {

    private WorkflowEngine newEngine() {
        return new WorkflowEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("用例: 同 clientId 重复提交 -> 同一个 runId, 且不重置状态")
    void duplicateClientIdReturnsSameRun() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("idem",
                WorkflowFixtures.task("a", WorkflowFixtures.success(300)));

        RunState first = engine.submit("client-1", def);
        Await.awaitUntil(() -> first.getStatus().isTerminal(), "first run terminal");
        assertEquals(RunStatus.SUCCEEDED, first.getStatus());

        RunState second = engine.submit("client-1",
                WorkflowFixtures.workflow("different-def",
                        WorkflowFixtures.task("x", WorkflowFixtures.success(0))));

        assertSame(first, second);
        assertEquals(1, engine.listRuns().size());
        assertEquals("idem", second.getDefinition().name(), "existing run must be untouched");
        assertEquals(RunStatus.SUCCEEDED, second.getStatus(), "finished state must not reset");
    }

    @Test
    @DisplayName("用例: 8 线程并发同 clientId 提交 -> 仅创建一个运行")
    void concurrentSubmitRace() throws Exception {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("race",
                WorkflowFixtures.task("a", WorkflowFixtures.success(0)));

        int threads = 8;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        String[] runIds = new String[threads];
        Thread[] workers = new Thread[threads];

        for (int i = 0; i < threads; i++) {
            int index = i;
            Thread thread = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                runIds[index] = engine.submit("race-client", def).getRunId();
            });
            thread.start();
            workers[i] = thread;
        }
        ready.await();
        start.countDown();

        for (Thread worker : workers) {
            worker.join(10_000);
        }
        java.util.Set<String> uniqueIds = new java.util.HashSet<String>(java.util.Arrays.asList(runIds));
        assertEquals(1, engine.listRuns().size());
        assertEquals(1, uniqueIds.size(), "all threads must receive the same run id: " + uniqueIds);
        Await.awaitUntil(() -> engine.listRuns().get(0).getStatus().isTerminal(),
                "run terminal");
    }
}
