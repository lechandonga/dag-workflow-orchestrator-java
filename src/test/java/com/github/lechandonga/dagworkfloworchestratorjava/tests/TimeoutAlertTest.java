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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：超时判定与告警。
 * 覆盖用例：超过阈值时看门狗恰好产生一次告警、重试成功后不重复告警、
 * 快速成功不告警、运行结束后不补告警。
 */
@DisplayName("业务-超时判定与告警")
class TimeoutAlertTest {

    private WorkflowEngine newEngine() {
        return new WorkflowEngine(new WorkflowValidator());
    }

    @Test
    @DisplayName("用例: 首次尝试超过阈值(随后重试成功) -> 仅一次告警, 首次尝试 timedOut=true")
    void exceedsThresholdThenRecovers() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("threshold-recover",
                WorkflowFixtures.task("a",
                        WorkflowFixtures.exceedsThresholdThenSucceeds(300, 80, 0, 20)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        TaskState task = run.getTasks().get("a");
        assertEquals(TaskStatus.SUCCEEDED, task.getStatus());
        assertTrue(task.isAlertRaised());
        assertEquals(2, task.getAttempts().size());
        assertTrue(task.getAttempts().get(0).timedOut());
        long alerts = run.getEvents().stream()
                .filter(event -> event.type() == EventType.TASK_ALERT_RAISED).count();
        assertEquals(1, alerts, "one task must alert at most once");
    }

    @Test
    @DisplayName("用例: 在阈值内快速完成 -> 不产生告警")
    void fastTaskNoAlert() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("fast",
                WorkflowFixtures.task("a",
                        WorkflowFixtures.custom(List.of(TaskBehavior.SUCCESS),
                                10L, 5000L, 0, 0L)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getStatus() == RunStatus.SUCCEEDED, "run succeeds");
        assertFalse(run.getTasks().get("a").isAlertRaised());
        long alerts = run.getEvents().stream()
                .filter(event -> event.type() == EventType.TASK_ALERT_RAISED).count();
        assertEquals(0, alerts);
    }

    @Test
    @DisplayName("用例: 每次尝试都超过阈值且重试耗尽 -> 仍只告警一次, 运行 FAILED")
    void alwaysExceedsThresholdAlertsOnce() {
        WorkflowEngine engine = newEngine();
        WorkflowDef def = WorkflowFixtures.workflow("threshold-fail",
                WorkflowFixtures.task("a",
                        WorkflowFixtures.custom(List.of(TaskBehavior.SUCCESS), 200L,
                                50L, 1, 10L)));
        RunState run = engine.submit(null, def);

        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");
        assertEquals(RunStatus.FAILED, run.getStatus());
        assertEquals(TaskStatus.FAILED, run.getTasks().get("a").getStatus());
        long alerts = run.getEvents().stream()
                .filter(event -> event.type() == EventType.TASK_ALERT_RAISED).count();
        assertEquals(1, alerts, "alert is never re-raised even across attempts");
    }
}
