package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.EventReplayer;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowValidator;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.Await;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 业务：事件流与状态还原。
 * 覆盖用例：成功流、失败/阻塞/跳过流都能仅靠有序事件流还原运行与各任务最终状态，
 * 且还原结果与内存实时状态一致。
 */
@DisplayName("业务-事件流与状态还原")
class EventReplayTest {

    private final EventReplayer replayer = new EventReplayer();

    @Test
    @DisplayName("用例: 菱形 DAG 全部成功 -> 事件流还原 SUCCEEDED + 每任务 SUCCEEDED")
    void replaySuccessDag() {
        WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator());
        WorkflowDef def = WorkflowFixtures.workflow("diamond",
                WorkflowFixtures.task("a", WorkflowFixtures.success(0)),
                WorkflowFixtures.task("b", WorkflowFixtures.success(0), "a"),
                WorkflowFixtures.task("c", WorkflowFixtures.success(0), "a"),
                WorkflowFixtures.task("d", WorkflowFixtures.success(0), "b", "c"));
        RunState run = engine.submit(null, def);
        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");

        EventReplayer.ReplayResult replay = replayer.replay(run.getEvents());
        assertEquals(RunStatus.SUCCEEDED, replay.runStatus());
        assertEquals(run.getStatus().name(), replay.runStatus().name());
        for (String taskId : java.util.List.of("a", "b", "c", "d")) {
            assertEquals(TaskStatus.SUCCEEDED, replay.taskStatus().get(taskId));
        }
    }

    @Test
    @DisplayName("用例: 上游失败 -> 事件流还原 FAILED/BLOCKED/SKIPPED 及尝试次数与告警标记")
    void replayFailurePropagation() {
        WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator());
        WorkflowDef def = WorkflowFixtures.workflow("fail-chain",
                WorkflowFixtures.task("a", WorkflowFixtures.alwaysFails(1, 5)),
                WorkflowFixtures.task("b", WorkflowFixtures.success(0), "a"),
                WorkflowFixtures.task("c", WorkflowFixtures.success(0), "b"));
        RunState run = engine.submit(null, def);
        Await.awaitUntil(() -> run.getStatus().isTerminal(), "run terminal");

        EventReplayer.ReplayResult replay = replayer.replay(run.getEvents());
        assertEquals(RunStatus.FAILED, replay.runStatus());
        assertEquals(TaskStatus.FAILED, replay.taskStatus().get("a"));
        assertEquals(TaskStatus.BLOCKED, replay.taskStatus().get("b"));
        assertEquals(TaskStatus.SKIPPED, replay.taskStatus().get("c"));
        assertEquals(2, replay.taskAttempts().get("a"));
    }
}
