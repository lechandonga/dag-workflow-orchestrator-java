package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.EventReplayer;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowRun;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：事件流按发生顺序记录，且仅凭事件流即可还原运行与全部任务终态。
 */
@ExtendWith(BusinessLogging.class)
@DisplayName("事件流与状态还原")
class EventStreamReplayTest {

    private final WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator());
    private final EventReplayer replayer = new EventReplayer();

    @Test
    @DisplayName("失败/跳过/重试/告警场景的最终状态可由事件流完整还原")
    void replayMatchesLiveState() {
        TaskDef bad = WorkflowFixtures.task("bad", List.of(),
                TaskBehavior.fail(60, "boom"), 1, 20L, 1_000L, 30L);
        TaskDef skipped = WorkflowFixtures.task("skipped", List.of("bad"),
                TaskBehavior.success(0), 0, 0L, null, null);
        TaskDef independent = WorkflowFixtures.task("independent", List.of(),
                TaskBehavior.success(0), 0, 0L, null, null);
        WorkflowRun run = engine.submit(
                new WorkflowDef("event-1", List.of(bad, skipped, independent)));
        awaitFinished(run);

        List<WorkflowEvent> events = run.snapshotEvents();
        // seq 严格递增
        for (int i = 1; i < events.size(); i++) {
            assertTrue(events.get(i).seq() > events.get(i - 1).seq());
        }

        EventReplayer.ReplayedRun replayed = replayer.replay(events);

        assertEquals(RunStatus.FAILED, replayed.runStatus());
        assertEquals(TaskState.FAILED, replayed.taskStates().get("bad"));
        assertEquals(TaskState.SKIPPED, replayed.taskStates().get("skipped"));
        assertEquals(TaskState.SUCCEEDED, replayed.taskStates().get("independent"));
        assertEquals(2, replayed.taskAttempts().get("bad"), "bad 执行了 2 次尝试");
        assertEquals(Boolean.TRUE, replayed.taskAlerts().get("bad"),
                "告警事实可从事件流还原");

        // 与实时状态完全一致。
        run.getTasks().forEach((id, task) ->
                assertEquals(task.getState(), replayed.taskStates().get(id)));
    }

    @Test
    @DisplayName("取消场景的最终状态同样可由事件流还原")
    void replayCancelledRun() {
        TaskDef slow = WorkflowFixtures.task("slow", List.of(),
                TaskBehavior.fail(0, "boom"), 1, 2_000L, null, null);
        TaskDef child = WorkflowFixtures.task("child", List.of("slow"),
                TaskBehavior.success(0), 0, 0L, null, null);
        WorkflowRun run = engine.submit(new WorkflowDef("event-2", List.of(slow, child)));
        engine.cancel(run.getRunId());
        awaitFinished(run);

        EventReplayer.ReplayedRun replayed = replayer.replay(run.snapshotEvents());
        assertEquals(RunStatus.CANCELLED, replayed.runStatus());
        assertEquals(TaskState.CANCELLED, replayed.taskStates().get("slow"));
        assertEquals(TaskState.CANCELLED, replayed.taskStates().get("child"));
    }
}
