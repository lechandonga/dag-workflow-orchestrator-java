package com.github.lechandonga.dagworkfloworchestratorjava.web;

import java.time.Instant;
import java.util.List;

import com.github.lechandonga.dagworkfloworchestratorjava.model.AttemptResult;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;

/** Static factory methods producing JSON-friendly immutable views. */
public final class RunViews {

    private RunViews() {
    }

    public static RunView of(RunState run) {
        List<TaskView> tasks = run.taskStatesInOrder().stream().map(RunViews::ofTask).toList();
        return new RunView(
                run.getRunId(),
                run.getClientId(),
                run.getDefinition().name(),
                run.getStatus().name(),
                run.getCreatedAt(),
                run.getFinishedAt(),
                tasks,
                run.getEvents().size());
    }

    public static TaskView ofTask(TaskState task) {
        return new TaskView(
                task.getTaskId(),
                task.getStatus().name(),
                task.getAttemptCount(),
                task.isAlertRaised(),
                task.getStartedAt(),
                task.getFinishedAt(),
                task.getAttempts());
    }

    public record RunView(
            String runId,
            String clientId,
            String name,
            String status,
            Instant createdAt,
            Instant finishedAt,
            List<TaskView> tasks,
            int eventCount
    ) {
    }

    public record TaskView(
            String taskId,
            String status,
            int attempts,
            boolean alertRaised,
            Instant startedAt,
            Instant finishedAt,
            List<AttemptResult> attemptDetails
    ) {
    }
}
