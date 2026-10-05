package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

/**
 * Snapshot view of a run returned by the HTTP API.
 */
public record RunView(
        String runId,
        String submissionId,
        RunStatus status,
        long createdAt,
        Long finishedAt,
        List<TaskView> tasks
) {
}
