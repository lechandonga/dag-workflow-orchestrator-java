package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.time.Instant;

/** One immutable entry of a run's ordered event stream. */
public record WorkflowEvent(
        long seq,
        Instant timestamp,
        EventType type,
        String taskId,
        Integer attempt,
        String detail
) {
}
