package com.github.lechandonga.dagworkfloworchestratorjava.model;

/**
 * One ordered event in a run's append-only event stream.
 * Replaying events in {@code seq} order reconstructs run and task final states.
 */
public record WorkflowEvent(
        long seq,
        long timestamp,
        String type,
        String taskId,
        Integer attempt,
        String fromState,
        String toState,
        String detail
) {
}
