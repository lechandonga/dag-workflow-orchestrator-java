package com.github.lechandonga.dagworkfloworchestratorjava.model;

/**
 * Result of one task attempt. Every attempt (including retries) is retained.
 */
public record AttemptResult(
        int attempt,
        TaskOutcome outcome,
        boolean timedOut,
        long startedAt,
        long finishedAt,
        String error
) {
}
