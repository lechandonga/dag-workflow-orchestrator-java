package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.time.Instant;

/** Result of one task attempt; every attempt (including retries) is retained. */
public record AttemptResult(
        int attempt,
        String outcome,
        Instant startedAt,
        Instant finishedAt,
        long durationMillis,
        boolean timedOut,
        String error
) {
}
