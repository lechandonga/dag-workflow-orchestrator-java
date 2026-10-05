package com.github.lechandonga.dagworkfloworchestratorjava.model;

/**
 * Lifecycle status of a single task.
 *
 * <pre>
 * PENDING -> SCHEDULED -> RUNNING -> SUCCEEDED
 *    |          |           |
 *    |          |           +-> RETRYING -> (back to SCHEDULED) ... -> FAILED
 *    |          |                                               |
 *    +----------+----------------------> BLOCKED / SKIPPED <----+
 * </pre>
 */
public enum TaskStatus {
    PENDING,
    SCHEDULED,
    RUNNING,
    RETRYING,
    SUCCEEDED,
    FAILED,
    BLOCKED,
    SKIPPED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == BLOCKED || this == SKIPPED;
    }
}
