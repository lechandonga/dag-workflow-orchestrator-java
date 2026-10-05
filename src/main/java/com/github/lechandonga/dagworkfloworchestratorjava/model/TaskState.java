package com.github.lechandonga.dagworkfloworchestratorjava.model;

/**
 * Lifecycle state of a task node within one run.
 * <pre>
 * PENDING -> RUNNING -> SUCCEEDED
 *                   \-> FAILED
 * PENDING -> BLOCKED / SKIPPED / CANCELLED
 * RUNNING -> CANCELLED
 * All terminal states are final.
 * </pre>
 */
public enum TaskState {
    PENDING(false),
    RUNNING(false),
    SUCCEEDED(true),
    FAILED(true),
    BLOCKED(true),
    SKIPPED(true),
    CANCELLED(true);

    private final boolean terminal;

    TaskState(boolean terminal) {
        this.terminal = terminal;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
