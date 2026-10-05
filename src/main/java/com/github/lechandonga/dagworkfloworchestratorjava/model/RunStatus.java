package com.github.lechandonga.dagworkfloworchestratorjava.model;

/**
 * Lifecycle status of a workflow run.
 * <pre>
 * PENDING -> RUNNING -> SUCCEEDED | FAILED
 * *        -> CANCEL_REQUESTED -> CANCELLED
 * </pre>
 */
public enum RunStatus {
    PENDING(false),
    RUNNING(false),
    CANCEL_REQUESTED(false),
    SUCCEEDED(true),
    FAILED(true),
    CANCELLED(true);

    private final boolean terminal;

    RunStatus(boolean terminal) {
        this.terminal = terminal;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
