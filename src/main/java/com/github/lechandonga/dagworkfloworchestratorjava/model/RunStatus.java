package com.github.lechandonga.dagworkfloworchestratorjava.model;

/** Lifecycle status of a workflow run. Terminal states can never regress. */
public enum RunStatus {
    RUNNING,
    CANCELLED,
    SUCCEEDED,
    FAILED;

    public boolean isTerminal() {
        return this != RUNNING;
    }
}
