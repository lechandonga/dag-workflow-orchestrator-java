package com.github.lechandonga.dagworkfloworchestratorjava.exception;

public class RunNotFoundException extends RuntimeException {
    public RunNotFoundException(String runId) {
        super("workflow run not found: " + runId);
    }
}
