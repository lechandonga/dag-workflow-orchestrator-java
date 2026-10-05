package com.github.lechandonga.dagworkfloworchestratorjava.exception;

import java.util.Set;

/**
 * Thrown when a submitted workflow graph is invalid. {@link #getReason()} is a
 * stable machine-readable code so callers can distinguish failure causes.
 */
public class WorkflowValidationException extends RuntimeException {

    public static final String EMPTY_GRAPH = "EMPTY_GRAPH";
    public static final String MISSING_SUBMISSION_ID = "MISSING_SUBMISSION_ID";
    public static final String MISSING_TASK_ID = "MISSING_TASK_ID";
    public static final String DUPLICATE_TASK_ID = "DUPLICATE_TASK_ID";
    public static final String UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY";
    public static final String SELF_DEPENDENCY = "SELF_DEPENDENCY";
    public static final String CYCLIC_DEPENDENCY = "CYCLIC_DEPENDENCY";

    private static final Set<String> REASONS = Set.of(
            EMPTY_GRAPH, MISSING_SUBMISSION_ID, MISSING_TASK_ID, DUPLICATE_TASK_ID,
            UNKNOWN_DEPENDENCY, SELF_DEPENDENCY, CYCLIC_DEPENDENCY);

    private final String reason;

    public WorkflowValidationException(String reason, String message) {
        super(message);
        if (!REASONS.contains(reason)) {
            throw new IllegalArgumentException("unknown validation reason: " + reason);
        }
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
