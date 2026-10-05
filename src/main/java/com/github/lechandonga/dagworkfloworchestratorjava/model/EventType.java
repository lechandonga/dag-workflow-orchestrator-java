package com.github.lechandonga.dagworkfloworchestratorjava.model;

/** Types of events appended, in order, to a run's event log. */
public enum EventType {
    RUN_SUBMITTED,
    TASK_SCHEDULED,
    TASK_STARTED,
    TASK_SUCCEEDED,
    TASK_FAILED,
    ATTEMPT_FAILED,
    TASK_RETRY_SCHEDULED,
    TASK_TIMED_OUT,
    TASK_ALERT_RAISED,
    TASK_BLOCKED,
    TASK_SKIPPED,
    RUN_CANCEL_REQUESTED,
    RUN_CANCELLED,
    RUN_SUCCEEDED,
    RUN_FAILED
}
