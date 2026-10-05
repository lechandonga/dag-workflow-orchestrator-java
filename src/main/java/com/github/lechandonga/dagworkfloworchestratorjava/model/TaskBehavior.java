package com.github.lechandonga.dagworkfloworchestratorjava.model;

/**
 * Deterministic, simulated behavior of a task.
 * <p>
 * Every attempt behaves the same way:
 * <ul>
 *     <li>{@link #SUCCESS} - sleep {@code delayMs} then succeed</li>
 *     <li>{@link #FAIL} - sleep {@code delayMs} then fail with {@code message}</li>
 * </ul>
 * Timeouts are reproduced by making an attempt's delay longer than its timeout.
 */
public record TaskBehavior(
        TaskOutcome outcome,
        long delayMs,
        String message
) {
    public static TaskBehavior success(long delayMs) {
        return new TaskBehavior(TaskOutcome.SUCCESS, delayMs, null);
    }

    public static TaskBehavior fail(long delayMs, String message) {
        return new TaskBehavior(TaskOutcome.FAILURE, delayMs, message);
    }
}
