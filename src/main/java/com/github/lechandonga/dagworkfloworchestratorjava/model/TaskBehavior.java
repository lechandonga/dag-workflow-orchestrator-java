package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

/**
 * Simulated runtime behavior of a task. Everything is optional so tests can
 * deterministically reproduce success, failure, retry and timeout paths.
 *
 * @param outcomes       per-attempt outcomes: "SUCCESS" / "FAIL" / "TIMEOUT";
 *                       attempts past the end reuse the last entry
 *                       (default: a single SUCCESS)
 * @param durationMillis per-attempt simulated execution duration (default 0)
 * @param durationsMillis optional per-attempt durations; overrides durationMillis
 *                       for attempts that have an entry
 * @param timeoutMillis  per-attempt timeout threshold; null or 0 disables timeout
 * @param maxRetries     max retry attempts after the first attempt fails/times out
 *                       (default 0 = no retry)
 * @param backoffMillis  fixed backoff before each retry (default 0)
 */
public record TaskBehavior(
        List<String> outcomes,
        Long durationMillis,
        List<Long> durationsMillis,
        Long timeoutMillis,
        Integer maxRetries,
        Long backoffMillis
) {
    public static final String SUCCESS = "SUCCESS";
    public static final String FAIL = "FAIL";
    public static final String TIMEOUT = "TIMEOUT";

    public TaskBehavior {
        if (outcomes == null || outcomes.isEmpty()) {
            outcomes = List.of(SUCCESS);
        }
    }

    public String outcomeFor(int attempt) {
        int index = Math.min(attempt - 1, outcomes.size() - 1);
        return outcomes.get(index);
    }

    public long duration() {
        return durationMillis == null ? 0L : durationMillis;
    }

    public long durationFor(int attempt) {
        if (durationsMillis != null && attempt - 1 < durationsMillis.size()) {
            Long value = durationsMillis.get(attempt - 1);
            if (value != null) {
                return value;
            }
        }
        return duration();
    }

    public long timeout() {
        return timeoutMillis == null ? 0L : timeoutMillis;
    }

    public int retries() {
        return maxRetries == null ? 0 : maxRetries;
    }

    public long backoff() {
        return backoffMillis == null ? 0L : backoffMillis;
    }
}
