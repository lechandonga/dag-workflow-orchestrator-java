package com.github.lechandonga.dagworkfloworchestratorjava.tests.support;

import java.util.List;

import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;

public final class WorkflowFixtures {

    private WorkflowFixtures() {
    }

    public static TaskBehavior success(long durationMillis) {
        return new TaskBehavior(List.of(TaskBehavior.SUCCESS), durationMillis, null, null, null, null);
    }

    public static TaskBehavior failsThenSucceeds(int failures, long durationMillis, long backoffMillis) {
        return new TaskBehavior(outcomes(failures, TaskBehavior.FAIL), durationMillis, null,
                null, failures, backoffMillis);
    }

    public static TaskBehavior alwaysFails(int maxRetries, long backoffMillis) {
        return new TaskBehavior(List.of(TaskBehavior.FAIL), 0L, null, null, maxRetries, backoffMillis);
    }

    public static TaskBehavior timeoutThenSucceeds(long durationMillis, long timeoutMillis, long backoffMillis) {
        return new TaskBehavior(List.of(TaskBehavior.TIMEOUT, TaskBehavior.SUCCESS), durationMillis,
                null, timeoutMillis, 1, backoffMillis);
    }

    public static TaskBehavior exceedsThresholdThenSucceeds(long slowDuration, long timeoutMillis,
                                                            long fastDuration, long backoffMillis) {
        return new TaskBehavior(List.of(TaskBehavior.SUCCESS, TaskBehavior.SUCCESS), null,
                List.of(slowDuration, fastDuration), timeoutMillis, 1, backoffMillis);
    }

    public static TaskBehavior custom(List<String> outcomes, Long duration, Long timeout,
                                      Integer maxRetries, Long backoff) {
        return new TaskBehavior(outcomes, duration, null, timeout, maxRetries, backoff);
    }

    private static List<String> outcomes(int failures, String failureOutcome) {
        java.util.List<String> list = new java.util.ArrayList<>();
        for (int i = 0; i < failures; i++) {
            list.add(failureOutcome);
        }
        list.add(TaskBehavior.SUCCESS);
        return list;
    }

    public static TaskDef task(String id, TaskBehavior behavior, String... deps) {
        return new TaskDef(id, List.of(deps), behavior);
    }

    public static WorkflowDef workflow(String name, TaskDef... tasks) {
        return new WorkflowDef(name, List.of(tasks));
    }
}
