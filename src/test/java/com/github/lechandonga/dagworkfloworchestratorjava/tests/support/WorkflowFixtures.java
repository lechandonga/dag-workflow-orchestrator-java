package com.github.lechandonga.dagworkfloworchestratorjava.tests.support;

import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;

import java.util.List;

public final class WorkflowFixtures {

    private WorkflowFixtures() {
    }

    public static TaskDef task(String id, TaskBehavior behavior, String... deps) {
        return new TaskDef(id, List.of(deps), behavior, 0, 0L, null, null);
    }

    public static TaskDef task(String id, List<String> deps, TaskBehavior behavior,
                               Integer retries, Long backoffMs, Long timeoutMs, Long alertAfterMs) {
        return new TaskDef(id, deps, behavior, retries, backoffMs, timeoutMs, alertAfterMs);
    }
}
