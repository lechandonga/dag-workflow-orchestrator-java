package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

/**
 * Definition of one task node in a workflow DAG.
 *
 * @param id           unique task id within the workflow
 * @param dependencies ids of tasks that must finish (success) before this task can run
 * @param behavior     simulated execution behavior
 */
public record TaskDef(
        String id,
        List<String> dependencies,
        TaskBehavior behavior
) {
    public TaskDef {
        if (dependencies == null) {
            dependencies = List.of();
        }
        if (behavior == null) {
            behavior = new TaskBehavior(null, null, null, null, null, null);
        }
    }
}
