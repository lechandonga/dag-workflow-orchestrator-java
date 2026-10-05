package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

/** Submitted workflow definition: a DAG of tasks. */
public record WorkflowDef(
        String name,
        List<TaskDef> tasks
) {
    public WorkflowDef {
        if (tasks == null) {
            tasks = List.of();
        }
    }
}
