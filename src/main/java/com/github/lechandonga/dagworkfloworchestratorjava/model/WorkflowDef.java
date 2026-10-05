package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

/**
 * Submitted workflow definition: an idempotency key plus a task graph.
 *
 * @param submissionId idempotency key; the same key always maps to the same run
 * @param tasks        task nodes forming a DAG
 */
public record WorkflowDef(
        String submissionId,
        List<TaskDef> tasks
) {
}
