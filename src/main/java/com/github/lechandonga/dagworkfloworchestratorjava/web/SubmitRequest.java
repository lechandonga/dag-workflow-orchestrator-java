package com.github.lechandonga.dagworkfloworchestratorjava.web;

import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;

/** Submit payload: optional idempotency key + workflow definition. */
public record SubmitRequest(
        String clientId,
        WorkflowDef workflow
) {
}
