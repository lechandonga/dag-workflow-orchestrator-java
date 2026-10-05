package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

/**
 * Definition of one task node in a workflow DAG.
 *
 * @param id          unique task id within the workflow
 * @param dependsOn   ids of tasks that must succeed before this task is eligible
 * @param behavior    simulated execution behavior
 * @param retries     max retry attempts after the first attempt fails (0 = no retry)
 * @param backoffMs   fixed backoff between attempts
 * @param timeoutMs   timeout for one attempt; null or non-positive means no timeout
 * @param alertAfterMs if one attempt runs longer than this without finishing, exactly
 *                     one alert is emitted for the task; null/non-positive disables it
 */
public record TaskDef(
        String id,
        List<String> dependsOn,
        TaskBehavior behavior,
        Integer retries,
        Long backoffMs,
        Long timeoutMs,
        Long alertAfterMs
) {
}
