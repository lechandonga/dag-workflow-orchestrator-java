package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.List;

public record TaskView(
        String id,
        TaskState state,
        int currentAttempt,
        List<AttemptResult> attempts,
        boolean alerted,
        long updatedAt
) {
}
