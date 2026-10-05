package com.github.lechandonga.dagworkfloworchestratorjava.tests.support;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowRun;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.awaitility.Awaitility.await;

public final class TestAwait {

    private TestAwait() {
    }

    public static void awaitFinished(WorkflowRun run) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10))
                .until(() -> run.getStatus().isTerminal());
    }

    public static void awaitStatus(WorkflowRun run, RunStatus status) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10))
                .until(() -> run.getStatus() == status);
    }

    public static void awaitState(WorkflowRun run, String taskId,
                                  com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState state) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10))
                .until(() -> run.getTasks().get(taskId).getState() == state);
    }

    public static void awaitTrue(BooleanSupplier condition) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10))
                .until(condition::getAsBoolean);
    }
}
