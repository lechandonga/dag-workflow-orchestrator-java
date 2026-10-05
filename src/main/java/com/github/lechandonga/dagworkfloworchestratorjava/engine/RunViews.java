package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import com.github.lechandonga.dagworkfloworchestratorjava.model.RunView;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskInstance;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskView;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds immutable API snapshots under the run lock.
 */
public final class RunViews {

    private RunViews() {
    }

    public static RunView snapshot(WorkflowRun run) {
        synchronized (run.getLock()) {
            List<TaskView> tasks = new ArrayList<>();
            for (TaskInstance task : run.getTasks().values()) {
                tasks.add(new TaskView(
                        task.getId(),
                        task.getState(),
                        task.getCurrentAttempt(),
                        List.copyOf(task.getAttempts()),
                        task.isAlerted(),
                        task.getStateUpdatedAt()));
            }
            return new RunView(
                    run.getRunId(),
                    run.getSubmissionId(),
                    run.getStatus(),
                    run.getCreatedAt(),
                    run.getFinishedAt(),
                    tasks);
        }
    }
}
