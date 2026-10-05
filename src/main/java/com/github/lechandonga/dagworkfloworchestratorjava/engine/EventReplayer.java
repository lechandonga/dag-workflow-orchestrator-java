package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.lechandonga.dagworkfloworchestratorjava.model.EventType;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowEvent;
import org.springframework.stereotype.Component;

/**
 * Replays a run's ordered event stream to reconstruct the final run status and
 * the final status of every referenced task. No live state is read.
 */
@Component
public class EventReplayer {

    public ReplayResult replay(List<WorkflowEvent> events) {
        Map<String, TaskStatus> taskStatus = new LinkedHashMap<>();
        Map<String, Integer> taskAttempts = new LinkedHashMap<>();
        Map<String, Boolean> taskAlerts = new LinkedHashMap<>();
        RunStatus runStatus = null;

        for (WorkflowEvent event : events) {
            String taskId = event.taskId();
            if (taskId != null) {
                taskStatus.putIfAbsent(taskId, TaskStatus.PENDING);
                if (event.attempt() != null) {
                    taskAttempts.merge(taskId, event.attempt(), Math::max);
                }
                switch (event.type()) {
                    case TASK_SCHEDULED, TASK_RETRY_SCHEDULED ->
                            taskStatus.put(taskId, TaskStatus.SCHEDULED);
                    case TASK_STARTED -> taskStatus.put(taskId, TaskStatus.RUNNING);
                    case TASK_SUCCEEDED -> taskStatus.put(taskId, TaskStatus.SUCCEEDED);
                    case TASK_FAILED -> taskStatus.put(taskId, TaskStatus.FAILED);
                    case TASK_BLOCKED -> taskStatus.put(taskId, TaskStatus.BLOCKED);
                    case TASK_SKIPPED -> taskStatus.put(taskId, TaskStatus.SKIPPED);
                    case TASK_ALERT_RAISED -> taskAlerts.put(taskId, true);
                    default -> {
                    }
                }
            }
            switch (event.type()) {
                case RUN_SUCCEEDED -> runStatus = RunStatus.SUCCEEDED;
                case RUN_FAILED -> runStatus = RunStatus.FAILED;
                case RUN_CANCELLED -> runStatus = RunStatus.CANCELLED;
                default -> {
                }
            }
        }

        if (runStatus == null) {
            runStatus = RunStatus.RUNNING;
        }
        return new ReplayResult(runStatus, taskStatus, taskAttempts, taskAlerts);
    }

    public record ReplayResult(
            RunStatus runStatus,
            Map<String, TaskStatus> taskStatus,
            Map<String, Integer> taskAttempts,
            Map<String, Boolean> taskAlerts
    ) {
    }
}
