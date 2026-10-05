package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowEvent;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconstructs the final status of a run and all its tasks purely from the
 * ordered event stream. Terminal states are never overwritten, mirroring the
 * engine's transition rules.
 */
@Component
public class EventReplayer {

    public record ReplayedRun(RunStatus runStatus, Map<String, TaskState> taskStates,
                              Map<String, Integer> taskAttempts, Map<String, Boolean> taskAlerts) {
    }

    public ReplayedRun replay(List<WorkflowEvent> events) {
        RunStatus runStatus = null;
        Map<String, TaskState> taskStates = new LinkedHashMap<>();
        Map<String, Integer> taskAttempts = new LinkedHashMap<>();
        Map<String, Boolean> taskAlerts = new LinkedHashMap<>();

        for (WorkflowEvent event : events) {
            if ("RUN_STATUS_CHANGED".equals(event.type()) && event.toState() != null) {
                runStatus = RunStatus.valueOf(event.toState());
            }
            if (event.taskId() != null) {
                taskStates.putIfAbsent(event.taskId(), TaskState.PENDING);
            }
            if ("TASK_STATE_CHANGED".equals(event.type()) && event.taskId() != null) {
                TaskState target = TaskState.valueOf(event.toState());
                TaskState current = taskStates.get(event.taskId());
                if (current == null || !current.isTerminal()) {
                    taskStates.put(event.taskId(), target);
                }
            }
            if ("TASK_STARTED".equals(event.type()) || "TASK_RETRY_STARTED".equals(event.type())) {
                taskAttempts.put(event.taskId(), event.attempt());
            }
            if ("TASK_ALERT".equals(event.type())) {
                taskAlerts.put(event.taskId(), Boolean.TRUE);
            }
        }
        return new ReplayedRun(runStatus, taskStates, taskAttempts, taskAlerts);
    }
}
