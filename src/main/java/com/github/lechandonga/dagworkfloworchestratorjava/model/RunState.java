package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Mutable runtime state of one workflow run, guarded by its own lock. */
public class RunState {
    private final String runId;
    private final String clientId;
    private final WorkflowDef definition;
    private final Instant createdAt = Instant.now();
    private final Map<String, TaskState> tasks = new LinkedHashMap<>();
    private final List<WorkflowEvent> events = new ArrayList<>();

    private volatile RunStatus status = RunStatus.RUNNING;
    private volatile Instant finishedAt;
    private volatile long eventSeq = 0;

    public RunState(String runId, String clientId, WorkflowDef definition) {
        this.runId = runId;
        this.clientId = clientId;
        this.definition = definition;
        for (TaskDef taskDef : definition.tasks()) {
            tasks.put(taskDef.id(), new TaskState(taskDef));
        }
    }

    public String getRunId() {
        return runId;
    }

    public String getClientId() {
        return clientId;
    }

    public WorkflowDef getDefinition() {
        return definition;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public RunStatus getStatus() {
        return status;
    }

    public void setStatus(RunStatus status) {
        this.status = status;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }

    public Map<String, TaskState> getTasks() {
        return tasks;
    }

    /** Append an event; caller MUST hold this run's lock. */
    public WorkflowEvent appendEventLocked(EventType type, String taskId, Integer attempt, String detail) {
        WorkflowEvent event = new WorkflowEvent(++eventSeq, Instant.now(), type, taskId, attempt, detail);
        events.add(event);
        return event;
    }

    public List<WorkflowEvent> getEvents() {
        synchronized (this) {
            return new ArrayList<>(events);
        }
    }

    public List<TaskState> taskStatesInOrder() {
        return Collections.unmodifiableList(new ArrayList<>(tasks.values()));
    }
}
