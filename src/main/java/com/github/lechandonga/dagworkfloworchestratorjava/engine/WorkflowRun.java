package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskInstance;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregate root of one workflow run. All state transitions and event appends
 * happen while holding {@link #lock}. The lock is per-run, so independent runs
 * never block each other.
 */
public class WorkflowRun {
    private final String runId;
    private final String submissionId;
    private final Map<String, TaskInstance> tasks = new LinkedHashMap<>();
    private final List<WorkflowEvent> events = new ArrayList<>();
    private final Object lock = new Object();
    private volatile RunStatus status = RunStatus.PENDING;
    private final long createdAt;
    private volatile Long finishedAt;
    private long eventSeq;

    public WorkflowRun(String runId, String submissionId, long createdAt) {
        this.runId = runId;
        this.submissionId = submissionId;
        this.createdAt = createdAt;
    }

    public String getRunId() {
        return runId;
    }

    public String getSubmissionId() {
        return submissionId;
    }

    public RunStatus getStatus() {
        return status;
    }

    public void setStatus(RunStatus status) {
        this.status = status;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public Long getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Long finishedAt) {
        this.finishedAt = finishedAt;
    }

    public Map<String, TaskInstance> getTasks() {
        return tasks;
    }

    public Object getLock() {
        return lock;
    }

    public List<WorkflowEvent> snapshotEvents() {
        synchronized (lock) {
            return new ArrayList<>(events);
        }
    }

    public WorkflowEvent appendEvent(String type, String taskId, Integer attempt,
                                     String fromState, String toState, String detail) {
        WorkflowEvent event = new WorkflowEvent(
                ++eventSeq, System.currentTimeMillis(), type, taskId, attempt,
                fromState, toState, detail);
        events.add(event);
        return event;
    }
}
