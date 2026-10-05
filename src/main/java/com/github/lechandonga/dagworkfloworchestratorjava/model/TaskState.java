package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Mutable runtime state of one task instance within a run. */
public class TaskState {
    private final String taskId;
    private final TaskDef def;
    private volatile TaskStatus status = TaskStatus.PENDING;
    private volatile int attemptCount = 0;
    private volatile Instant startedAt;
    private volatile Instant finishedAt;
    private volatile boolean alertRaised;
    private final List<AttemptResult> attempts = new ArrayList<>();

    public TaskState(TaskDef def) {
        this.taskId = def.id();
        this.def = def;
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskDef getDef() {
        return def;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }

    public boolean isAlertRaised() {
        return alertRaised;
    }

    public void setAlertRaised(boolean alertRaised) {
        this.alertRaised = alertRaised;
    }

    public List<AttemptResult> getAttempts() {
        synchronized (attempts) {
            return new ArrayList<>(attempts);
        }
    }

    public void addAttempt(AttemptResult result) {
        synchronized (attempts) {
            attempts.add(result);
        }
    }
}
