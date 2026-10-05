package com.github.lechandonga.dagworkfloworchestratorjava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Mutable runtime state of one task node.
 */
public class TaskInstance {
    private final String id;
    private final TaskDef def;
    private volatile TaskState state = TaskState.PENDING;
    private volatile int currentAttempt;
    private final List<AttemptResult> attempts = new ArrayList<>();
    private volatile long stateUpdatedAt;
    private volatile boolean alerted;

    public TaskInstance(TaskDef def, long now) {
        this.id = def.id();
        this.def = def;
        this.stateUpdatedAt = now;
    }

    public String getId() {
        return id;
    }

    public TaskDef getDef() {
        return def;
    }

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState state, long now) {
        this.state = state;
        this.stateUpdatedAt = now;
    }

    public int getCurrentAttempt() {
        return currentAttempt;
    }

    public void setCurrentAttempt(int currentAttempt) {
        this.currentAttempt = currentAttempt;
    }

    public List<AttemptResult> getAttempts() {
        return attempts;
    }

    public long getStateUpdatedAt() {
        return stateUpdatedAt;
    }

    public boolean isAlerted() {
        return alerted;
    }

    public void setAlerted(boolean alerted) {
        this.alerted = alerted;
    }
}
