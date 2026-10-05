package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import com.github.lechandonga.dagworkfloworchestratorjava.exception.IllegalRunStateException;
import com.github.lechandonga.dagworkfloworchestratorjava.exception.RunNotFoundException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.AttemptResult;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskInstance;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskOutcome;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.validation.WorkflowValidator;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * In-process DAG workflow orchestration engine.
 * <p>
 * Every run owns a monitor; all run/task state transitions and event appends are
 * performed while holding that monitor, which also guarantees one task is never
 * executed concurrently within the same run. Independent runs use separate
 * monitors and a shared cached thread pool, so independent branches and runs
 * execute in parallel.
 */
@Component
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowValidator validator;
    private final ConcurrentHashMap<String, WorkflowRun> runs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> submissionIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<?>> activeAttempts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> alertTimers = new ConcurrentHashMap<>();

    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;

    public WorkflowEngine(WorkflowValidator validator) {
        this.validator = validator;
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "workflow-worker");
            t.setDaemon(true);
            return t;
        });
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "workflow-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        workers.shutdownNow();
        scheduler.shutdownNow();
    }

    /**
     * Validates and submits a workflow. submissionId is an idempotency key:
     * submitting the same key again returns the existing run without resetting
     * any of its state.
     */
    public WorkflowRun submit(WorkflowDef def) {
        validator.validate(def);

        final WorkflowRun[] created = new WorkflowRun[1];
        String runId = submissionIndex.compute(def.submissionId(), (key, existingRunId) -> {
            if (existingRunId != null) {
                return existingRunId;
            }
            String newRunId = UUID.randomUUID().toString();
            WorkflowRun run = new WorkflowRun(newRunId, def.submissionId(), System.currentTimeMillis());
            synchronized (run.getLock()) {
                for (TaskDef taskDef : def.tasks()) {
                    run.getTasks().put(taskDef.id(), new TaskInstance(taskDef, run.getCreatedAt()));
                }
                run.appendEvent("RUN_CREATED", null, null, null, RunStatus.PENDING.name(), null);
                run.setStatus(RunStatus.RUNNING);
                run.appendEvent("RUN_STATUS_CHANGED", null, null,
                        RunStatus.PENDING.name(), RunStatus.RUNNING.name(), null);
            }
            runs.put(newRunId, run);
            created[0] = run;
            return newRunId;
        });

        if (created[0] != null) {
            log.info("run created runId={} submissionId={} tasks={}",
                    runId, def.submissionId(), def.tasks().size());
            scheduleReady(created[0]);
            return created[0];
        }
        WorkflowRun existing = runs.get(runId);
        log.info("duplicate submission ignored submissionId={} existingRunId={} status={}",
                def.submissionId(), runId, existing.getStatus());
        return existing;
    }

    public WorkflowRun getRun(String runId) {
        WorkflowRun run = runs.get(runId);
        if (run == null) {
            throw new RunNotFoundException(runId);
        }
        return run;
    }

    public List<WorkflowRun> allRuns() {
        return new ArrayList<>(runs.values());
    }

    /**
     * Cancels a run. Pending tasks are cancelled immediately; an attempt already
     * running is allowed to finish but its result is discarded, no retry is
     * scheduled, and no successor ever runs. Cancelling an ended run is rejected.
     */
    public WorkflowRun cancel(String runId) {
        WorkflowRun run = getRun(runId);
        synchronized (run.getLock()) {
            RunStatus status = run.getStatus();
            if (status.isTerminal()) {
                throw new IllegalRunStateException(
                        "cannot cancel run " + runId + ": run already ended with status " + status);
            }
            if (status == RunStatus.CANCEL_REQUESTED) {
                return run;
            }
            run.setStatus(RunStatus.CANCEL_REQUESTED);
            run.appendEvent("RUN_STATUS_CHANGED", null, null,
                    status.name(), RunStatus.CANCEL_REQUESTED.name(), "cancel requested");

            for (TaskInstance task : run.getTasks().values()) {
                if (task.getState() == TaskState.PENDING) {
                    transitionTask(run, task, TaskState.CANCELLED, null, "cancelled before start");
                }
            }
            finishIfDone(run);
        }
        return run;
    }

    /**
     * Starts every eligible pending task and skips tasks whose dependencies can no
     * longer succeed. Independent tasks are dispatched to the pool together, so a
     * task waiting for a retry/backoff never blocks unrelated branches.
     */
    private void scheduleReady(WorkflowRun run) {
        synchronized (run.getLock()) {
            if (run.getStatus() == RunStatus.CANCEL_REQUESTED) {
                finishIfDone(run);
                return;
            }
            boolean changed = true;
            while (changed) {
                changed = false;
                for (TaskInstance task : run.getTasks().values()) {
                    if (task.getState() != TaskState.PENDING) {
                        continue;
                    }
                    boolean allDepsSucceeded = true;
                    boolean blockedByTerminalDep = false;
                    for (String depId : depsOf(task)) {
                        TaskState depState = run.getTasks().get(depId).getState();
                        if (depState != TaskState.SUCCEEDED) {
                            allDepsSucceeded = false;
                            if (depState.isTerminal()) {
                                blockedByTerminalDep = true;
                            }
                        }
                    }
                    if (blockedByTerminalDep) {
                        transitionTask(run, task, TaskState.SKIPPED, null,
                                "upstream dependency did not succeed");
                        changed = true;
                    } else if (allDepsSucceeded) {
                        startAttempt(run, task, 1, false);
                    }
                }
            }
            finishIfDone(run);
        }
    }

    private void startAttempt(WorkflowRun run, TaskInstance task, int attempt, boolean isRetry) {
        // Caller holds run.getLock(). Guard makes double-dispatch impossible.
        if (task.getState() == TaskState.RUNNING) {
            return;
        }
        task.setState(TaskState.RUNNING, System.currentTimeMillis());
        task.setCurrentAttempt(attempt);
        run.appendEvent(isRetry ? "TASK_RETRY_STARTED" : "TASK_STARTED",
                task.getId(), attempt, TaskState.PENDING.name(), TaskState.RUNNING.name(), null);

        long alertAfter = orZero(task.getDef().alertAfterMs());
        if (alertAfter > 0 && !task.isAlerted()) {
            int scheduledAttempt = attempt;
            ScheduledFuture<?> alertTimer = scheduler.schedule(
                    () -> fireAlert(run, task.getId(), scheduledAttempt),
                    alertAfter, TimeUnit.MILLISECONDS);
            alertTimers.put(alertKey(run, task), alertTimer);
        }
        Future<?> future = workers.submit(() -> runAttempt(run, task.getId(), attempt));
        activeAttempts.put(attemptKey(run, task, attempt), future);
    }

    private void runAttempt(WorkflowRun run, String taskId, int attempt) {
        TaskDef def;
        synchronized (run.getLock()) {
            def = run.getTasks().get(taskId).getDef();
        }
        TaskBehavior behavior = def.behavior() != null
                ? def.behavior()
                : TaskBehavior.success(0);
        long startedAt = System.currentTimeMillis();
        AttemptResult result = simulate(behavior, def.timeoutMs(), startedAt);
        handleAttemptFinished(run, taskId, attempt, result);
    }

    private AttemptResult simulate(TaskBehavior behavior, Long timeoutMs, long startedAt) {
        Future<?> sleep = workers.submit(() -> {
            try {
                Thread.sleep(Math.max(0, behavior.delayMs()));
                return null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        });
        boolean timedOut = false;
        long timeout = orZero(timeoutMs);
        try {
            if (timeout > 0) {
                try {
                    sleep.get(timeout, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    timedOut = true;
                    sleep.cancel(true);
                }
            } else {
                sleep.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // ExecutionException of the interrupted sleep wrapper; outcome decided below.
        }

        boolean interrupted = Thread.currentThread().isInterrupted();
        TaskOutcome outcome;
        String error;
        if (timedOut) {
            outcome = TaskOutcome.FAILURE;
            error = "attempt timed out after " + timeout + "ms";
        } else if (!interrupted && behavior.outcome() == TaskOutcome.SUCCESS) {
            outcome = TaskOutcome.SUCCESS;
            error = null;
        } else {
            outcome = TaskOutcome.FAILURE;
            error = behavior.message() != null ? behavior.message() : "simulated failure";
        }
        return new AttemptResult(0, outcome, timedOut, startedAt, System.currentTimeMillis(), error);
    }

    private void handleAttemptFinished(WorkflowRun run, String taskId, int attempt,
                                       AttemptResult raw) {
        AttemptResult result = new AttemptResult(attempt, raw.outcome(), raw.timedOut(),
                raw.startedAt(), raw.finishedAt(), raw.error());
        boolean needSchedule;
        synchronized (run.getLock()) {
            TaskInstance task = run.getTasks().get(taskId);
            activeAttempts.remove(attemptKey(run, task, attempt));
            cancelAlertTimer(run, task);

            // A task that already reached a terminal state never regresses, and a
            // late callback from a superseded attempt is ignored.
            if (task.getState().isTerminal() || task.getCurrentAttempt() != attempt) {
                return;
            }
            task.getAttempts().add(result);

            if (run.getStatus() == RunStatus.CANCEL_REQUESTED || run.getStatus().isTerminal()) {
                run.appendEvent("TASK_ATTEMPT_DISCARDED", taskId, attempt, null, null,
                        "attempt finished after cancellation");
                transitionTask(run, task, TaskState.CANCELLED, attempt,
                        "attempt finished after cancellation");
                finishIfDone(run);
                return;
            }

            if (result.outcome() == TaskOutcome.SUCCESS) {
                run.appendEvent("TASK_ATTEMPT_SUCCEEDED", taskId, attempt, null, null, null);
                transitionTask(run, task, TaskState.SUCCEEDED, attempt, null);
            } else {
                run.appendEvent("TASK_ATTEMPT_FAILED", taskId, attempt, null, null,
                        result.timedOut() ? "timeout" : result.error());
                int maxRetries = task.getDef().retries() != null ? task.getDef().retries() : 0;
                if (attempt <= maxRetries) {
                    long backoff = orZero(task.getDef().backoffMs());
                    task.setState(TaskState.PENDING, System.currentTimeMillis());
                    run.appendEvent("TASK_RETRY_SCHEDULED", taskId, attempt,
                            TaskState.RUNNING.name(), TaskState.PENDING.name(),
                            "retry " + (attempt + 1) + " in " + backoff + "ms");
                    scheduler.schedule(() -> retry(run, taskId, attempt + 1),
                            backoff, TimeUnit.MILLISECONDS);
                    return;
                }
                transitionTask(run, task, TaskState.FAILED, attempt,
                        result.timedOut() ? "timeout, retries exhausted" : "retries exhausted");
            }
            needSchedule = true;
        }
        if (needSchedule) {
            scheduleReady(run);
        }
    }

    private void retry(WorkflowRun run, String taskId, int nextAttempt) {
        synchronized (run.getLock()) {
            TaskInstance task = run.getTasks().get(taskId);
            if (run.getStatus() == RunStatus.CANCEL_REQUESTED || run.getStatus().isTerminal()) {
                if (task.getState() == TaskState.PENDING) {
                    transitionTask(run, task, TaskState.CANCELLED, nextAttempt,
                            "pending retry cancelled");
                    finishIfDone(run);
                }
                return;
            }
            if (task.getState() != TaskState.PENDING) {
                return;
            }
            startAttempt(run, task, nextAttempt, true);
        }
    }

    private void fireAlert(WorkflowRun run, String taskId, int attempt) {
        synchronized (run.getLock()) {
            TaskInstance task = run.getTasks().get(taskId);
            RunStatus status = run.getStatus();
            if (task.isAlerted()
                    || task.getState() != TaskState.RUNNING
                    || task.getCurrentAttempt() != attempt
                    || status == RunStatus.CANCEL_REQUESTED
                    || status.isTerminal()) {
                return;
            }
            task.setAlerted(true);
            run.appendEvent("TASK_ALERT", taskId, attempt, null, null,
                    "task exceeded alert threshold");
            log.warn("task alert runId={} task={} attempt={}", run.getRunId(), taskId, attempt);
        }
    }

    private void finishIfDone(WorkflowRun run) {
        boolean allTerminal = run.getTasks().values().stream()
                .allMatch(t -> t.getState().isTerminal());
        if (!allTerminal || run.getStatus().isTerminal()) {
            return;
        }
        boolean anyFailed = run.getTasks().values().stream()
                .anyMatch(t -> t.getState() == TaskState.FAILED);
        RunStatus target;
        if (run.getStatus() == RunStatus.CANCEL_REQUESTED) {
            target = RunStatus.CANCELLED;
        } else if (anyFailed) {
            target = RunStatus.FAILED;
        } else {
            target = RunStatus.SUCCEEDED;
        }
        RunStatus from = run.getStatus();
        run.setStatus(target);
        run.setFinishedAt(System.currentTimeMillis());
        run.appendEvent("RUN_STATUS_CHANGED", null, null,
                from.name(), target.name(), null);
        log.info("run finished runId={} status={}", run.getRunId(), target);
    }

    private void transitionTask(WorkflowRun run, TaskInstance task, TaskState target,
                                Integer attempt, String detail) {
        TaskState from = task.getState();
        if (from.isTerminal() && from != target) {
            throw new IllegalStateException("illegal task transition " + from + " -> " + target
                    + " for task " + task.getId());
        }
        task.setState(target, System.currentTimeMillis());
        run.appendEvent("TASK_STATE_CHANGED", task.getId(), attempt,
                from.name(), target.name(), detail);
    }

    private List<String> depsOf(TaskInstance task) {
        List<String> deps = task.getDef().dependsOn();
        return deps != null ? deps : List.of();
    }

    private void cancelAlertTimer(WorkflowRun run, TaskInstance task) {
        ScheduledFuture<?> timer = alertTimers.remove(alertKey(run, task));
        if (timer != null) {
            timer.cancel(false);
        }
    }

    private static long orZero(Long value) {
        return value != null ? value : 0L;
    }

    private static String attemptKey(WorkflowRun run, TaskInstance task, int attempt) {
        return run.getRunId() + "|" + task.getId() + "|" + attempt;
    }

    private static String alertKey(WorkflowRun run, TaskInstance task) {
        return run.getRunId() + "|" + task.getId();
    }
}
