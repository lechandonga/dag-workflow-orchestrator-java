package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.lechandonga.dagworkfloworchestratorjava.error.ApiException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.AttemptResult;
import com.github.lechandonga.dagworkfloworchestratorjava.model.EventType;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskStatus;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * In-process DAG workflow orchestration engine.
 *
 * <p>All state lives in memory. Each run has a monitor lock guarding its state
 * machine and event stream; worker threads execute independent branches in
 * parallel on a shared thread pool.
 */
@Component
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowValidator validator;
    private final Map<String, RunState> runs = new ConcurrentHashMap<>();
    private final Map<String, String> clientIndex = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Thread>> activeThreads = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ScheduledAttempt>> watchdogHandles = new ConcurrentHashMap<>();

    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;

    public WorkflowEngine(WorkflowValidator validator) {
        this.validator = validator;
        AtomicInteger workerSeq = new AtomicInteger();
        ThreadFactory workerFactory = runnable -> {
            Thread thread = new Thread(runnable, "dag-worker-" + workerSeq.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.workers = Executors.newCachedThreadPool(workerFactory);
        this.scheduler = Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "dag-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PreDestroy
    public void shutdown() {
        workers.shutdownNow();
        scheduler.shutdownNow();
    }

    public RunState submit(String clientId, WorkflowDef def) {
        validator.validate(def);
        if (clientId != null && !clientId.isBlank()) {
            String existing = clientIndex.get(clientId);
            if (existing != null) {
                log.info("business=submit case=duplicate_client_id clientId={} runId={} -> existing run",
                        clientId, existing);
                return runs.get(existing);
            }
        }
        String runId = UUID.randomUUID().toString();
        RunState run = new RunState(runId, clientId, def);
        if (clientId != null && !clientId.isBlank()) {
            String raced = clientIndex.putIfAbsent(clientId, runId);
            if (raced != null) {
                log.info("business=submit case=duplicate_client_id_race clientId={} runId={} -> existing run",
                        clientId, raced);
                return runs.get(raced);
            }
        }
        runs.put(runId, run);
        activeThreads.put(runId, new ConcurrentHashMap<>());
        watchdogHandles.put(runId, new ConcurrentHashMap<>());

        synchronized (run) {
            run.appendEventLocked(EventType.RUN_SUBMITTED, null, null,
                    "name=" + def.name() + ", tasks=" + def.tasks().size());
            log.info("business=submit case=accepted runId={} clientId={} tasks={}",
                    runId, clientId, def.tasks().size());
            scheduleReadyLocked(run);
        }
        return run;
    }

    public RunState getRun(String runId) {
        RunState run = runs.get(runId);
        if (run == null) {
            throw new RunNotFoundException(runId);
        }
        return run;
    }

    public List<RunState> listRuns() {
        return List.copyOf(runs.values());
    }

    public RunState cancel(String runId) {
        RunState run = getRun(runId);
        synchronized (run) {
            if (run.getStatus().isTerminal()) {
                throw new ApiException(HttpStatus.CONFLICT, "RUN_ALREADY_TERMINAL",
                        "Run " + runId + " is already " + run.getStatus()
                                + "; terminal runs cannot be cancelled");
            }
            run.appendEventLocked(EventType.RUN_CANCEL_REQUESTED, null, null, "cancel requested");
            log.info("business=cancel case=cancel_requested runId={}", runId);
            finishCancellationLocked(run, "run cancelled");
        }
        return run;
    }

    private void finishCancellationLocked(RunState run, String reason) {
        for (TaskState task : run.getTasks().values()) {
            if (!task.getStatus().isTerminal()) {
                task.setStatus(TaskStatus.SKIPPED);
                task.setFinishedAt(Instant.now());
                run.appendEventLocked(EventType.TASK_SKIPPED, task.getTaskId(),
                        task.getAttemptCount(), reason);
            }
        }
        for (ScheduledAttempt handle : watchdogHandles.get(run.getRunId()).values()) {
            handle.cancel();
        }
        watchdogHandles.get(run.getRunId()).clear();
        for (Thread thread : activeThreads.get(run.getRunId()).values()) {
            thread.interrupt();
        }
        run.appendEventLocked(EventType.RUN_CANCELLED, null, null, reason);
        run.setStatus(RunStatus.CANCELLED);
        run.setFinishedAt(Instant.now());
        log.info("business=cancel case=cancelled runId={} reason='{}'", run.getRunId(), reason);
    }

    private void scheduleReadyLocked(RunState run) {
        if (run.getStatus() != RunStatus.RUNNING) {
            return;
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (TaskState task : run.getTasks().values()) {
                if (task.getStatus() == TaskStatus.PENDING && isReadyLocked(run, task)) {
                    launchAttemptLocked(run, task);
                    changed = true;
                }
            }
        }
        checkRunCompletionLocked(run);
    }

    private boolean isReadyLocked(RunState run, TaskState task) {
        for (String depId : task.getDef().dependencies()) {
            if (run.getTasks().get(depId).getStatus() != TaskStatus.SUCCEEDED) {
                return false;
            }
        }
        return true;
    }

    private void launchAttemptLocked(RunState run, TaskState task) {
        int attempt = task.getAttemptCount() + 1;
        boolean firstAttempt = attempt == 1;
        task.setStatus(TaskStatus.SCHEDULED);
        task.setAttemptCount(attempt);
        run.appendEventLocked(
                firstAttempt ? EventType.TASK_SCHEDULED : EventType.TASK_RETRY_SCHEDULED,
                task.getTaskId(), attempt,
                firstAttempt ? "scheduled" : "retry " + attempt + " scheduled");
        log.info("business=schedule case=attempt_scheduled runId={} task={} attempt={}",
                run.getRunId(), task.getTaskId(), attempt);
        workers.submit(() -> executeAttempt(run.getRunId(), task.getTaskId(), attempt));
    }

    // --------------------------------------------------------------- execution

    private void executeAttempt(String runId, String taskId, int attempt) {
        RunState run = runs.get(runId);
        TaskState task = run.getTasks().get(taskId);
        TaskBehavior behavior = task.getDef().behavior();
        String plannedOutcome = behavior.outcomeFor(attempt);
        long duration = behavior.durationFor(attempt);
        long timeout = behavior.timeout();

        Instant startedAt;
        synchronized (run) {
            if (run.getStatus() != RunStatus.RUNNING
                    || task.getStatus() != TaskStatus.SCHEDULED
                    || task.getAttemptCount() != attempt
                    || activeThreads.get(runId).containsKey(taskId)) {
                log.warn("business=concurrency case=stale_attempt_skipped runId={} task={} attempt={} status={}",
                        runId, taskId, attempt, task.getStatus());
                return;
            }
            activeThreads.get(runId).put(taskId, Thread.currentThread());
            task.setStatus(TaskStatus.RUNNING);
            startedAt = Instant.now();
            if (task.getStartedAt() == null) {
                task.setStartedAt(startedAt);
            }
            run.appendEventLocked(EventType.TASK_STARTED, taskId, attempt,
                    "planned=" + plannedOutcome + ", durationMs=" + duration
                            + (timeout > 0 ? ", timeoutMs=" + timeout : ""));
            log.info("business=execution case=started runId={} task={} attempt={} planned={} durationMs={}",
                    runId, taskId, attempt, plannedOutcome, duration);

            if (timeout > 0) {
                ScheduledAttempt watchdog = new ScheduledAttempt(scheduler.schedule(
                        () -> onTimeout(runId, taskId, attempt), timeout, TimeUnit.MILLISECONDS));
                watchdogHandles.get(runId).put(taskId, watchdog);
            }
        }

        boolean interrupted = false;
        try {
            if (duration > 0) {
                Thread.sleep(duration);
            }
        } catch (InterruptedException e) {
            interrupted = true;
            Thread.currentThread().interrupt();
        }

        Instant finishedAt = Instant.now();
        long elapsed = Duration.between(startedAt, finishedAt).toMillis();
        synchronized (run) {
            activeThreads.get(runId).remove(taskId, Thread.currentThread());
            ScheduledAttempt handle = watchdogHandles.get(runId).remove(taskId);
            if (handle != null) {
                handle.cancel();
            }

            if (run.getStatus() != RunStatus.RUNNING
                    || task.getStatus() != TaskStatus.RUNNING
                    || task.getAttemptCount() != attempt) {
                log.info("business=cancel case=attempt_result_discarded runId={} task={} attempt={} runStatus={}",
                        runId, taskId, attempt, run.getStatus());
                Thread.interrupted();
                return;
            }
            if (interrupted) {
                log.info("business=cancel case=interrupted_attempt_discarded runId={} task={} attempt={}",
                        runId, taskId, attempt);
                return;
            }

            boolean exceededThreshold = timeout > 0 && elapsed > timeout;
            boolean outcomeTimeout = TaskBehavior.TIMEOUT.equals(plannedOutcome);
            boolean timedOut = exceededThreshold || outcomeTimeout;
            boolean success = !timedOut && TaskBehavior.SUCCESS.equals(plannedOutcome);

            if (timedOut) {
                recordAttempt(task, attempt, plannedOutcome, startedAt, finishedAt, elapsed, true,
                        outcomeTimeout ? "simulated timeout"
                                : "exceeded timeout threshold " + timeout + "ms");
                run.appendEventLocked(EventType.TASK_TIMED_OUT, taskId, attempt,
                        "elapsedMs=" + elapsed + (timeout > 0 ? ", thresholdMs=" + timeout : ""));
                raiseAlertOnceLocked(run, task, attempt,
                        "task exceeded timeout threshold: elapsedMs=" + elapsed
                                + (timeout > 0 ? ", thresholdMs=" + timeout : ""));
                handleFailedAttemptLocked(run, task, attempt, finishedAt);
            } else if (success) {
                recordAttempt(task, attempt, TaskBehavior.SUCCESS, startedAt, finishedAt, elapsed, false, null);
                task.setStatus(TaskStatus.SUCCEEDED);
                task.setFinishedAt(finishedAt);
                run.appendEventLocked(EventType.TASK_SUCCEEDED, taskId, attempt, "elapsedMs=" + elapsed);
                log.info("business=execution case=succeeded runId={} task={} attempt={} elapsedMs={}",
                        runId, taskId, attempt, elapsed);
                propagateAndScheduleLocked(run);
            } else {
                recordAttempt(task, attempt, TaskBehavior.FAIL, startedAt, finishedAt, elapsed, false,
                        "simulated failure");
                run.appendEventLocked(EventType.ATTEMPT_FAILED, taskId, attempt, "simulated failure");
                log.info("business=retry case=attempt_failed runId={} task={} attempt={}",
                        runId, taskId, attempt);
                handleFailedAttemptLocked(run, task, attempt, finishedAt);
            }
        }
    }

    private void recordAttempt(TaskState task, int attempt, String outcome,
                               Instant startedAt, Instant finishedAt, long elapsed,
                               boolean timedOut, String error) {
        task.addAttempt(new AttemptResult(attempt, outcome, startedAt, finishedAt,
                elapsed, timedOut, error));
    }

    private void onTimeout(String runId, String taskId, int attempt) {
        RunState run = runs.get(runId);
        if (run == null) {
            return;
        }
        synchronized (run) {
            TaskState task = run.getTasks().get(taskId);
            if (run.getStatus() != RunStatus.RUNNING
                    || task.getStatus() != TaskStatus.RUNNING
                    || task.getAttemptCount() != attempt) {
                return;
            }
            log.warn("business=timeout case=threshold_reached runId={} task={} attempt={}",
                    runId, taskId, attempt);
            raiseAlertOnceLocked(run, task, attempt,
                    "task still running after timeout threshold, attempt=" + attempt);
        }
    }

    private void raiseAlertOnceLocked(RunState run, TaskState task, int attempt, String message) {
        if (task.isAlertRaised()) {
            return;
        }
        task.setAlertRaised(true);
        run.appendEventLocked(EventType.TASK_ALERT_RAISED, task.getTaskId(), attempt, message);
        log.warn("business=alert case=raised runId={} task={} attempt={} detail='{}'",
                run.getRunId(), task.getTaskId(), attempt, message);
    }

    private void handleFailedAttemptLocked(RunState run, TaskState task, int attempt, Instant finishedAt) {
        int maxRetries = task.getDef().behavior().retries();
        if (attempt <= maxRetries) {
            task.setStatus(TaskStatus.RETRYING);
            long backoff = task.getDef().behavior().backoff();
            log.info("business=retry case=backoff_scheduled runId={} task={} nextAttempt={} backoffMs={}",
                    run.getRunId(), task.getTaskId(), attempt + 1, backoff);
            scheduler.schedule(() -> retryDue(run.getRunId(), task.getTaskId(), attempt + 1),
                    backoff, TimeUnit.MILLISECONDS);
        } else {
            task.setStatus(TaskStatus.FAILED);
            task.setFinishedAt(finishedAt);
            run.appendEventLocked(EventType.TASK_FAILED, task.getTaskId(), attempt,
                    "attempts exhausted after " + attempt + " attempt(s)");
            log.warn("business=retry case=retries_exhausted runId={} task={} attempts={}",
                    run.getRunId(), task.getTaskId(), attempt);
            propagateAndScheduleLocked(run);
        }
    }

    private void retryDue(String runId, String taskId, int attempt) {
        RunState run = runs.get(runId);
        if (run == null) {
            return;
        }
        synchronized (run) {
            TaskState task = run.getTasks().get(taskId);
            if (run.getStatus() != RunStatus.RUNNING
                    || task.getStatus() != TaskStatus.RETRYING
                    || task.getAttemptCount() != attempt - 1) {
                log.info("business=cancel case=retry_skipped runId={} task={} attempt={} runStatus={}",
                        runId, taskId, attempt, run.getStatus());
                return;
            }
            launchAttemptLocked(run, task);
        }
    }

    private void propagateAndScheduleLocked(RunState run) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (TaskState task : run.getTasks().values()) {
                if (task.getStatus() != TaskStatus.PENDING) {
                    continue;
                }
                if (isReadyLocked(run, task)) {
                    launchAttemptLocked(run, task);
                    changed = true;
                    continue;
                }
                TaskStatus classification = classifyBlockedLocked(run, task);
                if (classification == TaskStatus.BLOCKED) {
                    task.setStatus(TaskStatus.BLOCKED);
                    task.setFinishedAt(Instant.now());
                    run.appendEventLocked(EventType.TASK_BLOCKED, task.getTaskId(), null,
                            "upstream task failed");
                    log.info("business=propagation case=blocked runId={} task={}",
                            run.getRunId(), task.getTaskId());
                    changed = true;
                } else if (classification == TaskStatus.SKIPPED) {
                    task.setStatus(TaskStatus.SKIPPED);
                    task.setFinishedAt(Instant.now());
                    run.appendEventLocked(EventType.TASK_SKIPPED, task.getTaskId(), null,
                            "upstream task blocked or skipped");
                    log.info("business=propagation case=skipped runId={} task={}",
                            run.getRunId(), task.getTaskId());
                    changed = true;
                }
            }
        }
        checkRunCompletionLocked(run);
    }

    private TaskStatus classifyBlockedLocked(RunState run, TaskState task) {
        boolean directlyFailed = false;
        boolean upstreamDead = false;
        for (String depId : task.getDef().dependencies()) {
            TaskStatus depStatus = run.getTasks().get(depId).getStatus();
            if (depStatus == TaskStatus.FAILED) {
                directlyFailed = true;
            }
            if (depStatus == TaskStatus.FAILED
                    || depStatus == TaskStatus.BLOCKED
                    || depStatus == TaskStatus.SKIPPED) {
                upstreamDead = true;
            }
            if (depStatus == TaskStatus.PENDING
                    || depStatus == TaskStatus.SCHEDULED
                    || depStatus == TaskStatus.RUNNING
                    || depStatus == TaskStatus.RETRYING) {
                return null;
            }
        }
        if (!upstreamDead) {
            return null;
        }
        return directlyFailed ? TaskStatus.BLOCKED : TaskStatus.SKIPPED;
    }

    private void checkRunCompletionLocked(RunState run) {
        if (run.getStatus().isTerminal()) {
            return;
        }
        for (TaskState task : run.getTasks().values()) {
            if (!task.getStatus().isTerminal()) {
                return;
            }
        }
        boolean allSucceeded = run.getTasks().values().stream()
                .allMatch(task -> task.getStatus() == TaskStatus.SUCCEEDED);
        if (allSucceeded) {
            run.appendEventLocked(EventType.RUN_SUCCEEDED, null, null, "all tasks succeeded");
            run.setStatus(RunStatus.SUCCEEDED);
            log.info("business=lifecycle case=run_succeeded runId={}", run.getRunId());
        } else {
            run.appendEventLocked(EventType.RUN_FAILED, null, null, "one or more tasks did not succeed");
            run.setStatus(RunStatus.FAILED);
            log.warn("business=lifecycle case=run_failed runId={}", run.getRunId());
        }
        run.setFinishedAt(Instant.now());
    }
}
