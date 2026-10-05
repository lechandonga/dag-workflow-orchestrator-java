package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.lechandonga.dagworkfloworchestratorjava.error.ApiException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/** Validates a submitted workflow definition before any run is created. */
@Component
public class WorkflowValidator {

    private static final Set<String> VALID_OUTCOMES = Set.of(
            TaskBehavior.SUCCESS, TaskBehavior.FAIL, TaskBehavior.TIMEOUT);

    public void validate(WorkflowDef def) {
        if (def == null || def.tasks() == null || def.tasks().isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "EMPTY_GRAPH",
                    "Workflow must contain at least one task");
        }

        Set<String> seen = new HashSet<>();
        for (TaskDef task : def.tasks()) {
            if (task == null || task.id() == null || task.id().isBlank()) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_TASK",
                        "Every task requires a non-blank id");
            }
            if (!seen.add(task.id())) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "DUPLICATE_TASK_ID",
                        "Duplicate task id: " + task.id());
            }
        }

        for (TaskDef task : def.tasks()) {
            if (task.dependencies() != null) {
                for (String dep : task.dependencies()) {
                    if (dep == null || dep.isBlank()) {
                        throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_DEPENDENCY",
                                "Task " + task.id() + " has a blank dependency");
                    }
                    if (!seen.contains(dep)) {
                        throw new ApiException(HttpStatus.BAD_REQUEST, "UNKNOWN_DEPENDENCY",
                                "Task " + task.id() + " depends on unknown task: " + dep);
                    }
                    if (dep.equals(task.id())) {
                        throw new ApiException(HttpStatus.BAD_REQUEST, "CYCLIC_DEPENDENCY",
                                "Task " + task.id() + " depends on itself");
                    }
                }
            }
            validateBehavior(task);
        }

        detectCycle(def, seen);
    }

    private void validateBehavior(TaskDef task) {
        TaskBehavior behavior = task.behavior();
        if (behavior == null) {
            return;
        }
        List<String> outcomes = behavior.outcomes();
        if (outcomes != null) {
            for (String outcome : outcomes) {
                if (!VALID_OUTCOMES.contains(outcome)) {
                    throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_BEHAVIOR",
                            "Task " + task.id() + " has unknown outcome: " + outcome
                                    + " (allowed: SUCCESS, FAIL, TIMEOUT)");
                }
            }
        }
        if (behavior.durationMillis() != null && behavior.durationMillis() < 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_BEHAVIOR",
                    "Task " + task.id() + " durationMillis must be >= 0");
        }
        if (behavior.timeoutMillis() != null && behavior.timeoutMillis() < 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_BEHAVIOR",
                    "Task " + task.id() + " timeoutMillis must be >= 0");
        }
        if (behavior.maxRetries() != null && behavior.maxRetries() < 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_BEHAVIOR",
                    "Task " + task.id() + " maxRetries must be >= 0");
        }
        if (behavior.backoffMillis() != null && behavior.backoffMillis() < 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_BEHAVIOR",
                    "Task " + task.id() + " backoffMillis must be >= 0");
        }
    }

    /** DFS cycle detection with WHITE/GRAY/BLACK coloring. */
    private void detectCycle(WorkflowDef def, Set<String> ids) {
        Map<String, List<String>> edges = new HashMap<>();
        for (TaskDef task : def.tasks()) {
            edges.put(task.id(),
                    task.dependencies() == null ? List.of() : List.copyOf(task.dependencies()));
        }
        Set<String> white = new HashSet<>(ids);
        Set<String> gray = new HashSet<>();
        Set<String> black = new HashSet<>();

        for (String start : ids) {
            if (!white.contains(start)) {
                continue;
            }
            Deque<Frame> dfs = new ArrayDeque<>();
            dfs.push(new Frame(start, edges.get(start).iterator()));
            white.remove(start);
            gray.add(start);
            while (!dfs.isEmpty()) {
                Frame frame = dfs.peek();
                if (!frame.deps.hasNext()) {
                    gray.remove(frame.node);
                    black.add(frame.node);
                    dfs.pop();
                    continue;
                }
                String next = frame.deps.next();
                if (gray.contains(next)) {
                    throw new ApiException(HttpStatus.BAD_REQUEST, "CYCLIC_DEPENDENCY",
                            "Dependency cycle detected involving task: " + next);
                }
                if (black.contains(next)) {
                    continue;
                }
                white.remove(next);
                gray.add(next);
                dfs.push(new Frame(next, edges.get(next).iterator()));
            }
        }
    }

    private record Frame(String node, java.util.Iterator<String> deps) {
    }
}
