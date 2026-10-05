package com.github.lechandonga.dagworkfloworchestratorjava.validation;

import com.github.lechandonga.dagworkfloworchestratorjava.exception.WorkflowValidationException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates a workflow definition before a run is created:
 * non-empty graph, unique task ids, known dependencies, no self edge, acyclic.
 */
@Component
public class WorkflowValidator {

    public void validate(WorkflowDef def) {
        if (def == null || def.submissionId() == null || def.submissionId().isBlank()) {
            throw new WorkflowValidationException(
                    WorkflowValidationException.MISSING_SUBMISSION_ID,
                    "submissionId must not be blank");
        }
        List<TaskDef> tasks = def.tasks();
        if (tasks == null || tasks.isEmpty()) {
            throw new WorkflowValidationException(
                    WorkflowValidationException.EMPTY_GRAPH,
                    "workflow must contain at least one task");
        }

        Map<String, TaskDef> byId = new LinkedHashMap<>();
        for (TaskDef task : tasks) {
            if (task == null || task.id() == null || task.id().isBlank()) {
                throw new WorkflowValidationException(
                        WorkflowValidationException.MISSING_TASK_ID,
                        "every task must declare a non-blank id");
            }
            if (byId.put(task.id(), task) != null) {
                throw new WorkflowValidationException(
                        WorkflowValidationException.DUPLICATE_TASK_ID,
                        "duplicated task id: " + task.id());
            }
        }

        for (TaskDef task : tasks) {
            List<String> deps = task.dependsOn();
            if (deps == null) {
                continue;
            }
            for (String dep : deps) {
                if (task.id().equals(dep)) {
                    throw new WorkflowValidationException(
                            WorkflowValidationException.SELF_DEPENDENCY,
                            "task " + task.id() + " depends on itself");
                }
                if (!byId.containsKey(dep)) {
                    throw new WorkflowValidationException(
                            WorkflowValidationException.UNKNOWN_DEPENDENCY,
                            "task " + task.id() + " depends on unknown task: " + dep);
                }
            }
        }

        detectCycle(byId);
    }

    private void detectCycle(Map<String, TaskDef> byId) {
        // Iterative Kahn-style DFS coloring (0=white,1=gray,2=black) to avoid recursion depth issues.
        Map<String, Integer> color = new LinkedHashMap<>();
        byId.keySet().forEach(id -> color.put(id, 0));

        for (String root : byId.keySet()) {
            if (color.get(root) != 0) {
                continue;
            }
            Deque<String> stack = new ArrayDeque<>();
            Deque<Integer> edgeIndex = new ArrayDeque<>();
            stack.push(root);
            edgeIndex.push(0);
            color.put(root, 1);

            while (!stack.isEmpty()) {
                String node = stack.peek();
                int idx = edgeIndex.peek();
                List<String> deps = byId.get(node).dependsOn();
                if (deps != null && idx < deps.size()) {
                    String next = deps.get(idx);
                    edgeIndex.pop();
                    edgeIndex.push(idx + 1);
                    if (color.get(next) == 1) {
                        throw new WorkflowValidationException(
                                WorkflowValidationException.CYCLIC_DEPENDENCY,
                                "dependency cycle detected at task: " + next);
                    }
                    if (color.get(next) == 0) {
                        color.put(next, 1);
                        stack.push(next);
                        edgeIndex.push(0);
                    }
                } else {
                    color.put(node, 2);
                    stack.pop();
                    edgeIndex.pop();
                }
            }
        }
    }
}
