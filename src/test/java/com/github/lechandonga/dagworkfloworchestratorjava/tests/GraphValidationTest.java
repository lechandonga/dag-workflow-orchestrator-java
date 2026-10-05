package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.exception.WorkflowValidationException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.BusinessLogging;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.WorkflowFixtures;
import com.github.lechandonga.dagworkfloworchestratorjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 业务：提交前任务图校验。
 */
@ExtendWith(BusinessLogging.class)
@DisplayName("任务图校验")
class GraphValidationTest {

    private final WorkflowValidator validator = new WorkflowValidator();

    private void assertReason(String reason, WorkflowDef def) {
        WorkflowValidationException ex =
                assertThrows(WorkflowValidationException.class, () -> validator.validate(def));
        assertEquals(reason, ex.getReason(),
                "应当返回可区分的错误原因, 实际: " + ex.getMessage());
    }

    @Test
    @DisplayName("空图被拒绝 (EMPTY_GRAPH)")
    void emptyGraph() {
        assertReason(WorkflowValidationException.EMPTY_GRAPH, new WorkflowDef("s1", List.of()));
    }

    @Test
    @DisplayName("缺少 submissionId 被拒绝 (MISSING_SUBMISSION_ID)")
    void missingSubmissionId() {
        TaskDef task = WorkflowFixtures.task("a", TaskBehavior.success(0));
        assertReason(WorkflowValidationException.MISSING_SUBMISSION_ID,
                new WorkflowDef("  ", List.of(task)));
    }

    @Test
    @DisplayName("重复任务标识被拒绝 (DUPLICATE_TASK_ID)")
    void duplicateTaskId() {
        TaskDef a1 = WorkflowFixtures.task("a", TaskBehavior.success(0));
        TaskDef a2 = WorkflowFixtures.task("a", TaskBehavior.success(0));
        assertReason(WorkflowValidationException.DUPLICATE_TASK_ID,
                new WorkflowDef("s2", List.of(a1, a2)));
    }

    @Test
    @DisplayName("未知依赖被拒绝 (UNKNOWN_DEPENDENCY)")
    void unknownDependency() {
        TaskDef a = WorkflowFixtures.task("a", TaskBehavior.success(0), "ghost");
        assertReason(WorkflowValidationException.UNKNOWN_DEPENDENCY,
                new WorkflowDef("s3", List.of(a)));
    }

    @Test
    @DisplayName("自依赖被拒绝 (SELF_DEPENDENCY)")
    void selfDependency() {
        TaskDef a = WorkflowFixtures.task("a", TaskBehavior.success(0), "a");
        assertReason(WorkflowValidationException.SELF_DEPENDENCY,
                new WorkflowDef("s4", List.of(a)));
    }

    @Test
    @DisplayName("依赖成环被拒绝 (CYCLIC_DEPENDENCY)")
    void cyclicDependency() {
        TaskDef a = WorkflowFixtures.task("a", TaskBehavior.fail(0, "x"), "c");
        TaskDef b = WorkflowFixtures.task("b", TaskBehavior.success(0), "a");
        TaskDef c = WorkflowFixtures.task("c", TaskBehavior.success(0), "b");
        assertReason(WorkflowValidationException.CYCLIC_DEPENDENCY,
                new WorkflowDef("s5", List.of(a, b, c)));
    }

    @Test
    @DisplayName("合法 DAG 校验通过")
    void validDag() {
        TaskDef a = WorkflowFixtures.task("a", TaskBehavior.success(0));
        TaskDef b = WorkflowFixtures.task("b", TaskBehavior.success(0), "a");
        validator.validate(new WorkflowDef("s6", List.of(a, b)));
    }
}
