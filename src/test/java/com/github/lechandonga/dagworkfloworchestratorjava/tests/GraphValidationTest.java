package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import java.util.List;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowValidator;
import com.github.lechandonga.dagworkfloworchestratorjava.error.ApiException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskBehavior;
import com.github.lechandonga.dagworkfloworchestratorjava.model.TaskDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 业务：任务图校验。
 * 覆盖用例：空图、缺少任务标识、重复任务标识、未知依赖、依赖成环、非法行为参数，以及合法 DAG 通过校验。
 */
@DisplayName("业务-任务图校验")
class GraphValidationTest {

    private WorkflowValidator validator;

    @BeforeEach
    void setUp() {
        validator = new WorkflowValidator();
    }

    private TaskDef task(String id, List<String> deps) {
        return new TaskDef(id, deps, new TaskBehavior(null, null, null, null, null, null));
    }

    private ApiException reject(WorkflowDef def) {
        return assertThrows(ApiException.class, () -> validator.validate(def));
    }

    @Test
    @DisplayName("用例: 空图(null) -> EMPTY_GRAPH")
    void emptyGraphNull() {
        ApiException error = reject(new WorkflowDef("empty", null));
        assertEquals("EMPTY_GRAPH", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 空任务列表 -> EMPTY_GRAPH")
    void emptyGraphList() {
        ApiException error = reject(new WorkflowDef("empty", List.of()));
        assertEquals("EMPTY_GRAPH", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 任务标识为空 -> INVALID_TASK")
    void blankTaskId() {
        ApiException error = reject(new WorkflowDef("w", List.of(task("  ", List.of()))));
        assertEquals("INVALID_TASK", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 重复任务标识 -> DUPLICATE_TASK_ID")
    void duplicateTaskId() {
        ApiException error = reject(new WorkflowDef("w",
                List.of(task("a", List.of()), task("a", List.of()))));
        assertEquals("DUPLICATE_TASK_ID", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 指向不存在任务的依赖 -> UNKNOWN_DEPENDENCY")
    void unknownDependency() {
        ApiException error = reject(new WorkflowDef("w",
                List.of(task("a", List.of("ghost")))));
        assertEquals("UNKNOWN_DEPENDENCY", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 任务依赖自己 -> CYCLIC_DEPENDENCY")
    void selfDependency() {
        ApiException error = reject(new WorkflowDef("w",
                List.of(task("a", List.of("a")))));
        assertEquals("CYCLIC_DEPENDENCY", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 两节点环 a->b->a -> CYCLIC_DEPENDENCY")
    void twoNodeCycle() {
        ApiException error = reject(new WorkflowDef("w",
                List.of(task("a", List.of("b")), task("b", List.of("a")))));
        assertEquals("CYCLIC_DEPENDENCY", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 三节点环 a->b->c->a -> CYCLIC_DEPENDENCY")
    void threeNodeCycle() {
        ApiException error = reject(new WorkflowDef("w",
                List.of(task("a", List.of("c")), task("b", List.of("a")), task("c", List.of("b")))));
        assertEquals("CYCLIC_DEPENDENCY", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 未知 outcome -> INVALID_BEHAVIOR")
    void invalidOutcome() {
        TaskDef bad = new TaskDef("a", List.of(),
                new TaskBehavior(List.of("EXPLODE"), 0L, null, null, null, null));
        ApiException error = reject(new WorkflowDef("w", List.of(bad)));
        assertEquals("INVALID_BEHAVIOR", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 负重试次数 -> INVALID_BEHAVIOR")
    void negativeRetries() {
        TaskDef bad = new TaskDef("a", List.of(),
                new TaskBehavior(null, 0L, null, null, -1, 0L));
        ApiException error = reject(new WorkflowDef("w", List.of(bad)));
        assertEquals("INVALID_BEHAVIOR", error.getErrorCode());
    }

    @Test
    @DisplayName("用例: 合法的菱形 DAG -> 校验通过")
    void validDiamondDag() {
        WorkflowDef def = new WorkflowDef("diamond", List.of(
                task("a", List.of()),
                task("b", List.of("a")),
                task("c", List.of("a")),
                task("d", List.of("b", "c"))));
        assertDoesNotThrow(() -> validator.validate(def));
    }
}
