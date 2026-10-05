package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import java.util.List;
import java.util.Map;

import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.Await;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：HTTP 接口端到端。
 * 覆盖用例：提交/查询/取消/事件流接口、图校验错误码、终态取消 409、
 * clientId 幂等、事件流回放结果与实时状态一致。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("业务-HTTP接口端到端")
class HttpApiIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + "/api/runs" + path;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> simpleWorkflow() {
        Map<String, Object> behavior = Map.of(
                "outcomes", List.of("SUCCESS"),
                "durationMillis", 0);
        Map<String, Object> task = Map.of("id", "a", "dependencies", List.of(),
                "behavior", behavior);
        return Map.of("name", "http-smoke", "tasks", List.of(task));
    }

    @Test
    @DisplayName("用例: 提交 -> 查询 -> 事件流回放状态一致")
    void submitQueryAndReplay() {
        ResponseEntity<Map> response = rest.postForEntity(url(""),
                Map.of("clientId", "http-1", "workflow", simpleWorkflow()), Map.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        String runId = (String) response.getBody().get("runId");
        assertNotNull(runId);

        Await.awaitUntil(() -> {
            Map<String, Object> view = rest.getForObject(url("/" + runId), Map.class);
            Object status = view.get("status");
            return "SUCCEEDED".equals(status) || "FAILED".equals(status)
                    || "CANCELLED".equals(status);
        }, "run terminal over HTTP");

        Map<String, Object> eventsBody = rest.getForObject(url("/" + runId + "/events"), Map.class);
        assertEquals("SUCCEEDED", eventsBody.get("liveStatus"));
        Map<String, Object> replayed = (Map<String, Object>) eventsBody.get("replayedFromEvents");
        assertEquals("SUCCEEDED", replayed.get("runStatus"));
        List<?> events = (List<?>) eventsBody.get("events");
        assertTrue(events.size() >= 4, "expected submitted/scheduled/started/succeeded events");
    }

    @Test
    @DisplayName("用例: 空图提交 -> 400 EMPTY_GRAPH")
    void emptyGraphRejected() {
        ResponseEntity<Map> response = rest.postForEntity(url(""),
                Map.of("workflow", Map.of("name", "empty", "tasks", List.of())), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("EMPTY_GRAPH", response.getBody().get("errorCode"));
    }

    @Test
    @DisplayName("用例: 成环提交 -> 400 CYCLIC_DEPENDENCY")
    void cyclicGraphRejected() {
        Map<String, Object> taskA = Map.of("id", "a", "dependencies", List.of("b"));
        Map<String, Object> taskB = Map.of("id", "b", "dependencies", List.of("a"));
        ResponseEntity<Map> response = rest.postForEntity(url(""),
                Map.of("workflow", Map.of("name", "cyc", "tasks", List.of(taskA, taskB))),
                Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("CYCLIC_DEPENDENCY", response.getBody().get("errorCode"));
    }

    @Test
    @DisplayName("用例: 缺少 workflow 字段 -> 400 MISSING_WORKFLOW")
    void missingWorkflow() {
        ResponseEntity<Map> response = rest.postForEntity(url(""),
                Map.of("clientId", "x"), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("MISSING_WORKFLOW", response.getBody().get("errorCode"));
    }

    @Test
    @DisplayName("用例: 查询不存在的运行 -> 404 RUN_NOT_FOUND")
    void unknownRun() {
        ResponseEntity<Map> response = rest.getForEntity(url("/missing"), Map.class);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals("RUN_NOT_FOUND", response.getBody().get("errorCode"));
    }

    @Test
    @DisplayName("用例: 重复 clientId 提交 -> 返回同一个 runId 且状态不重置")
    void idempotentOverHttp() {
        Map<String, Object> body = Map.of("clientId", "http-idem", "workflow", simpleWorkflow());
        ResponseEntity<Map> first = rest.postForEntity(url(""), body, Map.class);
        String firstRunId = (String) first.getBody().get("runId");
        Await.awaitUntil(() -> "SUCCEEDED"
                .equals(rest.getForObject(url("/" + firstRunId), Map.class).get("status")),
                "first run succeeds");

        ResponseEntity<Map> second = rest.postForEntity(url(""), body, Map.class);
        assertEquals(firstRunId, second.getBody().get("runId"));
        assertEquals("SUCCEEDED", second.getBody().get("status"));
    }

    @Test
    @DisplayName("用例: 终态运行取消 -> 409 RUN_ALREADY_TERMINAL")
    void cancelTerminalRun() {
        ResponseEntity<Map> created = rest.postForEntity(url(""),
                Map.of("workflow", simpleWorkflow()), Map.class);
        String runId = (String) created.getBody().get("runId");
        Await.awaitUntil(() -> "SUCCEEDED"
                .equals(rest.getForObject(url("/" + runId), Map.class).get("status")),
                "run succeeds");

        ResponseEntity<Map> cancel = rest.postForEntity(url("/" + runId + "/cancel"),
                null, Map.class);
        assertEquals(HttpStatus.CONFLICT, cancel.getStatusCode());
        assertEquals("RUN_ALREADY_TERMINAL", cancel.getBody().get("errorCode"));
    }
}
