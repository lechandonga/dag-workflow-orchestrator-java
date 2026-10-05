package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.BusinessLogging;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：HTTP 提交/查询/取消接口、可区分的校验错误、幂等提交、非法取消返回 409。
 */
@ExtendWith(BusinessLogging.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("HTTP 接口")
class WorkflowApiTests {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper mapper = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + "/api/workflows" + path;
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<Map> post(String path, Object body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForEntity(url(path), new HttpEntity<>(body, headers), Map.class);
    }

    private Map<String, Object> task(String id, List<String> deps, String outcome,
                                    long delay, int retries, long backoffMs,
                                    Long timeoutMs, Long alertAfterMs) {
        Map<String, Object> t = new java.util.LinkedHashMap<>();
        t.put("id", id);
        t.put("dependsOn", deps);
        t.put("behavior", Map.of("outcome", outcome, "delayMs", delay));
        t.put("retries", retries);
        t.put("backoffMs", backoffMs);
        if (timeoutMs != null) {
            t.put("timeoutMs", timeoutMs);
        }
        if (alertAfterMs != null) {
            t.put("alertAfterMs", alertAfterMs);
        }
        return t;
    }

    @Test
    @DisplayName("提交 -> 查询 -> 事件流 -> 回放 全链路成功")
    void submitQueryEventsAndReplay() {
        Map<String, Object> body = Map.of("submissionId", "api-1",
                "tasks", List.of(
                        task("a", List.of(), "SUCCESS", 0, 0, 0, null, null),
                        task("b", List.of("a"), "SUCCESS", 0, 0, 0, null, null)));

        ResponseEntity<Map> created = post("/runs", body);
        assertEquals(HttpStatus.OK, created.getStatusCode());
        String runId = (String) created.getBody().get("runId");
        assertNotNull(runId);

        JsonNode view = awaitTerminal(runId);
        assertEquals("SUCCEEDED", view.path("status").asText());

        ResponseEntity<List> events = rest.getForEntity(url("/runs/" + runId + "/events"), List.class);
        assertEquals(HttpStatus.OK, events.getStatusCode());
        assertNotNull(events.getBody());
        assertTrue(events.getBody().size() >= 5);

        ResponseEntity<Map> replay = rest.getForEntity(url("/runs/" + runId + "/replay"), Map.class);
        assertEquals(HttpStatus.OK, replay.getStatusCode());
        assertEquals("SUCCEEDED", replay.getBody().get("runStatus"));
    }

    @Test
    @DisplayName("同一提交标识重复提交返回同一个 runId，状态不重置")
    void duplicateSubmissionIsIdempotent() {
        Map<String, Object> body = Map.of("submissionId", "api-2",
                "tasks", List.of(task("a", List.of(), "SUCCESS", 100, 0, 0, null, null)));

        String first = (String) post("/runs", body).getBody().get("runId");
        String second = (String) post("/runs", body).getBody().get("runId");
        assertEquals(first, second);
        awaitTerminal(first);
        Map<String, Object> third = post("/runs", body).getBody();
        assertEquals(first, third.get("runId"));
        assertEquals("SUCCEEDED", third.get("status"));
    }

    @Test
    @DisplayName("空图/重复标识/未知依赖/成环返回可区分的 400 reason")
    void validationErrorsAreDistinguishable() throws Exception {
        assertReason("EMPTY_GRAPH", Map.of("submissionId", "api-3", "tasks", List.of()));

        JsonNode dup = mapper.valueToTree(Map.of("submissionId", "api-4", "tasks", List.of(
                task("a", List.of(), "SUCCESS", 0, 0, 0, null, null),
                task("a", List.of(), "SUCCESS", 0, 0, 0, null, null))));
        assertReasonRaw("DUPLICATE_TASK_ID", dup.toString());

        assertReason("UNKNOWN_DEPENDENCY", Map.of("submissionId", "api-5",
                "tasks", List.of(task("a", List.of("ghost"), "SUCCESS", 0, 0, 0, null, null))));

        JsonNode cycle = mapper.valueToTree(Map.of("submissionId", "api-6", "tasks", List.of(
                task("a", List.of("b"), "SUCCESS", 0, 0, 0, null, null),
                task("b", List.of("a"), "SUCCESS", 0, 0, 0, null, null))));
        assertReasonRaw("CYCLIC_DEPENDENCY", cycle.toString());
    }

    @Test
    @DisplayName("取消已结束运行返回 409 并说明原因；查询不存在运行返回 404")
    void illegalCancelAndMissingRun() {
        Map<String, Object> body = Map.of("submissionId", "api-7",
                "tasks", List.of(task("a", List.of(), "SUCCESS", 0, 0, 0, null, null)));
        String runId = (String) post("/runs", body).getBody().get("runId");
        awaitTerminal(runId);

        ResponseEntity<Map> cancelAgain = post("/runs/" + runId + "/cancel", Map.of());
        assertEquals(HttpStatus.CONFLICT, cancelAgain.getStatusCode());
        assertEquals("ILLEGAL_RUN_STATE", cancelAgain.getBody().get("reason"));
        assertTrue(String.valueOf(cancelAgain.getBody().get("message")).contains("already ended"));

        ResponseEntity<Map> missing = rest.getForEntity(url("/runs/no-such-run"), Map.class);
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
        assertEquals("RUN_NOT_FOUND", missing.getBody().get("reason"));
    }

    private void assertReason(String reason, Object body) {
        ResponseEntity<Map> response = post("/runs", body);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(reason, response.getBody().get("reason"));
    }

    private void assertReasonRaw(String reason, String rawJson) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<Map> response = rest.postForEntity(url("/runs"),
                new HttpEntity<>(rawJson, headers), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(reason, response.getBody().get("reason"));
    }

    private JsonNode awaitTerminal(String runId) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                JsonNode node = rest.getForObject(url("/runs/" + runId), JsonNode.class);
                String status = node.path("status").asText();
                if (List.of("SUCCEEDED", "FAILED", "CANCELLED").contains(status)) {
                    return node;
                }
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("run did not finish in time: " + runId);
    }
}
