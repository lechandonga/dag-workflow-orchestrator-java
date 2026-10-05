package com.github.lechandonga.dagworkfloworchestratorjava.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.EventReplayer;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.error.ApiException;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunState;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowEvent;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/runs")
public class WorkflowController {

    private final WorkflowEngine engine;
    private final EventReplayer replayer;

    public WorkflowController(WorkflowEngine engine, EventReplayer replayer) {
        this.engine = engine;
        this.replayer = replayer;
    }

    @PostMapping
    @org.springframework.web.bind.annotation.ResponseStatus(HttpStatus.CREATED)
    public RunViews.RunView submit(@RequestBody SubmitRequest request) {
        if (request == null || request.workflow() == null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "MISSING_WORKFLOW",
                    "Request body must contain a 'workflow' definition");
        }
        return RunViews.of(engine.submit(request.clientId(), request.workflow()));
    }

    @GetMapping
    public List<RunViews.RunView> list() {
        return engine.listRuns().stream().map(RunViews::of).toList();
    }

    @GetMapping("/{runId}")
    public RunViews.RunView get(@PathVariable String runId) {
        return RunViews.of(engine.getRun(runId));
    }

    @PostMapping("/{runId}/cancel")
    public RunViews.RunView cancel(@PathVariable String runId) {
        return RunViews.of(engine.cancel(runId));
    }

    @GetMapping("/{runId}/events")
    public Map<String, Object> events(@PathVariable String runId) {
        RunState run = engine.getRun(runId);
        List<WorkflowEvent> events = run.getEvents();
        EventReplayer.ReplayResult replay = replayer.replay(events);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("runId", runId);
        body.put("liveStatus", run.getStatus().name());
        body.put("events", events);

        Map<String, Object> replayed = new LinkedHashMap<>();
        replayed.put("runStatus", replay.runStatus().name());
        Map<String, Map<String, Object>> tasks = new LinkedHashMap<>();
        replay.taskStatus().forEach((taskId, status) -> {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("status", status.name());
            entry.put("attempts", replay.taskAttempts().getOrDefault(taskId, 0));
            entry.put("alertRaised", replay.taskAlerts().getOrDefault(taskId, false));
            tasks.put(taskId, entry);
        });
        replayed.put("tasks", tasks);
        body.put("replayedFromEvents", replayed);
        return body;
    }
}
