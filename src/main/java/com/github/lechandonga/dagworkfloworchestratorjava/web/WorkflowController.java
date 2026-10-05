package com.github.lechandonga.dagworkfloworchestratorjava.web;

import com.github.lechandonga.dagworkfloworchestratorjava.engine.EventReplayer;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.RunViews;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowEngine;
import com.github.lechandonga.dagworkfloworchestratorjava.engine.WorkflowRun;
import com.github.lechandonga.dagworkfloworchestratorjava.model.RunView;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowDef;
import com.github.lechandonga.dagworkfloworchestratorjava.model.WorkflowEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowEngine engine;
    private final EventReplayer replayer;

    public WorkflowController(WorkflowEngine engine, EventReplayer replayer) {
        this.engine = engine;
        this.replayer = replayer;
    }

    @PostMapping("/runs")
    public RunView submit(@RequestBody WorkflowDef def) {
        return RunViews.snapshot(engine.submit(def));
    }

    @GetMapping("/runs/{runId}")
    public RunView get(@PathVariable String runId) {
        return RunViews.snapshot(engine.getRun(runId));
    }

    @PostMapping("/runs/{runId}/cancel")
    public RunView cancel(@PathVariable String runId) {
        return RunViews.snapshot(engine.cancel(runId));
    }

    @GetMapping("/runs/{runId}/events")
    public List<WorkflowEvent> events(@PathVariable String runId) {
        return engine.getRun(runId).snapshotEvents();
    }

    /**
     * Replays the event stream into run/task final states, proving the stream is
     * sufficient to reconstruct the run.
     */
    @GetMapping("/runs/{runId}/replay")
    public Map<String, Object> replay(@PathVariable String runId) {
        WorkflowRun run = engine.getRun(runId);
        List<WorkflowEvent> events = run.snapshotEvents();
        EventReplayer.ReplayedRun replayed = replayer.replay(events);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("runId", runId);
        body.put("runStatus", replayed.runStatus());
        body.put("taskStates", replayed.taskStates());
        body.put("taskAttempts", replayed.taskAttempts());
        body.put("taskAlerts", replayed.taskAlerts());
        return body;
    }

    @GetMapping("/runs")
    public List<RunView> list() {
        return engine.allRuns().stream().map(RunViews::snapshot).toList();
    }
}
