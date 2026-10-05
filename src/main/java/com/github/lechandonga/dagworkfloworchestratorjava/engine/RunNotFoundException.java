package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import com.github.lechandonga.dagworkfloworchestratorjava.error.ApiException;
import org.springframework.http.HttpStatus;

public class RunNotFoundException extends ApiException {
    public RunNotFoundException(String runId) {
        super(HttpStatus.NOT_FOUND, "RUN_NOT_FOUND", "No run exists with id: " + runId);
    }
}
