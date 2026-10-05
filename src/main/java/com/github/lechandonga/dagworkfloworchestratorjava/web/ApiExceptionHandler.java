package com.github.lechandonga.dagworkfloworchestratorjava.web;

import com.github.lechandonga.dagworkfloworchestratorjava.exception.IllegalRunStateException;
import com.github.lechandonga.dagworkfloworchestratorjava.exception.RunNotFoundException;
import com.github.lechandonga.dagworkfloworchestratorjava.exception.WorkflowValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(WorkflowValidationException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(WorkflowValidationException ex) {
        return error(HttpStatus.BAD_REQUEST, ex.getReason(), ex.getMessage());
    }

    @ExceptionHandler(IllegalRunStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(IllegalRunStateException ex) {
        return error(HttpStatus.CONFLICT, "ILLEGAL_RUN_STATE", ex.getMessage());
    }

    @ExceptionHandler(RunNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(RunNotFoundException ex) {
        return error(HttpStatus.NOT_FOUND, "RUN_NOT_FOUND", ex.getMessage());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        return error(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST", "request body is missing or malformed");
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String reason, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("reason", reason);
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
