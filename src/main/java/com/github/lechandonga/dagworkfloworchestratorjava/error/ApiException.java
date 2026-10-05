package com.github.lechandonga.dagworkfloworchestratorjava.error;

import org.springframework.http.HttpStatus;

/** Base class for errors that map to a specific HTTP status + error code. */
public class ApiException extends RuntimeException {
    private final String errorCode;
    private final HttpStatus status;

    public ApiException(HttpStatus status, String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.status = status;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
