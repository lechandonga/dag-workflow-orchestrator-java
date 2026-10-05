package com.github.lechandonga.dagworkfloworchestratorjava.exception;

/**
 * Thrown for illegal lifecycle operations, e.g. cancelling a run that already ended.
 */
public class IllegalRunStateException extends RuntimeException {
    public IllegalRunStateException(String message) {
        super(message);
    }
}
