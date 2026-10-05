package com.github.lechandonga.dagworkfloworchestratorjava.tests.support;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

/** Tiny polling helper used by async engine tests. */
public final class Await {

    private Await() {
    }

    public static void awaitUntil(BooleanSupplier condition, Duration timeout, String description) {
        Instant deadline = Instant.now().plus(timeout);
        AssertionError lastError = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (AssertionError error) {
                lastError = error;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for " + description, e);
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new AssertionError("Timed out after " + timeout.toMillis() + "ms waiting for " + description);
    }

    public static void awaitUntil(BooleanSupplier condition, String description) {
        awaitUntil(condition, Duration.ofSeconds(10), description);
    }
}
