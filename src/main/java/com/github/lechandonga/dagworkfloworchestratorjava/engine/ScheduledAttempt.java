package com.github.lechandonga.dagworkfloworchestratorjava.engine;

import java.util.concurrent.ScheduledFuture;

/** Cancellable handle to a timeout watchdog for one in-flight attempt. */
class ScheduledAttempt {
    private final ScheduledFuture<?> future;

    ScheduledAttempt(ScheduledFuture<?> future) {
        this.future = future;
    }

    void cancel() {
        future.cancel(false);
    }
}
