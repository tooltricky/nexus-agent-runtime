package io.nexus.controlplane.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunStatusTest {
    @Test
    void acceptsDeclaredLifecycle() {
        assertDoesNotThrow(() -> RunStatus.CREATED.requireTransitionTo(RunStatus.QUEUED));
        assertDoesNotThrow(() -> RunStatus.QUEUED.requireTransitionTo(RunStatus.RUNNING));
        assertDoesNotThrow(() -> RunStatus.RUNNING.requireTransitionTo(RunStatus.SUCCEEDED));
        assertDoesNotThrow(() -> RunStatus.RUNNING.requireTransitionTo(RunStatus.FAILED));
    }

    @Test
    void rejectsSkippedAndTerminalTransitions() {
        assertThrows(IllegalStateException.class,
                () -> RunStatus.CREATED.requireTransitionTo(RunStatus.RUNNING));
        assertThrows(IllegalStateException.class,
                () -> RunStatus.QUEUED.requireTransitionTo(RunStatus.SUCCEEDED));
        assertThrows(IllegalStateException.class,
                () -> RunStatus.SUCCEEDED.requireTransitionTo(RunStatus.RUNNING));
        assertThrows(IllegalStateException.class,
                () -> RunStatus.FAILED.requireTransitionTo(RunStatus.QUEUED));
    }

    @Test
    void cancellationIsAllowedFromEveryUnfinishedState() {
        for (RunStatus status : java.util.List.of(RunStatus.CREATED, RunStatus.QUEUED, RunStatus.RUNNING)) {
            assertDoesNotThrow(() -> status.requireTransitionTo(RunStatus.CANCELLED), status.name());
        }
    }

    @Test
    void everyTerminalStateRejectsAllFurtherTransitionsIncludingItself() {
        for (RunStatus terminal : java.util.List.of(
                RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)) {
            for (RunStatus next : RunStatus.values()) {
                assertThrows(IllegalStateException.class, () -> terminal.requireTransitionTo(next),
                        terminal + " -> " + next);
            }
        }
    }
}
