package io.nexus.controlplane.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskStatusTest {
    @Test
    void acceptsDeclaredLifecycle() {
        assertDoesNotThrow(() -> TaskStatus.PENDING.requireTransitionTo(TaskStatus.LEASED));
        assertDoesNotThrow(() -> TaskStatus.LEASED.requireTransitionTo(TaskStatus.EXECUTING));
        assertDoesNotThrow(() -> TaskStatus.LEASED.requireTransitionTo(TaskStatus.RETRYING));
        assertDoesNotThrow(() -> TaskStatus.LEASED.requireTransitionTo(TaskStatus.FAILED));
        assertDoesNotThrow(() -> TaskStatus.EXECUTING.requireTransitionTo(TaskStatus.SUCCEEDED));
        assertDoesNotThrow(() -> TaskStatus.EXECUTING.requireTransitionTo(TaskStatus.FAILED));
        assertDoesNotThrow(() -> TaskStatus.EXECUTING.requireTransitionTo(TaskStatus.RETRYING));
        assertDoesNotThrow(() -> TaskStatus.RETRYING.requireTransitionTo(TaskStatus.LEASED));
    }

    @Test
    void rejectsSkippedAndTerminalTransitions() {
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.PENDING.requireTransitionTo(TaskStatus.EXECUTING));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.LEASED.requireTransitionTo(TaskStatus.SUCCEEDED));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.EXECUTING.requireTransitionTo(TaskStatus.PENDING));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.RETRYING.requireTransitionTo(TaskStatus.SUCCEEDED));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.SUCCEEDED.requireTransitionTo(TaskStatus.EXECUTING));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.FAILED.requireTransitionTo(TaskStatus.LEASED));
    }

    @Test
    void cancellationIsAllowedFromEveryUnfinishedState() {
        for (TaskStatus status : java.util.List.of(TaskStatus.PENDING, TaskStatus.LEASED, TaskStatus.EXECUTING, TaskStatus.RETRYING)) {
            assertDoesNotThrow(() -> status.requireTransitionTo(TaskStatus.CANCELLED), status.name());
        }
    }

    @Test
    void everyTerminalStateRejectsAllFurtherTransitionsIncludingItself() {
        for (TaskStatus terminal : java.util.List.of(
                TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED)) {
            for (TaskStatus next : TaskStatus.values()) {
                assertThrows(IllegalStateException.class, () -> terminal.requireTransitionTo(next),
                        terminal + " -> " + next);
            }
        }
    }
}
