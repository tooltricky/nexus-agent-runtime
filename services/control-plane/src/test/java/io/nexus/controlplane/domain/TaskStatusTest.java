package io.nexus.controlplane.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskStatusTest {
    @Test
    void acceptsDeclaredLifecycle() {
        assertDoesNotThrow(() -> TaskStatus.PENDING.requireTransitionTo(TaskStatus.LEASED));
        assertDoesNotThrow(() -> TaskStatus.LEASED.requireTransitionTo(TaskStatus.EXECUTING));
        assertDoesNotThrow(() -> TaskStatus.EXECUTING.requireTransitionTo(TaskStatus.SUCCEEDED));
        assertDoesNotThrow(() -> TaskStatus.EXECUTING.requireTransitionTo(TaskStatus.FAILED));
    }

    @Test
    void rejectsSkippedAndTerminalTransitions() {
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.PENDING.requireTransitionTo(TaskStatus.EXECUTING));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.LEASED.requireTransitionTo(TaskStatus.SUCCEEDED));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.SUCCEEDED.requireTransitionTo(TaskStatus.EXECUTING));
        assertThrows(IllegalStateException.class,
                () -> TaskStatus.FAILED.requireTransitionTo(TaskStatus.LEASED));
    }
}
