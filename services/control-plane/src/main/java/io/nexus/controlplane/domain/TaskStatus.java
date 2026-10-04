package io.nexus.controlplane.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum TaskStatus {
    PENDING,
    LEASED,
    EXECUTING,
    SUCCEEDED,
    FAILED;

    private static final Map<TaskStatus, Set<TaskStatus>> TRANSITIONS = Map.of(
            PENDING, EnumSet.of(LEASED),
            LEASED, EnumSet.of(PENDING, EXECUTING),
            EXECUTING, EnumSet.of(PENDING, SUCCEEDED, FAILED),
            SUCCEEDED, EnumSet.noneOf(TaskStatus.class),
            FAILED, EnumSet.noneOf(TaskStatus.class)
    );

    public void requireTransitionTo(TaskStatus next) {
        if (!TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateException("Illegal Task transition: " + this + " -> " + next);
        }
    }
}
