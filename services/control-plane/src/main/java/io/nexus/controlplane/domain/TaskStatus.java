package io.nexus.controlplane.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum TaskStatus {
    PENDING,
    LEASED,
    EXECUTING,
    RETRYING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    private static final Map<TaskStatus, Set<TaskStatus>> TRANSITIONS = Map.of(
            PENDING, EnumSet.of(LEASED, CANCELLED),
            LEASED, EnumSet.of(EXECUTING, RETRYING, FAILED, CANCELLED),
            EXECUTING, EnumSet.of(RETRYING, SUCCEEDED, FAILED, CANCELLED),
            RETRYING, EnumSet.of(LEASED, CANCELLED),
            SUCCEEDED, EnumSet.noneOf(TaskStatus.class),
            FAILED, EnumSet.noneOf(TaskStatus.class),
            CANCELLED, EnumSet.noneOf(TaskStatus.class)
    );

    public void requireTransitionTo(TaskStatus next) {
        if (!TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateException("Illegal Task transition: " + this + " -> " + next);
        }
    }
}
