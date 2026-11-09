package io.nexus.controlplane.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum TaskStatus {
    PENDING,
    LEASED,
    EXECUTING,
    RETRYING,
    PAUSED,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    TIMEOUT;

    private static final Map<TaskStatus, Set<TaskStatus>> TRANSITIONS = Map.of(
            PENDING, EnumSet.of(LEASED, PAUSED, CANCELLED, TIMEOUT),
            LEASED, EnumSet.of(EXECUTING, RETRYING, PAUSED, FAILED, CANCELLED, TIMEOUT),
            EXECUTING, EnumSet.of(RETRYING, PAUSED, SUCCEEDED, FAILED, CANCELLED, TIMEOUT),
            RETRYING, EnumSet.of(LEASED, PAUSED, CANCELLED, TIMEOUT),
            PAUSED, EnumSet.of(PENDING, RETRYING, FAILED, CANCELLED, TIMEOUT),
            SUCCEEDED, EnumSet.noneOf(TaskStatus.class),
            FAILED, EnumSet.noneOf(TaskStatus.class),
            CANCELLED, EnumSet.noneOf(TaskStatus.class),
            TIMEOUT, EnumSet.noneOf(TaskStatus.class)
    );

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED || this == TIMEOUT;
    }

    public void requireTransitionTo(TaskStatus next) {
        if (!TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateException("Illegal Task transition: " + this + " -> " + next);
        }
    }
}
