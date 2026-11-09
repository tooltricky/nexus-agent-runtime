package io.nexus.controlplane.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum RunStatus {
    CREATED,
    QUEUED,
    RUNNING,
    PAUSED,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    TIMEOUT;

    private static final Map<RunStatus, Set<RunStatus>> TRANSITIONS = Map.of(
            CREATED, EnumSet.of(QUEUED, CANCELLED, TIMEOUT),
            QUEUED, EnumSet.of(RUNNING, PAUSED, CANCELLED, TIMEOUT),
            RUNNING, EnumSet.of(PAUSED, SUCCEEDED, FAILED, CANCELLED, TIMEOUT),
            PAUSED, EnumSet.of(QUEUED, RUNNING, FAILED, CANCELLED, TIMEOUT),
            SUCCEEDED, EnumSet.noneOf(RunStatus.class),
            FAILED, EnumSet.noneOf(RunStatus.class),
            CANCELLED, EnumSet.noneOf(RunStatus.class),
            TIMEOUT, EnumSet.noneOf(RunStatus.class)
    );

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED || this == TIMEOUT;
    }

    public void requireTransitionTo(RunStatus next) {
        if (!TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateException("Illegal Run transition: " + this + " -> " + next);
        }
    }
}

