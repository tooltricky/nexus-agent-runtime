package io.nexus.controlplane.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum RunStatus {
    CREATED,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    private static final Map<RunStatus, Set<RunStatus>> TRANSITIONS = Map.of(
            CREATED, EnumSet.of(QUEUED, CANCELLED),
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(SUCCEEDED, FAILED, CANCELLED),
            SUCCEEDED, EnumSet.noneOf(RunStatus.class),
            FAILED, EnumSet.noneOf(RunStatus.class),
            CANCELLED, EnumSet.noneOf(RunStatus.class)
    );

    public void requireTransitionTo(RunStatus next) {
        if (!TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateException("Illegal Run transition: " + this + " -> " + next);
        }
    }
}

