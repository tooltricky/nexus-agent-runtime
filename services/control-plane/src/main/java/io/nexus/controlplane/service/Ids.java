package io.nexus.controlplane.service;

import java.util.UUID;

final class Ids {
    private Ids() {
    }

    static String sessionId() {
        return "ses_" + UUID.randomUUID();
    }

    static String runId() {
        return "run_" + UUID.randomUUID();
    }

    static String taskId() {
        return "tsk_" + UUID.randomUUID();
    }

    static String eventId() {
        return "evt_" + UUID.randomUUID();
    }
}

