package io.nexus.controlplane.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "nexus.watchdog.enabled", havingValue = "true", matchIfMissing = true)
public class LeaseWatchdogService {
    private final TaskFailureService failures;
    private final int batchSize;

    public LeaseWatchdogService(
            TaskFailureService failures,
            @Value("${nexus.worker.watchdog-batch-size:50}") int batchSize
    ) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Watchdog batch size must be positive");
        }
        this.failures = failures;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${nexus.worker.watchdog-poll-delay-ms:1000}", scheduler = "watchdogTaskScheduler")
    public void recoverExpiredLeases() {
        failures.recoverExpiredLeases(batchSize);
    }
}
