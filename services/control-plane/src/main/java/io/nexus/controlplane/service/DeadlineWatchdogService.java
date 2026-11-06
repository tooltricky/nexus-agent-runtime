package io.nexus.controlplane.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "nexus.timeout.enabled", havingValue = "true", matchIfMissing = true)
public class DeadlineWatchdogService {
    private final TaskTimeoutService timeouts;
    private final TimeoutConfiguration configuration;
    public DeadlineWatchdogService(TaskTimeoutService timeouts, TimeoutConfiguration configuration) {
        this.timeouts = timeouts;
        this.configuration = configuration;
    }
    @Scheduled(fixedDelayString = "${nexus.timeout.poll-delay-ms:1000}", scheduler = "deadlineTaskScheduler")
    public void expireDueTasks() { timeouts.expireDueTasks(configuration.batchSize()); }
}
