package io.nexus.controlplane.service;

import io.nexus.controlplane.domain.Records.TaskRecord;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks only work held by this process; a stale callback cannot remove a newer registration. */
@Service
public class LeaseHeartbeatService {
    private static final Logger log = LoggerFactory.getLogger(LeaseHeartbeatService.class);
    private final WorkerTransactionService transactions;
    private final ConcurrentHashMap<Key, Registration> active = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public LeaseHeartbeatService(WorkerTransactionService transactions) {
        this.transactions = transactions;
    }

    public synchronized void track(TaskRecord task, String owner, Duration duration) {
        if (!closed) active.put(new Key(task.id(), task.attempt(), owner), new Registration(task, owner, duration));
    }

    public void stop(TaskRecord task, String owner) {
        Key key = new Key(task.id(), task.attempt(), owner);
        active.computeIfPresent(key, (ignored, current) -> current.task().version() == task.version() ? null : current);
    }

    @Scheduled(fixedDelayString = "${nexus.worker.heartbeat-interval-ms:10000}", scheduler = "heartbeatTaskScheduler")
    public void renewLeases() {
        active.forEach((key, registration) -> {
            if (closed) return;
            try {
                if (!transactions.renewLease(registration.task(), registration.owner(), registration.duration())) {
                    active.remove(key, registration);
                }
            } catch (CannotAcquireLockException busy) {
                // This Task is busy, not stale. Retry next tick without holding up other Tasks.
            } catch (DataAccessException | TransactionException unavailable) {
                // Keep the registration for the next tick; the database still decides whether it is live.
                // Do not log the exception text, which may contain database credentials or input.
                log.warn("Lease heartbeat database failure for Task {} attempt {}", key.taskId(), key.attempt());
            }
        });
    }

    @PreDestroy
    public synchronized void close() {
        closed = true;
        active.clear();
    }

    int activeCount() { return active.size(); }

    private record Key(String taskId, int attempt, String owner) { }
    private record Registration(TaskRecord task, String owner, Duration duration) { }
}
