package io.nexus.controlplane.service;

import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ExecutionControlRegistryTest {
    @Test void monitorKeepsRegistrationsAfterReadFailureAndChecksAllTasksInBoundedBatches() {
        var heartbeats = mock(LeaseHeartbeatService.class);
        var controls = new ExecutionControlRegistry(heartbeats);
        var store = mock(RuntimeStore.class);
        var monitor = new ExecutionTimeoutMonitor(store, controls, new TimeoutConfiguration(100, 1));
        var old = task("task", 1);
        var current = task("task", 2);
        var other = task("other", 1);
        try (var first = controls.register(old); var second = controls.register(current);
             var unrelated = controls.register(other)) {
            when(store.findTimedOutTasks(anySet())).thenThrow(new org.springframework.dao.DataAccessResourceFailureException("unavailable"));
            assertThrows(org.springframework.dao.DataAccessResourceFailureException.class, monitor::reconcile);
            assertThat(first.isStopRequested()).isFalse();
            assertThat(second.isStopRequested()).isFalse();
            assertThat(controls.activeCount()).isEqualTo(3);
            clearInvocations(store);
            doReturn(java.util.List.of(current)).when(store).findTimedOutTasks(java.util.Set.of("task"));
            doReturn(java.util.List.of()).when(store).findTimedOutTasks(java.util.Set.of("other"));
            monitor.reconcile();
            assertThat(first.isStopRequested()).isTrue();
            assertThat(second.isStopRequested()).isTrue();
            assertThat(unrelated.isStopRequested()).isFalse();
            verify(store).findTimedOutTasks(java.util.Set.of("task"));
            verify(store).findTimedOutTasks(java.util.Set.of("other"));
            verifyNoMoreInteractions(store);
            clearInvocations(store);
            monitor.reconcile();
            verify(store).findTimedOutTasks(java.util.Set.of("other"));
            verifyNoMoreInteractions(store);
        } finally { controls.close(); Thread.interrupted(); }
    }

    @Test void pauseFenceStopsOlderAttemptAndVersionButNotReplacementOrOtherTask() {
        var heartbeats = new LeaseHeartbeatService(mock(WorkerTransactionService.class));
        var controls = new ExecutionControlRegistry(heartbeats);
        var old = task(1); var sameAttempt = withVersion(old, old.version() + 1); var newer = task(2);
        var other = task("other", 1);
        var cp = checkpoint(old);
        heartbeats.track(old, old.leaseOwner(), java.time.Duration.ofSeconds(30));
        heartbeats.track(sameAttempt, sameAttempt.leaseOwner(), java.time.Duration.ofSeconds(30));
        heartbeats.track(newer, newer.leaseOwner(), java.time.Duration.ofSeconds(30));
        try (var first = controls.register(old); var replacement = controls.register(sameAttempt);
             var next = controls.register(newer); var unrelated = controls.register(other)) {
            controls.stopPaused(cp);
            assertThat(first.isStopRequested()).isTrue();
            assertThat(replacement.isStopRequested()).isFalse();
            assertThat(next.isStopRequested()).isFalse();
            assertThat(unrelated.isStopRequested()).isFalse();
            assertThat(heartbeats.activeCount()).isEqualTo(2);
            Thread.interrupted(); controls.stopPaused(cp);
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
        } finally { controls.close(); heartbeats.close(); Thread.interrupted(); }
    }

    @Test void pauseMonitorRetriesReadFailuresAndUsesConsumedCheckpointsInBoundedBatches() {
        var controls = new ExecutionControlRegistry(mock(LeaseHeartbeatService.class));
        var store = mock(RuntimeStore.class);
        var monitor = new ExecutionPauseMonitor(store, controls, new TimeoutConfiguration(100, 1));
        var old = task(1); var next = task(2); var other = task("other", 1);
        try (var first = controls.register(old); var replacement = controls.register(next); var unrelated = controls.register(other)) {
            when(store.findPauseCheckpoints(anySet())).thenThrow(new org.springframework.dao.DataAccessResourceFailureException("unavailable"));
            assertThrows(org.springframework.dao.DataAccessResourceFailureException.class, monitor::reconcile);
            assertThat(first.isStopRequested()).isFalse();
            doReturn(java.util.List.of(checkpoint(old))).when(store).findPauseCheckpoints(java.util.Set.of("task"));
            doReturn(java.util.List.of()).when(store).findPauseCheckpoints(java.util.Set.of("other"));
            clearInvocations(store); monitor.reconcile();
            verify(store).findPauseCheckpoints(java.util.Set.of("task"));
            verify(store).findPauseCheckpoints(java.util.Set.of("other")); verifyNoMoreInteractions(store);
            assertThat(first.isStopRequested()).isTrue();
            assertThat(replacement.isStopRequested()).isFalse(); assertThat(unrelated.isStopRequested()).isFalse();
        } finally { controls.close(); Thread.interrupted(); }
    }

    private io.nexus.controlplane.domain.Records.PauseCheckpointRecord checkpoint(TaskRecord task) {
        return new io.nexus.controlplane.domain.Records.PauseCheckpointRecord(1, java.util.UUID.randomUUID().toString(),
                task.id(), task.runId(), io.nexus.controlplane.domain.RunStatus.RUNNING, task.status(), task.attempt(),
                task.version(), 1, task.nextAttemptAt(), task.createdAt(), task.updatedAt(), "RESUMED");
    }
    private TaskRecord withVersion(TaskRecord t, long version) {
        return new TaskRecord(t.id(), t.runId(), t.name(), t.deadlineAt(), t.status(), t.input(), t.output(),
                t.leaseOwner(), t.leaseExpiresAt(), t.attempt(), t.maxAttempts(), t.nextAttemptAt(), version, t.createdAt(), t.updatedAt(), t.tenantId(), t.projectId());
    }

    @Test void timeoutStopsAllOutstandingAttemptsThroughCurrentAttempt() {
        var heartbeats = new LeaseHeartbeatService(mock(WorkerTransactionService.class));
        var controls = new ExecutionControlRegistry(heartbeats);
        var old = task(1);
        var current = task(2);
        var newer = task(3);
        var other = new TaskRecord("other", "other-run", old.name(), old.deadlineAt(), old.status(),
                null, null, old.leaseOwner(), old.leaseExpiresAt(), 1, 3, old.nextAttemptAt(), 2, old.createdAt(), old.updatedAt(), old.tenantId(), old.projectId());
        for (var task : java.util.List.of(old, current, newer, other)) {
            heartbeats.track(task, task.leaseOwner(), java.time.Duration.ofSeconds(30));
        }
        try (var first = controls.register(old); var second = controls.register(current);
             var future = controls.register(newer); var unrelated = controls.register(other)) {
            controls.stop(current);
            assertThat(first.isStopRequested()).isTrue();
            assertThat(second.isStopRequested()).isTrue();
            assertThat(future.isStopRequested()).isFalse();
            assertThat(unrelated.isStopRequested()).isFalse();
            assertThat(heartbeats.activeCount()).isEqualTo(2);
            // Prove the remaining heartbeats belong to the newer attempt and unrelated task.
            heartbeats.stop(newer, newer.leaseOwner());
            heartbeats.stop(other, other.leaseOwner());
            assertThat(heartbeats.activeCount()).isZero();
        } finally { controls.close(); heartbeats.close(); Thread.interrupted(); }
    }

    @Test void lateStopAndCleanupDoNotAffectReplacementAttemptOnSameThread() {
        var heartbeats = mock(LeaseHeartbeatService.class);
        var controls = new ExecutionControlRegistry(heartbeats);
        var old = task(1);
        var replacement = task(2);
        try (var first = controls.register(old); var second = controls.register(replacement)) {
            controls.stop(old);
            assertThat(first.isStopRequested()).isTrue();
            assertThrows(CancellationException.class, first::throwIfStopRequested);
            assertThat(second.isStopRequested()).isFalse();
            first.close();
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            assertThat(controls.activeCount()).isEqualTo(1);
            // A callback after close cannot interrupt the pooled thread now running replacement work.
            controls.stop(old);
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            assertThat(second.isStopRequested()).isFalse();
            verify(heartbeats, times(2)).stopAttemptsThrough(old.id(), old.attempt());
            controls.stop(replacement);
            first.close();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(second.isStopRequested()).isTrue();
        } finally { controls.close(); Thread.interrupted(); }
        assertThat(controls.activeCount()).isZero();
    }

    @Test void shutdownSignalsActiveExecutionsAndRefusesNewRegistration() {
        var controls = new ExecutionControlRegistry(mock(LeaseHeartbeatService.class));
        try (var active = controls.register(task(1))) {
            controls.close();
            assertThat(active.isStopRequested()).isTrue();
            assertThat(controls.activeCount()).isZero();
            try (var late = controls.register(task(2))) {
                assertThat(late.isStopRequested()).isTrue();
                assertThat(controls.activeCount()).isZero();
            }
        } finally { Thread.interrupted(); }
    }

    private TaskRecord task(int attempt) {
        return task("task", attempt);
    }
    private TaskRecord task(String id, int attempt) {
        Instant now = Instant.now();
        return new TaskRecord(id, "run", "test", now.plusSeconds(60), TaskStatus.EXECUTING,
                null, null, "owner", now.plusSeconds(30), attempt, 3, now, attempt * 2L, now, now, "tenant", "project");
    }
}
