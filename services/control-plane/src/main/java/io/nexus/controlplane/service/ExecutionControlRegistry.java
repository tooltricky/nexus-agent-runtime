package io.nexus.controlplane.service;

import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.Records.PauseCheckpointRecord;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Local stop delivery is advisory; persisted state and result fences remain authoritative. */
@Component
public class ExecutionControlRegistry {
    private final ConcurrentHashMap<Key, Signal> active = new ConcurrentHashMap<>();
    private final LeaseHeartbeatService heartbeats;
    private boolean closed;

    public ExecutionControlRegistry(LeaseHeartbeatService heartbeats) { this.heartbeats = heartbeats; }

    public synchronized Signal register(TaskRecord task) {
        Signal signal = new Signal(new Key(task.id(), task.attempt(), task.version()), Thread.currentThread());
        if (closed) signal.requestStop();
        else {
            Signal previous = active.putIfAbsent(signal.key, signal);
            if (previous != null) throw new IllegalStateException("Execution already registered: " + task.id());
        }
        return signal;
    }

    public void stop(TaskRecord task) {
        heartbeats.stopAttemptsThrough(task.id(), task.attempt());
        active.forEach((key, signal) -> {
            // Lease loss may leave older executors running after a newer attempt takes over.
            // Bound the notification so a stale callback cannot stop a newer attempt.
            if (key.taskId().equals(task.id()) && key.attempt() <= task.attempt()) signal.requestStop();
        });
    }

    public void stopPaused(PauseCheckpointRecord checkpoint) {
        heartbeats.stopThroughFence(checkpoint.taskId(), checkpoint.attempt(), checkpoint.taskVersion());
        active.forEach((key, signal) -> {
            if (key.taskId().equals(checkpoint.taskId()) && (key.attempt() < checkpoint.attempt()
                    || (key.attempt() == checkpoint.attempt() && key.version() <= checkpoint.taskVersion()))) {
                signal.requestStop();
            }
        });
    }

    @PreDestroy
    public synchronized void close() {
        closed = true;
        active.values().forEach(Signal::requestStop);
        active.clear();
    }

    int activeCount() { return active.size(); }
    java.util.Set<String> activeTaskIds() {
        return active.entrySet().stream().filter(entry -> !entry.getValue().isStopRequested())
                .map(entry -> entry.getKey().taskId()).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
    private record Key(String taskId, int attempt, long version) { }

    public final class Signal implements AutoCloseable {
        private final Key key;
        private final Thread thread;
        private final AtomicBoolean stopped = new AtomicBoolean();
        private boolean finished;
        private Signal(Key key, Thread thread) { this.key = key; this.thread = thread; }
        public boolean isStopRequested() { return stopped.get(); }
        public void throwIfStopRequested() {
            if (isStopRequested()) throw new CancellationException("Execution stopped");
        }
        private synchronized void requestStop() {
            if (!finished && stopped.compareAndSet(false, true)) {
                thread.interrupt();
            }
        }
        @Override
        public synchronized void close() {
            if (finished) return;
            finished = true;
            active.remove(key, this);
            // Do not leave our interruption on a pooled thread for its next task.
            if (stopped.get() && Thread.currentThread() == thread) Thread.interrupted();
        }
    }
}
