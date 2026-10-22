package io.nexus.controlplane.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.service.FakeTaskExecutor;
import io.nexus.controlplane.service.FakeWorkerService;
import io.nexus.controlplane.service.TaskFailureService;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Helpers shared by tests using the explicitly configured disposable database. */
public final class RuntimeTestSupport {
    private RuntimeTestSupport() { }

    public static void clearDatabase(JdbcTemplate jdbc) {
        jdbc.execute("TRUNCATE domain_events, tasks, runs, sessions");
    }

    public static FakeWorkerService worker(ApplicationContext context) {
        ObjectMapper mapper = context.getBean(ObjectMapper.class);
        return new FakeWorkerService(context.getBean(RuntimeStore.class), mapper,
                context.getBean(TaskFailureService.class), new FakeTaskExecutor(mapper), 30_000);
    }

    public static void tick(ApplicationContext context, FakeWorkerService worker) {
        new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
                .executeWithoutResult(s -> worker.advanceOneTask());
    }

    public static <T> List<T> concurrently(List<Callable<T>> actions) throws Exception {
        CountDownLatch ready = new CountDownLatch(actions.size());
        CountDownLatch start = new CountDownLatch(1);
        var threads = Executors.newFixedThreadPool(actions.size());
        List<Future<T>> futures = new ArrayList<>();
        try {
            for (Callable<T> action : actions) {
                futures.add(threads.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Concurrent test start timed out");
                    }
                    return action.call();
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get(15, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            start.countDown();
            futures.forEach(future -> future.cancel(true));
            threads.shutdownNow();
            assertThat(threads.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    public static void race(Runnable first, Runnable second) throws Exception {
        concurrently(List.of(() -> { first.run(); return true; }, () -> { second.run(); return true; }));
    }
}
