package io.nexus.controlplane;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.Executor;

@Configuration
public class SchedulingConfiguration {
    @Bean(name = "workerTaskScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler workerTaskScheduler() {
        return scheduler("nexus-worker-");
    }

    @Bean(name = "watchdogTaskScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler watchdogTaskScheduler() {
        return scheduler("nexus-watchdog-");
    }

    @Bean(name = "heartbeatTaskScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler heartbeatTaskScheduler() {
        return scheduler("nexus-heartbeat-");
    }

    @Bean(name = "deadlineTaskScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler deadlineTaskScheduler() { return scheduler("nexus-deadline-"); }

    @Bean(name = "workerExecutionExecutor", destroyMethod = "shutdown")
    public Executor workerExecutionExecutor(
            @Value("${nexus.worker.execution-concurrency:16}") int executionConcurrency
    ) {
        if (executionConcurrency < 1) {
            throw new IllegalArgumentException("Worker execution concurrency must be positive");
        }
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(executionConcurrency);
        executor.setMaxPoolSize(executionConcurrency);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("nexus-worker-exec-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(5);
        executor.initialize();
        return executor;
    }

    private ThreadPoolTaskScheduler scheduler(String threadNamePrefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(5);
        return scheduler;
    }
}
