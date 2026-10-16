package io.nexus.controlplane.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryPolicyTest {
    @Test
    void appliesCappedExponentialBackoff() {
        RetryPolicy policy = new RetryPolicy(100, 350);
        assertEquals(Duration.ofMillis(100), policy.delayAfter(1));
        assertEquals(Duration.ofMillis(200), policy.delayAfter(2));
        assertEquals(Duration.ofMillis(350), policy.delayAfter(3));
        assertEquals(Duration.ofMillis(350), policy.delayAfter(9));
    }

    @Test
    void rejectsUnclaimedAttempt() {
        RetryPolicy policy = new RetryPolicy(100, 350);
        assertThrows(IllegalArgumentException.class, () -> policy.delayAfter(0));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, 100));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(100, 50));
    }

    @Test
    void largeAttemptsAndDelaysDoNotOverflow() {
        RetryPolicy policy = new RetryPolicy(Long.MAX_VALUE / 2 + 1, Long.MAX_VALUE);
        assertEquals(Duration.ofMillis(Long.MAX_VALUE), policy.delayAfter(Integer.MAX_VALUE));
    }
}
