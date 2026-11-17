package com.caseware.fanout;

import java.time.Duration;

/**
 * @param callTimeout  per-call deadline (calls take ~1 minute, so well above that)
 * @param leaseMargin  extra lease time beyond the timeout, for writing the result
 * @param maxPerTenant live calls one firm may hold, so a 40k-engagement firm cannot take every slot
 */
public record WorkerSettings(Duration callTimeout,
                             Duration leaseMargin,
                             int maxAttempts,
                             Duration baseBackoff,
                             Duration maxBackoff,
                             int maxPerTenant) {

    public WorkerSettings {
        if (callTimeout.isNegative() || callTimeout.isZero() || leaseMargin.isNegative() || leaseMargin.isZero()) {
            throw new IllegalArgumentException("callTimeout and leaseMargin must be positive");
        }
        if (maxAttempts < 1 || maxPerTenant < 1) {
            throw new IllegalArgumentException("maxAttempts and maxPerTenant must be >= 1");
        }
        if (maxBackoff.compareTo(baseBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must be >= baseBackoff");
        }
    }

    public static WorkerSettings defaults() {
        return new WorkerSettings(Duration.ofMinutes(3), Duration.ofMinutes(2), 6,
                Duration.ofMinutes(2), Duration.ofHours(1), 5);
    }

    /** Lease always outlives a call that respects its timeout, so a healthy call is never reclaimed. */
    public Duration leaseDuration() {
        return callTimeout.plus(leaseMargin);
    }
}
