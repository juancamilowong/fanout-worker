package com.caseware.fanout;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable verification work, one item per engagement. Shared by every worker instance in the region
 * (an Aurora table in production; see README for the SQL). All operations are atomic.
 *
 * <p>Every transition after {@link #claim} presents the lease. It returns {@code false} when the lease
 * expired and another worker reclaimed the item; the caller then discards its late result.
 */
public interface WorkQueue {

    enum Status { PENDING, LEASED, DONE, DROPPED, DEAD }

    /** @param attempt 1-based; attempts that crashed a worker count too */
    record Lease(EngagementRef engagement, int attempt, UUID token, Instant expiresAt) {}

    /**
     * Idempotent. Creates work, or reopens finished work; no-op if work is already open.
     * Many publishes for one engagement therefore collapse into one verification.
     *
     * @return true if work was created or reopened
     */
    boolean enqueue(EngagementRef engagement, Instant now);

    /**
     * Leases the next due item (pending and due, or leased with an expired lease). Skips firms that
     * already hold {@code maxLeasesPerTenant} live leases, and serves firms round-robin.
     */
    Optional<Lease> claim(Instant now, Duration leaseFor, int maxLeasesPerTenant);

    boolean complete(Lease lease);

    /** @param countAttempt false when the failure is systemic (overload, outage), not this item's fault */
    boolean retryAt(Lease lease, Instant when, boolean countAttempt);

    /** Ends the item as {@link Status#DROPPED} (nothing to do) or {@link Status#DEAD} (gave up, alert). */
    boolean finish(Lease lease, Status terminal, String reason);

    Optional<Status> status(String engagementId);
}
