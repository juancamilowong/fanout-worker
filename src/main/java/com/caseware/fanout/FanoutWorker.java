package com.caseware.fanout;

import com.caseware.fanout.PermitPool.Permit;
import com.caseware.fanout.WorkQueue.Lease;
import com.caseware.fanout.WorkQueue.Status;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drains verification work into the downstream at the rate its owners allow.
 *
 * <ul>
 *   <li><b>Capacity:</b> a call starts only with a permit from the shared {@link PermitPool} (the agreed
 *       quota) and a lease the queue grants only while the firm is under its per-tenant cap.</li>
 *   <li><b>At-least-once:</b> an item is finished only after its result is written. If the worker dies, its
 *       lease and permit expire and another instance picks the item up. The call is read-only, so a
 *       repeat costs a slot but changes nothing.</li>
 *   <li><b>Fencing:</b> transitions present the lease; a late result from a reclaimed lease is discarded.</li>
 *   <li><b>Failures:</b> overload pauses the region; an outage opens the circuit; neither burns attempts.
 *       Item-specific failures back off exponentially and dead-letter after {@code maxAttempts}.</li>
 * </ul>
 *
 * Thread-safe: any number of threads or instances may call {@link #pollOnce()} concurrently.
 */
public final class FanoutWorker {

    private static final System.Logger LOG = System.getLogger(FanoutWorker.class.getName());

    private final WorkQueue queue;
    private final PermitPool permits;
    private final CircuitBreaker breaker;
    private final VerificationClient client;
    private final VersionIndex index;
    private final WorkerSettings settings;
    private final Clock clock;
    private final Executor executor;

    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean stopped;

    public FanoutWorker(WorkQueue queue, PermitPool permits, CircuitBreaker breaker, VerificationClient client,
                        VersionIndex index, WorkerSettings settings, Clock clock, Executor executor) {
        this.queue = queue;
        this.permits = permits;
        this.breaker = breaker;
        this.client = client;
        this.index = index;
        this.settings = settings;
        this.clock = clock;
        this.executor = executor;
    }

    /**
     * Starts as many calls as capacity allows. Called on a schedule (e.g. every second).
     *
     * @return number of calls started
     */
    public int pollOnce() {
        int started = 0;
        while (!stopped && breaker.allowCall()) {
            Optional<Permit> permit = permits.tryAcquire(clock.instant());
            if (permit.isEmpty()) {
                breaker.onNeutral();
                break; // quota used up, or the region is paused
            }
            Optional<Lease> lease = queue.claim(clock.instant(), settings.leaseDuration(), settings.maxPerTenant());
            if (lease.isEmpty()) {
                permits.release(permit.get());
                breaker.onNeutral();
                break; // nothing due, or every waiting firm is at its cap
            }
            inFlight.incrementAndGet();
            executor.execute(() -> process(lease.get(), permit.get()));
            started++;
        }
        return started;
    }

    private void process(Lease lease, Permit permit) {
        try {
            if (lease.attempt() > settings.maxAttempts()) {
                breaker.onNeutral();
                finish(lease, Status.DEAD, "max attempts reached, including attempts that crashed a worker");
                return;
            }
            VerificationResult result = call(lease.engagement());
            Instant now = clock.instant();
            switch (result) {
                case VerificationResult.Verified v -> {
                    breaker.onSuccess();
                    index.recordVerified(lease.engagement().engagementId(), v.templateVersion(), v.revision());
                    if (!queue.complete(lease)) {
                        staleResult(lease);
                    }
                }
                case VerificationResult.NotFound n -> {
                    breaker.onSuccess();
                    finish(lease, Status.DROPPED, "engagement no longer exists");
                }
                case VerificationResult.Busy b -> {
                    breaker.onSuccess();
                    finish(lease, Status.DROPPED, "open by a user; the Loaded hook verifies it");
                }
                case VerificationResult.Overloaded o -> {
                    breaker.onNeutral();
                    Instant resume = now.plus(o.retryAfter());
                    permits.pauseUntil(resume);
                    retry(lease, resume, false);
                }
                case VerificationResult.Failure f -> {
                    boolean outage = breaker.onFailure();
                    if (outage) {
                        retry(lease, now.plus(settings.baseBackoff()), false);
                    } else if (lease.attempt() >= settings.maxAttempts()) {
                        finish(lease, Status.DEAD, "gave up after " + lease.attempt() + " attempts: " + f.reason());
                    } else {
                        retry(lease, now.plus(backoff(lease.attempt())), true);
                    }
                }
            }
        } catch (RuntimeException e) {
            // Index or queue unavailable: do nothing; the lease expires and the item is retried.
            LOG.log(System.Logger.Level.WARNING, "failed to record " + lease.engagement() + "; lease will expire", e);
        } finally {
            permits.release(permit);
            inFlight.decrementAndGet();
        }
    }

    private VerificationResult call(EngagementRef engagement) {
        try {
            VerificationResult result = client.verify(engagement, settings.callTimeout());
            return result != null ? result : new VerificationResult.Failure("null result");
        } catch (RuntimeException e) {
            return new VerificationResult.Failure(e.toString());
        }
    }

    private void retry(Lease lease, Instant when, boolean countAttempt) {
        if (!queue.retryAt(lease, when, countAttempt)) {
            staleResult(lease);
        }
    }

    private void finish(Lease lease, Status terminal, String reason) {
        if (!queue.finish(lease, terminal, reason)) {
            staleResult(lease);
        } else if (terminal == Status.DEAD) {
            LOG.log(System.Logger.Level.ERROR, "dead-lettered " + lease.engagement() + ": " + reason);
        }
    }

    private void staleResult(Lease lease) {
        LOG.log(System.Logger.Level.INFO, "discarded result for " + lease.engagement() + ": lease was reclaimed");
    }

    /** Exponential backoff with jitter in [d/2, d], so failed items don't come back as one wave. */
    Duration backoff(int attempt) {
        long base = settings.baseBackoff().toMillis();
        long capped = Math.min(settings.maxBackoff().toMillis(), base << Math.min(attempt - 1, 20));
        return Duration.ofMillis(capped / 2 + ThreadLocalRandom.current().nextLong(capped / 2 + 1));
    }

    /** Stops starting calls. In-flight calls finish; anything left over is reclaimed after its lease expires. */
    public void stop() {
        stopped = true;
    }

    public int inFlight() {
        return inFlight.get();
    }
}
