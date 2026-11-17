package com.caseware.fanout;

import com.caseware.fanout.Fixtures.FakeClient;
import com.caseware.fanout.Fixtures.FakeDirectory;
import com.caseware.fanout.Fixtures.FakeIndex;
import com.caseware.fanout.Fixtures.ManualClock;
import com.caseware.fanout.VerificationResult.Busy;
import com.caseware.fanout.VerificationResult.Failure;
import com.caseware.fanout.VerificationResult.NotFound;
import com.caseware.fanout.VerificationResult.Overloaded;
import com.caseware.fanout.VerificationResult.Verified;
import com.caseware.fanout.WorkQueue.Lease;
import com.caseware.fanout.WorkQueue.Status;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Behaviour tests. Calls run on the calling thread and time is a manual clock, so every
 * transition is deterministic. Real concurrency is in {@link FanoutWorkerConcurrencyTest}.
 */
public class FanoutWorkerTest {

    private final ManualClock clock = new ManualClock();
    private final FakeIndex index = new FakeIndex();
    private final FakeDirectory directory = new FakeDirectory(index);
    private final FakeClient client = new FakeClient();
    private final InMemoryWorkQueue queue = new InMemoryWorkQueue();
    private final WorkerSettings settings = new WorkerSettings(
            Duration.ofMinutes(3), Duration.ofMinutes(2), 3, Duration.ofMinutes(1), Duration.ofMinutes(10), 2);
    private final CircuitBreaker breaker = new CircuitBreaker(5, Duration.ofMinutes(5), clock);
    private final PublishHandler publisher = new PublishHandler(directory, queue, clock);

    private FanoutWorker worker(PermitPool permits) {
        return new FanoutWorker(queue, permits, breaker, client, index, settings, clock, Runnable::run);
    }

    private FanoutWorker worker() {
        return worker(new InMemoryPermitPool(20, settings.leaseDuration()));
    }

    private void publish(int version) {
        publisher.onTemplatePublished(new TemplatePublished("evt-" + version, "audit", version));
    }

    private Status status(String engagementId) {
        return queue.status(engagementId).orElseThrow();
    }

    // ---- idempotency ------------------------------------------------------------------------

    @Test
    public void redeliveredAndSuccessivePublishesCollapseIntoOneCallPerEngagement() {
        directory.add("firm-a", 10);

        assertEquals(10, publisher.onTemplatePublished(new TemplatePublished("evt-5", "audit", 5)));
        assertEquals("redelivery", 0, publisher.onTemplatePublished(new TemplatePublished("evt-5", "audit", 5)));
        assertEquals("next version", 0, publisher.onTemplatePublished(new TemplatePublished("evt-6", "audit", 6)));

        worker().pollOnce();

        assertEquals(10, client.calls.size());
        assertEquals(10, index.entries.size());
        assertEquals("verified engagements need no more calls", 0,
                publisher.onTemplatePublished(new TemplatePublished("evt-7", "audit", 7)));
    }

    @Test
    public void slowVerificationNeverOverwritesANewerHook() {
        directory.add("firm-a", 1);
        publish(5);
        index.recordVerified("firm-a-0", 5, 10); // user applied v5 while our call was running
        client.behaviour = e -> new Verified(3, 9); // the call read the engagement before that

        worker().pollOnce();

        assertEquals(5, index.entries.get("firm-a-0").version());
    }

    // ---- downstream capacity and fairness -------------------------------------------------------

    @Test
    public void smallFirmIsServedEarlyDespiteALargeFirmsBacklog() {
        directory.add("big-firm", 50).add("small-firm", 3);
        publish(5);

        worker().pollOnce();

        List<String> order = client.calls.stream().map(EngagementRef::tenantId).toList();
        assertTrue("small firm done by call " + (order.lastIndexOf("small-firm") + 1),
                order.lastIndexOf("small-firm") < 6);
    }

    @Test
    public void overloadPausesTheWholeRegionUntilRetryAfter() {
        directory.add("firm-a", 3);
        publish(5);
        AtomicInteger calls = new AtomicInteger();
        client.behaviour = e -> calls.incrementAndGet() == 1 ? new Overloaded(Duration.ofSeconds(30)) : new Verified(5, 1);
        FanoutWorker worker = worker();

        assertEquals("stops as soon as the downstream pushes back", 1, worker.pollOnce());
        clock.advance(Duration.ofSeconds(29));
        assertEquals("still paused", 0, worker.pollOnce());

        clock.advance(Duration.ofSeconds(2));
        worker.pollOnce();
        assertEquals(Status.DONE, status("firm-a-0"));
        assertEquals(4, client.calls.size());
    }

    @Test
    public void overloadDoesNotConsumeTheRetryBudget() {
        directory.add("firm-a", 1);
        publish(5);
        AtomicInteger calls = new AtomicInteger();
        client.behaviour = e -> calls.incrementAndGet() <= 5 ? new Overloaded(Duration.ofSeconds(10)) : new Verified(5, 1);
        FanoutWorker worker = worker();

        for (int i = 0; i < 6; i++) {
            worker.pollOnce();
            clock.advance(Duration.ofSeconds(11));
        }

        assertEquals("5 overloads > maxAttempts (3), still not dead-lettered", Status.DONE, status("firm-a-0"));
    }

    // ---- failures -------------------------------------------------------------------------------

    @Test
    public void transientFailureBacksOffThenSucceeds() {
        directory.add("firm-a", 1);
        publish(5);
        AtomicInteger calls = new AtomicInteger();
        client.behaviour = e -> calls.incrementAndGet() <= 2 ? new Failure("HTTP 503") : new Verified(5, 1);
        FanoutWorker worker = worker();

        worker.pollOnce();
        assertEquals("not retried before the backoff", 0, worker.pollOnce());
        clock.advance(Duration.ofMinutes(1));  // backoff(1) is within [30 s, 1 min]
        worker.pollOnce();
        clock.advance(Duration.ofMinutes(2));  // backoff(2) is within [1 min, 2 min]
        worker.pollOnce();

        assertEquals(Status.DONE, status("firm-a-0"));
        assertEquals(3, client.calls.size());
    }

    @Test
    public void itemThatKeepsFailingIsDeadLettered() {
        directory.add("firm-a", 1);
        publish(5);
        client.behaviour = e -> new Failure("timeout");
        FanoutWorker worker = worker();

        for (int i = 0; i < 5; i++) {
            worker.pollOnce();
            clock.advance(Duration.ofMinutes(15));
        }

        assertEquals(Status.DEAD, status("firm-a-0"));
        assertEquals("maxAttempts", 3, client.calls.size());
    }

    @Test
    public void busyAndNotFoundAreDroppedWithoutRetry() {
        directory.add("firm-a", 2);
        publish(5);
        client.behaviour = e -> e.engagementId().equals("firm-a-0") ? new Busy() : new NotFound();
        FanoutWorker worker = worker();

        worker.pollOnce();
        clock.advance(Duration.ofHours(1));
        worker.pollOnce();

        assertEquals(Status.DROPPED, status("firm-a-0"));
        assertEquals(Status.DROPPED, status("firm-a-1"));
        assertEquals(2, client.calls.size());
    }

    @Test
    public void exceptionFromTheClientIsTreatedAsATransientFailure() {
        directory.add("firm-a", 1);
        publish(5);
        AtomicInteger calls = new AtomicInteger();
        client.behaviour = e -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("connection reset");
            }
            return new Verified(5, 1);
        };
        FanoutWorker worker = worker();

        worker.pollOnce();
        clock.advance(Duration.ofMinutes(1));
        worker.pollOnce();

        assertEquals(Status.DONE, status("firm-a-0"));
    }

    @Test
    public void outageOpensTheCircuitThenOneProbeAtATimeUntilRecovery() {
        directory.add("firm-a", 8);
        publish(5);
        client.behaviour = e -> new Failure("connection refused");
        FanoutWorker worker = worker();

        worker.pollOnce();
        assertEquals("trips after 5 consecutive failures", 5, client.calls.size());
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());

        clock.advance(Duration.ofMinutes(4));
        assertEquals("open: no calls", 0, worker.pollOnce());

        clock.advance(Duration.ofMinutes(1));
        worker.pollOnce();
        assertEquals("half-open: a single probe, which fails", 6, client.calls.size());
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());

        client.behaviour = e -> new Verified(5, 1);
        clock.advance(Duration.ofMinutes(5));
        worker.pollOnce();
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
        for (int i = 0; i < 8; i++) {
            assertEquals("outage did not dead-letter anything", Status.DONE, status("firm-a-" + i));
        }
    }

    // ---- crashes and leases ---------------------------------------------------------------------

    @Test
    public void crashedWorkerLeaksNeitherTheItemNorTheSlot() {
        directory.add("firm-a", 1);
        publish(5);
        InMemoryPermitPool onlyOneSlot = new InMemoryPermitPool(1, settings.leaseDuration());
        // Another instance claims the item and the only slot, then dies.
        Lease crashed = queue.claim(clock.instant(), settings.leaseDuration(), 2).orElseThrow();
        onlyOneSlot.tryAcquire(clock.instant()).orElseThrow();
        FanoutWorker worker = worker(onlyOneSlot);

        assertEquals("slot and item still held", 0, worker.pollOnce());

        clock.advance(settings.leaseDuration().plusSeconds(1));
        worker.pollOnce();

        assertEquals(Status.DONE, status("firm-a-0"));
        assertFalse("the dead worker's late result is rejected", queue.complete(crashed));
    }

    @Test
    public void itemThatKeepsCrashingWorkersIsDeadLetteredWithoutAnotherCall() {
        directory.add("firm-a", 1);
        publish(5);
        for (int i = 0; i < settings.maxAttempts(); i++) {
            queue.claim(clock.instant(), settings.leaseDuration(), 2).orElseThrow();
            clock.advance(settings.leaseDuration().plusSeconds(1));
        }

        worker().pollOnce();

        assertEquals(Status.DEAD, status("firm-a-0"));
        assertTrue(client.calls.isEmpty());
    }

    @Test
    public void indexOutageLeavesTheItemToBeRetriedAfterItsLease() {
        directory.add("firm-a", 1);
        publish(5);
        AtomicInteger writes = new AtomicInteger();
        VersionIndex flakyIndex = (id, version, revision) -> {
            if (writes.incrementAndGet() == 1) {
                throw new IllegalStateException("database unavailable");
            }
            index.recordVerified(id, version, revision);
        };
        FanoutWorker worker = new FanoutWorker(queue, new InMemoryPermitPool(20, settings.leaseDuration()),
                breaker, client, flakyIndex, settings, clock, Runnable::run);

        worker.pollOnce();
        assertEquals(Status.LEASED, status("firm-a-0"));

        clock.advance(settings.leaseDuration().plusSeconds(1));
        worker.pollOnce();
        assertEquals(Status.DONE, status("firm-a-0"));
        assertEquals("at-least-once: called twice, harmless because read-only", 2, client.calls.size());
    }

    // ---- configuration --------------------------------------------------------------------------

    @Test
    public void invalidSettingsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new WorkerSettings(
                Duration.ofMinutes(3), Duration.ZERO, 3, Duration.ofMinutes(1), Duration.ofMinutes(10), 2));
        assertThrows(IllegalArgumentException.class, () -> new WorkerSettings(
                Duration.ofMinutes(3), Duration.ofMinutes(1), 3, Duration.ofMinutes(10), Duration.ofMinutes(1), 2));
        assertTrue(WorkerSettings.defaults().leaseDuration().compareTo(WorkerSettings.defaults().callTimeout()) > 0);
    }
}
