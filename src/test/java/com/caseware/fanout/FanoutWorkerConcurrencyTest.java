package com.caseware.fanout;

import com.caseware.fanout.Fixtures.FakeClient;
import com.caseware.fanout.Fixtures.FakeDirectory;
import com.caseware.fanout.Fixtures.FakeIndex;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Real threads: three worker instances race for a shared quota of 4 slots, max 2 per firm. */
public class FanoutWorkerConcurrencyTest {

    @Test(timeout = 20_000)
    public void quotaAndPerFirmCapHoldUnderConcurrencyAndEachEngagementIsCalledOnce() throws Exception {
        FakeIndex index = new FakeIndex();
        FakeDirectory directory = new FakeDirectory(index).add("firm-a", 20).add("firm-b", 20).add("firm-c", 20);
        InMemoryWorkQueue queue = new InMemoryWorkQueue();
        Clock clock = Clock.systemUTC();
        new PublishHandler(directory, queue, clock).onTemplatePublished(new TemplatePublished("evt-5", "audit", 5));

        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Map<String, AtomicInteger> runningPerFirm = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> peakPerFirm = new ConcurrentHashMap<>();
        FakeClient client = new FakeClient();
        client.behaviour = e -> {
            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
            AtomicInteger firm = runningPerFirm.computeIfAbsent(e.tenantId(), k -> new AtomicInteger());
            peakPerFirm.computeIfAbsent(e.tenantId(), k -> new AtomicInteger())
                    .accumulateAndGet(firm.incrementAndGet(), Math::max);
            try {
                Thread.sleep(10); // stands in for the ~1 minute load
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            firm.decrementAndGet();
            running.decrementAndGet();
            return new VerificationResult.Verified(5, 1);
        };

        WorkerSettings settings = new WorkerSettings(
                Duration.ofMinutes(3), Duration.ofMinutes(2), 3, Duration.ofSeconds(1), Duration.ofSeconds(10), 2);
        PermitPool sharedQuota = new InMemoryPermitPool(4, settings.leaseDuration());

        try (var calls = Executors.newVirtualThreadPerTaskExecutor();
             var instances = Executors.newFixedThreadPool(3)) {
            for (int i = 0; i < 3; i++) {
                FanoutWorker instance = new FanoutWorker(queue, sharedQuota,
                        new CircuitBreaker(5, Duration.ofMinutes(1), clock), client, index, settings, clock, calls);
                instances.submit(() -> {
                    while (index.entries.size() < 60) {
                        instance.pollOnce();
                        Thread.sleep(1);
                    }
                    return null;
                });
            }
        }

        assertTrue("global peak " + peak.get(), peak.get() <= 4);
        assertEquals("the quota was actually used", 4, peak.get());
        peakPerFirm.forEach((firm, p) -> assertTrue(firm + " peak " + p.get(), p.get() <= 2));
        assertEquals("each engagement called exactly once", 60, client.calls.size());
    }
}
