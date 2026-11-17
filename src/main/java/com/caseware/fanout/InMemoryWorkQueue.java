package com.caseware.fanout;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Reference implementation of {@link WorkQueue}. One lock keeps it obviously correct. */
public final class InMemoryWorkQueue implements WorkQueue {

    private static final class Item {
        final EngagementRef engagement;
        final long seq;
        Status status = Status.PENDING;
        int attempts;
        Instant notBefore;
        UUID leaseToken;
        Instant leaseExpiresAt;

        Item(EngagementRef engagement, long seq, Instant now) {
            this.engagement = engagement;
            this.seq = seq;
            this.notBefore = now;
        }

        boolean isDue(Instant now) {
            return (status == Status.PENDING && !notBefore.isAfter(now))
                    || (status == Status.LEASED && !leaseExpiresAt.isAfter(now)); // crashed holder
        }

        boolean liveLease(Instant now) {
            return status == Status.LEASED && leaseExpiresAt.isAfter(now);
        }

        boolean heldBy(Lease lease) {
            return status == Status.LEASED && lease.token().equals(leaseToken);
        }

        void toPending(Instant when) {
            status = Status.PENDING;
            notBefore = when;
            leaseToken = null;
            leaseExpiresAt = null;
        }
    }

    private final Map<String, Item> items = new LinkedHashMap<>();
    private final Map<String, Long> lastServed = new HashMap<>();
    private long counter;

    @Override
    public synchronized boolean enqueue(EngagementRef engagement, Instant now) {
        Item item = items.get(engagement.engagementId());
        if (item == null) {
            items.put(engagement.engagementId(), new Item(engagement, counter++, now));
            return true;
        }
        if (item.status == Status.PENDING || item.status == Status.LEASED) {
            return false; // already queued: one verification answers every publish
        }
        item.attempts = 0;
        item.toPending(now); // stale again since the last verification
        return true;
    }

    @Override
    public synchronized Optional<Lease> claim(Instant now, Duration leaseFor, int maxLeasesPerTenant) {
        Map<String, Integer> live = new HashMap<>();
        items.values().stream().filter(i -> i.liveLease(now))
                .forEach(i -> live.merge(i.engagement.tenantId(), 1, Integer::sum));

        Optional<Item> next = items.values().stream()
                .filter(i -> i.isDue(now))
                .filter(i -> live.getOrDefault(i.engagement.tenantId(), 0) < maxLeasesPerTenant)
                .min(Comparator.comparingLong((Item i) -> lastServed.getOrDefault(i.engagement.tenantId(), -1L))
                        .thenComparingLong(i -> i.seq));
        if (next.isEmpty()) {
            return Optional.empty();
        }
        Item item = next.get();
        item.status = Status.LEASED;
        item.attempts++;
        item.leaseToken = UUID.randomUUID();
        item.leaseExpiresAt = now.plus(leaseFor);
        lastServed.put(item.engagement.tenantId(), counter++);
        return Optional.of(new Lease(item.engagement, item.attempts, item.leaseToken, item.leaseExpiresAt));
    }

    @Override
    public synchronized boolean complete(Lease lease) {
        Item item = held(lease);
        if (item == null) {
            return false;
        }
        item.status = Status.DONE;
        item.leaseToken = null;
        return true;
    }

    @Override
    public synchronized boolean retryAt(Lease lease, Instant when, boolean countAttempt) {
        Item item = held(lease);
        if (item == null) {
            return false;
        }
        if (!countAttempt) {
            item.attempts--; // refund: a systemic failure is not this item's fault
        }
        item.toPending(when);
        return true;
    }

    @Override
    public synchronized boolean finish(Lease lease, Status terminal, String reason) {
        if (terminal != Status.DROPPED && terminal != Status.DEAD) {
            throw new IllegalArgumentException("finish() takes DROPPED or DEAD, not " + terminal);
        }
        Item item = held(lease);
        if (item == null) {
            return false;
        }
        item.status = terminal;
        item.leaseToken = null;
        return true;
    }

    @Override
    public synchronized Optional<Status> status(String engagementId) {
        return Optional.ofNullable(items.get(engagementId)).map(i -> i.status);
    }

    private Item held(Lease lease) {
        Item item = items.get(lease.engagement().engagementId());
        return item != null && item.heldBy(lease) ? item : null;
    }
}
