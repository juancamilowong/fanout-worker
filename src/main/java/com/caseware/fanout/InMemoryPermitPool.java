package com.caseware.fanout;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Reference implementation of {@link PermitPool}. */
public final class InMemoryPermitPool implements PermitPool {

    private final UUID[] holders;
    private final Instant[] expiresAt;
    private final Duration permitTtl;
    private Instant pausedUntil = Instant.MIN;

    /** @param permitTtl must exceed the worker's lease duration, or a slow call could lose its slot */
    public InMemoryPermitPool(int slots, Duration permitTtl) {
        if (slots < 1) {
            throw new IllegalArgumentException("slots must be >= 1");
        }
        this.holders = new UUID[slots];
        this.expiresAt = new Instant[slots];
        this.permitTtl = permitTtl;
    }

    @Override
    public synchronized Optional<Permit> tryAcquire(Instant now) {
        if (now.isBefore(pausedUntil)) {
            return Optional.empty();
        }
        for (int slot = 0; slot < holders.length; slot++) {
            if (holders[slot] == null || !expiresAt[slot].isAfter(now)) {
                holders[slot] = UUID.randomUUID();
                expiresAt[slot] = now.plus(permitTtl);
                return Optional.of(new Permit(slot, holders[slot]));
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized void release(Permit permit) {
        if (permit.token().equals(holders[permit.slot()])) { // ignore a release after expiry and reuse
            holders[permit.slot()] = null;
        }
    }

    @Override
    public synchronized void pauseUntil(Instant until) {
        if (until.isAfter(pausedUntil)) {
            pausedUntil = until;
        }
    }

    public synchronized int inUse(Instant now) {
        int n = 0;
        for (int slot = 0; slot < holders.length; slot++) {
            if (holders[slot] != null && expiresAt[slot].isAfter(now)) {
                n++;
            }
        }
        return n;
    }
}
