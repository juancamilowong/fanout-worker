package com.caseware.fanout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Stops calling a downstream that is down, instead of burning every item's retry budget against it.
 *
 * <pre>
 * CLOSED --N consecutive failures--> OPEN --after openFor--> HALF_OPEN (one probe call)
 *   ^                                  ^                         |
 *   +------------ probe succeeds ------|------- probe fails -----+
 * </pre>
 * Process-local on purpose: every instance observes the same downstream and trips on its own.
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final int failureThreshold;
    private final Duration openFor;
    private final Clock clock;

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private Instant openedAt;
    private boolean probeInFlight;

    public CircuitBreaker(int failureThreshold, Duration openFor, Clock clock) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        this.failureThreshold = failureThreshold;
        this.openFor = openFor;
        this.clock = clock;
    }

    /** Asks to start one call. In HALF_OPEN only a single probe is allowed. */
    public synchronized boolean allowCall() {
        if (state == State.OPEN && !clock.instant().isBefore(openedAt.plus(openFor))) {
            state = State.HALF_OPEN;
        }
        return switch (state) {
            case CLOSED -> true;
            case OPEN -> false;
            case HALF_OPEN -> {
                if (probeInFlight) {
                    yield false;
                }
                probeInFlight = true;
                yield true;
            }
        };
    }

    /** The downstream answered sensibly (including NotFound / Busy). */
    public synchronized void onSuccess() {
        state = State.CLOSED;
        consecutiveFailures = 0;
        probeInFlight = false;
    }

    /** @return true if the breaker is now open, i.e. the failure is systemic rather than this item's */
    public synchronized boolean onFailure() {
        consecutiveFailures++;
        if (state == State.HALF_OPEN || consecutiveFailures >= failureThreshold) {
            state = State.OPEN;
            openedAt = clock.instant();
        }
        probeInFlight = false;
        return state == State.OPEN;
    }

    /** The call was not made, or said nothing about health (e.g. overload): free the probe slot. */
    public synchronized void onNeutral() {
        probeInFlight = false;
    }

    public synchronized State state() {
        return state;
    }
}
