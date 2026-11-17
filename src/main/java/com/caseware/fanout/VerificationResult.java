package com.caseware.fanout;

import java.time.Duration;

/** Every outcome of one downstream call. Sealed, so the worker must handle each case. */
public sealed interface VerificationResult {

    /** The engagement was loaded. {@code revision} orders this result against lifecycle hooks. */
    record Verified(int templateVersion, long revision) implements VerificationResult {}

    /** The engagement no longer exists. */
    record NotFound() implements VerificationResult {}

    /** A user has the engagement open. Their open fires a Loaded hook, which verifies it for free. */
    record Busy() implements VerificationResult {}

    /** The downstream is at capacity (429 / Retry-After). */
    record Overloaded(Duration retryAfter) implements VerificationResult {}

    /** Timeout, 5xx, connection error. */
    record Failure(String reason) implements VerificationResult {}
}
