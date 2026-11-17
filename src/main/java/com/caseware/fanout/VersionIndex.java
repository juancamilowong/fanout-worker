package com.caseware.fanout;

/** Where verified versions are written: the regional Engagement Version Index. */
public interface VersionIndex {

    /**
     * Records a verification. <b>Monotonic</b>: applied only if {@code revision} is newer than the stored one,
     * so a slow verification (read v3) can never overwrite a newer hook (user applied v5 meanwhile).
     * Idempotent: recording the same result twice is a no-op.
     */
    void recordVerified(String engagementId, int templateVersion, long revision);
}
