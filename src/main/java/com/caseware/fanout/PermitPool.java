package com.caseware.fanout;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * The downstream team's concurrency quota (e.g. 20 per region), shared by every worker instance
 * (a table of N slot rows in Aurora in production). A permit is held for the whole call.
 * Permits expire, so a crashed worker cannot leak a slot forever.
 */
public interface PermitPool {

    record Permit(int slot, UUID token) {}

    /** Empty when every slot is taken or the region is paused. */
    Optional<Permit> tryAcquire(Instant now);

    void release(Permit permit);

    /** Region-wide pause, honouring the downstream's Retry-After. */
    void pauseUntil(Instant until);
}
